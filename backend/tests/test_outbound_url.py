"""Outbound URL guards (dev scanner SSRF mitigation)."""

from __future__ import annotations

import ipaddress
import re
import sys
import unicodedata
from urllib.parse import urlparse

import pytest

from backend.utils.outbound_url import (
    _IDNA_IGNORED_RANGES,
    destination_host_blocked,
    ip_is_non_global,
    normalize_host,
    parse_ip_literal,
    unwrap_embedded_ipv4,
    validate_dev_scanner_url,
)

# Public names resolve from the conftest fake_dns table, never the network.
pytestmark = pytest.mark.usefixtures("fake_dns")

# P3.3: every non-global address is refused. The first six are the plan's cases:
# CGNAT (Tailscale nodes and the 100.100.100.200 metadata-style address), CGNAT
# wrapped in an IPv4-mapped IPv6 address, benchmarking, IETF protocol assignments.
NON_GLOBAL_LITERALS = [
    "100.64.0.1",
    "100.127.255.254",
    "100.100.100.200",
    "::ffff:100.64.0.1",
    "198.18.0.1",
    "192.0.0.8",
    # 6to4 addresses carrying a non-global IPv4 (2002:WWXX:YYZZ::/48)
    "2002:6440:1::1",  # 100.64.0.1
    "2002:a00:1::1",  # 10.0.0.1
    "2002:7f00:1::",  # 127.0.0.1
    # IPv4-mapped forms of the classic ranges
    "::ffff:127.0.0.1",
    "::ffff:10.1.2.3",
    "::ffff:169.254.169.254",
    "::ffff:198.18.0.1",
    # the ranges the earlier flag list already caught
    "10.0.0.1",
    "172.16.0.1",
    "192.168.1.89",
    "127.0.0.1",
    "169.254.169.254",
    "0.0.0.0",
    "224.0.0.1",
    "240.0.0.1",
    "255.255.255.255",
    "192.0.2.1",
    "198.51.100.7",
    "203.0.113.9",
    "::1",
    "::",
    "fe80::1",
    "fc00::1",
    "fd7a:115c:a1e0::1",  # Tailscale's IPv6 ULA range
    "ff02::1",
    "2001:db8::1",
    "3fff::1",
    "2001::1",  # Teredo
    "fec0::1",  # deprecated site-local (RFC 3879); is_global is True for it
    "feff::1",  # top of fec0::/10
    "::6440:1",  # IPv4-compatible (reserved ::/8)
    "64:ff9b::a00:1",  # NAT64 well-known prefix (reserved ::/8)
]

GLOBAL_LITERALS = [
    "8.8.8.8",
    "1.1.1.1",
    "93.184.215.14",
    "100.63.255.255",  # just below CGNAT
    "100.128.0.0",  # just above CGNAT
    "198.20.0.1",  # just above benchmarking (198.18.0.0/15)
    "192.0.1.1",  # just above 192.0.0.0/24
    "2606:4700:4700::1111",
    "2001:4860:4860::8888",
    "::ffff:8.8.8.8",
    "2002:808:808::1",  # 6to4 carrying 8.8.8.8
]

# Legacy IPv4 spellings browsers and inet_aton still resolve to an address.
LEGACY_NON_GLOBAL_HOSTS = [
    "127.1",
    "2130706433",  # 127.0.0.1
    "0x7f000001",
    "0x7f.0.0.1",
    "0177.0.0.1",  # octal 127.0.0.1
    "0x64.0x40.0.1",  # 100.64.0.1
    "1681915905",  # 100.64.0.1
    "10.1",
]


def test_dev_scanner_url_accepts_public_https() -> None:
    assert validate_dev_scanner_url("https://www.example-dealer.com/inventory") is None


def test_dev_scanner_url_blocks_names_resolving_to_private(fake_dns) -> None:
    fake_dns.table["intranet.example-dealer.com"] = "10.1.2.3"
    fake_dns.table["gone.example-dealer.com"] = None
    assert validate_dev_scanner_url("https://intranet.example-dealer.com/") == "url_host_blocked"
    # Unresolvable names are blocked: fail closed against DNS rebinding (owner decision 2026-10-05).
    assert validate_dev_scanner_url("https://gone.example-dealer.com/") == "url_host_blocked"
    assert "intranet.example-dealer.com" in fake_dns.lookups


def test_dev_scanner_url_blocks_private_hosts() -> None:
    assert validate_dev_scanner_url("http://127.0.0.1/inventory") == "url_host_blocked"
    assert validate_dev_scanner_url("http://192.168.0.5/") == "url_host_blocked"
    assert validate_dev_scanner_url("http://localhost/") == "url_host_blocked"


def test_dev_scanner_url_blocks_non_http_scheme() -> None:
    assert validate_dev_scanner_url("file:///etc/passwd") == "url_scheme"
    assert validate_dev_scanner_url("javascript:alert(1)") == "url_scheme"


def test_dev_scanner_url_blocks_embedded_credentials() -> None:
    assert validate_dev_scanner_url("https://user:pass@example.com/") == "url_credentials"


@pytest.mark.parametrize("literal", NON_GLOBAL_LITERALS)
def test_non_global_literal_is_blocked(literal: str) -> None:
    assert ip_is_non_global(ipaddress.ip_address(literal)) is True
    assert destination_host_blocked(literal) is True
    assert destination_host_blocked(f"[{literal}]") is True


@pytest.mark.parametrize("literal", GLOBAL_LITERALS)
def test_global_literal_is_allowed(literal: str) -> None:
    assert ip_is_non_global(ipaddress.ip_address(literal)) is False
    assert destination_host_blocked(literal) is False


@pytest.mark.parametrize("literal", ["100.64.0.1", "100.100.100.200", "198.18.0.1", "192.0.0.8"])
def test_dev_scanner_url_blocks_non_global_ipv4_literal(literal: str) -> None:
    assert validate_dev_scanner_url(f"https://{literal}/inventory") == "url_host_blocked"


@pytest.mark.parametrize("literal", ["::ffff:100.64.0.1", "2002:6440:1::1", "fd7a:115c:a1e0::1"])
def test_dev_scanner_url_blocks_non_global_ipv6_literal(literal: str) -> None:
    assert validate_dev_scanner_url(f"https://[{literal}]/inventory") == "url_host_blocked"


@pytest.mark.parametrize("literal", ["8.8.8.8", "[2606:4700:4700::1111]"])
def test_dev_scanner_url_allows_public_ip_literal(literal: str) -> None:
    assert validate_dev_scanner_url(f"https://{literal}/inventory") is None


@pytest.mark.parametrize("host", LEGACY_NON_GLOBAL_HOSTS)
def test_legacy_ipv4_spelling_is_blocked(host: str) -> None:
    assert parse_ip_literal(host) is not None
    assert destination_host_blocked(host) is True


def test_legacy_ipv4_spelling_of_public_address_is_allowed() -> None:
    assert parse_ip_literal("134744072") == ipaddress.IPv4Address("8.8.8.8")
    assert destination_host_blocked("134744072") is False


@pytest.mark.parametrize("host", ["1.2.3.4.5", "999.1.1.1", "example.123", "0xzz.1"])
def test_numeric_host_that_is_no_address_is_blocked(host: str) -> None:
    assert destination_host_blocked(host) is True


@pytest.mark.parametrize("host", ["www.example-dealer.com", "cdn.dealer.com.", "1e100.net", "img-100-64-0-1.example"])
def test_dns_names_pass_the_literal_check(host: str) -> None:
    assert parse_ip_literal(host) is None
    assert destination_host_blocked(host) is False


@pytest.mark.parametrize("host", ["", " ", "[]", "localhost", "LOCALHOST.", "api.localhost", "printer.local"])
def test_empty_and_local_names_are_blocked(host: str) -> None:
    assert destination_host_blocked(host) is True


def test_unwrap_embedded_ipv4() -> None:
    v4 = ipaddress.IPv4Address("100.64.0.1")
    assert unwrap_embedded_ipv4(ipaddress.ip_address("::ffff:100.64.0.1")) == v4
    assert unwrap_embedded_ipv4(ipaddress.ip_address("2002:6440:1::1")) == v4
    assert unwrap_embedded_ipv4(v4) == v4
    plain = ipaddress.ip_address("2606:4700:4700::1111")
    assert unwrap_embedded_ipv4(plain) == plain


def test_zone_suffixed_link_local_is_blocked() -> None:
    assert destination_host_blocked("fe80::1%en0") is True


# A client connects to the host after percent-decoding it (browsers and requests)
# and, in a browser, after UTS #46 folding, so these spellings are the addresses
# and names they turn into (reviewer repro: each passed the literal check).
ENCODED_NON_GLOBAL_HOSTS = [
    "127.0.0.%31",
    "100.64.0.%31",
    "%31%30%30.64.0.1",
    "\uff11\uff10\uff10.\uff16\uff14.\uff10.\uff11",  # fullwidth digits: 100.64.0.1
    "100\u300264\u30020\u30021",  # ideographic full stop
    "100\uff0e64\uff0e0\uff0e1",  # fullwidth full stop
    "100\uff6164\uff610\uff611",  # halfwidth ideographic full stop
    "\uff4c\uff4f\uff43\uff41\uff4c\uff48\uff4f\uff53\uff54",  # fullwidth "localhost"
    "localhos%74",
    "LOCALHOS%54",
    "local\u00adhost",  # soft hyphen: UTS #46 drops it
    "127.0.0.1\u200b",  # zero width space: UTS #46 drops it
    "%3a%3a1",  # ::1
    "[%3A%3Affff%3A100.64.0.1]",
    "fe80::1%25en0",  # URL-encoded zone on a link-local address
    "printer.loca%6c",
    "%E2%91%A0%E2%91%A1%E2%91%A6.0.0.1",  # percent-encoded circled digits fold to 127
]

# Hosts a browser refuses and requests cannot resolve as written: refused outright,
# never passed on as a DNS name. One decoding pass only, so %2531 stays %31.
FORBIDDEN_NAME_HOSTS = [
    "100.64.0.%2531",
    "127.0.0.1%20",
    "127.0.0.1%20x",  # inet_aton reads "127.0.0.1 x" as 127.0.0.1
    "localhost%00",
    "dealer%2fexample.com",
    "dealer.example%3a8080",
    "100.64.0.\uff05\uff13\uff11",  # fullwidth percent sign folds to "%31"
    "dealer\u0378.example",  # U+0378 is unassigned
]


@pytest.mark.parametrize("host", ENCODED_NON_GLOBAL_HOSTS)
def test_encoded_spelling_of_non_global_host_is_blocked(host: str) -> None:
    assert destination_host_blocked(host) is True


@pytest.mark.parametrize("host", FORBIDDEN_NAME_HOSTS)
def test_host_a_browser_would_not_fetch_is_blocked(host: str) -> None:
    assert destination_host_blocked(host) is True


@pytest.mark.parametrize(
    "host",
    ["images.dealer.com", "b\u00fccher.example", "xn--bcher-kva.example", "img%2Ddealer.example", "dealer_cdn.example"],
)
def test_encoded_or_unicode_dns_name_is_allowed(host: str) -> None:
    assert destination_host_blocked(host) is False


def test_normalize_host_decodes_once_and_folds_unicode() -> None:
    assert normalize_host("100.64.0.%31") == "100.64.0.1"
    assert normalize_host("[%3A%3A1]") == "::1"
    assert normalize_host("\uff11\uff10\uff10\uff0e\uff16\uff14\u30020\uff611.") == "100.64.0.1"
    assert normalize_host("LOCAL\u00adHOST.") == "localhost"
    assert normalize_host("100.64.0.%2531") == "100.64.0.%31"
    assert parse_ip_literal("100.64.0.%2531") is None
    assert parse_ip_literal("100.64.0.%31") == ipaddress.IPv4Address("100.64.0.1")


def test_outlined_digits_are_blocked_on_every_unicode_version() -> None:
    # U+1CCF0..U+1CCF9 (Unicode 16) fold to 0-9 in a current browser. Python 3.14
    # folds them too; Python 3.12 (Unicode 15.0) does not know them and refuses.
    host = "\U0001ccf1\U0001ccf0\U0001ccf0.64.0.1"
    assert destination_host_blocked(host) is True


def test_requests_decodes_percent_escapes_in_the_host() -> None:
    # Why the literal check decodes: requests connects to the decoded host.
    requests = pytest.importorskip("requests")
    url = "https://100.64.0.%31/x.jpg"
    prepared = requests.Request("GET", url).prepare().url
    assert prepared == "https://100.64.0.1/x.jpg"
    assert normalize_host(urlparse(url).hostname) == urlparse(prepared).hostname


@pytest.mark.parametrize(
    "host",
    ["postgres.railway.internal", "internal", "INTERNAL.", "nas.home.arpa", "home.arpa", "local", "x.localhost"],
)
def test_private_use_names_are_blocked(host: str) -> None:
    assert destination_host_blocked(host) is True


@pytest.mark.parametrize("host", ["internal.example.com", "local.example.com", "home.arpa.example.com", "myhome.arpa"])
def test_names_merely_containing_private_use_labels_pass(host: str) -> None:
    assert destination_host_blocked(host) is False


def test_idna_folding_matches_uts46() -> None:
    """
    Drift alarm against the UTS #46 table the idna package ships: every code point
    the table drops is in ``_IDNA_IGNORED_RANGES``, and every code point it maps to
    letters, digits or dots folds the same way here. A code point this
    interpreter's Unicode database does not know is skipped: the literal check
    refuses it (``test_host_a_browser_would_not_fetch_is_blocked``).
    """
    idna = pytest.importorskip("idna")
    ignored = {cp for lo, hi in _IDNA_IGNORED_RANGES for cp in range(lo, hi + 1)}
    relevant = re.compile(r"[0-9a-z.]*")
    mismatches = []
    for cp in range(0x80, sys.maxunicode + 1):
        if 0xD800 <= cp <= 0xDFFF:
            continue
        c = chr(cp)
        try:
            mapped = idna.uts46_remap(c, std3_rules=False)
        except ValueError:  # disallowed: a browser refuses the host
            if cp in ignored:
                mismatches.append((hex(cp), "in table but disallowed"))
            continue
        if (mapped == "") != (cp in ignored) and unicodedata.category(c) != "Cn":
            mismatches.append((hex(cp), "ignored-table drift"))
            continue
        if unicodedata.category(c) == "Cn" or not relevant.fullmatch(mapped):
            continue
        folded = normalize_host(f"a{c}b")
        if folded != f"a{mapped}b":
            mismatches.append((hex(cp), mapped, folded))
    assert mismatches == []
