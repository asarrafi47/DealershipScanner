"""Outbound URL guards (dev scanner SSRF mitigation)."""

from __future__ import annotations

import ipaddress

import pytest

from backend.utils.outbound_url import (
    destination_host_blocked,
    ip_is_non_global,
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
