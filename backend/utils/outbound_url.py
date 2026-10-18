"""Outbound HTTP(S) URL guards (SSRF mitigation for operator tooling).

The address rule (P3.3): a destination is allowed only when it is globally
reachable. IPv4-mapped (``::ffff:a.b.c.d``) and 6to4 (``2002:aabb:ccdd::/48``)
IPv6 addresses are unwrapped first and judged by the IPv4 address they carry, so
``::ffff:100.64.0.1`` is refused exactly like ``100.64.0.1``. The test is
``not is_global`` rather than a list of ``is_private``-style flags because
``is_private`` is False for CGNAT (100.64.0.0/10), the range Tailscale gives
every tailnet node, which the old flag list let through.

``ip_is_non_global`` is the one place the rule lives; ``safe_listing_url`` and
any later resolver helper (P16.4) call it rather than restating it.
"""

from __future__ import annotations

import ipaddress
import re
import socket
from urllib.parse import urlparse

_MAX_DEV_SCANNER_URL_LEN = 2048

IPAddress = ipaddress.IPv4Address | ipaddress.IPv6Address

# Special-purpose blocks (IANA registries) that are not globally reachable, listed
# so the verdict does not depend on the interpreter's patch level: before 3.12.4
# (gh-113171) ``ipaddress`` reports 192.0.0.8 and 3fff::/20 as global. The whole
# 192.0.0.0/24 is refused, including the two anycast addresses (.9, .10) newer
# Pythons call global; no listing or dealer host lives there. CGNAT is listed
# even though every supported Python already handles it, because it is the range
# this rule exists for.
_NON_GLOBAL_NETWORKS: tuple[ipaddress.IPv4Network | ipaddress.IPv6Network, ...] = (
    ipaddress.ip_network("100.64.0.0/10"),  # shared address space (CGNAT, Tailscale)
    ipaddress.ip_network("192.0.0.0/24"),  # IETF protocol assignments
    ipaddress.ip_network("3fff::/20"),  # documentation (RFC 9637)
)

# A host whose last label is all digits, or 0x-hex, is an IPv4 address to a browser
# (WHATWG URL host parsing) and to ``inet_aton``: ``127.1``, ``2130706433``,
# ``0x7f.0.0.1`` and ``0177.0.0.1`` all mean 127.0.0.1. No DNS name ends that way.
_NUMERIC_LAST_LABEL = re.compile(r"^(?:0x[0-9a-f]*|[0-9]+)$")


def normalize_host(host: str) -> str:
    """Lowercase, trim, drop a trailing root dot and IPv6 brackets."""
    h = (host or "").strip().lower().rstrip(".")
    if h.startswith("[") and h.endswith("]"):
        h = h[1:-1]
    return h


def unwrap_embedded_ipv4(ip: IPAddress) -> IPAddress:
    """Return the IPv4 address an IPv4-mapped or 6to4 IPv6 address carries, else ``ip``."""
    if isinstance(ip, ipaddress.IPv6Address):
        inner = ip.ipv4_mapped or ip.sixtofour
        if inner is not None:
            return inner
    return ip


def ip_is_non_global(ip: IPAddress) -> bool:
    """
    True when ``ip`` must not be a destination: anything that is not globally
    reachable, after unwrapping IPv4-mapped and 6to4 addresses.

    Covers private, loopback, link-local, unspecified, CGNAT (100.64.0.0/10),
    benchmarking (198.18.0.0/15), IETF protocol assignments (192.0.0.0/24),
    documentation ranges, unique-local and Teredo. Multicast is added because
    ``is_global`` is True for it. ``is_reserved`` is kept from the earlier rule so
    the reserved IPv6 space stays refused (``::a.b.c.d`` IPv4-compatible and the
    NAT64 prefix 64:ff9b::/96 sit in ::/8, which ``is_global`` calls global).
    """
    ip = unwrap_embedded_ipv4(ip)
    return bool(not ip.is_global or ip.is_multicast or ip.is_reserved or any(ip in net for net in _NON_GLOBAL_NETWORKS))


def parse_ip_literal(host: str) -> IPAddress | None:
    """
    Parse ``host`` as an IP address literal; ``None`` when it is a DNS name.

    Accepts a bracketed IPv6 form, a ``%zone`` suffix, and the legacy IPv4
    spellings browsers still resolve (see ``_NUMERIC_LAST_LABEL``). Raises
    ``ValueError`` for a numeric-looking host that is no valid address
    (``1.2.3.4.5``); callers treat that as blocked.
    """
    h = normalize_host(host)
    try:
        return ipaddress.ip_address(h)
    except ValueError:
        pass
    if not _NUMERIC_LAST_LABEL.match(h.rsplit(".", 1)[-1]):
        return None
    try:
        return ipaddress.IPv4Address(socket.inet_aton(h))
    except OSError as exc:
        raise ValueError(f"numeric host is not an IPv4 address: {host!r}") from exc


def destination_host_blocked(hostname: str) -> bool:
    """
    Literal check, no DNS: block localhost names, ``.local`` names, numeric hosts
    that are no valid address, and every IP literal that ``ip_is_non_global``
    refuses. A DNS name passes; ``destination_host_blocked_after_dns`` resolves it.
    """
    hl = normalize_host(hostname)
    if not hl:
        return True
    if hl == "localhost" or hl.endswith(".localhost"):
        return True
    if hl.endswith(".local"):
        return True
    try:
        ip = parse_ip_literal(hl)
    except ValueError:
        return True
    if ip is None:
        return False
    return ip_is_non_global(ip)


def destination_host_blocked_after_dns(hostname: str) -> bool:
    """
    True when the hostname is blocked literally, does not resolve, or resolves to
    any non-global address (``ip_is_non_global``). Fails closed: an unresolvable
    name, an answer with no IPv4/IPv6 address, or an address that does not parse
    is blocked. A name that fails the check and resolves privately a moment later
    is the DNS-rebinding gap, so unresolvable is never treated as safe.
    """
    if destination_host_blocked(hostname):
        return True
    h = normalize_host(hostname)
    try:
        infos = socket.getaddrinfo(h, None, type=socket.SOCK_STREAM)
    except (socket.gaierror, UnicodeError):
        return True
    checked = False
    for fam, _, _, _, sockaddr in infos:
        if fam not in (socket.AF_INET, socket.AF_INET6):
            continue
        try:
            ip = ipaddress.ip_address(sockaddr[0])
        except ValueError:
            return True
        checked = True
        if ip_is_non_global(ip):
            return True
    return not checked


def validate_dev_scanner_url(url: str) -> str | None:
    """
    Validate a dealership URL before spawning the Node scanner subprocess.

    Returns an error code string, or ``None`` when the URL is acceptable.
    """
    raw = (url or "").strip()
    if not raw:
        return "url_required"
    if len(raw) > _MAX_DEV_SCANNER_URL_LEN:
        return "url_too_long"
    low = raw.lower()
    if not low.startswith(("http://", "https://")):
        return "url_scheme"
    try:
        parsed = urlparse(raw)
    except Exception:
        return "url_invalid"
    if parsed.scheme not in ("http", "https"):
        return "url_scheme"
    host = (parsed.hostname or "").strip()
    if not host or destination_host_blocked_after_dns(host):
        return "url_host_blocked"
    if parsed.username or parsed.password:
        return "url_credentials"
    return None
