"""Outbound HTTP(S) URL guards (SSRF mitigation for operator tooling)."""

from __future__ import annotations

import ipaddress
import socket
from urllib.parse import urlparse

_MAX_DEV_SCANNER_URL_LEN = 2048


def destination_host_blocked(hostname: str) -> bool:
    """Block localhost, obvious SSRF literals, and RFC-private / special-use IPs."""
    if not hostname:
        return True
    hl = hostname.strip().lower().rstrip(".")
    if hl == "localhost" or hl.endswith(".localhost"):
        return True
    if hl.endswith(".local"):
        return True
    try:
        ip = ipaddress.ip_address(hl)
        return bool(
            ip.is_private
            or ip.is_loopback
            or ip.is_link_local
            or ip.is_multicast
            or ip.is_reserved
            or ip.is_unspecified
        )
    except ValueError:
        return False


def destination_host_blocked_after_dns(hostname: str) -> bool:
    """
    True when the hostname is blocked literally or resolves to a non-public address.
    Unresolvable hostnames are allowed (scanner may fail later on the public internet).
    """
    if destination_host_blocked(hostname):
        return True
    h = hostname.strip().lower().rstrip(".")
    try:
        infos = socket.getaddrinfo(h, None, type=socket.SOCK_STREAM)
    except socket.gaierror:
        return False
    for fam, _, _, _, sockaddr in infos:
        if fam not in (socket.AF_INET, socket.AF_INET6):
            continue
        ip_str = sockaddr[0]
        try:
            ip = ipaddress.ip_address(ip_str)
        except ValueError:
            continue
        if (
            ip.is_loopback
            or ip.is_private
            or ip.is_link_local
            or ip.is_multicast
            or ip.is_reserved
            or ip.is_unspecified
        ):
            return True
    return False


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
