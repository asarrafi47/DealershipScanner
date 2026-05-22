"""Outbound HTTP(S) URL guards (SSRF mitigation for operator tooling)."""

from __future__ import annotations

import ipaddress
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
    if not host or destination_host_blocked(host):
        return "url_host_blocked"
    if parsed.username or parsed.password:
        return "url_credentials"
    return None
