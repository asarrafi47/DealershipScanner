"""Safe URL helpers for listing images served to web + mobile clients."""

from __future__ import annotations

import ipaddress
import re
from typing import Any
from urllib.parse import urlparse

from backend.utils.runtime_env import is_production_env

_BLOCKED_SCHEMES = re.compile(r"^(javascript|data|file|vbscript):", re.I)


def _host_is_blocked(hostname: str) -> bool:
    host = (hostname or "").strip().lower()
    if not host:
        return True
    if host in ("localhost", "127.0.0.1", "::1"):
        return is_production_env()
    try:
        ip = ipaddress.ip_address(host.strip("[]"))
        return bool(
            ip.is_private
            or ip.is_loopback
            or ip.is_link_local
            or ip.is_reserved
            or ip.is_multicast
        )
    except ValueError:
        pass
    return False


def _normalize_http_url(val: Any, *, https_only_in_production: bool) -> str | None:
    if val is None:
        return None
    u = str(val).strip()
    if not u or u == "—":
        return None
    if _BLOCKED_SCHEMES.match(u):
        return None
    low = u.lower()
    if not (low.startswith("https://") or low.startswith("http://")):
        return None
    if https_only_in_production and is_production_env() and low.startswith("http://"):
        return None
    parsed = urlparse(u)
    if parsed.username or parsed.password:
        return None
    host = (parsed.hostname or "").strip()
    if _host_is_blocked(host):
        return None
    return u


def normalize_listing_image_url(val: Any) -> str | None:
    """
    Inventory image URLs for grid JSON: HTTPS in production, allow ``/car-images/``,
    block script/data URLs and private-network hosts.
    """
    if val is None:
        return None
    u = str(val).strip()
    if not u or u == "—":
        return None
    if _BLOCKED_SCHEMES.match(u):
        return None
    if u.startswith("/car-images/"):
        return u
    return _normalize_http_url(u, https_only_in_production=True)


def normalize_safe_http_url(val: Any) -> str | None:
    """External link URLs (dealer websites): http(s) only, no credentials or private hosts."""
    return _normalize_http_url(val, https_only_in_production=False)
