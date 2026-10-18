"""Safe URL helpers for listing images and dealer links served to web + mobile clients.

Threat model. These URLs come from dealer feeds and are written into pages and
grid JSON; the visitor's browser fetches the images and follows the links. The
host check is client-side hardening, not SSRF protection: a listing must not make
a visitor's browser request their router, a tailnet (CGNAT 100.64.0.0/10) node or
their own machine. It applies the same non-global address rule as the server-side
guard (``outbound_url.destination_host_blocked``, P3.3), on the literal host only:
nothing here resolves DNS, so a public name that resolves to a private address
passes. A server-side fetch needs ``outbound_url.destination_host_blocked_after_dns``;
two server-side callers still rely on these helpers alone (the enrichment image
fetch, ``enrichment/service.py`` ``_fetch_image_b64_optimized``, and the admin
onboard URL in ``dealer/admin/onboard_api.py``).
"""

from __future__ import annotations

import re
from typing import Any
from urllib.parse import urlparse

from backend.utils.outbound_url import destination_host_blocked, normalize_host
from backend.utils.runtime_env import is_production_env

_BLOCKED_SCHEMES = re.compile(r"^(javascript|data|file|vbscript):", re.I)

# Local development serves images from the dev box; production refuses them.
_DEV_LOCALHOST_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})


def _host_is_blocked(hostname: str) -> bool:
    host = normalize_host(hostname)
    if not host:
        return True
    if host in _DEV_LOCALHOST_HOSTS:
        return is_production_env()
    return destination_host_blocked(host)


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
    try:
        parsed = urlparse(u)
        host = (parsed.hostname or "").strip()
    except ValueError:  # malformed bracketed host, e.g. "https://[::1/x.jpg"
        return None
    if parsed.username or parsed.password:
        return None
    if _host_is_blocked(host):
        return None
    return u


def normalize_listing_image_url(val: Any) -> str | None:
    """
    Inventory image URLs for grid JSON: HTTPS in production, allow ``/car-images/``,
    block script/data URLs and non-global hosts (``outbound_url.ip_is_non_global``).
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
    """External link URLs (dealer websites): http(s) only, no credentials or non-global hosts."""
    return _normalize_http_url(val, https_only_in_production=False)
