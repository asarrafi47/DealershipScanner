"""Client IP for rate limiting and logging — never trust X-Forwarded-For without a trusted proxy."""

from __future__ import annotations

import ipaddress
import os

from flask import Request

_UNKNOWN = "unknown"


def trust_proxy_headers() -> bool:
    """When True, X-Forwarded-For is used (set only behind a proxy that appends to it)."""
    return (os.environ.get("TRUST_PROXY_HEADERS") or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def trusted_proxy_hops() -> int:
    """Number of trusted proxies between the client and this app. Railway's edge = 1."""
    raw = (os.environ.get("TRUSTED_PROXY_HOPS") or "").strip()
    if not raw:
        return 1
    try:
        return max(1, int(raw))
    except ValueError:
        return 1


def _normalize_ip(token: str) -> str | None:
    """Validate one X-Forwarded-For entry, stripping any port. None when it is not an IP."""
    value = token.strip()
    if not value:
        return None
    if value.startswith("["):
        # [2001:db8::1]:443 — bracketed IPv6, optionally with a port.
        closing = value.find("]")
        if closing == -1:
            return None
        value = value[1:closing]
    elif value.count(":") == 1:
        # 203.0.113.7:44321 — IPv4 with a port. A bare IPv6 always has 2+ colons.
        value = value.split(":", 1)[0]
    try:
        return str(ipaddress.ip_address(value))
    except ValueError:
        return None


def client_ip(request: Request) -> str:
    """The client's IP, counting back TRUSTED_PROXY_HOPS from the right of X-Forwarded-For.

    A proxy appends the address it saw to X-Forwarded-For, so with one trusted proxy the
    client is the *last* entry. Anything a client injects itself lands to the left of that
    and can never be selected. Reading the first entry instead would let any client forge
    its own rate-limit key by sending its own X-Forwarded-For.

    Falls back to remote_addr — the trusted socket peer, never spoofable — whenever the
    header is missing, malformed, or shorter than the configured hop count.
    """
    if trust_proxy_headers():
        forwarded = request.headers.get("X-Forwarded-For") or ""
        parts = [p for p in (piece.strip() for piece in forwarded.split(",")) if p]
        index = len(parts) - trusted_proxy_hops()
        if 0 <= index < len(parts):
            candidate = _normalize_ip(parts[index])
            if candidate:
                return candidate
    return request.remote_addr or _UNKNOWN
