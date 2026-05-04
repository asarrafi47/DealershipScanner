"""Client IP for rate limiting and logging — never trust X-Forwarded-For without a trusted proxy."""

from __future__ import annotations

import os

from flask import Request


def trust_proxy_headers() -> bool:
    """When True, first hop in X-Forwarded-For is used (set only behind a proxy that sets/overwrites it)."""
    return (os.environ.get("TRUST_PROXY_HEADERS") or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def client_ip(request: Request) -> str:
    if trust_proxy_headers():
        forwarded = (request.headers.get("X-Forwarded-For") or "").split(",")[0].strip()
        if forwarded:
            return forwarded
    return request.remote_addr or "unknown"
