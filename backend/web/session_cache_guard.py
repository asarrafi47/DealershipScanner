"""Session interface that sets private cache headers on responses that set cookies.

``save_session`` runs after every ``after_request`` hook, so it sees every
``Set-Cookie`` the session writes (with ``SESSION_REFRESH_EACH_REQUEST`` a signed-in
browser gets a fresh session cookie on most responses). When a response that sets a
cookie is marked ``public`` or carries ``s-maxage`` / ``proxy-revalidate``, those
directives are removed and ``private`` is added. The other directives the response
already had (``max-age``, ``immutable``, ``stale-while-revalidate``, ...) are kept, so
static assets stay cached in the browser. With no ``max-age`` left, the response
becomes ``private, no-cache``. The ETag is kept and ``Cookie`` is added to ``Vary``.

Responses that set no cookie (anonymous visitors) keep their own Cache-Control.
"""

from __future__ import annotations

import logging

from flask import request
from flask.sessions import SecureCookieSessionInterface

_log = logging.getLogger(__name__)

PRIVATE_CACHE_CONTROL = "private, no-cache"

# Directives that only address shared caches (or opt a response into them).
_SHARED_ONLY = ("public", "s-maxage", "proxy-revalidate")


def _directive_name(part: str) -> str:
    return part.split("=", 1)[0].strip().lower()


def _is_shared_cacheable(cache_control: str) -> bool:
    return any(_directive_name(p) in _SHARED_ONLY for p in (cache_control or "").split(","))


def private_cache_control(cache_control: str) -> str:
    """``cache_control`` without shared-cache directives, marked ``private``."""
    kept = [
        p.strip()
        for p in (cache_control or "").split(",")
        if p.strip() and _directive_name(p) not in _SHARED_ONLY + ("private",)
    ]
    if not any(_directive_name(p) == "max-age" for p in kept):
        return PRIVATE_CACHE_CONTROL
    return ", ".join(["private", *kept])


class CookieSafeSessionInterface(SecureCookieSessionInterface):
    """The default signed-cookie session, plus a cache guard after it is saved."""

    def save_session(self, app, session, response) -> None:  # type: ignore[override]
        super().save_session(app, session, response)
        cc = response.headers.get("Cache-Control", "")
        if "Set-Cookie" in response.headers and _is_shared_cacheable(cc):
            _log.debug(
                "session cache guard: %s sets a cookie; Cache-Control set to private",
                request.path,
            )
            response.headers["Cache-Control"] = private_cache_control(cc)
            response.vary.add("Cookie")
