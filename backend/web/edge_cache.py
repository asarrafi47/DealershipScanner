"""Edge (Cloudflare) caching for anonymous marketing pages (perf 2026-11-20).

A page opts in from its view (today: ``/`` and ``/find-dealers``). The rule is simple and
does NOT rely on ``Vary: Cookie`` (Cloudflare ignores it when caching):

* A request that carries no cookie this app sets (``session``, ``sc_dev``) and no
  ``Authorization`` header is anonymous. Its response is rendered with no CSRF token, no
  CSP nonce and no per-user value, and is sent with :data:`PUBLIC_CACHE_CONTROL`, a weak
  ETag and 304 handling.
* Any other request to the same page gets :data:`PRIVATE_CACHE_CONTROL`.
* :class:`EdgeSafeSessionInterface` is the last line: the session is saved after every
  ``after_request`` hook, so if anything still wrote the session (or set any cookie) on a
  response marked public, the response is downgraded to private before it leaves.

The Cloudflare Cache Rule that makes the edge store these pages (HTML is never cached by
default) is in ``docs/web_speed_search_2026_11/EDGE_CACHE.md``.
"""

from __future__ import annotations

import hashlib
import logging

from flask import Response, current_app, g, request
from flask.sessions import SecureCookieSessionInterface

_log = logging.getLogger(__name__)

# s-maxage 300: the landing's only live values are the rounded inventory count and four
# featured cars, both cached in process for 300 s already, so a 5-minute edge copy shows
# nothing staler than the origin itself would. stale-while-revalidate 600: after that the
# edge answers at once and refetches in the background. stale-if-error 86400: a deploy or
# a crashed worker still gets the last good landing instead of a 5xx. max-age=0: browsers
# always revalidate (cheap, 304 from the edge), so a deploy is visible on the next view.
EDGE_S_MAXAGE_S = 300
EDGE_STALE_WHILE_REVALIDATE_S = 600
EDGE_STALE_IF_ERROR_S = 86400
PUBLIC_CACHE_CONTROL = (
    f"public, max-age=0, s-maxage={EDGE_S_MAXAGE_S}, "
    f"stale-while-revalidate={EDGE_STALE_WHILE_REVALIDATE_S}, "
    f"stale-if-error={EDGE_STALE_IF_ERROR_S}"
)
# ``no-cache``, not ``no-store``: shared caches may not keep a ``private`` response either way,
# and ``no-store`` would take the page out of the browser's back/forward cache (review
# 2026-11-20). The browser still revalidates every time.
PRIVATE_CACHE_CONTROL = "private, no-cache"


def app_cookie_names() -> tuple[str, ...]:
    """Every cookie this app sets: the Flask session and the device cookie."""
    from backend.utils.device_id import COOKIE_NAME as DEVICE_COOKIE

    return (current_app.config.get("SESSION_COOKIE_NAME") or "session", DEVICE_COOKIE)


def anonymous_cacheable_request() -> bool:
    """True when the response to this request may be shared with every visitor."""
    if request.method not in ("GET", "HEAD"):
        return False
    if request.headers.get("Authorization"):
        return False
    return not any(name in request.cookies for name in app_cookie_names())


def begin_edge_page() -> bool:
    """Call at the top of an opted-in view, before rendering. Returns cacheability.

    The page is rendered with the nonce-free CSP whether or not it is cacheable, so the
    anonymous body is the same for every anonymous visitor (the page has no inline script).
    """
    from backend.web.security import use_nonce_free_csp

    use_nonce_free_csp()
    cacheable = anonymous_cacheable_request()
    g.edge_cacheable = cacheable
    return cacheable


def _weak_etag(body: bytes) -> str:
    return hashlib.blake2b(body, digest_size=16).hexdigest()


def finish_edge_page(resp: Response) -> Response:
    """Apply the cache policy chosen by :func:`begin_edge_page` to the view's response."""
    resp.vary.add("Accept-Encoding")
    resp.vary.add("Cookie")
    if getattr(g, "edge_cacheable", False) and resp.status_code == 200 and not resp.direct_passthrough:
        resp.headers["Cache-Control"] = PUBLIC_CACHE_CONTROL
        # Weak: the gzip hook may compress the body after this; both encodings are the
        # same page, which is exactly what a weak validator promises.
        resp.set_etag(_weak_etag(resp.get_data()), weak=True)
        resp.make_conditional(request)
        return resp
    resp.headers["Cache-Control"] = PRIVATE_CACHE_CONTROL
    return resp


def private_response(resp: Response) -> Response:
    """Mark a response (e.g. the signed-in redirect off ``/``) as never shared."""
    resp.headers["Cache-Control"] = PRIVATE_CACHE_CONTROL
    resp.vary.add("Cookie")
    return resp


def _is_shared_cacheable(cache_control: str) -> bool:
    cc = (cache_control or "").lower()
    return "public" in cc or "s-maxage" in cc


class EdgeSafeSessionInterface(SecureCookieSessionInterface):
    """The default signed-cookie session, plus one guard after it is saved.

    ``save_session`` runs after every ``after_request`` hook, so this is the one place that
    sees every Set-Cookie. A response that sets a cookie is never shareable: if it was
    marked public, it is downgraded to ``private, no-cache``. Its ETag is kept: the browser
    that received the cookie may still revalidate its own copy (the listings APIs depend on
    that for signed-in shoppers), and no shared cache may store a private response.
    """

    def save_session(self, app, session, response) -> None:  # type: ignore[override]
        super().save_session(app, session, response)
        if "Set-Cookie" in response.headers and _is_shared_cacheable(
            response.headers.get("Cache-Control", "")
        ):
            _log.debug(
                "edge cache: %s set a cookie on a public response; sent private instead",
                request.path,
            )
            response.headers["Cache-Control"] = PRIVATE_CACHE_CONTROL
