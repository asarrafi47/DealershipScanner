"""Response gzip, the global template context, jinja filters and the 404 page.

Moved out of ``backend/main.py`` (monolith audit 2026-10-01, W1).
"""

from __future__ import annotations

import gzip
import unicodedata
from urllib.parse import unquote

from flask import Flask, g, jsonify, render_template, request, session, url_for

from backend.auth.apple_oauth import apple_oauth_configured, apple_signin_visible
from backend.auth.google_oauth import google_oauth_configured, google_signin_visible
from backend.auth.session import billing_enabled as _billing_enabled
from backend.billing import access as paid_access
from backend.utils.car_serialize import format_display_value
from backend.utils.csrf import ensure_csrf_token
from backend.utils.runtime_env import is_production_env
from backend.web.static import static_cache_ver


def _gzip_large_json(resp):
    """Shrink large listings payloads over the wire (browser must send Accept-Encoding: gzip).

    text/html is included because the server-rendered /listings document is the single
    biggest response the site sends -- it carries the facet checkboxes and the packed
    cascade table inline, and at ~823 KB uncompressed it was the critical-path bottleneck
    for time-to-first-cards. It is highly repetitive markup, so gzip takes roughly an
    order of magnitude off it.
    """
    if resp.status_code != 200 or resp.direct_passthrough:
        return resp
    ct = (resp.content_type or "").split(";")[0].strip().lower()
    if ct not in ("application/json", "text/html"):
        return resp
    if resp.headers.get("Content-Encoding"):
        return resp
    if "gzip" not in (request.headers.get("Accept-Encoding") or "").lower():
        return resp
    raw = resp.get_data()
    if len(raw) < 2048:
        return resp
    compressed = gzip.compress(raw, compresslevel=5)
    resp.set_data(compressed)
    resp.headers["Content-Encoding"] = "gzip"
    resp.headers["Content-Length"] = str(len(compressed))
    resp.headers["Vary"] = "Accept-Encoding"
    return resp


def inject_csrf_and_flags():
    from backend.routes.site_misc import _app_version
    from backend.utils.roles import is_dealer_portal_role

    static_ver = static_cache_ver()
    # One DB read per request (cached on g) serves the admin flag, the store-ops
    # nav and every paid-access flag; the cookie's role is never trusted here.
    access_ctx = paid_access.current_access()
    _is_admin = access_ctx.is_admin
    store_ops_nav = False
    prof = access_ctx.profile
    if prof and not _is_admin:
        store_ops_nav = bool(
            (prof.get("dealer_id") or "").strip()
            or prof.get("dealership_registry_id")
        )
    return {
        "csrf_token": ensure_csrf_token(),
        "csp_nonce": getattr(g, "csp_nonce", "") or "",
        "is_production": is_production_env(),
        "logged_in_user": session.get("username") or session.get("admin_username"),
        "is_admin": _is_admin,
        "is_store_admin": _is_admin,
        **paid_access.template_context(),
        "billing_stripe_enabled": _billing_enabled(),
        "show_dealer_inventory_nav": bool(session.get("user_id"))
        and is_dealer_portal_role(session.get("user_role")),
        "show_store_ops_nav": store_ops_nav,
        "static_cache_ver": static_ver,
        "personal_home_url": (
            url_for("app_home") if session.get("user_id") else url_for("home")
        ),
        "google_signin_enabled": google_oauth_configured(),
        "google_signin_visible": google_signin_visible(),
        "apple_signin_enabled": apple_oauth_configured(),
        "apple_signin_visible": apple_signin_visible(),
        "password_reset_enabled": _password_reset_enabled(),
        "app_version": _app_version(),
    }


def _password_reset_enabled() -> bool:
    from backend.auth.password_reset import password_reset_enabled

    return password_reset_enabled()


def _jinja_fmt_spec(value):
    return format_display_value(value)


def _jinja_http_url(value):
    """Scheme-validate a stored URL before it lands in an href.

    Dealer/listing URLs come from scraped feeds; a poisoned row could carry a
    javascript:/data: scheme. Autoescape stops attribute breakout but not a
    hostile scheme, so hrefs must go through this (returns "" for anything
    that is not http(s))."""
    from backend.utils.field_clean import normalize_optional_url

    u = normalize_optional_url(value)
    return u if u and u.lower().startswith(("http://", "https://")) else ""


# The css_url filter: image URLs inside style="background-image:url('...')".
CSS_URL_PLACEHOLDER = "/static/placeholder.svg"
# Same-origin image paths this app serves: static assets, images the downloader
# saved (routes/site_misc.py /car-images/) and dealer-portal uploads
# (dealer/routes.py /dealer-uploads/, the photos on inventory/{dashboard,listings}).
_CSS_URL_LOCAL_PREFIXES = ("/static/", "/car-images/", "/dealer-uploads/")
# Characters that can end a CSS string or url() token, or an HTML attribute.
_CSS_URL_ENCODE_ASCII = frozenset("'\"()\\<>")


def _css_url_local_path_ok(path: str) -> bool:
    """A same-origin image path with no dot segments (``..`` or ``%2e%2e``).

    Browsers resolve dot segments before the request, so ``/car-images/../x``
    would fetch ``/x`` with the visitor's cookies. Real image paths never
    carry one."""
    if not path.startswith(_CSS_URL_LOCAL_PREFIXES):
        return False
    bare = path.split("#", 1)[0].split("?", 1)[0]
    return all(unquote(seg) not in (".", "..") for seg in bare.split("/"))


def _css_url_percent_encode(url: str) -> str | None:
    """Percent-encode every character that could break out of ``url('...')``.

    Encodes ``' " ( ) \\ < >``, whitespace (any Unicode space) and control
    characters (C0, DEL, C1). Everything else, ``%`` included, is left alone,
    so an ordinary CDN URL comes back byte-identical. Returns None for a lone
    surrogate, which cannot be encoded."""
    out = []
    for ch in url:
        cat = unicodedata.category(ch)
        if cat == "Cs":
            return None
        if ch in _CSS_URL_ENCODE_ASCII or ch.isspace() or cat == "Cc":
            out.append("".join(f"%{b:02X}" for b in ch.encode("utf-8")))
        else:
            out.append(ch)
    return "".join(out)


def _jinja_css_url(value) -> str:
    """Make a stored image URL safe inside ``style="background-image:url('...')"``.

    Autoescape turns ``'`` into ``&#39;``, but the HTML parser decodes that back
    to a quote before the CSS parser sees the attribute, so a dealer image URL
    containing ``');`` used to inject CSS. This filter accepts only:

    * http(s) URLs that ``normalize_listing_image_url`` passes (no script/data
      schemes, credentials or private hosts; https only in production);
    * same-origin ``/static/``, ``/car-images/`` and ``/dealer-uploads/`` paths
      without dot segments;

    percent-encodes the characters that can end the CSS string, and returns the
    placeholder for anything else. The result is a plain ``str``: autoescape
    still escapes ``&`` to ``&amp;``, so a literal ``&#39;`` in a URL reaches CSS
    as text, never as a quote. Keep this filter in autoescaped templates."""
    from backend.utils.safe_listing_url import normalize_listing_image_url

    raw = value.strip() if isinstance(value, str) else ""
    if raw.startswith("/"):
        safe = raw if _css_url_local_path_ok(raw) else None
    else:
        safe = normalize_listing_image_url(raw) if raw else None
    if not safe:
        return CSS_URL_PLACEHOLDER
    return _css_url_percent_encode(safe) or CSS_URL_PLACEHOLDER


def page_not_found(_exc):
    if request.path.startswith("/api/") or (
        request.accept_mimetypes.best_match(["application/json", "text/html"]) == "application/json"
        and request.accept_mimetypes["application/json"] > request.accept_mimetypes["text/html"]
    ):
        return jsonify({"error": "not_found"}), 404
    return render_template("not_found.html"), 404


def register_templating(app: Flask) -> None:
    """gzip after_request, the global context processor, filters, the 404 handler."""
    app.after_request(_gzip_large_json)
    app.context_processor(inject_csrf_and_flags)
    app.add_template_filter(_jinja_fmt_spec, "fmt_spec")
    app.add_template_filter(_jinja_http_url, "http_url")
    app.add_template_filter(_jinja_css_url, "css_url")
    app.register_error_handler(404, page_not_found)
