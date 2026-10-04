"""Request security hooks: CSP nonce, CSRF, the paid-org billing gate, security headers.

Moved out of ``backend/main.py`` (monolith audit 2026-10-01, W1). The CSRF hook
matches bare endpoint names; a new state-changing route must be added to one of
the lists below (or match a prefix rule) to be protected.
"""

from __future__ import annotations

import inspect
import secrets

from flask import Flask, g, redirect, request, session, url_for

from backend.auth.session import (
    billing_enabled as _billing_enabled,
    require_paid_org_session as _require_paid_org_session,
    session_belongs_to_paid_org as _session_belongs_to_paid_org,
)
from backend.billing import access as paid_access
from backend.config import Config
from backend.utils.csrf import validate_csrf_form, validate_csrf_header
from backend.utils.runtime_env import is_production_env, session_cookie_secure_default

if not (validate_csrf_header.__code__.co_flags & inspect.CO_VARARGS):
    raise ImportError(
        "backend.utils.csrf.validate_csrf_header must be defined with *args (see repo csrf.py). "
        "Restart the server after git pull; check PYTHONPATH is not shadowing backend/utils/csrf.py."
    )


def _per_request_csp_nonce() -> None:
    g.csp_nonce = secrets.token_urlsafe(16)


# State-changing JSON routes served on DELETE. The browser clients already send
# X-CSRF-Token on these; SameSite=Lax and the absence of CORS kept them safe,
# this makes the token mandatory too (security review 2026-09-28).
_CSRF_HEADER_DELETE_ENDPOINTS = frozenset({
    "api_saved_searches_delete",
    "api_hidden_dealers_remove",
    "api_search_history_delete",
    "api_search_history_clear",
})

# POST endpoints whose HTML forms carry the csrf_token field.
_CSRF_FORM_POST_ENDPOINTS = (
    "login_page",
    "register_page",
    "logout_page",
    "verify_email_page",
    "resend_verification",
    "forgot_password_page",
    "reset_password_page",
    "account_password_page",
    "account_profile_page",
    "dev.admin_login",
    "dev.admin_register",
    "dev.admin_logout",
    "dealership_submit_review",
    "dealership_report_review",
)

# POST endpoints called from JS with the X-CSRF-Token header.
_CSRF_HEADER_POST_ENDPOINTS = (
    "api_search_smart",
    "api_car_chat",
    "api_compare_chat",
    "ai_chat_bp.api_ai_chat",
    "api_toggle_save",
    "api_saved_searches_create",
    "api_hidden_dealers_add",
    "api_session_listings_geo",
    "api_car_packages_ensure",
    "api_car_vehicle_history_intelligence",
    "api_auth_login",
    "api_auth_register",
    "api_auth_logout",
    "api_admin_dealer_onboard",
    "api_admin_dealer_job_retry",
    "api_admin_dealer_job_smart_retry",
    "api_admin_dealer_job_diagnose",
)


def _csrf_mutating_requests():
    if request.method == "DELETE":
        # Only a logged-in session can be forged cross-site; anonymous callers
        # get the view's own 401 rather than a 403 about a token they lack.
        if (request.endpoint or "") in _CSRF_HEADER_DELETE_ENDPOINTS and session.get("user_id"):
            validate_csrf_header()
        return
    if request.method != "POST":
        return
    ep = request.endpoint or ""
    if ep in _CSRF_FORM_POST_ENDPOINTS:
        csrf_resp = validate_csrf_form()
        if csrf_resp is not None:
            return csrf_resp
    elif ep and str(ep).startswith("dealer_portal."):
        csrf_resp = validate_csrf_form()
        if csrf_resp is not None:
            return csrf_resp
    elif ep and str(ep).startswith("store_admin."):
        csrf_resp = validate_csrf_form()
        if csrf_resp is not None:
            return csrf_resp
    elif ep in _CSRF_HEADER_POST_ENDPOINTS or (ep and str(ep).startswith("api_admin_operator_")):
        validate_csrf_header()
    return None


def _billing_gate_paid_routes():
    if not _billing_enabled():
        return None
    ep = request.endpoint or ""
    if not ep:
        return None
    if ep in ("login_page", "register_page", "logout_page", "favicon"):
        return None
    if str(ep).startswith("google_oauth."):
        return None
    if str(ep).startswith("apple_oauth."):
        return None
    if str(ep).startswith("dev.") or str(ep).startswith("billing."):
        return None
    if not session.get("user_id"):
        return None
    if paid_access.is_site_admin():
        return None
    if not _session_belongs_to_paid_org():
        return None
    if ep in ("app_home", "dashboard") or str(ep).startswith("dealer_portal.") or str(ep).startswith("store_admin."):
        if not _require_paid_org_session():
            return redirect(url_for("billing.billing_required"))
    return None


def _csp_enforce_wanted() -> bool:
    v = Config.csp_enforce_raw()
    if v in ("0", "false", "no", "off"):
        return False
    if v in ("1", "true", "yes", "on"):
        return True
    return is_production_env()


def _csp_header_value_enforced(nonce: str) -> str:
    # style-src: 'unsafe-inline' for existing inline style="" attributes; script nonces for all script elements.
    return (
        "default-src 'self'; "
        "base-uri 'self'; "
        "form-action 'self'; "
        "frame-ancestors 'none'; "
        "object-src 'none'; "
        "frame-src 'self'; "
        "img-src 'self' data: https: http: blob:; "
        "font-src 'self' data:; "
        "style-src 'self' 'unsafe-inline'; "
        "style-src-elem 'self'; "
        f"script-src 'self' 'nonce-{nonce}' https://esm.sh; "
        "connect-src 'self' https://esm.sh https://tile.openstreetmap.org; "
        "worker-src 'self'; "
    )


# Report-only CSP (SEC-032): opt-in via CSP_REPORT_ONLY=1; use for violation collection when not enforcing.
_CSP_REPORT_ONLY = (
    "default-src 'self'; "
    "base-uri 'self'; "
    "form-action 'self'; "
    "frame-ancestors 'none'; "
    "object-src 'none'; "
    "img-src 'self' data: https: http: blob:; "
    "font-src 'self' data:; "
    "style-src 'self' 'unsafe-inline'; "
    "style-src-elem 'self'; "
    "script-src 'self' https://esm.sh; "
    "connect-src 'self' https://esm.sh https://tile.openstreetmap.org; "
    "worker-src 'self'; "
)


def _csp_headers(response):
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
    if session_cookie_secure_default():
        response.headers.setdefault(
            "Strict-Transport-Security",
            "max-age=31536000; includeSubDomains",
        )
    if _csp_enforce_wanted():
        nonce = (getattr(g, "csp_nonce", None) or "") or ""
        if nonce and not response.headers.get("Content-Security-Policy"):
            response.headers["Content-Security-Policy"] = _csp_header_value_enforced(nonce)
        return response
    if Config.csp_report_only_enabled():
        if not response.headers.get("Content-Security-Policy-Report-Only"):
            response.headers["Content-Security-Policy-Report-Only"] = _CSP_REPORT_ONLY
    return response


def register_security(app: Flask) -> None:
    """Install, in this order: CSP nonce, CSRF, billing gate (before); CSP headers (after)."""
    app.before_request(_per_request_csp_nonce)
    app.before_request(_csrf_mutating_requests)
    app.before_request(_billing_gate_paid_routes)
    app.after_request(_csp_headers)
