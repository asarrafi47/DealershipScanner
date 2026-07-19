"""Sarrafi Collection — Flask web application."""

from backend.utils.project_env import load_project_dotenv

load_project_dotenv()

from backend.utils.kmac_vault import load_kmac_vault_secrets

load_kmac_vault_secrets()

# Central env access (single read point; see backend/config.py).
from backend.config import Config

import gzip
import inspect
import json
import logging
import os
import re
import secrets
import sqlite3
import time
from datetime import timedelta
from pathlib import Path
from typing import Any

from flask import Flask, abort, g, jsonify, make_response, redirect, render_template, request, send_from_directory, session, url_for
from werkzeug.exceptions import HTTPException

from backend.intelligence.ai.agent import run_car_page_chat, run_compare_chat
from backend.auth.apple_oauth import apple_oauth_configured, apple_signin_visible
from backend.auth.apple_oauth import bp as apple_oauth_bp
from backend.auth.google_oauth import bp as google_oauth_bp
from backend.auth.google_oauth import google_oauth_configured, google_signin_visible
from backend.billing.routes import bp as billing_bp
from backend.billing.catalog import (
    FEATURE_AI_CAR_CHAT,
    FEATURE_AI_COMPARE_CHAT,
    FEATURE_MARKET_INTEL,
    FEATURE_NEARBY_DEALERS,
    FEATURE_PACKAGES_ENSURE,
    FEATURE_VEHICLE_HISTORY,
    FEATURE_WINDOW_STICKER,
    get_plan,
    minimum_plan_for_feature,
)
from backend.billing.entitlements import entitlements_from_session, require_feature as billing_require_feature
from backend.dealer.admin import store_admin_bp

# Site-admin hub pages (must load before first url_for in templates).
import backend.dealer.admin.data_quality_hub  # noqa: F401
import backend.dealer.admin.scanner_ops_hub  # noqa: F401

from backend.dealer.routes import bp as dealer_portal_bp
from backend.dev.console import register_dev_console
from backend.routes.ai_narrate_bp import ai_narrate_bp
from backend.routes.ai_chat_bp import ai_chat_bp
from backend.routes.health import bp as health_api_bp
from backend.dev.routes import dev_bp
from backend.db.admin_users_db import init_admin_db
from backend.db.dealer_portal_db import init_dealer_portal_db
from backend.db.inventory_db import (
    get_car_by_id,
    get_cars_by_ids,
    get_filter_options,
    get_saved_car_ids,
    init_inventory_db,
    is_car_saved,
    listings_geo_coords_maps,
    listings_grid_cache_etag,
    listings_grid_serialized_cars,
    save_car,
    search_cars,
    search_cars_by_make_model_pairs,
    serialize_car_for_listings_grid,
    unsave_car,
)
from backend.db.user_history_db import (
    count_viewed_cars,
    get_recent_compared_car_ids,
    get_recent_viewed_car_ids,
    record_car_view,
    record_compare_session,
)
from backend.db.users_db import (
    authenticate_app_user,
    change_user_password,
    update_user_profile,
    check_user,
    get_user_by_login,
    get_user_profile,
    init_users_db,
    sync_env_admin_user_row,
)
from backend.enrichment.knowledge_engine import prepare_car_detail_context
from backend.listings.geo_session import (
    apply_listings_geo_to_session,
    listings_geo_kwargs_from_session,
    persist_listings_geo_from_request,
)
from backend.listings.routes import listings_page
from backend.utils.car_serialize import format_display_value, serialize_car_for_api
from backend.utils.listing_completeness import INCOMPLETE_FIELD_LABELS
from backend.utils.car_chat_policy import car_chat_rate_limits, car_chat_user_daily_limit, web_research_playwright_allowed
from backend.utils.client_ip import client_ip as _client_ip_from_request
from backend.utils.csrf import ensure_csrf_token, validate_csrf_form, validate_csrf_header

if not (validate_csrf_header.__code__.co_flags & inspect.CO_VARARGS):
    raise ImportError(
        "backend.utils.csrf.validate_csrf_header must be defined with *args (see repo csrf.py). "
        "Restart the server after git pull; check PYTHONPATH is not shadowing backend/utils/csrf.py."
    )
from backend.utils.ip_rate_limit import allow_request
from backend.utils.query_parser import parse_natural_query
from backend.utils.runtime_env import is_production_env, session_cookie_secure_default
from backend.utils.roles import (
    is_admin_role,
    normalize_role,
)

_MIN_PASSWORD_LEN = Config.MIN_PASSWORD_LENGTH
_logger = logging.getLogger(__name__)


def _socketio_cors_allowed_origins() -> str | list[str]:
    """Socket.IO browser origins. Production defaults avoid wildcard CORS (SEC-063)."""
    raw = Config.socketio_cors_origins_raw()
    if raw == "*":
        if is_production_env():
            _logger.warning(
                "SOCKETIO_CORS_ORIGINS=* in production allows any browser origin for Socket.IO; "
                "prefer a comma-separated allowlist."
            )
        return "*"
    if raw:
        return [x.strip() for x in raw.split(",") if x.strip()]
    if not is_production_env():
        return "*"
    origins: list[str] = []
    for raw_base in (Config.public_base_url(), Config.mfa_qr_base_url()):
        base = raw_base.rstrip("/")
        if base and base not in origins:
            origins.append(base)
    if origins:
        return origins
    _logger.warning(
        "Production Socket.IO: SOCKETIO_CORS_ORIGINS unset and no PUBLIC_BASE_URL/MFA_QR_BASE_URL; "
        "using an empty CORS allowlist (tightest same-site behavior). If phone QR or Socket.IO fail, "
        "set SOCKETIO_CORS_ORIGINS to your public app origin(s), comma-separated."
    )
    return []


_CHAT_MAX_MESSAGE = Config.CHAT_MAX_MESSAGE_CHARS
_CHAT_MAX_BODY = Config.CHAT_MAX_BODY_BYTES
# Cap JSON POST bodies (smart search, chat) and allow dealer multipart uploads (8 MiB+).
_MAX_REQUEST_BODY = Config.MAX_REQUEST_BODY_BYTES
_SMART_SEARCH_RPM = Config.RATE_LIMIT_SMART_SEARCH_PER_MIN
_LOGIN_RPM = Config.RATE_LIMIT_LOGIN_PER_MIN
_REGISTER_RPM = Config.RATE_LIMIT_REGISTER_PER_MIN
_DEALER_LOCATOR_RPM = Config.RATE_LIMIT_DEALER_LOCATOR_PER_MIN
_NHTSA_RECALLS_RPM = Config.RATE_LIMIT_NHTSA_RECALLS_PER_MIN


# Backward-compatible re-import: shared with the extracted route modules.
from backend.routes._shared import _client_ip  # noqa: E402


def _session_belongs_to_paid_org() -> bool:
    """Stripe subscription (when enabled) applies only to users tied to a dealership org."""
    if not session.get("user_id"):
        return False
    oid = session.get("org_id")
    if oid is None:
        return False
    try:
        return int(oid) > 0
    except (TypeError, ValueError):
        return False


def _post_login_redirect():
    post_intent = session.pop("post_auth_intent", None)
    if (
        _billing_enabled()
        and (not is_admin_role(session.get("user_role")))
        and _session_belongs_to_paid_org()
        and (not _require_paid_org_session())
    ):
        return redirect(url_for("billing.billing_required"))
    if post_intent == "premium":
        return redirect(url_for("premium_page"))
    return redirect(url_for("app_home"))


app = Flask(
    __name__,
    template_folder="../frontend/templates",
    static_folder="../frontend/static",
)

_raw_secret = Config.secret_key_raw()
if is_production_env():
    if not _raw_secret:
        raise RuntimeError(
            "SECRET_KEY or FLASK_SECRET_KEY must be set when FLASK_ENV=production (SEC-001)."
        )
    app.secret_key = _raw_secret
else:
    import secrets as _secrets_mod
    app.secret_key = _raw_secret or _secrets_mod.token_hex(32)

app.config["MAX_CONTENT_LENGTH"] = _MAX_REQUEST_BODY

app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.config["SESSION_COOKIE_SECURE"] = session_cookie_secure_default()
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(days=14)
app.config["SESSION_REFRESH_EACH_REQUEST"] = True

from backend.utils.production_security import assert_production_security_config

assert_production_security_config()

from backend.db.inventory_pg import assert_inventory_backend_configured

assert_inventory_backend_configured()

init_users_db()
init_admin_db()
init_inventory_db()
init_dealer_portal_db()

from backend.scanner.job_queue import init_job_queue_schema
init_job_queue_schema()


def _prewarm_listings_inventory_cache() -> None:
    """Background-build listings JSON cache so first /listings visit is not cold."""
    import threading

    def _run() -> None:
        try:
            from backend.db.inventory_db import (
                _incomplete_car_ids_for_listings,
                listings_grid_serialized_cars,
            )

            t0 = time.perf_counter()
            _incomplete_car_ids_for_listings()
            n = len(listings_grid_serialized_cars())
            _logger.info(
                "Listings grid cache prewarmed (%d cars, %.1fs)",
                n,
                time.perf_counter() - t0,
            )
            from backend.enrichment.dictionary_catalog import _epa_paths_by_make_norm

            t1 = time.perf_counter()
            makes = len(_epa_paths_by_make_norm())
            _logger.info(
                "EPA dictionary index prewarmed (%d makes, %.1fs)",
                makes,
                time.perf_counter() - t1,
            )
        except Exception:
            _logger.exception("Listings grid prewarm failed")

    threading.Thread(target=_run, name="listings-prewarm", daemon=True).start()


_prewarm_listings_inventory_cache()
app.register_blueprint(dev_bp, url_prefix="/dev")
app.register_blueprint(store_admin_bp)

from backend.dealer.admin.operator_api import register_admin_operator_api

register_admin_operator_api(app)
app.register_blueprint(dealer_portal_bp)
app.register_blueprint(billing_bp)
app.register_blueprint(google_oauth_bp)
app.register_blueprint(apple_oauth_bp)
app.register_blueprint(ai_narrate_bp)
app.register_blueprint(ai_chat_bp)
app.register_blueprint(health_api_bp)
register_dev_console(app)

# Route modules extracted from this file. Each exposes ``register(app)`` and
# keeps the original bare endpoint names (templates, the CSRF hook, and the
# billing gate all match endpoints by bare name). The modules resolve
# main-owned helpers through this module at request time so existing
# ``backend.main`` monkeypatch targets and ``importlib.reload(backend.main)``
# keep working — see backend/routes/_shared.py.
from backend.routes import admin_dealer_api as _admin_dealer_api_routes  # noqa: E402
from backend.routes import cars_pages as _cars_pages_routes  # noqa: E402
from backend.routes import dealers_recalls as _dealers_recalls_routes  # noqa: E402
from backend.routes import dealer_reviews as _dealer_reviews_routes  # noqa: E402
from backend.routes import dealership_page as _dealership_page_routes  # noqa: E402
from backend.routes import fuel_api as _fuel_api_routes  # noqa: E402
from backend.routes import home_dashboard as _home_dashboard_routes  # noqa: E402
from backend.routes import listings_api as _listings_api_routes  # noqa: E402
from backend.routes import site_misc as _site_misc_routes  # noqa: E402

_site_misc_routes.register(app)
_home_dashboard_routes.register(app)
_listings_api_routes.register(app)
_cars_pages_routes.register(app)
_dealers_recalls_routes.register(app)
_dealership_page_routes.register(app)
_dealer_reviews_routes.register(app)
_fuel_api_routes.register(app)
_admin_dealer_api_routes.register(app)

# Backward-compatible re-exports: these names historically lived here and are
# still imported from backend.main (e.g. backend/dev/scan_lab_routes.py) or
# resolved through this module by the extracted route modules.
from backend.routes.cars_pages import (  # noqa: E402
    _build_car_detail_view_context,
    _car_window_sticker_preview_url,
    _serve_car_window_sticker_preview,
)
from backend.routes.fuel_api import (  # noqa: E402
    _fuel_market_payload,
    _load_live_gas_prices_payload,
    _normalize_fuel_tier_param,
    _resolve_fuel_lookup_state,
    _resolve_live_electricity_lookup,
    _resolve_live_gas_lookup,
)
from backend.routes.home_dashboard import (  # noqa: E402
    _inventory_count_display,
    _invalidate_reco_cache,
    _recommendations_for_user,
    _render_personal_home,
)
from backend.routes.listings_api import (  # noqa: E402
    _highlight_params_from_filters,
    _listings_client_poll_ms,
)


@app.after_request
def _gzip_large_json(resp):
    """Shrink large listings JSON over the wire (browser must send Accept-Encoding: gzip)."""
    if resp.status_code != 200 or resp.direct_passthrough:
        return resp
    ct = (resp.content_type or "").split(";")[0].strip().lower()
    if ct != "application/json":
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


@app.context_processor
def inject_csrf_and_flags():
    role = (session.get("user_role") or "").strip().lower()
    from backend.routes.site_misc import _app_version
    from backend.utils.roles import is_dealer_portal_role

    static_ver = "1"
    try:
        static_ver = str(int(Path(app.static_folder).resolve().joinpath("style.css").stat().st_mtime))
    except OSError:
        pass
    _is_admin = is_admin_role(role)
    store_ops_nav = False
    uid = session.get("user_id")
    if uid and not _is_admin:
        try:
            from backend.db.users_db import get_user_profile

            prof = get_user_profile(int(uid))
            if prof:
                store_ops_nav = bool(
                    (prof.get("dealer_id") or "").strip()
                    or prof.get("dealership_registry_id")
                )
        except (TypeError, ValueError):
            store_ops_nav = False
    return {
        "csrf_token": ensure_csrf_token(),
        "csp_nonce": getattr(g, "csp_nonce", "") or "",
        "is_production": is_production_env(),
        "logged_in_user": session.get("username") or session.get("admin_username"),
        "is_admin": _is_admin,
        "is_store_admin": _is_admin,
        "has_paid_access": _session_has_paid_access(),
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


def _billing_enabled() -> bool:
    return Config.billing_stripe_enabled()


def _org_subscription_active(status: str | None) -> bool:
    s = (status or "").strip().lower()
    return s in ("active", "trialing")


def _require_paid_org_session() -> bool:
    if not _billing_enabled():
        return True
    if not session.get("user_id"):
        return True
    if is_admin_role(session.get("user_role")):
        return True
    st = session.get("org_subscription_status")
    return bool(_org_subscription_active(st))


@app.before_request
def _per_request_csp_nonce() -> None:
    g.csp_nonce = secrets.token_urlsafe(16)


@app.before_request
def _csrf_mutating_requests():
    if request.method != "POST":
        return
    ep = request.endpoint or ""
    if ep in (
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
    ):
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
    elif ep in (
        "api_search_smart",
        "api_car_chat",
        "api_compare_chat",
        "api_toggle_save",
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
    ) or (ep and str(ep).startswith("api_admin_operator_")):
        validate_csrf_header()
    return None


def _dev_operator_grants_premium() -> bool:
    """Authenticated ``/dev`` operator (scan lab, dashboard) — local tooling, not public users."""
    try:
        from backend.dev.routes import _admin_session_ok

        return _admin_session_ok()
    except Exception:
        return False


def _session_has_paid_access() -> bool:
    """Premium, active org subscription, or app admin (matches context_processor ``has_paid_access``)."""
    if is_admin_role(session.get("user_role")):
        return True
    if _dev_operator_grants_premium():
        return True
    if bool(session.get("user_is_premium")):
        return True
    return bool(_org_subscription_active(session.get("org_subscription_status")))


def _viewer_sees_premium_features() -> bool:
    """Premium UI (trim ladder, window sticker, packages) for paid users or logged-in when billing is off."""
    if _session_has_paid_access():
        return True
    if session.get("user_id") and not _billing_enabled():
        return True
    return False


def _require_feature(feature_id: str) -> tuple[bool, str]:
    """Per-plan feature gate (C1). Returns (ok, error_code)."""
    if _dev_operator_grants_premium():
        return True, ""
    uid = session.get("user_id")
    if not uid:
        if _billing_enabled() or is_production_env():
            return False, "login_required"
    if not _billing_enabled():
        return True, ""
    ok, err = billing_require_feature(session, feature_id)
    if not ok and err == "feature_required":
        return False, "premium_required"
    return ok, err


def _feature_denied_json(feature_id: str, err: str, **extra: Any) -> dict[str, Any]:
    """403 JSON with upgrade hint when billing blocks a feature (C4)."""
    body: dict[str, Any] = {"ok": False, "error": err, **extra}
    if err == "premium_required":
        plan_id = minimum_plan_for_feature(feature_id)
        if plan_id:
            plan = get_plan(plan_id)
            body["required_feature"] = feature_id
            body["upgrade_plan_id"] = plan_id
            body["upgrade_plan_name"] = plan.name if plan else plan_id
            body["upgrade_url"] = url_for("premium_page") + f"?plan={plan_id}"
    return body


def _require_premium_feature() -> tuple[bool, str]:
    """
    Paid surfaces require login in production; when Stripe billing is enabled, also require
    premium/subscription/admin. Returns (ok, error_code).
    """
    if _dev_operator_grants_premium():
        return True, ""
    uid = session.get("user_id")
    if not uid:
        if _billing_enabled() or is_production_env():
            return False, "login_required"
    if not _billing_enabled():
        return True, ""
    if _session_has_paid_access():
        return True, ""
    return False, "premium_required"


@app.before_request
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
    if is_admin_role(session.get("user_role")):
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
        "font-src 'self' https://fonts.gstatic.com data:; "
        "style-src 'self' https://fonts.googleapis.com 'unsafe-inline'; "
        "style-src-elem 'self' https://fonts.googleapis.com; "
        f"script-src 'self' 'nonce-{nonce}' https://esm.sh; "
        "connect-src 'self' https://esm.sh https://fonts.googleapis.com https://tile.openstreetmap.org; "
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
    "font-src 'self' https://fonts.gstatic.com data:; "
    "style-src 'self' https://fonts.googleapis.com 'unsafe-inline'; "
    "style-src-elem 'self' https://fonts.googleapis.com; "
    "script-src 'self' https://esm.sh; "
    "connect-src 'self' https://esm.sh https://fonts.googleapis.com https://tile.openstreetmap.org; "
    "worker-src 'self'; "
)


@app.after_request
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


@app.template_filter("fmt_spec")
def _jinja_fmt_spec(value):
    return format_display_value(value)


@app.errorhandler(404)
def page_not_found(_exc):
    if request.path.startswith("/api/") or (
        request.accept_mimetypes.best_match(["application/json", "text/html"]) == "application/json"
        and request.accept_mimetypes["application/json"] > request.accept_mimetypes["text/html"]
    ):
        return jsonify({"error": "not_found"}), 404
    return render_template("not_found.html"), 404


@app.route("/login", methods=["GET", "POST"])
def login_page():
    if request.method == "GET":
        err = request.args.get("_error", "")
        if err == "session_expired":
            return render_template("login.html", error="Your session expired — please log in again.")
        oauth_err = session.pop("oauth_error", None)
        if oauth_err:
            return render_template("login.html", error=oauth_err)
    if request.method == "POST":
        ip = _client_ip()
        if not allow_request(f"login:{ip}", max_events=_LOGIN_RPM, window_seconds=60.0):
            return render_template("login.html", error="Too many login attempts. Try again in a minute."), 429
        login_input = (request.form.get("login") or "").strip()
        password = (request.form.get("password") or "").strip()
        if not login_input or not password:
            return render_template("login.html", error="Enter username/email and password.")
        u = authenticate_app_user(login_input, password)
        if u:
            sync_env_admin_user_row(int(u["id"]))
            session.clear()
            if not _finalize_app_session(int(u["id"])):
                return render_template("login.html", error="Login failed. Try again.")
            return _post_login_redirect()
        return render_template("login.html", error="Invalid username/email or password.")
    return render_template("login.html")


@app.route("/register", methods=["GET", "POST"])
def register_page():
    if request.method == "POST":
        ip = _client_ip()
        if not allow_request(f"register:{ip}", max_events=_REGISTER_RPM, window_seconds=60.0):
            return render_template("register.html", error="Too many registration attempts. Try again later."), 429
        from backend.auth.app_registration import register_general_app_user

        plan = request.form.get("plan", "free").strip().lower()
        uid, err_code, err_msg, wants_premium = register_general_app_user(
            request.form.get("username", ""),
            request.form.get("email", ""),
            request.form.get("password", ""),
            plan=plan,
            min_password_len=_MIN_PASSWORD_LEN,
        )
        if err_code:
            return render_template("register.html", error=err_msg or "Registration failed.")
        session.clear()
        if wants_premium:
            session["post_auth_intent"] = "premium"
        if not _finalize_app_session(int(uid)):
            return render_template("register.html", error="Registration failed. Try again.")
        from backend.auth.email_verification import user_needs_email_verification

        if user_needs_email_verification(int(uid)):
            session["email_verify_notice"] = True
        if wants_premium:
            return _post_login_redirect()
        return redirect(url_for("listings"))
    return render_template("register.html")


@app.route("/verify-email")
def verify_email_page():
    from backend.auth.email_verification import verify_email_token

    token = (request.args.get("token") or "").strip()
    ok, message = verify_email_token(token)
    return render_template("verify_email.html", ok=ok, message=message)


@app.route("/resend-verification", methods=["POST"])
def resend_verification():
    uid = session.get("user_id")
    if not uid:
        return redirect(url_for("login_page"))
    ip = _client_ip()
    if not allow_request(f"resend_verify:{ip}", max_events=5, window_seconds=3600.0):
        return render_template("verify_email.html", ok=False, message="Too many requests. Try again later."), 429
    from backend.auth.email_verification import (
        issue_and_send_verification_email,
        user_needs_email_verification,
    )
    from backend.db.users_db import get_user_profile

    if not user_needs_email_verification(int(uid)):
        return render_template(
            "verify_email.html",
            ok=True,
            message="Your email is already verified.",
        )
    profile = get_user_profile(int(uid)) or {}
    email = (profile.get("email") or "").strip()
    if not issue_and_send_verification_email(user_id=int(uid), to_email=email):
        return render_template(
            "verify_email.html",
            ok=False,
            message="Could not send verification email. Try again later.",
        ), 503
    return render_template(
        "verify_email.html",
        ok=True,
        message="Verification email sent. Check your inbox.",
    )


@app.route("/forgot-password", methods=["GET", "POST"])
def forgot_password_page():
    from backend.auth.password_reset import password_reset_enabled, request_password_reset

    if not password_reset_enabled():
        return render_template(
            "forgot_password.html",
            error="Password reset is not enabled on this site.",
        ), 503
    if request.method == "POST":
        ip = _client_ip()
        if not allow_request(f"forgot_pw:{ip}", max_events=10, window_seconds=3600.0):
            return render_template(
                "forgot_password.html",
                error="Too many requests. Try again later.",
            ), 429
        request_password_reset(login_input=request.form.get("login", ""))
        return render_template(
            "forgot_password.html",
            success=True,
            message="If an account exists for that username or email, we sent reset instructions.",
        )
    return render_template("forgot_password.html")


@app.route("/reset-password", methods=["GET", "POST"])
def reset_password_page():
    from backend.auth.password_reset import complete_password_reset, password_reset_enabled

    if not password_reset_enabled():
        return render_template(
            "reset_password.html",
            error="Password reset is not enabled on this site.",
        ), 503
    token = (request.args.get("token") or request.form.get("token") or "").strip()
    if request.method == "POST":
        ok, message = complete_password_reset(
            token=token,
            new_password=(request.form.get("new_password") or "").strip(),
        )
        if ok:
            return render_template("reset_password.html", ok=True, message=message)
        return render_template("reset_password.html", error=message, token=token)
    if not token:
        return render_template("reset_password.html", error="Missing reset token.")
    return render_template("reset_password.html", token=token)


@app.route("/logout", methods=["POST"])
def logout_page():
    session.clear()
    return redirect(url_for("login_page"))


def _account_profile_context(uid: int, **extra):
    u = get_user_profile(int(uid)) or {}
    role = normalize_role(u.get("role"))
    role_labels = {
        "admin": "Site administrator",
        "general_user": "Member",
        "dealership_owner": "Dealership owner",
        "dealership_admin": "Dealership admin",
        "dealership_member": "Dealership member",
    }
    ctx = {
        "username": (u.get("username") or "").strip(),
        "email": (u.get("email") or "").strip(),
        "role": role,
        "role_label": role_labels.get(role, role.replace("_", " ").title()),
        "is_premium": bool(u.get("is_premium")),
        "min_password_len": _MIN_PASSWORD_LEN,
    }
    ctx.update(extra)
    return ctx


@app.route("/account/password", methods=["GET", "POST"])
def account_password_page():
    """Backward-compatible alias for the password section on the profile page."""
    if request.method == "POST":
        return account_profile_page()
    return redirect(url_for("account_profile_page", _anchor="password"))


@app.route("/account/profile", methods=["GET", "POST"])
def account_profile_page():
    """Signed-in users can update profile info and change password."""
    uid = session.get("user_id")
    if not uid:
        return redirect(url_for("login_page"))
    uid = int(uid)

    if request.method == "POST":
        action = (request.form.get("form_action") or "profile").strip().lower()
        if action == "password":
            current_pw = (request.form.get("current_password") or "").strip()
            new_pw = (request.form.get("new_password") or "").strip()
            confirm_pw = (request.form.get("confirm_password") or "").strip()
            if new_pw != confirm_pw:
                return render_template(
                    "account_profile.html",
                    **_account_profile_context(uid, password_error="New passwords do not match."),
                )
            from backend.utils.registration_validation import registration_form_error

            u = get_user_profile(uid) or {}
            fmt_err = registration_form_error(
                u.get("username") or "user",
                u.get("email") or "user@local",
                new_pw,
                min_password_len=_MIN_PASSWORD_LEN,
            )
            if fmt_err:
                return render_template(
                    "account_profile.html",
                    **_account_profile_context(uid, password_error=fmt_err),
                )
            err = change_user_password(uid, current_pw, new_pw)
            if err:
                return render_template(
                    "account_profile.html",
                    **_account_profile_context(uid, password_error=err),
                )
            return render_template(
                "account_profile.html",
                **_account_profile_context(uid, password_success=True),
            )

        username_in = (request.form.get("username") or "").strip()
        email_in = (request.form.get("email") or "").strip()
        err = update_user_profile(uid, username_in, email_in)
        if err:
            return render_template(
                "account_profile.html",
                **_account_profile_context(
                    uid,
                    profile_error=err,
                    username=username_in,
                    email=email_in,
                ),
            )
        sync_env_admin_user_row(uid)
        u = get_user_profile(uid) or {}
        session["username"] = u.get("username")
        session["user_email"] = (u.get("email") or "").strip()
        session["user_role"] = normalize_role(u.get("role"))
        return render_template(
            "account_profile.html",
            **_account_profile_context(uid, profile_success=True),
        )

    return render_template("account_profile.html", **_account_profile_context(uid))


@app.route("/account/billing")
def account_billing_page():
    """Plan summary and Stripe Customer Portal entry (C3 scaffold)."""
    uid = session.get("user_id")
    if not uid:
        return redirect(url_for("login_page", next="/account/billing"))
    uid = int(uid)
    from backend.billing.catalog import get_plan
    from backend.billing.entitlements import FEATURE_LABELS, entitlements_from_session
    from backend.billing.stripe_billing import billing_enabled
    from backend.db.users_db import get_user_billing_snapshot

    billing = get_user_billing_snapshot(uid) or {}
    plan_id = (billing.get("subscription_plan_id") or "").strip().lower()
    if not plan_id:
        plan_id = "complete" if billing.get("is_premium") else "free"
    plan = get_plan(plan_id)
    feats = sorted(entitlements_from_session(session))
    feat_labels = [FEATURE_LABELS.get(f, f.replace("_", " ").title()) for f in feats]
    has_portal = bool(
        billing_enabled()
        and (billing.get("premium_stripe_customer_id") or "").strip()
    )
    return render_template(
        "account_billing.html",
        billing_enabled=billing_enabled(),
        plan=plan,
        plan_id=plan_id,
        is_premium=bool(billing.get("is_premium")),
        feature_labels=feat_labels,
        has_portal=has_portal,
        stripe_configured=billing_enabled(),
    )


@app.route("/account/billing/portal")
def account_billing_portal():
    uid = session.get("user_id")
    if not uid:
        return redirect(url_for("login_page", next="/account/billing"))
    from backend.billing.stripe_billing import billing_enabled, create_customer_portal_session
    from backend.db.users_db import get_user_billing_snapshot

    if not billing_enabled():
        return redirect(url_for("account_billing_page"))
    billing = get_user_billing_snapshot(int(uid)) or {}
    customer_id = (billing.get("premium_stripe_customer_id") or "").strip()
    if not customer_id:
        return redirect(url_for("account_billing_page"))
    try:
        portal = create_customer_portal_session(request=request, customer_id=customer_id)
        url = (portal.get("url") or "").strip()
        if url:
            return redirect(url)
    except Exception:
        _logger.exception("customer portal session failed user_id=%s", uid)
    return redirect(url_for("account_billing_page"))


def _auth_user_payload(u: dict) -> dict:
    return {
        "id": int(u["id"]),
        "username": u.get("username"),
        "email": u.get("email"),
        "role": normalize_role(u.get("role")),
        "is_premium": bool(u.get("is_premium")),
        "has_paid_access": _session_has_paid_access(),
    }


@app.route("/api/auth/csrf", methods=["GET"])
def api_auth_csrf():
    return jsonify({"ok": True, "csrf_token": ensure_csrf_token()})


@app.route("/api/auth/me", methods=["GET"])
def api_auth_me():
    uid = session.get("user_id")
    if not uid:
        return jsonify({"ok": False, "error": "not_logged_in"}), 401
    u = get_user_profile(int(uid))
    if not u:
        return jsonify({"ok": False, "error": "not_logged_in"}), 401
    return jsonify({"ok": True, "user": _auth_user_payload(u)})


@app.route("/api/auth/login", methods=["POST"])
def api_auth_login():
    ip = _client_ip()
    if not allow_request(f"login:{ip}", max_events=_LOGIN_RPM, window_seconds=60.0):
        return jsonify({"ok": False, "error": "rate_limited"}), 429
    data = request.get_json(silent=True) or {}
    login_input = (data.get("login") or "").strip()
    password = (data.get("password") or "").strip()
    if not login_input or not password:
        return jsonify({"ok": False, "error": "missing_credentials"}), 400
    u = authenticate_app_user(login_input, password)
    if not u:
        return jsonify({"ok": False, "error": "invalid_credentials"}), 401
    sync_env_admin_user_row(int(u["id"]))
    session.clear()
    if not _finalize_app_session(int(u["id"])):
        return jsonify({"ok": False, "error": "login_failed"}), 500
    return jsonify({"ok": True, "user": _auth_user_payload(u)})


@app.route("/api/auth/register", methods=["POST"])
def api_auth_register():
    """Create app user in ``users.db`` (same as HTML ``/register``) for native clients."""
    ip = _client_ip()
    if not allow_request(f"register:{ip}", max_events=_REGISTER_RPM, window_seconds=60.0):
        return jsonify({"ok": False, "error": "rate_limited"}), 429

    data = request.get_json(silent=True) or {}
    if not isinstance(data, dict):
        return jsonify({"ok": False, "error": "json_object"}), 400

    from backend.auth.app_registration import register_general_app_user

    plan = (data.get("plan") or "free").strip().lower()
    uid, err_code, err_msg, wants_premium = register_general_app_user(
        (data.get("username") or "").strip(),
        (data.get("email") or "").strip(),
        (data.get("password") or "").strip(),
        plan=plan,
        min_password_len=_MIN_PASSWORD_LEN,
    )
    if err_code:
        status = 400
        if err_code == "duplicate_user":
            status = 409
        elif err_code == "registration_blocked":
            status = 403
        return (
            jsonify(
                {
                    "ok": False,
                    "error": err_code,
                    "message": err_msg,
                }
            ),
            status,
        )

    session.clear()
    if wants_premium:
        session["post_auth_intent"] = "premium"
    if not _finalize_app_session(int(uid)):
        return jsonify({"ok": False, "error": "registration_failed"}), 500

    u = get_user_profile(int(uid))
    if not u:
        return jsonify({"ok": False, "error": "registration_failed"}), 500
    return jsonify({"ok": True, "user": _auth_user_payload(u)})


@app.route("/api/auth/logout", methods=["POST"])
def api_auth_logout():
    session.clear()
    return jsonify({"ok": True})




def _app_mfa_gone():
    """2FA removed: old bookmarks and session redirects land here."""
    if session.get("user_id"):
        return redirect(url_for("app_home"))
    return redirect(url_for("login_page"))


for _mfa_legacy_path in (
    "/mfa/choose",
    "/mfa/setup",
    "/mfa/verify",
    "/mfa/qr",
    "/mfa/qr-wait",
    "/mfa/qr-approve-png",
):
    app.add_url_rule(
        _mfa_legacy_path,
        endpoint=f"mfa_legacy_{_mfa_legacy_path.strip('/').replace('/', '_')}",
        view_func=_app_mfa_gone,
        methods=["GET", "POST"],
    )


@app.route("/mfa/qr/complete", methods=["POST"])
def mfa_qr_complete_gone():
    return _app_mfa_gone()


@app.route("/mfa/qr-confirm/<path:token>", methods=["GET", "POST"])
def mfa_qr_confirm_gone(token):
    return _app_mfa_gone()


def _finalize_app_session(user_id: int) -> bool:
    from backend.db.users_db import get_org

    try:
        uid = int(user_id)
    except (TypeError, ValueError):
        return False
    u = get_user_profile(uid)
    if not u:
        return False
    from backend.db.users_db import _user_row_is_active

    if not _user_row_is_active(u):
        return False
    session["user_id"] = int(u["id"])
    session["username"] = u["username"]
    session["user_email"] = (u.get("email") or "").strip()
    session["user_role"] = normalize_role(u.get("role"))
    session["user_dealer_id"] = (u.get("dealer_id") or "").strip()
    rid = u.get("dealership_registry_id")
    session["user_dealership_registry_id"] = str(int(rid)) if rid is not None else ""
    session["org_id"] = int(u.get("org_id") or 0) if u.get("org_id") else 0
    session["user_is_premium"] = bool(u.get("is_premium"))
    session["subscription_plan_id"] = (u.get("subscription_plan_id") or "").strip() or None
    session["entitlements"] = sorted(entitlements_from_session(session))
    for _stale in (
        "mfa_pending_user_id",
        "mfa_pending_login",
        "mfa_pending_method",
        "mfa_qr_attempt_id",
        "mfa_test_last_code",
        "mfa_next",
    ):
        session.pop(_stale, None)
    session["mfa_ok"] = True
    session["org_subscription_status"] = None
    if session.get("org_id"):
        try:
            org = get_org(int(session["org_id"]))
            if org:
                session["org_subscription_status"] = (
                    (org.get("stripe_subscription_status") or "").strip().lower() or None
                )
        except Exception:
            session["org_subscription_status"] = None
    return True


# Recommendation scoring walks recent views/compares against live inventory
# (~1s+); Home and Dashboard both need it on every visit, so cache per
# (user, geo) briefly and invalidate on new view/compare signals.
_RECO_CACHE_TTL_S = 120.0
_reco_cache: dict[tuple, tuple[float, list, dict]] = {}


_LIVE_GAS_PRICES_PATH = (
    Path(__file__).resolve().parent / "dictionary" / "derived" / "live_gas_prices.json"
)
_live_gas_prices_cache: dict[str, Any] | None = None
_live_gas_prices_cache_mtime: float | None = None


def _nhtsa_recalls_lookup_payload(
    *,
    vin_raw: str,
    make: str | None = None,
    model: str | None = None,
    year: str | None = None,
    rate_key: str,
) -> tuple[dict, int]:
    """Shared NHTSA recall lookup for HTML page and JSON API."""
    from backend.enrichment.vehicle_history_intelligence import fetch_nhtsa_recalls
    from backend.utils.hybrid_search import _normalize_listings_vin_query

    if not allow_request(
        rate_key,
        max_events=_NHTSA_RECALLS_RPM,
        window_seconds=60.0,
    ):
        return {"ok": False, "error": "rate_limited", "recalls": []}, 429

    vin_norm = _normalize_listings_vin_query(vin_raw) if vin_raw else None
    ymm_make = (make or "").strip() or None
    ymm_model = (model or "").strip() or None
    ymm_year = (year or "").strip() or None
    vehicle_label = (
        f"{ymm_year} {ymm_make} {ymm_model}".strip()
        if ymm_make and ymm_model and ymm_year
        else None
    )

    if vin_raw and not vin_norm:
        return {
            "ok": False,
            "error": "invalid_vin",
            "vin": None,
            "vin_raw": vin_raw,
            "recalls": [],
            "vehicle_label": vehicle_label,
        }, 400
    if not vin_norm:
        return {
            "ok": False,
            "error": "missing_vin",
            "recalls": [],
            "vehicle_label": vehicle_label,
        }, 400

    recalls, api_err = fetch_nhtsa_recalls(
        vin_norm,
        make=ymm_make,
        model=ymm_model,
        year=ymm_year,
    )
    payload: dict = {
        "ok": api_err is None,
        "vin": vin_norm,
        "recalls": recalls,
        "vehicle_label": vehicle_label,
        "error": api_err,
    }
    if api_err:
        return payload, 502
    return payload, 200


from flask_socketio import SocketIO  # noqa: E402

_socketio_cors = _socketio_cors_allowed_origins()
socketio = SocketIO(
    app,
    async_mode="threading",
    cors_allowed_origins=_socketio_cors,
    manage_session=True,
)
if getattr(app, "extensions", None) is None:
    app.extensions = {}
app.extensions["socketio"] = socketio
