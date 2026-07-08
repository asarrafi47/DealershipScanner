"""Sarrafi Collection — Flask web application."""

from backend.utils.project_env import load_project_dotenv

load_project_dotenv()

from backend.utils.kmac_vault import load_kmac_vault_secrets

load_kmac_vault_secrets()

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

_MIN_PASSWORD_LEN = max(8, int(os.environ.get("MIN_PASSWORD_LENGTH", "8")))
_logger = logging.getLogger(__name__)


def _socketio_cors_allowed_origins() -> str | list[str]:
    """Socket.IO browser origins. Production defaults avoid wildcard CORS (SEC-063)."""
    raw = (os.environ.get("SOCKETIO_CORS_ORIGINS") or "").strip()
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
    for key in ("PUBLIC_BASE_URL", "MFA_QR_BASE_URL"):
        base = (os.environ.get(key) or "").strip().rstrip("/")
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


def _listings_client_poll_ms() -> int:
    """Optional client refresh of ``/api/listings/cars`` (0 = off)."""
    raw = (os.environ.get("LISTINGS_CLIENT_POLL_MS") or "0").strip() or "0"
    try:
        return max(0, int(raw.split()[0]))
    except (TypeError, ValueError, IndexError):
        return 0
_CHAT_MAX_MESSAGE = int(os.environ.get("CHAT_MAX_MESSAGE_CHARS", "4000"))
_CHAT_MAX_BODY = int(os.environ.get("CHAT_MAX_BODY_BYTES", "65536"))
# Cap JSON POST bodies (smart search, chat) and allow dealer multipart uploads (8 MiB+).
_DEFAULT_MAX_CONTENT = max(9 * 1024 * 1024, _CHAT_MAX_BODY * 2)
_MAX_REQUEST_BODY = int(os.environ.get("MAX_REQUEST_BODY_BYTES", str(_DEFAULT_MAX_CONTENT)))
_SMART_SEARCH_RPM = int(os.environ.get("RATE_LIMIT_SMART_SEARCH_PER_MIN", "90"))
_LOGIN_RPM = int(os.environ.get("RATE_LIMIT_LOGIN_PER_MIN", "30"))
_REGISTER_RPM = int(os.environ.get("RATE_LIMIT_REGISTER_PER_MIN", "10"))
_DEALER_LOCATOR_RPM = int(os.environ.get("RATE_LIMIT_DEALER_LOCATOR_PER_MIN", "30"))
_NHTSA_RECALLS_RPM = int(os.environ.get("RATE_LIMIT_NHTSA_RECALLS_PER_MIN", "30"))


def _client_ip() -> str:
    return _client_ip_from_request(request)


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

_raw_secret = (os.environ.get("SECRET_KEY") or os.environ.get("FLASK_SECRET_KEY") or "").strip()
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
register_dev_console(app)


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
    }


def _password_reset_enabled() -> bool:
    from backend.auth.password_reset import password_reset_enabled

    return password_reset_enabled()


def _billing_enabled() -> bool:
    return (os.environ.get("BILLING_STRIPE_ENABLED") or "").strip().lower() in ("1", "true", "yes", "on")


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


def _car_window_sticker_preview_url(car_id: int) -> str:
    return f"/car/{car_id}/window-sticker-preview.png"


def _serve_car_window_sticker_preview(car_id: int):
    """Render page 1 of stored Monroney PDF as PNG (same gate as chat/packages; SEC-072/073)."""
    car = get_car_by_id(car_id, include_inactive=False)
    if not car:
        abort(404)
    ok, _err = _require_feature(FEATURE_WINDOW_STICKER)
    if not ok:
        abort(403)
    from backend.enrichment.window_sticker_service import (
        ensure_sticker_preview_png,
        ensure_window_sticker_for_car,
        window_sticker_visual_local_path,
    )

    ensure_window_sticker_for_car(car_id, allow_vision_fallback=False)
    path = window_sticker_visual_local_path(
        car.get("vin"),
        dealer_id=car.get("dealer_id"),
        dealership_registry_id=car.get("dealership_registry_id"),
    )
    if not path:
        abort(404)
    png = ensure_sticker_preview_png(path)
    if not png or not png.is_file():
        abort(503)
    mime = "image/png"
    suffix = png.suffix.lower()
    if suffix in (".jpg", ".jpeg"):
        mime = "image/jpeg"
    elif suffix == ".webp":
        mime = "image/webp"
    return send_from_directory(
        str(png.parent),
        png.name,
        mimetype=mime,
        as_attachment=False,
        download_name=f"window-sticker-{car_id}.png",
    )


@app.route("/car/<int:car_id>/window-sticker-preview.png")
def car_window_sticker_preview_png(car_id: int):
    return _serve_car_window_sticker_preview(car_id)


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
    v = (os.environ.get("CSP_ENFORCE") or "").strip().lower()
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
    if (os.environ.get("CSP_REPORT_ONLY") or "").strip().lower() in ("1", "true", "yes", "on"):
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


def _app_version() -> str:
    version_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "VERSION")
    try:
        with open(version_path, encoding="utf-8") as fh:
            return (fh.read() or "").strip() or "dev"
    except OSError:
        return "dev"


@app.route("/health")
def health():
    return jsonify({"status": "ok", "version": _app_version()}), 200


@app.route("/favicon.ico")
def favicon():
    return send_from_directory(app.static_folder, "favicon.svg", mimetype="image/svg+xml")


# Serve locally-downloaded car images (written by image_downloader.py).
# Stored under <project_root>/car_images/<dealer_id>/<vin>/<file>.
_CAR_IMAGES_DIR = Path(__file__).resolve().parent.parent / "car_images"


@app.route("/car-images/<path:filename>")
def serve_car_image(filename: str):
    # Block path traversal: reject any component that starts with '.' or contains separators
    parts = Path(filename).parts
    if not parts or any(p.startswith(".") or p in ("/", "\\") for p in parts):
        abort(400)
    safe_path = _CAR_IMAGES_DIR.joinpath(*parts).resolve()
    if not safe_path.is_relative_to(_CAR_IMAGES_DIR.resolve()):
        abort(400)
    return send_from_directory(str(_CAR_IMAGES_DIR), str(Path(*parts)))


def _inventory_count_display() -> str:
    from backend.db.inventory_db import public_listings_count

    n = public_listings_count()
    if n >= 1000:
        rounded = (n // 100) * 100
        return f"{rounded:,}+"
    if n > 0:
        return f"{n:,}"
    return "Live"


@app.route("/")
def home():
    """Public marketing landing; signed-in users go to ``/home``."""
    if session.get("user_id"):
        return redirect(url_for("app_home"))
    from datetime import datetime

    return render_template(
        "landing.html",
        now=datetime.utcnow(),
        inventory_count_display=_inventory_count_display(),
    )


@app.route("/home")
def app_home():
    """Signed-in feed: recommendations, saved cars, browsing history."""
    if not session.get("user_id"):
        return redirect(url_for("login_page"))
    return _render_personal_home()


@app.route("/compare")
def compare_page():
    """Side-by-side specs; public (ids in query string), history recorded for signed-in users."""
    from backend.utils.compare_specs import build_compare_context, parse_compare_car_ids

    ids = parse_compare_car_ids(request.args.get("ids"))
    if ids and session.get("user_id"):
        try:
            record_compare_session(int(session["user_id"]), ids)
            _invalidate_reco_cache(int(session["user_id"]))
        except (TypeError, ValueError):
            pass
    raw_cars = get_cars_by_ids(ids)
    ctx = build_compare_context(raw_cars)
    show_compare_chat = bool(
        ctx["cars"]
        and (
            _session_has_paid_access()
            or (session.get("user_id") and not _billing_enabled())
        )
    )
    return render_template(
        "compare.html",
        cars=ctx["cars"],
        compare_rows=ctx["compare_rows"],
        compare_ids=ctx["compare_car_ids"],
        show_compare_chat=show_compare_chat,
    )


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


def _similar_recommendation_rows(
    seen_mm: list[tuple[str, str]],
    viewed_id_set: set[int],
    limit: int,
    geo_kw: dict,
) -> list[dict]:
    """Cars matching recent make/model, excluding viewed ids; optional ZIP radius via ``geo_kw``."""
    if not seen_mm:
        return []
    candidates = search_cars_by_make_model_pairs(
        seen_mm[:5],
        sql_limit=max(limit * 6, 60),
        **geo_kw,
    )
    seen_rec: set[int] = set()
    recs: list[dict] = []
    for c in candidates:
        cid = c.get("id")
        if cid and cid not in viewed_id_set and cid not in seen_rec:
            seen_rec.add(cid)
            recs.append(c)
            if len(recs) >= limit:
                return recs
    return recs


def _make_model_pairs_from_cars(cars: list[dict]) -> list[tuple[str, str]]:
    seen_mm: list[tuple[str, str]] = []
    seen_mm_set: set[tuple[str, str]] = set()
    for c in cars:
        make = (c.get("make") or "").strip()
        model = (c.get("model") or "").strip()
        if make and model:
            key = (make.lower(), model.lower())
            if key not in seen_mm_set:
                seen_mm_set.add(key)
                seen_mm.append((make, model))
    return seen_mm


# Recommendation scoring walks recent views/compares against live inventory
# (~1s+); Home and Dashboard both need it on every visit, so cache per
# (user, geo) briefly and invalidate on new view/compare signals.
_RECO_CACHE_TTL_S = 120.0
_reco_cache: dict[tuple, tuple[float, list, dict]] = {}


def _invalidate_reco_cache(user_id: int) -> None:
    for key in [k for k in _reco_cache if k[0] == int(user_id)]:
        _reco_cache.pop(key, None)


def _recommendations_for_user(
    user_id: int,
    limit: int = 20,
    *,
    serialize: bool = True,
    **geo_kw: object,
) -> tuple[list[dict] | int, dict[str, str]]:
    """Cached wrapper: serialized rows (or their count) + heading copy."""
    key = (int(user_id), int(limit), tuple(sorted((k, str(v)) for k, v in geo_kw.items())))
    now = time.monotonic()
    hit = _reco_cache.get(key)
    if hit is not None and hit[0] > now:
        rows, heading = hit[1], hit[2]
        return (list(rows) if serialize else len(rows)), dict(heading)
    rows, heading = _recommendations_for_user_uncached(user_id, limit, serialize=True, **geo_kw)
    if len(_reco_cache) >= 500:
        _reco_cache.clear()
    _reco_cache[key] = (now + _RECO_CACHE_TTL_S, rows, heading)
    return (list(rows) if serialize else len(rows)), dict(heading)


def _recommendations_for_user_uncached(
    user_id: int,
    limit: int = 20,
    *,
    serialize: bool = True,
    **geo_kw: object,
) -> tuple[list[dict] | int, dict[str, str]]:
    """Return serialized carousel rows and heading copy for the dashboard."""
    default_heading = {
        "eyebrow": "Based on your history",
        "title": "Recommended for You",
        "hint": "",
    }
    viewed_ids = get_recent_viewed_car_ids(user_id, limit=30)
    compared_ids = get_recent_compared_car_ids(user_id, limit=30)
    if not viewed_ids and not compared_ids:
        return (0 if not serialize else []), default_heading

    viewed_cars = get_cars_by_ids(viewed_ids) if viewed_ids else []
    compared_cars = get_cars_by_ids(compared_ids) if compared_ids else []
    if not viewed_cars and not compared_cars:
        return (0 if not serialize else []), default_heading

    by_id: dict[int, dict] = {}
    for c in viewed_cars + compared_cars:
        cid = c.get("id")
        if cid is not None:
            by_id[int(cid)] = c

    seen_mm = _make_model_pairs_from_cars(viewed_cars)
    seen_keys = {(m.lower(), d.lower()) for m, d in seen_mm}
    for pair in _make_model_pairs_from_cars(compared_cars):
        key = (pair[0].lower(), pair[1].lower())
        if key not in seen_keys:
            seen_keys.add(key)
            seen_mm.append(pair)

    signal_id_set = set(viewed_ids) | set(compared_ids)
    heading = dict(default_heading)
    if compared_ids and viewed_ids:
        heading["eyebrow"] = "Based on your browsing and comparisons"
    elif compared_ids:
        heading["eyebrow"] = "Based on your comparisons"
        heading["title"] = "Similar to what you compared"
    raw_recs: list[dict] = []
    geo_kw_dict = dict(geo_kw)
    geo_active = bool(geo_kw_dict.get("zip_code") and geo_kw_dict.get("radius_miles"))

    if seen_mm:
        if geo_active:
            raw_recs = _similar_recommendation_rows(seen_mm, signal_id_set, limit, geo_kw_dict)
            if not raw_recs:
                heading["eyebrow"] = "Outside your search radius"
                heading["title"] = "Recommended & recently viewed"
                raw_recs = _similar_recommendation_rows(seen_mm, signal_id_set, limit, {})
                if raw_recs:
                    heading["hint"] = (
                        "No similar listings near your saved ZIP and radius. "
                        "Showing similar inventory beyond that area and cars from your history."
                    )
                else:
                    heading["hint"] = (
                        "No close matches in inventory right now. Here are cars from your recent history."
                    )
        else:
            raw_recs = _similar_recommendation_rows(seen_mm, signal_id_set, limit, {})
    else:
        heading["eyebrow"] = "Your history"
        heading["title"] = "Recently viewed"
        heading["hint"] = ""

    out_cars: list[dict] = []
    seen_out: set[int] = set()
    for c in raw_recs:
        if len(out_cars) >= limit:
            break
        cid = c.get("id")
        if cid is None:
            continue
        cid_i = int(cid)
        if cid_i not in seen_out:
            seen_out.add(cid_i)
            out_cars.append(c)

    for vid in viewed_ids:
        if len(out_cars) >= limit:
            break
        vid_i = int(vid)
        if vid_i in seen_out:
            continue
        row = by_id.get(vid_i)
        if row:
            out_cars.append(row)
            seen_out.add(vid_i)

    for cid in compared_ids:
        if len(out_cars) >= limit:
            break
        cid_i = int(cid)
        if cid_i in seen_out:
            continue
        row = by_id.get(cid_i)
        if row:
            out_cars.append(row)
            seen_out.add(cid_i)

    if not out_cars:
        return (0 if not serialize else []), default_heading

    if not serialize:
        return len(out_cars[:limit]), heading

    return (
        [serialize_car_for_listings_grid(c) for c in out_cars[:limit]],
        heading,
    )


def _recently_compared_for_user(user_id: int, limit: int = 12) -> list[dict]:
    """Serialized listing rows for cars the user recently compared."""
    compared_ids = get_recent_compared_car_ids(user_id, limit=limit)
    if not compared_ids:
        return []
    raw = get_cars_by_ids(compared_ids)
    by_id = {int(c["id"]): c for c in raw if c.get("id") is not None}
    ordered = [by_id[cid] for cid in compared_ids if cid in by_id]
    return [serialize_car_for_listings_grid(c) for c in ordered]


def _recently_viewed_for_user(user_id: int, limit: int = 12) -> list[dict]:
    """Serialized listing rows for cars the user recently opened."""
    viewed_ids = get_recent_viewed_car_ids(user_id, limit=limit)
    if not viewed_ids:
        return []
    raw = get_cars_by_ids(viewed_ids)
    by_id = {int(c["id"]): c for c in raw if c.get("id") is not None}
    ordered = [by_id[cid] for cid in viewed_ids if cid in by_id]
    return [serialize_car_for_listings_grid(c) for c in ordered]


def _browse_trends_for_user(user_id: int, limit: int = 6) -> list[tuple[str, int]]:
    """Top makes from recent view history for dashboard trends."""
    from collections import Counter

    viewed_ids = get_recent_viewed_car_ids(user_id, limit=40)
    if not viewed_ids:
        return []
    counts: Counter[str] = Counter()
    for c in get_cars_by_ids(viewed_ids):
        make = (c.get("make") or "").strip()
        if make:
            counts[make] += 1
    return counts.most_common(limit)


def _render_personal_home():
    """Logged-in landing: recommendations, saved cars, browsing history."""
    user_id = session.get("user_id")
    recommendations = []
    saved_cars_list = []
    recently_compared = []
    recently_viewed = []
    recommendations_eyebrow = ""
    recommendations_title = ""
    recommendations_hint = ""
    if user_id:
        uid = int(user_id)
        recommendations, rec_heading = _recommendations_for_user(uid, **listings_geo_kwargs_from_session(session))
        recommendations_eyebrow = rec_heading.get("eyebrow") or ""
        recommendations_title = rec_heading.get("title") or "Recommended for You"
        recommendations_hint = rec_heading.get("hint") or ""
        saved_ids = get_saved_car_ids(uid)
        raw_saved = get_cars_by_ids(saved_ids)
        saved_cars_list = [serialize_car_for_listings_grid(c) for c in raw_saved]
        recently_compared = _recently_compared_for_user(uid)
        recently_viewed = _recently_viewed_for_user(uid)
    return render_template(
        "home.html",
        saved_cars=saved_cars_list,
        recommendations=recommendations,
        recently_compared=recently_compared,
        recently_viewed=recently_viewed,
        recommendations_eyebrow=recommendations_eyebrow,
        recommendations_title=recommendations_title,
        recommendations_hint=recommendations_hint,
    )


@app.route("/dashboard")
def dashboard():
    """Stats, preferences, and quick actions (personal feed lives on Home)."""
    if not session.get("user_id"):
        return redirect(url_for("login_page"))
    uid = int(session["user_id"])
    geo = listings_geo_kwargs_from_session(session)
    saved_ids = get_saved_car_ids(uid)
    reco_count, _ = _recommendations_for_user(uid, limit=20, serialize=False, **geo)
    return render_template(
        "dashboard.html",
        viewed_count=count_viewed_cars(uid),
        saved_count=len(saved_ids),
        reco_count=reco_count,
        geo_zip=geo.get("zip_code") or "",
        geo_radius=geo.get("radius_miles"),
        browse_trends=_browse_trends_for_user(uid),
    )


@app.route("/search")
def search():
    """Backward-compatible alias: inventory search lives at ``/listings``."""
    dest = url_for("listings")
    qs = request.query_string.decode("utf-8")
    if qs:
        dest = f"{dest}?{qs}"
    return redirect(dest, code=302)


@app.route("/api/session/listings-geo", methods=["POST"])
def api_session_listings_geo():
    """Remember ZIP + radius for dashboard recommendations (session cookie)."""
    body = request.get_json(silent=True)
    if not isinstance(body, dict):
        return jsonify({"ok": False, "error": "json_object"}), 400
    zip_code = str(body.get("zip_code") or "").strip()
    try:
        radius_mi = float(body.get("radius"))
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "bad_radius"}), 400
    if not apply_listings_geo_to_session(session, zip_code, radius_mi):
        return jsonify({"ok": False, "error": "invalid_zip_or_radius"}), 400
    return jsonify({"ok": True})


@app.route("/api/listings/filter-options")
def api_listings_filter_options():
    """Facet metadata for listings filters (same source as the listings page sidebar)."""
    opts = get_filter_options(include_all_cars=False)
    return jsonify(
        {
            "ok": True,
            "makes": opts.get("makes") or [],
            "model_rows": opts.get("model_rows") or [],
            "trim_rows": opts.get("trim_rows") or [],
            "fuel_types": opts.get("fuel_types") or [],
            "cylinders": opts.get("cylinders") or [],
            "transmissions": opts.get("transmissions") or [],
            "drivetrains": opts.get("drivetrains") or [],
            "forced_inductions": opts.get("forced_inductions") or [],
            "body_styles": opts.get("body_styles") or [],
            "exterior_colors": opts.get("exterior_colors") or [],
            "interior_colors": opts.get("interior_colors") or [],
            "package_rows": opts.get("package_rows") or [],
            "all_package_names": opts.get("all_package_names") or [],
        }
    )


@app.route("/api/listings/geo-coords")
def api_listings_geo_coords():
    """Lazy ZIP + dealer coordinate maps for listings radius filtering."""
    maps = listings_geo_coords_maps()
    resp = make_response(
        jsonify(
            {
                "ok": True,
                "zip_coords": maps.get("zip_coords") or {},
                "dealer_coords": maps.get("dealer_coords") or {},
                "registry_id_by_host": maps.get("registry_id_by_host") or {},
            }
        )
    )
    resp.headers["Cache-Control"] = "private, max-age=300"
    return resp


@app.route("/api/listings/cars")
def api_listings_cars():
    """Read-only JSON for the listings grid; supports client refresh while a scan is running."""
    etag = listings_grid_cache_etag()
    inm = (request.headers.get("If-None-Match") or "").strip()
    if inm and inm == etag:
        resp = make_response("", 304)
        resp.headers["ETag"] = etag
        resp.headers["Cache-Control"] = "private, no-cache"
        return resp
    cars = listings_grid_serialized_cars()
    etag = listings_grid_cache_etag()
    resp = make_response(jsonify({"ok": True, "cars": cars}))
    resp.headers["ETag"] = etag
    resp.headers["Cache-Control"] = "private, no-cache"
    return resp


@app.route("/api/listings/market-stats")
def api_listings_market_stats():
    """Trim-level average prices for premium listings grid (cached server-side)."""
    ok, err = _require_feature(FEATURE_MARKET_INTEL)
    if not ok:
        return jsonify(_feature_denied_json(FEATURE_MARKET_INTEL, err)), 403
    from backend.listings.geo_session import listings_geo_kwargs_from_session
    from backend.utils.market_price import trim_price_stats_for_client

    zip_code = (request.args.get("zip_code") or "").strip()
    radius_s = (request.args.get("radius") or "").strip()
    geo = listings_geo_kwargs_from_session(session)
    if not zip_code and geo.get("zip_code"):
        zip_code = str(geo["zip_code"])
    if not radius_s and geo.get("radius_miles") is not None:
        radius_s = str(geo["radius_miles"])
    radius_mi = None
    if radius_s:
        try:
            radius_mi = float(radius_s)
        except (TypeError, ValueError):
            radius_mi = None

    payload = trim_price_stats_for_client(zip_code=zip_code or None, radius_miles=radius_mi)
    return jsonify({"ok": True, **payload})


@app.route("/api/zip-coords")
def api_zip_coords():
    """Return {lat, lon} for a US zip code via pgeocode."""
    zip_code = request.args.get("zip", "").strip()
    if not zip_code:
        return jsonify({"error": "zip required"}), 400
    try:
        from backend.db.geo import zip_to_coords
        coords = zip_to_coords(zip_code)
        if coords is None:
            return jsonify({"error": "not found"}), 404
        import math
        if math.isnan(coords[0]) or math.isnan(coords[1]):
            return jsonify({"error": "not found"}), 404
        return jsonify({"lat": float(coords[0]), "lon": float(coords[1])})
    except Exception:
        return jsonify({"error": "lookup failed"}), 500


@app.route("/api/coords-to-zip")
def api_coords_to_zip():
    """Return nearest US ZIP for lat/lon (browser geolocation → listings ZIP)."""
    try:
        lat = float(request.args.get("lat", ""))
        lon = float(request.args.get("lon", ""))
    except (TypeError, ValueError):
        return jsonify({"error": "lat and lon required"}), 400
    if not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0):
        return jsonify({"error": "invalid coordinates"}), 400
    try:
        from backend.db.geo import nearest_us_postal_meta

        meta = nearest_us_postal_meta(lat, lon)
        if not meta or not meta.get("postal_code"):
            return jsonify({"error": "not found"}), 404
        return jsonify({"zip_code": meta["postal_code"], "lat": lat, "lon": lon})
    except Exception:
        return jsonify({"error": "lookup failed"}), 500


_LIVE_GAS_PRICES_PATH = (
    Path(__file__).resolve().parent / "dictionary" / "derived" / "live_gas_prices.json"
)
_FUEL_TIER_ALIASES: dict[str, str] = {
    "regular": "regular",
    "mid": "mid",
    "midgrade": "mid",
    "mid-grade": "mid",
    "mid_grade": "mid",
    "premium": "premium",
    "diesel": "diesel",
}
_FUEL_TIER_JSON_KEYS: dict[str, tuple[str, ...]] = {
    "regular": ("regular", "Regular"),
    "mid": ("mid", "midgrade", "midGrade", "mid-grade", "Mid-Grade", "Mid Grade"),
    "premium": ("premium", "Premium"),
    "diesel": ("diesel", "Diesel"),
}
_live_gas_prices_cache: dict[str, Any] | None = None
_live_gas_prices_cache_mtime: float | None = None


def _normalize_fuel_tier_param(raw: str | None) -> str:
    key = (raw or "regular").strip().lower().replace(" ", "-")
    if key in _FUEL_TIER_ALIASES:
        return _FUEL_TIER_ALIASES[key]
    if "premium" in key:
        return "premium"
    if "diesel" in key:
        return "diesel"
    if "mid" in key:
        return "mid"
    return "regular"


def _load_live_gas_prices_payload() -> dict[str, Any] | None:
    global _live_gas_prices_cache, _live_gas_prices_cache_mtime
    path = _LIVE_GAS_PRICES_PATH
    if not path.is_file():
        return None
    try:
        mtime = path.stat().st_mtime
    except OSError:
        return None
    if _live_gas_prices_cache is not None and _live_gas_prices_cache_mtime == mtime:
        return _live_gas_prices_cache
    try:
        with path.open(encoding="utf-8") as fh:
            payload = json.load(fh)
    except (OSError, json.JSONDecodeError) as exc:
        _logger.warning("live_gas_prices.json unreadable: %s", exc)
        return None
    if not isinstance(payload, dict):
        return None
    _live_gas_prices_cache = payload
    _live_gas_prices_cache_mtime = mtime
    return payload


def _fuel_market_payload() -> dict[str, Any]:
    """Load cached EIA market data, falling back to 2026 anchors when missing."""
    payload = _load_live_gas_prices_payload()
    if payload:
        return payload
    from backend.cron.sync_gas_prices import fallback_payload

    return fallback_payload()


def _live_gas_region_block(payload: dict[str, Any], state_code: str) -> dict[str, Any] | None:
    raw = (state_code or "").strip()
    if not raw:
        return None
    code = raw.lower() if raw.lower() == "national" else raw.upper()
    states = payload.get("states")
    if isinstance(states, dict):
        for key in (code, raw, raw.upper(), raw.lower()):
            block = states.get(key)
            if isinstance(block, dict):
                return block
    for key in (code, raw, raw.upper(), raw.lower()):
        block = payload.get(key)
        if isinstance(block, dict):
            return block
    return None


def _live_gas_region_name(block: dict[str, Any], state_code: str, *, used_national: bool) -> str:
    for key in ("region_name", "regionName", "name", "region", "label"):
        val = block.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
    if used_national:
        return "National Average"
    return state_code


def _live_gas_rate_from_block(block: dict[str, Any], fuel_tier: str) -> float | None:
    keys = _FUEL_TIER_JSON_KEYS.get(fuel_tier, (fuel_tier,))
    for key in keys:
        raw = block.get(key)
        if raw is None:
            continue
        try:
            rate = float(raw)
        except (TypeError, ValueError):
            continue
        if rate > 0:
            return rate
    prices = block.get("prices")
    if isinstance(prices, dict):
        for key in keys:
            raw = prices.get(key)
            if raw is None:
                continue
            try:
                rate = float(raw)
            except (TypeError, ValueError):
                continue
            if rate > 0:
                return rate
    return None


def _live_electricity_rate_from_block(block: dict[str, Any]) -> float | None:
    for key in ("electricity_rate", "electricityRate", "residential_electricity_rate"):
        raw = block.get(key)
        if raw is None:
            continue
        try:
            rate = float(raw)
        except (TypeError, ValueError):
            continue
        if rate > 0:
            return rate
    return None


def _resolve_live_gas_lookup(state_code: str, fuel_tier: str) -> tuple[float, str] | None:
    payload = _fuel_market_payload()
    state_block = _live_gas_region_block(payload, state_code)
    used_national = False
    if state_block is None:
        state_block = _live_gas_region_block(payload, "national")
        used_national = True
    if state_block is None:
        return None
    rate = _live_gas_rate_from_block(state_block, fuel_tier)
    if rate is None and not used_national:
        national_block = _live_gas_region_block(payload, "national")
        if national_block is not None:
            rate = _live_gas_rate_from_block(national_block, fuel_tier)
            if rate is not None:
                state_block = national_block
                used_national = True
    if rate is None:
        return None
    region_name = _live_gas_region_name(state_block, state_code, used_national=used_national)
    return rate, region_name


def _resolve_live_electricity_lookup(state_code: str) -> tuple[float, str]:
    payload = _fuel_market_payload()
    state_block = _live_gas_region_block(payload, state_code)
    used_national = False
    if state_block is None:
        state_block = _live_gas_region_block(payload, "national")
        used_national = True
    if state_block is None:
        national = payload.get("national")
        if isinstance(national, dict):
            state_block = national
            used_national = True
    rate = _live_electricity_rate_from_block(state_block or {})
    if rate is None and not used_national:
        national_block = _live_gas_region_block(payload, "national")
        if national_block is not None:
            rate = _live_electricity_rate_from_block(national_block)
            if rate is not None:
                state_block = national_block
                used_national = True
    if rate is None:
        national = payload.get("national")
        if isinstance(national, dict):
            rate = _live_electricity_rate_from_block(national)
            if rate is not None:
                state_block = national
                used_national = True
    if rate is None:
        from backend.cron.sync_gas_prices import FALLBACK_NATIONAL_ELECTRICITY

        rate = FALLBACK_NATIONAL_ELECTRICITY
        state_block = state_block or {"region_name": "National Average"}
        used_national = True
    region_name = _live_gas_region_name(state_block, state_code, used_national=used_national)
    return rate, region_name


def _resolve_fuel_lookup_state(request, session_obj: object) -> tuple[str, str | None]:
    """Resolve a two-letter state for fuel lookup; optional ZIP used for resolution."""
    zip_raw = (request.args.get("zip_code") or request.args.get("zip") or "").strip()
    if not zip_raw:
        geo = listings_geo_kwargs_from_session(session_obj)
        zip_raw = str(geo.get("zip_code") or "").strip()
    if zip_raw:
        zip_code = zip_raw[:5]
        if re.fullmatch(r"\d{5}", zip_code):
            try:
                from backend.db.geo import us_postal_meta_for_zip

                meta = us_postal_meta_for_zip(zip_code)
                if meta and meta.get("state_code"):
                    return str(meta["state_code"]).strip().upper(), zip_code
            except Exception:
                pass
    state_raw = (request.args.get("state") or "NC").strip().upper()
    state_code = state_raw[:2] if re.fullmatch(r"[A-Z]{2}", state_raw[:2] or "") else "NC"
    return state_code, None


@app.route("/api/fuel/lookup", methods=["GET"])
def api_fuel_lookup():
    """Cached EIA regional fuel and residential electricity rates for TCO."""
    fuel_tier = _normalize_fuel_tier_param(request.args.get("fuel_tier"))
    state_code, zip_code = _resolve_fuel_lookup_state(request, session)
    resolved = _resolve_live_gas_lookup(state_code, fuel_tier)
    electricity_rate, electricity_region = _resolve_live_electricity_lookup(state_code)
    payload_data = _fuel_market_payload()
    if resolved is None:
        national = payload_data.get("national")
        if isinstance(national, dict):
            rate = _live_gas_rate_from_block(national, fuel_tier)
            region_name = str(national.get("region_name") or "National Average")
        else:
            from backend.cron.sync_gas_prices import fallback_payload

            fb = fallback_payload()
            national_fb = fb.get("national") or {}
            rate = _live_gas_rate_from_block(national_fb, fuel_tier) or 4.29
            region_name = str(national_fb.get("region_name") or "National Average")
    else:
        rate, region_name = resolved
    payload: dict[str, Any] = {
        "rate": rate,
        "electricity_rate": electricity_rate,
        "region_name": region_name,
        "electricity_region_name": electricity_region,
        "state": state_code,
        "source": payload_data.get("source"),
    }
    if zip_code:
        payload["zip_code"] = zip_code
    return jsonify(payload)


def _build_car_detail_view_context(car_id: int, car_raw: dict) -> dict:
    uid = session.get("user_id")
    car_is_saved = False
    if uid:
        try:
            record_car_view(int(uid), car_id)
            _invalidate_reco_cache(int(uid))
        except Exception:
            pass
        try:
            car_is_saved = is_car_saved(int(uid), car_id)
        except Exception:
            pass
    ctx = prepare_car_detail_context(car_raw)
    from backend.enrichment.window_sticker_service import (
        window_sticker_available,
        window_sticker_has_visual,
    )
    from backend.scanner.window_sticker import (
        car_listing_may_have_sticker,
        car_listing_sticker_urls,
        cdjr_oem_window_sticker_eligible,
        get_window_sticker_url,
        is_cdjr_stellantis_car,
        show_window_sticker_panel,
        sticker_embed_preview_url,
    )

    cdjr_sticker_eligible = cdjr_oem_window_sticker_eligible(car_raw)
    show_sticker_ui = show_window_sticker_panel(car_raw, ctx)
    sticker_ready = show_sticker_ui and window_sticker_available(car_raw)
    sticker_visual = show_sticker_ui and window_sticker_has_visual(car_raw)
    listing_sticker_urls = car_listing_sticker_urls(car_raw) if car_listing_may_have_sticker(car_raw) else []
    listing_sticker_image_url = listing_sticker_urls[0] if listing_sticker_urls else None
    premium_viewer = _viewer_sees_premium_features()
    sticker_preview_api = _car_window_sticker_preview_url(car_id)
    # Window sticker fetch runs async via car_packages.js — avoid blocking VDP render.
    sticker_preview_embed_url = (
        sticker_embed_preview_url(
            car_raw,
            preview_api_url=sticker_preview_api,
            has_paid_access=premium_viewer,
        )
        if show_sticker_ui
        else None
    )
    sticker_fetch_pending = bool(
        show_sticker_ui and not sticker_ready and (listing_sticker_image_url or cdjr_sticker_eligible)
    )

    window_sticker_oem_url = None
    window_sticker_pdf_url = None
    if cdjr_sticker_eligible:
        window_sticker_oem_url = get_window_sticker_url(str(car_raw.get("vin") or ""))
    if show_sticker_ui and premium_viewer:
        if sticker_visual or cdjr_sticker_eligible:
            window_sticker_pdf_url = url_for("api_car_window_sticker", car_id=car_id)
        if not window_sticker_oem_url and listing_sticker_urls:
            window_sticker_oem_url = listing_sticker_urls[0]
    if show_sticker_ui and premium_viewer and not sticker_preview_embed_url:
        sticker_preview_embed_url = sticker_preview_api if sticker_visual else None

    car = serialize_car_for_api(
        car_raw,
        include_verified=False,
        verified_specs=ctx.get("verified_specs") or {},
    )
    from backend.db.incomplete_listings_db import get_missing_field_codes_for_car_id

    _missing_codes = get_missing_field_codes_for_car_id(car_id)
    listing_incomplete_fields = [
        {"code": c, "label": INCOMPLETE_FIELD_LABELS.get(c, c.replace("_", " ").title())}
        for c in _missing_codes
    ]
    dealer_info = None
    reg_id = car_raw.get("dealership_registry_id")
    if reg_id:
        try:
            from backend.db.dealerships_db import get_dealership_by_id
            from backend.utils.field_clean import normalize_optional_url

            raw_dealer = get_dealership_by_id(int(reg_id))
            if raw_dealer:
                dealer_info = dict(raw_dealer)
                for url_key in ("dealer_website_url", "website_url"):
                    if url_key in dealer_info:
                        dealer_info[url_key] = normalize_optional_url(dealer_info.get(url_key))
        except Exception:
            pass
    from backend.listings.dealer_map import build_dealer_map_for_car

    dealer_map = build_dealer_map_for_car(car_raw, dealer_info)
    market_intel = None
    trim_ladder = None
    if _session_has_paid_access():
        from backend.utils.market_price import market_price_for_car

        geo = listings_geo_kwargs_from_session(session)
        market_intel = market_price_for_car(
            car_raw,
            zip_code=geo.get("zip_code"),
            radius_miles=geo.get("radius_miles"),
        )
    if _viewer_sees_premium_features():
        try:
            ladder_year = int(car_raw.get("year") or 0)
        except (TypeError, ValueError):
            ladder_year = 0
        if not ladder_year or ladder_year >= 2010:
            from backend.enrichment.trim_ladder import resolve_trim_ladder

            trim_ladder = resolve_trim_ladder(
                make=car_raw.get("make"),
                model=car_raw.get("model"),
                year=car_raw.get("year"),
                trim=car_raw.get("trim"),
            )
    listings_geo = listings_geo_kwargs_from_session(session)
    return {
        "car": car,
        "market_intel": market_intel,
        "trim_ladder": trim_ladder,
        "car_is_saved": car_is_saved,
        "logged_in": bool(uid),
        "listings_geo_zip": listings_geo.get("zip_code") or "",
        "listing_incomplete_fields": listing_incomplete_fields,
        "gallery_images": ctx.get("gallery_images") or [],
        "verified_specs": ctx.get("verified_specs") or {},
        "listing_packages_sections": ctx.get("listing_packages_sections") or [],
        "listing_standalone_features": ctx.get("listing_standalone_features") or [],
        "listing_observed_features": ctx.get("listing_observed_features") or [],
        "listing_monroney_options": ctx.get("listing_monroney_options") or [],
        "listing_monroney_standard": ctx.get("listing_monroney_standard") or [],
        "listing_sticker_options": ctx.get("listing_sticker_options") or [],
        "listing_sticker_option_groups": ctx.get("listing_sticker_option_groups") or [],
        "listing_sticker_option_sections": ctx.get("listing_sticker_option_sections") or {},
        "listing_possible_packages": ctx.get("listing_possible_packages") or [],
        "listing_photo_detected_equipment": ctx.get("listing_photo_detected_equipment") or [],
        "sticker_exterior_color": ctx.get("sticker_exterior_color"),
        "sticker_interior_color": ctx.get("sticker_interior_color"),
        "sticker_interior_material": ctx.get("sticker_interior_material"),
        "sticker_spec_lines": ctx.get("sticker_spec_lines") or [],
        "interior_from_listing_description": bool(ctx.get("interior_from_listing_description")),
        "interior_from_llava_vision": bool(ctx.get("interior_from_llava_vision")),
        "packages_panel_has_content": bool(ctx.get("packages_panel_has_content")),
        "llava_interior_section": ctx.get("llava_interior_section"),
        "window_sticker_available": sticker_ready,
        "window_sticker_visual_available": sticker_visual,
        "show_window_sticker_ui": show_sticker_ui,
        "listing_sticker_image_url": listing_sticker_image_url,
        "sticker_fetch_pending": sticker_fetch_pending,
        "is_cdjr_stellantis": is_cdjr_stellantis_car(car_raw),
        "cdjr_window_sticker_eligible": cdjr_sticker_eligible,
        "window_sticker_preview_api_url": sticker_preview_api,
        "window_sticker_preview_url": sticker_preview_embed_url,
        "hide_photo_analysis": bool(ctx.get("hide_photo_analysis")),
        "window_sticker_oem_url": window_sticker_oem_url,
        "window_sticker_pdf_url": window_sticker_pdf_url,
        "dealer_info": dealer_info,
        "dealer_map": dealer_map,
    }


@app.route("/car/<int:car_id>")
def car_detail(car_id):
    car_raw = get_car_by_id(car_id, include_inactive=False)
    if not car_raw:
        abort(404)
    embed = request.args.get("embed") in ("1", "true", "yes")
    ctx = _build_car_detail_view_context(car_id, car_raw)
    ctx["car_embed"] = embed
    resp = make_response(render_template("car.html", **ctx))
    resp.headers["Cache-Control"] = "private, max-age=180"
    return resp


@app.route("/api/cars/<int:car_id>", methods=["GET"])
def api_car_detail(car_id):
    car_raw = get_car_by_id(car_id, include_inactive=False)
    if not car_raw:
        return jsonify({"ok": False, "error": "not_found"}), 404
    return jsonify({"ok": True, **_build_car_detail_view_context(car_id, car_raw)})


@app.route("/api/cars/<int:car_id>/window-sticker")
def api_car_window_sticker(car_id: int):
    """Serve stored OEM window sticker PDF (premium only)."""
    ok, err = _require_feature(FEATURE_WINDOW_STICKER)
    if not ok:
        return jsonify(_feature_denied_json(FEATURE_WINDOW_STICKER, err)), 403
    car = get_car_by_id(car_id, include_inactive=False)
    if not car:
        return jsonify({"ok": False, "error": "not_found"}), 404
    from backend.enrichment.window_sticker_service import (
        ensure_window_sticker_for_car,
        window_sticker_local_path,
        window_sticker_visual_local_path,
    )

    path = window_sticker_visual_local_path(
        car.get("vin"),
        dealer_id=car.get("dealer_id"),
        dealership_registry_id=car.get("dealership_registry_id"),
    )
    if not path:
        ensure_window_sticker_for_car(car_id, allow_vision_fallback=False)
        path = window_sticker_visual_local_path(
            car.get("vin"),
            dealer_id=car.get("dealer_id"),
            dealership_registry_id=car.get("dealership_registry_id"),
        )
    if not path:
        return jsonify({"ok": False, "error": "sticker_not_available"}), 404
    mime = "application/pdf"
    suffix = path.suffix.lower()
    if suffix in (".jpg", ".jpeg"):
        mime = "image/jpeg"
    elif suffix == ".png":
        mime = "image/png"
    elif suffix == ".webp":
        mime = "image/webp"
    elif suffix == ".txt":
        mime = "text/plain; charset=utf-8"
    return send_from_directory(
        str(path.parent),
        path.name,
        mimetype=mime,
        as_attachment=False,
        download_name=f"window-sticker-{car_id}{path.suffix.lower()}",
    )


@app.route("/api/cars/<int:car_id>/window-sticker-preview")
def api_car_window_sticker_preview(car_id: int):
    """PNG preview of page 1 (legacy API path; prefer /car/<id>/window-sticker-preview.png)."""
    return _serve_car_window_sticker_preview(car_id)


@app.route("/api/cars/<int:car_id>/nhtsa-recalls", methods=["GET"])
def api_car_nhtsa_recalls(car_id: int):
    """NHTSA recall campaigns for a listing VIN (public JSON)."""
    car = get_car_by_id(car_id, include_inactive=False)
    if not car:
        return jsonify({"ok": False, "error": "not_found"}), 404
    payload, status = _nhtsa_recalls_lookup_payload(
        vin_raw=str(car.get("vin") or "").strip(),
        make=str(car.get("make") or "").strip() or None,
        model=str(car.get("model") or "").strip() or None,
        year=str(car.get("year") or "").strip() or None,
        rate_key=f"nhtsa-recalls-api:{_client_ip()}",
    )
    return jsonify(payload), status


@app.route("/api/cars/<int:car_id>/vehicle-history-intelligence", methods=["GET"])
def api_car_vehicle_history_intelligence(car_id: int):
    """NHTSA recalls + vPIC validation + listing title flags (premium; no Carfax)."""
    ok, err = _require_feature(FEATURE_VEHICLE_HISTORY)
    if not ok:
        return jsonify(_feature_denied_json(FEATURE_VEHICLE_HISTORY, err)), 403
    car = get_car_by_id(car_id, include_inactive=False)
    if not car:
        return jsonify({"ok": False, "error": "not_found"}), 404
    from backend.enrichment.vehicle_history_intelligence import build_vehicle_history_intelligence

    payload = build_vehicle_history_intelligence(car)
    status = 200 if payload.get("ok") else 400
    return jsonify(payload), status


@app.route("/api/cars/<int:car_id>/packages/ensure", methods=["POST"])
def api_car_packages_ensure(car_id: int):
    """Fetch/analyze window sticker and merge packages (premium only)."""
    ok, err = _require_feature(FEATURE_PACKAGES_ENSURE)
    if not ok:
        return jsonify(_feature_denied_json(FEATURE_PACKAGES_ENSURE, err)), 403
    try:
        validate_csrf_header()
    except HTTPException:
        return jsonify({"ok": False, "error": "csrf_required"}), 403
    car = get_car_by_id(car_id, include_inactive=False)
    if not car:
        return jsonify({"ok": False, "error": "not_found"}), 404

    from backend.enrichment.listing_packages_service import ensure_listing_packages_for_car

    allow_vision = request.args.get("vision", "0").strip().lower() in ("1", "true", "yes")
    status = ensure_listing_packages_for_car(
        car_id,
        allow_vision_fallback=allow_vision,
        refetch_description=True,
    )
    car2 = get_car_by_id(car_id, include_inactive=False) or car
    ctx = prepare_car_detail_context(car2)
    status["packages_panel_has_content"] = bool(ctx.get("packages_panel_has_content"))
    status["window_sticker_available"] = bool(status.get("window_sticker_available"))
    from backend.enrichment.window_sticker_service import sticker_panel_payload, window_sticker_available, window_sticker_has_visual
    from backend.scanner.window_sticker import (
        car_listing_sticker_urls,
        cdjr_oem_window_sticker_eligible,
        get_window_sticker_url,
        is_cdjr_stellantis_car,
        show_window_sticker_panel,
        sticker_embed_preview_url,
    )

    status.update(sticker_panel_payload(ctx, car2))
    show_sticker = show_window_sticker_panel(car2, ctx)
    status["show_window_sticker_ui"] = show_sticker
    status["window_sticker_available"] = bool(
        show_sticker
        and (
            status.get("window_sticker_available")
            or window_sticker_available(car2)
        )
    )
    status["window_sticker_visual_available"] = bool(
        show_sticker and window_sticker_has_visual(car2)
    )
    vin = str(car2.get("vin") or "")
    if show_sticker and vin:
        if cdjr_oem_window_sticker_eligible(car2):
            status["window_sticker_oem_url"] = get_window_sticker_url(vin)
        else:
            listing_urls = car_listing_sticker_urls(car2)
            if listing_urls:
                status["window_sticker_oem_url"] = listing_urls[0]
    if show_sticker and cdjr_oem_window_sticker_eligible(car2):
        status["window_sticker_view_url"] = url_for("api_car_window_sticker", car_id=car_id)
    elif status.get("window_sticker_visual_available"):
        status["window_sticker_view_url"] = url_for(
            "api_car_window_sticker", car_id=car_id
        )
    if status.get("window_sticker_visual_available") or status.get("window_sticker_view_url"):
        status["window_sticker_preview_url"] = sticker_embed_preview_url(
            car2,
            preview_api_url=_car_window_sticker_preview_url(car_id),
            has_paid_access=True,
        ) or _car_window_sticker_preview_url(car_id)
    status["listing_photo_detected_equipment"] = ctx.get("listing_photo_detected_equipment") or []
    status["packages_panel_has_content"] = bool(ctx.get("packages_panel_has_content"))
    return jsonify(status)


@app.route("/api/nearby-dealers")
def api_nearby_dealers():
    """Return dealerships within radius of a ZIP code (max 50 mi). Premium when billing enabled."""
    ok, err = _require_feature(FEATURE_NEARBY_DEALERS)
    if not ok:
        return jsonify(_feature_denied_json(FEATURE_NEARBY_DEALERS, err, dealers=[])), 403
    from backend.listings.nearby_dealers import resolve_nearby_dealers_for_listings

    zip_code = (request.args.get("zip_code") or "").strip()
    try:
        radius = min(float(request.args.get("radius") or 50), 50.0)
    except (ValueError, TypeError):
        radius = 50.0
    if not zip_code:
        return jsonify({"ok": False, "dealers": []})
    q = (request.args.get("q") or request.args.get("search") or "").strip()
    payload = resolve_nearby_dealers_for_listings(
        zip_code=zip_code,
        radius_miles=radius,
        search_query=q or None,
    )
    return jsonify(payload)


@app.route("/find-dealers")
def find_dealers_page():
    from backend.db.inventory_pg import is_inventory_postgres
    from backend.utils.roles import is_admin_role

    return render_template(
        "find_dealers.html",
        site_admin_onboard=is_admin_role(session.get("user_role")),
        dealer_onboard_enabled=is_inventory_postgres(),
    )


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


@app.route("/api/nhtsa-recalls", methods=["GET"])
def api_nhtsa_recalls_lookup():
    """JSON NHTSA recall lookup for inline VDP (same inputs as /nhtsa-recalls)."""
    payload, status = _nhtsa_recalls_lookup_payload(
        vin_raw=(request.args.get("vin") or "").strip(),
        make=request.args.get("make"),
        model=request.args.get("model"),
        year=request.args.get("year") or request.args.get("modelYear"),
        rate_key=f"nhtsa-recalls-api:{_client_ip()}",
    )
    return jsonify(payload), status


@app.route("/nhtsa-recalls")
def nhtsa_recalls_lookup():
    """VIN recall lookup using NHTSA public API (auto-runs on page load)."""
    vin_raw = (request.args.get("vin") or "").strip()
    payload, status = _nhtsa_recalls_lookup_payload(
        vin_raw=vin_raw,
        make=request.args.get("make"),
        model=request.args.get("model"),
        year=request.args.get("year") or request.args.get("modelYear"),
        rate_key=f"nhtsa-recalls:{_client_ip()}",
    )
    return render_template(
        "nhtsa_recalls.html",
        vin=payload.get("vin"),
        vin_raw=vin_raw,
        recalls=payload.get("recalls") or [],
        lookup_error=payload.get("error"),
        rate_limited=payload.get("error") == "rate_limited" or status == 429,
        vehicle_label=payload.get("vehicle_label"),
    )


@app.route("/api/dealer-locator")
def api_dealer_locator():
    """Nearby car dealers: registry + optional Google Places (server-side)."""
    ip = _client_ip()
    if not allow_request(
        f"dealer-locator:{ip}",
        max_events=_DEALER_LOCATOR_RPM,
        window_seconds=60.0,
    ):
        return jsonify({"ok": False, "error": "rate_limited", "dealers": []}), 429

    from backend.listings.dealer_locator import find_nearby_dealers

    lat = lon = None
    try:
        if request.args.get("lat") not in (None, "") and request.args.get("lon") not in (None, ""):
            lat = float(request.args.get("lat"))
            lon = float(request.args.get("lon"))
    except (TypeError, ValueError):
        lat = lon = None

    zip_code = (request.args.get("zip") or request.args.get("zip_code") or "").strip()
    city = (request.args.get("city") or "").strip()
    state = (request.args.get("state") or "").strip()
    try:
        radius = min(max(float(request.args.get("radius") or 25), 1.0), 50.0)
    except (TypeError, ValueError):
        radius = 25.0
    include_google = (request.args.get("include_google") or "1").strip().lower() not in (
        "0",
        "false",
        "no",
    )

    google_key = (os.environ.get("GOOGLE_MAPS_API_KEY") or "").strip()
    require_login = (os.environ.get("DEALER_LOCATOR_REQUIRE_LOGIN") or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )
    if (
        require_login
        and is_production_env()
        and include_google
        and google_key
        and not session.get("user_id")
    ):
        return jsonify({"ok": False, "error": "login_required", "dealers": []}), 403

    payload = find_nearby_dealers(
        lat=lat,
        lon=lon,
        zip_code=zip_code or None,
        city=city or None,
        state=state or None,
        radius_miles=radius,
        include_google=include_google,
    )
    status = 200 if payload.get("ok") else 400
    return jsonify(payload), status


@app.route("/api/admin/dealer-onboard", methods=["POST"])
def api_admin_dealer_onboard():
    """Site admin: queue Smart Import onboard from Find dealers (locator → scrape bridge)."""
    if not is_admin_role(session.get("user_role")):
        return jsonify({"ok": False, "error": "forbidden"}), 403

    ip = _client_ip()
    if not allow_request(f"dealer-onboard:{ip}", max_events=30, window_seconds=3600.0):
        return jsonify({"ok": False, "error": "rate_limited"}), 429

    body = request.get_json(silent=True) or {}
    from backend.dealer.admin.onboard_api import request_dealer_onboard

    ok, err, data = request_dealer_onboard(
        url=(body.get("url") or "").strip(),
        dealer_id=(body.get("dealer_id") or "").strip() or None,
        name=(body.get("name") or "").strip() or None,
    )
    if not ok:
        status = 503 if err == "postgres_required" else 400
        if err == "forbidden":
            status = 403
        return jsonify({"ok": False, "error": err}), status
    return jsonify({"ok": True, **data})


def _serialize_dealer_job_row(row: dict) -> dict:
    import json as _json

    out = dict(row)
    for key in ("payload_json", "result_json"):
        raw = out.pop(key, None)
        parsed: dict = {}
        if raw:
            try:
                parsed = _json.loads(raw) if isinstance(raw, str) else (raw or {})
            except (_json.JSONDecodeError, TypeError, ValueError):
                parsed = {}
        out["payload" if key == "payload_json" else "result"] = parsed
    result = out.get("result") or {}
    if isinstance(result, dict) and isinstance(result.get("ai_diagnosis"), dict):
        out["ai_diagnosis"] = result["ai_diagnosis"]
    if isinstance(result, dict) and isinstance(result.get("scrape_confidence"), dict):
        out["scrape_confidence"] = result["scrape_confidence"]
    from backend.scanner.scrape_confidence import job_display_status

    out["display_status"] = job_display_status(
        str(out.get("status") or ""),
        str(out.get("job_type") or ""),
        result if isinstance(result, dict) else {},
    )
    return out


@app.route("/api/admin/dealer-jobs")
def api_admin_dealer_jobs():
    """Site admin: poll dealer_jobs for live onboarding progress."""
    if not is_admin_role(session.get("user_role")):
        return jsonify({"ok": False, "error": "forbidden"}), 403

    from backend.db.inventory_pg import is_inventory_postgres
    from backend.scanner.job_queue import list_dealer_catalog, list_recent_jobs

    if not is_inventory_postgres():
        return jsonify({"ok": False, "error": "postgres_required", "jobs": [], "catalog": []}), 503

    try:
        limit = min(max(int(request.args.get("limit") or 30), 1), 100)
    except (TypeError, ValueError):
        limit = 30

    jobs = [_serialize_dealer_job_row(j) for j in list_recent_jobs(limit=limit)]
    catalog = list_dealer_catalog(limit=50)
    active = sum(1 for j in jobs if (j.get("status") or "") in ("queued", "running"))
    return jsonify(
        {
            "ok": True,
            "jobs": jobs,
            "catalog": catalog,
            "active_count": active,
        }
    )


@app.route("/api/admin/dealer-jobs/<int:job_id>")
def api_admin_dealer_job_detail(job_id: int):
    """Site admin: full job detail for sidebar (payload, result log, error)."""
    if not is_admin_role(session.get("user_role")):
        return jsonify({"ok": False, "error": "forbidden"}), 403

    from backend.db.inventory_pg import is_inventory_postgres
    from backend.scanner.job_queue import get_job

    if not is_inventory_postgres():
        return jsonify({"ok": False, "error": "postgres_required"}), 503

    row = get_job(job_id)
    if not row:
        return jsonify({"ok": False, "error": "not_found"}), 404
    return jsonify({"ok": True, "job": _serialize_dealer_job_row(row)})


@app.route("/api/admin/dealer-jobs/<int:job_id>/retry", methods=["POST"])
def api_admin_dealer_job_retry(job_id: int):
    """Site admin: re-queue a failed dealer job."""
    if not is_admin_role(session.get("user_role")):
        return jsonify({"ok": False, "error": "forbidden"}), 403

    ip = _client_ip()
    if not allow_request(f"dealer-job-retry:{ip}", max_events=60, window_seconds=3600.0):
        return jsonify({"ok": False, "error": "rate_limited"}), 429

    from backend.scanner.job_queue import retry_failed_job

    ok, err, data = retry_failed_job(job_id)
    if not ok:
        status = 503 if err == "postgres_required" else 400
        if err == "not_found":
            status = 404
        return jsonify({"ok": False, "error": err, **data}), status
    return jsonify({"ok": True, **data})


@app.route("/api/admin/dealer-jobs/<int:job_id>/diagnose", methods=["POST"])
def api_admin_dealer_job_diagnose(job_id: int):
    """Site admin: AI/rule diagnosis for a failed dealer job."""
    if not is_admin_role(session.get("user_role")):
        return jsonify({"ok": False, "error": "forbidden"}), 403

    from backend.db.inventory_pg import is_inventory_postgres
    from backend.scanner.job_queue import diagnose_job_row, get_job, _merge_result_diagnosis

    if not is_inventory_postgres():
        return jsonify({"ok": False, "error": "postgres_required"}), 503

    row = get_job(job_id)
    if not row:
        return jsonify({"ok": False, "error": "not_found"}), 404
    from backend.scanner.job_queue import _job_retry_eligible

    if not _job_retry_eligible(row)[0]:
        return jsonify({"ok": False, "error": "not_failed", "status": row.get("status")}), 400

    body = request.get_json(silent=True) or {}
    use_llm = body.get("use_llm", True) is not False
    diagnosis = diagnose_job_row(row, use_llm=use_llm)
    _merge_result_diagnosis(job_id, diagnosis)
    return jsonify({"ok": True, "job_id": job_id, "diagnosis": diagnosis})


@app.route("/api/admin/dealer-jobs/<int:job_id>/smart-retry", methods=["POST"])
def api_admin_dealer_job_smart_retry(job_id: int):
    """Site admin: diagnose then re-queue a failed job with guided retry hints."""
    if not is_admin_role(session.get("user_role")):
        return jsonify({"ok": False, "error": "forbidden"}), 403

    ip = _client_ip()
    if not allow_request(f"dealer-job-smart-retry:{ip}", max_events=30, window_seconds=3600.0):
        return jsonify({"ok": False, "error": "rate_limited"}), 429

    from backend.scanner.job_queue import smart_retry_failed_job

    body = request.get_json(silent=True) or {}
    use_llm = body.get("use_llm", True) is not False
    ok, err, data = smart_retry_failed_job(job_id, use_llm=use_llm)
    if not ok:
        status = 503 if err == "postgres_required" else 400
        if err == "not_found":
            status = 404
        return jsonify({"ok": False, "error": err, **data}), status
    return jsonify({"ok": True, **data})


@app.route("/api/saved-cars", methods=["GET"])
def api_saved_cars():
    """Saved inventory for the signed-in user (native clients)."""
    uid = session.get("user_id")
    if not uid:
        return jsonify({"ok": False, "error": "not_logged_in"}), 401
    saved_ids = get_saved_car_ids(int(uid))
    raw_saved = get_cars_by_ids(saved_ids)
    cars = [serialize_car_for_listings_grid(c) for c in raw_saved]
    return jsonify({"ok": True, "cars": cars})


@app.route("/api/cars/<int:car_id>/save", methods=["POST"])
def api_toggle_save(car_id):
    try:
        uid = session["user_id"]
    except KeyError:
        uid = None
    if not uid:
        return jsonify({"ok": False, "error": "not_logged_in"}), 401
    uid = int(uid)
    if not get_car_by_id(car_id, include_inactive=False):
        return jsonify({"ok": False, "error": "not_found"}), 404
    currently_saved = is_car_saved(uid, car_id)
    if currently_saved:
        unsave_car(uid, car_id)
    else:
        save_car(uid, car_id)
    return jsonify({"ok": True, "saved": not currently_saved})


@app.route("/listings")
def listings():
    return listings_page(listings_poll_ms=_listings_client_poll_ms())


@app.route("/premium")
def premium_page():
    from backend.billing.catalog import plan_display_list
    from backend.billing.entitlements import FEATURE_LABELS
    from backend.billing.stripe_billing import billing_enabled as stripe_billing_enabled

    welcome_source = session.pop("auth_welcome_source", None)
    embed = request.args.get("embed") in ("1", "true", "yes")
    return render_template(
        "premium.html",
        auth_welcome_source=welcome_source,
        premium_embed=embed,
        subscription_plans=plan_display_list(),
        billing_enabled=stripe_billing_enabled(),
        current_plan_id=(session.get("subscription_plan_id") or "").strip().lower() or None,
        feature_labels=FEATURE_LABELS,
    )


def _highlight_params_from_filters(filters: dict) -> list[str]:
    """UI filter control keys for styling (matches data-param / form names)."""
    keys = []
    for k in filters:
        if k == "exterior_color":
            keys.append("exterior_color")
        elif k == "drivetrain":
            keys.append("drivetrain")
        elif k == "body_style":
            keys.append("body_style")
        elif k == "max_price":
            keys.append("max_price")
        elif k == "max_mileage":
            keys.append("max_mileage")
        elif k in ("min_year", "max_year"):
            if "year" not in keys:
                keys.append("year")
        elif k in ("make", "model", "vehicle_or"):
            if "make" not in keys:
                keys.append("make")
            if "model" not in keys:
                keys.append("model")
        elif k == "interior_color":
            keys.append("interior_color")
        elif k in ("engine_displacement_l_min", "engine_displacement_l_max", "engine_l_min", "engine_l_max"):
            if "engine_l_min" not in keys:
                keys.append("engine_l_min")
            if "engine_l_max" not in keys:
                keys.append("engine_l_max")
        elif k in ("packages_json_contains", "packages_json_contains_list", "package_contains"):
            if "package" not in keys:
                keys.append("package")
    return keys


@app.route("/api/search/smart/parse", methods=["GET"])
def api_search_smart_parse():
    """Fast parse-only for listings instant preview (no DB search)."""
    ip = _client_ip()
    if not allow_request(f"smart:{ip}", max_events=_SMART_SEARCH_RPM, window_seconds=60.0):
        return jsonify({"ok": False, "error": "rate_limited"}), 429
    q = (request.args.get("query") or request.args.get("q") or "").strip()
    filters = parse_natural_query(q)
    return jsonify(
        {
            "ok": True,
            "filters": filters,
            "highlight": _highlight_params_from_filters(filters),
        }
    )


@app.route("/api/search/smart", methods=["POST"])
def api_search_smart():
    """Listings search bar: local ``parse_natural_query`` + SQL/pgvector only (no Claude)."""
    ip = _client_ip()
    if not allow_request(f"smart:{ip}", max_events=_SMART_SEARCH_RPM, window_seconds=60.0):
        return jsonify({"ok": False, "error": "rate_limited"}), 429

    if request.content_length is not None and request.content_length > _CHAT_MAX_BODY:
        return jsonify({"ok": False, "error": "payload_too_large"}), 413

    data = request.get_json() or {}
    q = (data.get("query") or data.get("q") or "").strip()
    filters = parse_natural_query(q)
    from backend.utils.hybrid_search import hybrid_smart_search

    geo_kw = {}
    zc = str(data.get("zip_code") or data.get("zip") or "").strip()
    rad_raw = data.get("radius")
    if zc and rad_raw is not None and str(rad_raw).strip() != "":
        try:
            rm = float(rad_raw)
            if rm > 0:
                geo_kw = {"zip_code": zc, "radius_miles": rm}
        except (TypeError, ValueError):
            pass

    from backend.utils.hybrid_search import NO_PARSE_MATCH_MESSAGE, public_search_meta

    results, search_meta = hybrid_smart_search(
        q, filters, vector_top_k=50, listing_geo_kwargs=geo_kw if geo_kw else None
    )
    safe_results = [serialize_car_for_listings_grid(c) for c in results]
    try:
        from backend.db.search_analytics_db import analytics_session_key, record_search_event

        uid_raw = session.get("user_id")
        user_id = int(uid_raw) if uid_raw is not None else None
        record_search_event(
            source="smart_search",
            query_text=q or None,
            filters={**filters, **geo_kw},
            result_count=len(safe_results),
            user_id=user_id,
            session_key=analytics_session_key(session),
            geo_zip=geo_kw.get("zip_code") if geo_kw else None,
        )
    except Exception:
        pass
    empty_message = None
    if not safe_results and search_meta.get("mode") == "no_parse_match":
        empty_message = NO_PARSE_MATCH_MESSAGE
    return jsonify(
        {
            "ok": True,
            "filters": filters,
            "results": safe_results,
            "highlight": _highlight_params_from_filters(filters),
            "search_meta": public_search_meta(search_meta),
            "empty_message": empty_message,
        }
    )


@app.route("/api/car/<int:car_id>/chat", methods=["POST"])
def api_car_chat(car_id: int):
    ok, err = _require_feature(FEATURE_AI_CAR_CHAT)
    if not ok:
        return jsonify(_feature_denied_json(FEATURE_AI_CAR_CHAT, err)), 403

    ip = _client_ip()
    rpm_pair, rpm_ip, rpm_global = car_chat_rate_limits()

    if rpm_global > 0 and not allow_request(
        "chat:global",
        max_events=rpm_global,
        window_seconds=60.0,
    ):
        return jsonify({"ok": False, "error": "rate_limited"}), 429

    if not allow_request(f"chat:ip:{ip}", max_events=rpm_ip, window_seconds=60.0):
        return jsonify({"ok": False, "error": "rate_limited"}), 429

    if not allow_request(f"chat:{ip}:{car_id}", max_events=rpm_pair, window_seconds=60.0):
        return jsonify({"ok": False, "error": "rate_limited"}), 429

    daily_limit = car_chat_user_daily_limit()
    if daily_limit > 0:
        uid = session.get("user_id")
        if uid:
            daily_key = f"chat:daily:user:{int(uid)}"
        else:
            daily_key = f"chat:daily:ip:{ip}"
        if not allow_request(
            daily_key,
            max_events=daily_limit,
            window_seconds=86400.0,
        ):
            return jsonify({"ok": False, "error": "user_chat_limit_reached"}), 429

    if request.content_length is not None and request.content_length > _CHAT_MAX_BODY:
        return jsonify({"ok": False, "error": "payload_too_large"}), 413

    car_raw = get_car_by_id(car_id, include_inactive=False)
    if not car_raw:
        return jsonify({"ok": False, "error": "not_found"}), 404

    # Enrich car dict with dealer location from dealerships table for map link
    car_dict = dict(car_raw)
    try:
        reg_id = car_dict.get("dealership_registry_id")
        if reg_id:
            from backend.db.inventory_db import get_conn
            with get_conn() as _conn:
                _row = _conn.execute(
                    "SELECT street_address, city, state, zip_code, latitude, longitude FROM dealerships WHERE id=?",
                    (reg_id,)
                ).fetchone()
                if _row:
                    addr_parts = [p for p in [_row["street_address"], _row["city"], _row["state"], _row["zip_code"]] if p]
                    car_dict["dealer_address"] = ", ".join(addr_parts) or None
                    car_dict["dealer_lat"] = _row["latitude"]
                    car_dict["dealer_lon"] = _row["longitude"]
    except Exception:
        pass

    body = request.get_json() or {}

    message = (body.get("message") or body.get("q") or "").strip()
    if not message:
        return jsonify({"ok": False, "error": "message_required"}), 400
    if len(message) > _CHAT_MAX_MESSAGE:
        return jsonify({"ok": False, "error": "message_too_long"}), 400

    allow_playwright = web_research_playwright_allowed(session.get("user_id"))
    out = run_car_page_chat(car_dict, message, allow_web_research=allow_playwright)
    err = out.get("error")
    return jsonify(
        {
            "ok": err is None,
            "reply": out.get("reply") or "",
            "error": err,
        }
    )


@app.route("/api/compare/chat", methods=["POST"])
def api_compare_chat():
    """Premium compare assistant — side-by-side listing Q&A (up to 4 cars)."""
    ok, err = _require_feature(FEATURE_AI_COMPARE_CHAT)
    if not ok:
        return jsonify(_feature_denied_json(FEATURE_AI_COMPARE_CHAT, err)), 403

    ip = _client_ip()
    rpm_pair, rpm_ip, rpm_global = car_chat_rate_limits()

    if rpm_global > 0 and not allow_request(
        "chat:global",
        max_events=rpm_global,
        window_seconds=60.0,
    ):
        return jsonify({"ok": False, "error": "rate_limited"}), 429

    if not allow_request(f"chat:ip:{ip}", max_events=rpm_ip, window_seconds=60.0):
        return jsonify({"ok": False, "error": "rate_limited"}), 429

    if not allow_request(f"chat:compare:ip:{ip}", max_events=rpm_pair, window_seconds=60.0):
        return jsonify({"ok": False, "error": "rate_limited"}), 429

    daily_limit = car_chat_user_daily_limit()
    if daily_limit > 0:
        uid = session.get("user_id")
        if uid:
            daily_key = f"chat:daily:user:{int(uid)}"
        else:
            daily_key = f"chat:daily:ip:{ip}"
        if not allow_request(
            daily_key,
            max_events=daily_limit,
            window_seconds=86400.0,
        ):
            return jsonify({"ok": False, "error": "user_chat_limit_reached"}), 429

    if request.content_length is not None and request.content_length > _CHAT_MAX_BODY:
        return jsonify({"ok": False, "error": "payload_too_large"}), 413

    body = request.get_json(silent=True) or {}
    if not isinstance(body, dict):
        return jsonify({"ok": False, "error": "json_object"}), 400

    from backend.utils.compare_specs import parse_compare_car_ids

    raw_ids = body.get("car_ids") or body.get("ids") or []
    if isinstance(raw_ids, str):
        id_list = parse_compare_car_ids(raw_ids)
    elif isinstance(raw_ids, list):
        id_list = parse_compare_car_ids(",".join(str(x) for x in raw_ids))
    else:
        id_list = []

    if not id_list:
        return jsonify({"ok": False, "error": "car_ids_required"}), 400

    message = (body.get("message") or body.get("q") or "").strip()
    if not message:
        return jsonify({"ok": False, "error": "message_required"}), 400
    if len(message) > _CHAT_MAX_MESSAGE:
        return jsonify({"ok": False, "error": "message_too_long"}), 400

    raw_cars = get_cars_by_ids(id_list)
    if not raw_cars:
        return jsonify({"ok": False, "error": "not_found"}), 404

    allow_playwright = web_research_playwright_allowed(session.get("user_id"))
    out = run_compare_chat(raw_cars, message, allow_web_research=allow_playwright)
    err = out.get("error")
    return jsonify(
        {
            "ok": err is None,
            "reply": out.get("reply") or "",
            "error": err,
        }
    )


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
