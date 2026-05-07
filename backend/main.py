"""Sarrafi Collection — Flask web application."""

from backend.utils.project_env import load_project_dotenv

load_project_dotenv()

import inspect
import logging
import os
import secrets
import sqlite3
import time
from pathlib import Path

from flask import Flask, abort, g, jsonify, redirect, render_template, request, send_from_directory, session, url_for

from backend.intelligence.ai.agent import run_car_page_chat
from backend.billing.routes import bp as billing_bp
from backend.dealer.admin import store_admin_bp
from backend.dealer.routes import bp as dealer_portal_bp
from backend.auth.mfa import bp as mfa_qr_bp
from backend.auth.mfa import register_mfa_qr_socketio
from backend.dev.console import register_dev_console
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
    listings_grid_serialized_cars,
    save_car,
    search_cars,
    serialize_car_for_listings_grid,
    unsave_car,
)
from backend.db.user_history_db import get_recent_viewed_car_ids, record_car_view
from backend.db.users_db import (
    check_user,
    get_user_by_login,
    get_user_profile,
    get_user_totp,
    init_users_db,
    save_user,
    set_user_totp,
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
from backend.utils.listing_completeness import INCOMPLETE_FIELD_LABELS, listing_missing_field_codes
from backend.utils.oem_links import mopar_vin_lookup_url
from backend.utils.client_ip import client_ip as _client_ip_from_request
from backend.utils.csrf import ensure_csrf_token, validate_csrf_form, validate_csrf_header

if not (validate_csrf_header.__code__.co_flags & inspect.CO_VARARGS):
    raise ImportError(
        "backend.utils.csrf.validate_csrf_header must be defined with *args (see repo csrf.py). "
        "Restart the server after git pull; check PYTHONPATH is not shadowing backend/utils/csrf.py."
    )
from backend.utils.ip_rate_limit import allow_request
from backend.utils.query_parser import parse_natural_query
from backend.utils.registration_validation import registration_form_error
from backend.utils.mfa_action_log import log_mfa_action
from backend.utils.mfa_delivery import send_email_code
from backend.utils.mfa_otp import clear_session_otp, issue_session_otp, verify_session_otp
from backend.utils.mfa_qr_store import mfa_qr_channel_available, mfa_qr_create_attempt
from backend.utils.totp import new_base32_secret, otpauth_uri, verify_totp
from backend.utils.runtime_env import is_production_env, session_cookie_secure_default
from backend.utils.roles import (
    MFA_INTENT_DEALER,
    MFA_INTENT_GENERAL,
    ROLE_ADMIN,
    ROLE_DEALERSHIP_OWNER,
    ROLE_GENERAL,
    email_is_admin,
    is_admin_role,
    normalize_role,
    username_is_admin,
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
_CHAT_RPM = int(os.environ.get("RATE_LIMIT_CAR_CHAT_PER_MIN", "40"))
_LOGIN_RPM = int(os.environ.get("RATE_LIMIT_LOGIN_PER_MIN", "30"))
_REGISTER_RPM = int(os.environ.get("RATE_LIMIT_REGISTER_PER_MIN", "10"))
_MFA_VERIFY_RPM = int(os.environ.get("RATE_LIMIT_MFA_VERIFY_PER_MIN", "20"))
_MFA_TOTP_ENROLL_RPM = int(os.environ.get("RATE_LIMIT_MFA_TOTP_ENROLL_PER_MIN", "10"))
_MFA_ISSUER = (os.environ.get("MFA_ISSUER") or "Sarrafi Collection").strip() or "Sarrafi Collection"


def _client_ip() -> str:
    return _client_ip_from_request(request)


def _session_belongs_to_paid_org() -> bool:
    """Stripe subscription (when enabled) applies only to users tied to a dealership org."""
    oid = session.get("org_id")
    if oid is None:
        return False
    try:
        return int(oid) > 0
    except (TypeError, ValueError):
        return False


def _mfa_landing_url() -> str:
    if (session.get("mfa_intent") or MFA_INTENT_GENERAL) == MFA_INTENT_DEALER:
        return url_for("dealer_portal.dealer_login")
    return url_for("login_page")


def _post_mfa_success_redirect():
    intent = (session.get("mfa_intent") or MFA_INTENT_GENERAL).strip().lower()
    if intent == MFA_INTENT_DEALER:
        session.pop("mfa_intent", None)
        if (
            _billing_enabled()
            and (not is_admin_role(session.get("user_role")))
            and _session_belongs_to_paid_org()
            and (not _require_paid_org_session())
        ):
            return redirect(url_for("billing.billing_required"))
        return redirect(url_for("dealer_portal.inventory_dashboard"))
    session.pop("mfa_intent", None)
    if (
        _billing_enabled()
        and (not is_admin_role(session.get("user_role")))
        and _session_belongs_to_paid_org()
        and (not _require_paid_org_session())
    ):
        return redirect(url_for("billing.billing_required"))
    return redirect(url_for("listings"))


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
    app.secret_key = _raw_secret or "dealership-scanner-dev-insecure"

app.config["MAX_CONTENT_LENGTH"] = _MAX_REQUEST_BODY

app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.config["SESSION_COOKIE_SECURE"] = session_cookie_secure_default()

init_users_db()
init_admin_db()
init_inventory_db()
init_dealer_portal_db()
app.register_blueprint(dev_bp, url_prefix="/dev")
app.register_blueprint(store_admin_bp)
app.register_blueprint(dealer_portal_bp)
app.register_blueprint(billing_bp)
app.register_blueprint(mfa_qr_bp)
register_dev_console(app)


@app.context_processor
def inject_csrf_and_flags():
    role = (session.get("user_role") or "").strip().lower()
    has_scope = bool(
        (session.get("user_dealer_id") or "").strip() or (session.get("user_dealership_registry_id") or "").strip()
    )
    nav_store_admin = bool(session.get("user_id")) and (role == "admin" or has_scope)
    static_ver = "1"
    try:
        static_ver = str(int(Path(app.static_folder).resolve().joinpath("style.css").stat().st_mtime))
    except OSError:
        pass
    return {
        "csrf_token": ensure_csrf_token(),
        "csp_nonce": getattr(g, "csp_nonce", "") or "",
        "is_production": is_production_env(),
        "logged_in_user": session.get("username"),
        "nav_store_admin": nav_store_admin,
        "static_cache_ver": static_ver,
    }


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
        "mfa_choose",
        "mfa_setup",
        "mfa_verify",
        "mfa_qr.mfa_qr_complete",
        "dev.admin_login",
        "dev.admin_register",
        "dev.admin_logout",
    ):
        validate_csrf_form()
    elif ep and str(ep).startswith("dealer_portal."):
        validate_csrf_form()
    elif ep and str(ep).startswith("store_admin."):
        validate_csrf_form()
    elif ep in ("api_search_smart", "api_car_chat", "api_toggle_save", "api_session_listings_geo"):
        validate_csrf_header()


@app.before_request
def _billing_gate_paid_routes():
    if not _billing_enabled():
        return None
    ep = request.endpoint or ""
    if not ep:
        return None
    if ep in ("login_page", "register_page", "logout_page", "favicon", "mfa_choose"):
        return None
    if str(ep).startswith("dev.") or str(ep).startswith("billing."):
        return None
    if not session.get("user_id"):
        return None
    if is_admin_role(session.get("user_role")):
        return None
    if not _session_belongs_to_paid_org():
        return None
    if ep == "dashboard" or str(ep).startswith("dealer_portal.") or str(ep).startswith("store_admin."):
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
        "img-src 'self' data: https: http: blob:; "
        "font-src 'self' https://fonts.gstatic.com data:; "
        "style-src 'self' https://fonts.googleapis.com 'unsafe-inline'; "
        f"script-src 'self' 'nonce-{nonce}' https://esm.sh; "
        "connect-src 'self' https://esm.sh https://fonts.googleapis.com; "
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
    "script-src 'self' https://esm.sh; "
    "connect-src 'self' https://esm.sh https://fonts.googleapis.com; "
    "worker-src 'self'; "
)


@app.after_request
def _csp_headers(response):
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


@app.route("/health")
def health():
    return jsonify({"status": "ok"}), 200


@app.route("/favicon.ico")
def favicon():
    return send_from_directory(app.static_folder, "favicon.svg", mimetype="image/svg+xml")


# Serve locally-downloaded car images (written by image_downloader.py).
# Stored under <project_root>/car_images/<dealer_id>/<vin>/<file>.
_CAR_IMAGES_DIR = Path(__file__).resolve().parent.parent / "car_images"


@app.route("/car-images/<path:filename>")
def serve_car_image(filename):
    return send_from_directory(str(_CAR_IMAGES_DIR), filename)


@app.route("/")
def home():
    if session.get("user_id"):
        return redirect("/dashboard")
    from datetime import datetime
    return render_template("landing.html", now=datetime.utcnow())


@app.route("/login", methods=["GET", "POST"])
def login_page():
    if request.method == "POST":
        ip = _client_ip()
        if not allow_request(f"login:{ip}", max_events=_LOGIN_RPM, window_seconds=60.0):
            return render_template("login.html", error="Too many login attempts. Try again in a minute."), 429
        login_input = (request.form.get("login") or "").strip()
        password = (request.form.get("password") or "").strip()
        if not login_input or not password:
            return render_template("login.html", error="Enter username/email and password.")
        if check_user(login_input, password):
            u = get_user_by_login(login_input)
            if not u:
                return render_template("login.html", error="Invalid username/email or password.")
            sync_env_admin_user_row(int(u["id"]))
            u = get_user_by_login(login_input)
            if not u:
                return render_template("login.html", error="Invalid username/email or password.")
            if is_admin_role(normalize_role(u.get("role"))):
                session.clear()
                session["mfa_intent"] = MFA_INTENT_GENERAL
                if _finalize_app_session(int(u["id"])):
                    log_mfa_action(
                        event="login.session",
                        surface="app",
                        fields={"user_id": int(u["id"]), "mfa_skipped": True, "reason": "app_admin", "client_ip": _client_ip()},
                    )
                    return _post_mfa_success_redirect()
            session.clear()
            session["mfa_intent"] = MFA_INTENT_GENERAL
            session["mfa_ok"] = False
            session["mfa_pending_user_id"] = int(u["id"])
            session["mfa_pending_login"] = (u.get("email") or u.get("username") or "").strip()
            clear_session_otp(session, kind="mfa")
            totp_row = get_user_totp(int(u["id"])) or {}
            if totp_row.get("enabled") and (totp_row.get("secret") or "").strip():
                session["mfa_pending_method"] = "totp"
                log_mfa_action(
                    event="login.mfa_start",
                    surface="app",
                    fields={
                        "pending_user_id": int(u["id"]),
                        "next": "mfa_verify",
                        "mfa_method": "totp",
                        "client_ip": _client_ip(),
                    },
                )
                return redirect(url_for("mfa_verify"))
            session["mfa_pending_method"] = "choose"
            log_mfa_action(
                event="login.mfa_start",
                surface="app",
                fields={
                    "pending_user_id": int(u["id"]),
                    "next": "mfa_choose",
                    "mfa_method": "choose",
                    "client_ip": _client_ip(),
                },
            )
            return redirect(url_for("mfa_choose"))
        return render_template("login.html", error="Invalid username/email or password.")
    return render_template("login.html")


@app.route("/register", methods=["GET", "POST"])
def register_page():
    if request.method == "POST":
        ip = _client_ip()
        if not allow_request(f"register:{ip}", max_events=_REGISTER_RPM, window_seconds=60.0):
            return render_template("register.html", error="Too many registration attempts. Try again later."), 429
        username = request.form.get("username", "")
        email = request.form.get("email", "")
        password = request.form.get("password", "")
        err = registration_form_error(
            username, email, password, min_password_len=_MIN_PASSWORD_LEN
        )
        if err:
            return render_template("register.html", error=err)
        is_admin = email_is_admin(email) or username_is_admin(username)
        if is_admin:
            role = ROLE_ADMIN
            org_id = None
        else:
            role = ROLE_GENERAL
            org_id = None
        try:
            uid = save_user(username.strip(), email.strip(), password, role=role, org_id=org_id)
        except sqlite3.IntegrityError:
            return render_template(
                "register.html",
                error="That username or email is already registered.",
            )
        if is_admin:
            session.clear()
            session["mfa_intent"] = MFA_INTENT_GENERAL
            if _finalize_app_session(int(uid)):
                log_mfa_action(
                    event="register.session",
                    surface="app",
                    fields={"user_id": int(uid), "mfa_skipped": True, "reason": "app_admin", "client_ip": _client_ip()},
                )
                return _post_mfa_success_redirect()
        session.clear()
        session["mfa_intent"] = MFA_INTENT_GENERAL
        session["mfa_ok"] = False
        session["mfa_pending_user_id"] = int(uid)
        session["mfa_pending_login"] = email.strip() or username.strip()
        clear_session_otp(session, kind="mfa")
        session["mfa_pending_method"] = "choose"
        log_mfa_action(
            event="register.mfa_start",
            surface="app",
            fields={
                "pending_user_id": int(uid),
                "next": "mfa_choose",
                "client_ip": _client_ip(),
            },
        )
        return redirect(url_for("mfa_choose"))
    return render_template("register.html")


@app.route("/logout", methods=["POST"])
def logout_page():
    session.clear()
    return redirect(url_for("login_page"))


def _require_mfa_pending_user() -> int:
    uid = session.get("mfa_pending_user_id")
    try:
        uid_i = int(uid)
    except (TypeError, ValueError):
        return 0
    return uid_i if uid_i > 0 else 0


def _mfa_has_active_email_otp() -> bool:
    try:
        exp = int(session.get("mfa_otp_exp") or 0)
    except (TypeError, ValueError):
        return False
    if exp <= int(time.time()):
        return False
    return bool(session.get("mfa_otp_hash"))


def _mfa_email_ready_for_verify() -> bool:
    m = (session.get("mfa_pending_method") or "").strip().lower()
    if m == "email":
        return _mfa_has_active_email_otp()
    return False


def _mfa_dev_show_code_hint() -> bool:
    if is_production_env():
        return False
    v = (os.environ.get("MFA_DEV_UI_CODE") or "1").strip().lower()
    if v in ("0", "false", "no", "off"):
        return False
    if (os.environ.get("MFA_DELIVERY_MODE") or "smtp").strip().lower() not in (
        "log",
        "test",
    ):
        return False
    return bool(session.get("mfa_test_last_code"))


@app.route("/mfa/choose", methods=["GET", "POST"])
def mfa_choose():
    """Pick email or TOTP; then issue OTP and send (or go to TOTP verify)."""
    uid = _require_mfa_pending_user()
    if not uid:
        return redirect(_mfa_landing_url())
    u = get_user_profile(uid) or {}
    totp_row = get_user_totp(uid) or {}
    has_totp = bool(totp_row.get("enabled") and (totp_row.get("secret") or "").strip())
    email_to = (u.get("email") or "").strip()
    can_email = bool(email_to and "@" in email_to)
    can_qr = bool(mfa_qr_channel_available())
    if not can_email and not has_totp and not can_qr:
        return render_template(
            "mfa_choose.html",
            error="This account has no email on file and no authenticator app. You cannot complete sign-in. Add an email to your account or contact support.",
            can_email=False,
            email_to="",
            has_totp=False,
            can_qr=False,
        )

    if (request.args.get("change") or "").strip().lower() in ("1", "true", "yes", "on"):
        clear_session_otp(session, kind="mfa")
        session["mfa_pending_method"] = "choose"
        session.pop("mfa_qr_attempt_id", None)
        log_mfa_action(
            event="mfa_choose.restart",
            surface="app",
            fields={"pending_user_id": uid, "client_ip": _client_ip(), "change_channel": "1"},
        )

    m = (session.get("mfa_pending_method") or "").strip().lower()
    if m == "totp":
        log_mfa_action(
            event="mfa_choose.skip_to_verify",
            surface="app",
            fields={"pending_user_id": uid, "mfa_method": "totp"},
        )
        return redirect(url_for("mfa_verify"))
    if m == "qr" and mfa_qr_channel_available() and (session.get("mfa_qr_attempt_id") or "").strip():
        return redirect(url_for("mfa_qr.mfa_qr_wait"))
    if m == "email" and _mfa_email_ready_for_verify():
        log_mfa_action(
            event="mfa_choose.skip_to_verify",
            surface="app",
            fields={"pending_user_id": uid, "mfa_method": m, "has_otp": True},
        )
        return redirect(url_for("mfa_verify"))

    if request.method == "POST":
        ip = _client_ip()
        if not allow_request(
            f"mfa_choose:{ip}", max_events=_MFA_VERIFY_RPM, window_seconds=60.0
        ):
            log_mfa_action(
                event="mfa_choose.rate_limited",
                surface="app",
                fields={"pending_user_id": uid, "client_ip": ip},
            )
            return (
                render_template(
                    "mfa_choose.html",
                    error="Too many attempts. Try again in a minute.",
                    can_email=can_email,
                    email_to=email_to,
                    has_totp=has_totp,
                    can_qr=can_qr,
                ),
                429,
            )
        channel = (request.form.get("channel") or "").strip().lower()
        dmode = (os.environ.get("MFA_DELIVERY_MODE") or "smtp").strip().lower()
        log_mfa_action(
            event="mfa_choose.post",
            surface="app",
            fields={
                "pending_user_id": uid,
                "channel": channel or "(empty)",
                "delivery_mode": dmode,
                "dev_ui_code_enabled": dmode in ("log", "test"),
                "client_ip": ip,
            },
        )
        if channel == "totp":
            if not has_totp:
                log_mfa_action(
                    event="mfa_choose.totp_rejected",
                    surface="app",
                    fields={"pending_user_id": uid, "reason": "not_enrolled"},
                )
                return render_template(
                    "mfa_choose.html",
                    error="Authenticator is not set up for this account.",
                    can_email=can_email,
                    email_to=email_to,
                    has_totp=has_totp,
                    can_qr=can_qr,
                )
            session["mfa_pending_method"] = "totp"
            log_mfa_action(
                event="mfa_choose.totp_selected",
                surface="app",
                fields={"pending_user_id": uid},
            )
            return redirect(url_for("mfa_verify"))
        if channel == "qr":
            if not mfa_qr_channel_available():
                return render_template(
                    "mfa_choose.html",
                    error="Phone QR sign-in is not available (set REDIS_URL in production, or use another method).",
                    can_email=can_email,
                    email_to=email_to,
                    has_totp=has_totp,
                    can_qr=can_qr,
                )
            mfa_intent = (session.get("mfa_intent") or MFA_INTENT_GENERAL).strip() or MFA_INTENT_GENERAL
            ac = mfa_qr_create_attempt(
                user_id=int(uid), mfa_intent=mfa_intent, stream="app"
            )
            if not ac:
                return render_template(
                    "mfa_choose.html",
                    error="Could not start phone QR sign-in. Try again or use email or an authenticator app.",
                    can_email=can_email,
                    email_to=email_to,
                    has_totp=has_totp,
                    can_qr=can_qr,
                )
            clear_session_otp(session, kind="mfa")
            session.pop("mfa_test_last_code", None)
            session["mfa_pending_method"] = "qr"
            session["mfa_qr_attempt_id"] = ac
            log_mfa_action(
                event="mfa_choose.qr_channel",
                surface="app",
                fields={"pending_user_id": uid, "client_ip": ip},
            )
            return redirect(url_for("mfa_qr.mfa_qr_wait"))
        if channel == "email":
            if not can_email:
                log_mfa_action(
                    event="mfa_choose.email_rejected",
                    surface="app",
                    fields={"pending_user_id": uid, "reason": "no_email_on_file"},
                )
                return render_template(
                    "mfa_choose.html",
                    error="This account has no email on file. Set up an authenticator app on this account first, or add an email.",
                    can_email=can_email,
                    email_to=email_to,
                    has_totp=has_totp,
                    can_qr=can_qr,
                )
            clear_session_otp(session, kind="mfa")
            code = issue_session_otp(session, kind="mfa", ttl_seconds=600)
            if dmode in ("test", "log"):
                session["mfa_test_last_code"] = code
            else:
                session.pop("mfa_test_last_code", None)
            log_mfa_action(
                event="mfa_choose.otp_issued",
                surface="app",
                fields={"pending_user_id": uid, "channel": "email", "mfa_test_last_set": dmode in ("log", "test")},
            )
            try:
                send_email_code(to_email=email_to, code=code, mfa_log_surface="app")
            except (RuntimeError, OSError) as e:
                clear_session_otp(session, kind="mfa")
                log_mfa_action(
                    event="mfa_choose.send_email_failed",
                    surface="app",
                    fields={"pending_user_id": uid, "error": (str(e) or "?")[:500], "err_type": type(e).__name__},
                )
                return render_template(
                    "mfa_choose.html",
                    error=(str(e) or "Could not send email. Check RESEND_API_KEY / RESEND_FROM or SMTP settings."),
                    can_email=can_email,
                    email_to=email_to,
                    has_totp=has_totp,
                    can_qr=can_qr,
                )
            except Exception as e:  # noqa: BLE001
                clear_session_otp(session, kind="mfa")
                log_mfa_action(
                    event="mfa_choose.send_email_failed",
                    surface="app",
                    fields={"pending_user_id": uid, "error": (str(e) or "?")[:500], "err_type": type(e).__name__},
                )
                return render_template(
                    "mfa_choose.html",
                    error=(str(e) or "Could not send email."),
                    can_email=can_email,
                    email_to=email_to,
                    has_totp=has_totp,
                    can_qr=can_qr,
                )
            session["mfa_pending_method"] = "email"
            log_mfa_action(
                event="mfa_choose.redirect_verify",
                surface="app",
                fields={"pending_user_id": uid, "mfa_method": "email", "mfa_test_last_set": dmode in ("log", "test")},
            )
            return redirect(url_for("mfa_verify"))
        log_mfa_action(
            event="mfa_choose.bad_channel",
            surface="app",
            fields={"pending_user_id": uid, "channel": channel or ""},
        )
        return render_template(
            "mfa_choose.html",
            error="Choose how to sign in.",
            can_email=can_email,
            email_to=email_to,
            has_totp=has_totp,
            can_qr=can_qr,
        )

    log_mfa_action(
        event="mfa_choose.get",
        surface="app",
        fields={
            "pending_user_id": uid,
            "can_email": can_email,
            "has_totp": has_totp,
            "can_qr": can_qr,
            "mfa_pending_method": m,
            "client_ip": _client_ip(),
        },
    )
    return render_template(
        "mfa_choose.html",
        can_email=can_email,
        email_to=email_to,
        has_totp=has_totp,
        can_qr=can_qr,
    )


def _finalize_app_session(user_id: int) -> bool:
    from backend.db.users_db import get_org

    try:
        uid = int(user_id)
    except (TypeError, ValueError):
        return False
    u = get_user_profile(uid)
    if not u:
        return False
    session["user_id"] = int(u["id"])
    session["username"] = u["username"]
    session["user_email"] = (u.get("email") or "").strip()
    session["user_role"] = normalize_role(u.get("role"))
    session["user_dealer_id"] = (u.get("dealer_id") or "").strip()
    rid = u.get("dealership_registry_id")
    session["user_dealership_registry_id"] = str(int(rid)) if rid is not None else ""
    session["org_id"] = int(u.get("org_id") or 0) if u.get("org_id") else 0
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
    session["mfa_ok"] = True
    session.pop("mfa_pending_user_id", None)
    session.pop("mfa_pending_login", None)
    session.pop("mfa_qr_attempt_id", None)
    session.pop("mfa_totp_setup_secret", None)
    session.pop("mfa_totp_setup_otpauth", None)
    return True


def _mfa_setup_totp_page(
    *, method: str, error: str | None = None, status: int | None = None
):
    """Build optional TOTP enrollment page; keeps the same secret in-session until confirm or explicit regenerate."""
    pending_login = (session.get("mfa_pending_login") or "").strip()
    want_regen = (request.args.get("regenerate") or "").strip().lower() in ("1", "true", "yes", "on")
    if (not session.get("mfa_totp_setup_secret")) or want_regen:
        sec = new_base32_secret()
        session["mfa_totp_setup_secret"] = sec
        session["mfa_totp_setup_otpauth"] = otpauth_uri(
            secret=sec, account_name=pending_login or "user", issuer=_MFA_ISSUER
        )
    secret = (session.get("mfa_totp_setup_secret") or "").strip()
    otpauth = (session.get("mfa_totp_setup_otpauth") or "").strip()
    tpl = (
        "mfa_setup.html",
        {
            "error": error,
            "method": method,
            "issuer": _MFA_ISSUER,
            "account_name": pending_login,
            "secret": secret,
            "otpauth": otpauth,
        },
    )
    if status and status >= 400:
        return render_template(tpl[0], **tpl[1]), status
    return render_template(tpl[0], **tpl[1])


@app.route("/mfa/setup", methods=["GET", "POST"])
def mfa_setup():
    uid = _require_mfa_pending_user()
    if not uid:
        return redirect(_mfa_landing_url())
    method = (request.form.get("method") or request.args.get("method") or "email").strip().lower()
    pending_login = (session.get("mfa_pending_login") or "").strip()
    mfa_action = (request.form.get("mfa_action") or "").strip().lower() if request.method == "POST" else ""

    if request.method == "POST" and mfa_action == "confirm_totp":
        ip = _client_ip()
        if not allow_request(
            f"mfa_totp_enroll:{ip}", max_events=_MFA_TOTP_ENROLL_RPM, window_seconds=60.0
        ):
            return _mfa_setup_totp_page(
                method=method,
                error="Too many setup attempts. Try again in a minute.",
                status=429,
            )
        code = (request.form.get("code") or "").strip()
        sec = (session.get("mfa_totp_setup_secret") or "").strip()
        if not verify_totp(secret=sec, code=code):
            return _mfa_setup_totp_page(
                method=method,
                error="Invalid code. Check the time on your device and try again.",
            )
        if not set_user_totp(uid, secret=sec, enabled=True):
            return _mfa_setup_totp_page(
                method=method, error="Could not save 2FA. Please try again."
            )
        session.pop("mfa_totp_setup_secret", None)
        session.pop("mfa_totp_setup_otpauth", None)
        if not _finalize_app_session(uid):
            return redirect(_mfa_landing_url())
        return _post_mfa_success_redirect()

    # email: send a one-time code and go to /mfa/verify
    if request.method == "POST" and method == "email":
        dmode = (os.environ.get("MFA_DELIVERY_MODE") or "smtp").strip().lower()
        clear_session_otp(session, kind="mfa")
        code = issue_session_otp(session, kind="mfa", ttl_seconds=600)
        if dmode in ("test", "log"):
            session["mfa_test_last_code"] = code
        else:
            session.pop("mfa_test_last_code", None)
        if not pending_login or "@" not in pending_login:
            log_mfa_action(
                event="mfa_setup.email_rejected",
                surface="app",
                fields={"pending_user_id": uid, "reason": "no_pending_email"},
            )
            return _mfa_setup_totp_page(
                method=method, error="Email is required for email 2FA."
            )
        log_mfa_action(
            event="mfa_setup.send_email",
            surface="app",
            fields={"pending_user_id": uid, "mfa_test_last_set": dmode in ("log", "test"), "to_email": pending_login},
        )
        send_email_code(to_email=pending_login, code=code, mfa_log_surface="app")
        session["mfa_pending_method"] = "email"
        return redirect(url_for("mfa_verify"))

    return _mfa_setup_totp_page(method=method)


@app.route("/mfa/verify", methods=["GET", "POST"])
def mfa_verify():
    uid = _require_mfa_pending_user()
    if not uid:
        return redirect(_mfa_landing_url())
    mpm = (session.get("mfa_pending_method") or "").strip().lower()
    if mpm in ("", "choose"):
        return redirect(url_for("mfa_choose"))
    if mpm == "qr":
        return redirect(url_for("mfa_qr.mfa_qr_wait"))
    if mpm == "email" and not _mfa_email_ready_for_verify():
        return redirect(url_for("mfa_choose"))
    totp_mode = mpm == "totp"

    def _mfa_verify_render(**kwargs):
        kwargs.setdefault("mfa_is_totp", totp_mode)
        kwargs.setdefault("mfa_channel", mpm)
        kwargs["mfa_test_last_code"] = (
            session.get("mfa_test_last_code") if _mfa_dev_show_code_hint() else None
        )
        return render_template("mfa_verify.html", **kwargs)

    if request.method == "POST":
        ip = _client_ip()
        if not allow_request(
            f"mfa_verify:{ip}", max_events=_MFA_VERIFY_RPM, window_seconds=60.0
        ):
            log_mfa_action(
                event="mfa_verify.rate_limited",
                surface="app",
                fields={"pending_user_id": uid, "client_ip": ip, "mfa_method": mpm},
            )
            return (
                _mfa_verify_render(
                    error="Too many attempts. Try again in a minute.",
                ),
                429,
            )
        code = (request.form.get("code") or "").strip()
        method = (session.get("mfa_pending_method") or "email").strip().lower()

        if method == "email":
            if not verify_session_otp(session, kind="mfa", code=code):
                log_mfa_action(
                    event="mfa_verify.otp_mismatch",
                    surface="app",
                    fields={"pending_user_id": uid, "mfa_method": method, "client_ip": ip},
                )
                return _mfa_verify_render(
                    error="Invalid or expired code. Try again.",
                )
            clear_session_otp(session, kind="mfa")
        else:
            totp = get_user_totp(uid) or {"enabled": False, "secret": ""}
            if not (totp.get("enabled") and (totp.get("secret") or "").strip()):
                log_mfa_action(
                    event="mfa_verify.totp_not_ready",
                    surface="app",
                    fields={"pending_user_id": uid},
                )
                return redirect(url_for("mfa_setup"))
            if not verify_totp(secret=totp.get("secret") or "", code=code):
                log_mfa_action(
                    event="mfa_verify.totp_mismatch",
                    surface="app",
                    fields={"pending_user_id": uid, "client_ip": ip},
                )
                return _mfa_verify_render(error="Invalid code. Try again.")

        log_mfa_action(
            event="mfa_verify.success",
            surface="app",
            fields={"pending_user_id": uid, "mfa_method": method, "client_ip": ip},
        )
        if not _finalize_app_session(uid):
            return redirect(_mfa_landing_url())
        return _post_mfa_success_redirect()
    return _mfa_verify_render()


@app.route("/mfa/qr")
def mfa_qr():
    from flask import Response

    from backend.utils.qr_segno import png_bytes

    data = (session.get("mfa_totp_setup_otpauth") or "").strip()
    if not data:
        return Response(status=404)
    raw = png_bytes(data=data, box_size=6)
    if not raw:
        return Response(status=500)
    return Response(raw, mimetype="image/png")


def _similar_recommendation_rows(
    seen_mm: list[tuple[str, str]],
    viewed_id_set: set[int],
    limit: int,
    geo_kw: dict,
) -> list[dict]:
    """Cars matching recent make/model, excluding viewed ids; optional ZIP radius via ``geo_kw``."""
    seen_rec: set[int] = set()
    recs: list[dict] = []
    for make, model in seen_mm[:5]:
        for c in search_cars(makes=[make], models=[model], **geo_kw):
            cid = c.get("id")
            if cid and cid not in viewed_id_set and cid not in seen_rec:
                seen_rec.add(cid)
                recs.append(c)
                if len(recs) >= limit:
                    return recs
    return recs


def _recommendations_for_user(user_id: int, limit: int = 20, **geo_kw: object) -> tuple[list[dict], dict[str, str]]:
    """Return serialized carousel rows and heading copy for the dashboard."""
    default_heading = {
        "eyebrow": "Based on your history",
        "title": "Recommended for You",
        "hint": "",
    }
    viewed_ids = get_recent_viewed_car_ids(user_id, limit=30)
    if not viewed_ids:
        return [], default_heading

    viewed_cars = get_cars_by_ids(viewed_ids)
    if not viewed_cars:
        return [], default_heading

    by_id: dict[int, dict] = {}
    for c in viewed_cars:
        cid = c.get("id")
        if cid is not None:
            by_id[int(cid)] = c

    seen_mm: list[tuple[str, str]] = []
    seen_mm_set: set[tuple[str, str]] = set()
    for c in viewed_cars:
        make = (c.get("make") or "").strip()
        model = (c.get("model") or "").strip()
        if make and model:
            key = (make.lower(), model.lower())
            if key not in seen_mm_set:
                seen_mm_set.add(key)
                seen_mm.append((make, model))

    viewed_id_set = set(viewed_ids)
    heading = dict(default_heading)
    raw_recs: list[dict] = []
    geo_kw_dict = dict(geo_kw)
    geo_active = bool(geo_kw_dict.get("zip_code") and geo_kw_dict.get("radius_miles"))

    if seen_mm:
        if geo_active:
            raw_recs = _similar_recommendation_rows(seen_mm, viewed_id_set, limit, geo_kw_dict)
            if not raw_recs:
                heading["eyebrow"] = "Outside your search radius"
                heading["title"] = "Recommended & recently viewed"
                raw_recs = _similar_recommendation_rows(seen_mm, viewed_id_set, limit, {})
                if raw_recs:
                    heading["hint"] = (
                        "No similar listings near your saved ZIP and radius. "
                        "Showing similar inventory beyond that area and cars you opened recently."
                    )
                else:
                    heading["hint"] = (
                        "No close matches in inventory right now. Here are cars you opened recently."
                    )
        else:
            raw_recs = _similar_recommendation_rows(seen_mm, viewed_id_set, limit, {})
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

    if not out_cars:
        return [], default_heading

    return (
        [serialize_car_for_listings_grid(c) for c in out_cars[:limit]],
        heading,
    )


@app.route("/dashboard")
def dashboard():
    if session.get("mfa_pending_user_id") and not session.get("mfa_ok"):
        return redirect(url_for("mfa_verify"))
    user_id = session.get("user_id")
    recommendations = []
    saved_cars_list = []
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
    return render_template(
        "dashboard.html",
        saved_cars=saved_cars_list,
        recommendations=recommendations,
        recommendations_eyebrow=recommendations_eyebrow,
        recommendations_title=recommendations_title,
        recommendations_hint=recommendations_hint,
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


@app.route("/api/listings/cars")
def api_listings_cars():
    """Read-only JSON for the listings grid; supports client refresh while a scan is running."""
    return jsonify({"ok": True, "cars": listings_grid_serialized_cars()})


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


@app.route("/car/<int:car_id>")
def car_detail(car_id):
    car_raw = get_car_by_id(car_id, include_inactive=False)
    if not car_raw:
        abort(404)
    uid = session.get("user_id")
    car_is_saved = False
    if uid:
        try:
            record_car_view(int(uid), car_id)
        except Exception:
            pass
        try:
            car_is_saved = is_car_saved(int(uid), car_id)
        except Exception:
            pass
    ctx = prepare_car_detail_context(car_raw)
    car = serialize_car_for_api(
        car_raw,
        include_verified=False,
        verified_specs=ctx.get("verified_specs") or {},
    )
    _missing_codes = listing_missing_field_codes(car_raw, for_public_filter=False)
    listing_incomplete_fields = [
        {"code": c, "label": INCOMPLETE_FIELD_LABELS.get(c, c.replace("_", " ").title())}
        for c in _missing_codes
    ]
    return render_template(
        "car.html",
        car=car,
        car_is_saved=car_is_saved,
        logged_in=bool(uid),
        listing_incomplete_fields=listing_incomplete_fields,
        gallery_images=ctx.get("gallery_images") or [],
        verified_specs=ctx.get("verified_specs") or {},
        listing_packages_sections=ctx.get("listing_packages_sections") or [],
        listing_standalone_features=ctx.get("listing_standalone_features") or [],
        listing_observed_features=ctx.get("listing_observed_features") or [],
        listing_monroney_options=ctx.get("listing_monroney_options") or [],
        listing_monroney_standard=ctx.get("listing_monroney_standard") or [],
        interior_from_listing_description=bool(ctx.get("interior_from_listing_description")),
        interior_from_llava_vision=bool(ctx.get("interior_from_llava_vision")),
        packages_panel_has_content=bool(ctx.get("packages_panel_has_content")),
        llava_interior_section=ctx.get("llava_interior_section"),
        mopar_vin_lookup_url=mopar_vin_lookup_url(make=car.get("make"), vin=car.get("vin")),
    )


@app.route("/api/cars/<int:car_id>/save", methods=["POST"])
def api_toggle_save(car_id):
    try:
        uid = session["user_id"]
    except KeyError:
        uid = None
    if not uid:
        return jsonify({"ok": False, "error": "not_logged_in"}), 401
    uid = int(uid)
    currently_saved = is_car_saved(uid, car_id)
    if currently_saved:
        unsave_car(uid, car_id)
    else:
        save_car(uid, car_id)
    return jsonify({"ok": True, "saved": not currently_saved})


@app.route("/listings")
def listings():
    return listings_page(listings_poll_ms=_listings_client_poll_ms())


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
        elif k in ("make", "model"):
            keys.append(k)
        elif k == "interior_color":
            keys.append("interior_color")
        elif k in ("engine_displacement_l_min", "engine_displacement_l_max", "engine_l_min", "engine_l_max"):
            if "engine_l_min" not in keys:
                keys.append("engine_l_min")
            if "engine_l_max" not in keys:
                keys.append("engine_l_max")
    return keys


@app.route("/api/search/smart", methods=["POST"])
def api_search_smart():
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

    results, search_meta = hybrid_smart_search(
        q, filters, vector_top_k=100, listing_geo_kwargs=geo_kw if geo_kw else None
    )
    safe_results = [serialize_car_for_listings_grid(c) for c in results]
    return jsonify(
        {
            "filters": filters,
            "results": safe_results,
            "highlight": _highlight_params_from_filters(filters),
            "search_meta": search_meta,
        }
    )


@app.route("/api/car/<int:car_id>/chat", methods=["POST"])
def api_car_chat(car_id: int):
    ip = _client_ip()
    if not allow_request(f"chat:{ip}:{car_id}", max_events=_CHAT_RPM, window_seconds=60.0):
        return jsonify({"ok": False, "error": "rate_limited"}), 429

    if request.content_length is not None and request.content_length > _CHAT_MAX_BODY:
        return jsonify({"ok": False, "error": "payload_too_large"}), 413

    car_raw = get_car_by_id(car_id, include_inactive=False)
    if not car_raw:
        return jsonify({"ok": False, "error": "not_found"}), 404
    body = request.get_json() or {}
    message = (body.get("message") or body.get("q") or "").strip()
    if not message:
        return jsonify({"ok": False, "error": "message_required"}), 400
    if len(message) > _CHAT_MAX_MESSAGE:
        return jsonify({"ok": False, "error": "message_too_long"}), 400

    out = run_car_page_chat(car_raw, message)
    err = out.get("error")
    return jsonify(
        {
            "ok": err is None,
            "reply": out.get("reply") or "",
            "error": err,
        }
    )


# Realtime (QR sign-in) + same-process Socket.IO for /mfa/qr-wait
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
register_mfa_qr_socketio(socketio)
