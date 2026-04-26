"""DealershipScanner Flask application."""

from backend.utils.project_env import load_project_dotenv

load_project_dotenv()

import os
import sqlite3
import time

from flask import Flask, abort, jsonify, redirect, render_template, request, send_from_directory, session, url_for

from backend.ai_agent import run_car_page_chat
from backend.billing.routes import bp as billing_bp
from backend.dealer_admin import store_admin_bp
from backend.dealer_portal import bp as dealer_portal_bp
from backend.mfa_qr import bp as mfa_qr_bp
from backend.mfa_qr import register_mfa_qr_socketio
from backend.dev_console import register_dev_console
from backend.dev_routes import dev_bp
from backend.db.admin_users_db import init_admin_db
from backend.db.dealer_portal_db import init_dealer_portal_db
from backend.db.inventory_db import (
    get_car_by_id,
    get_filter_options,
    init_inventory_db,
    search_cars,
)
from backend.db.users_db import (
    check_user,
    get_user_by_login,
    get_user_profile,
    get_user_totp,
    init_users_db,
    save_user,
    set_user_totp,
)
from backend.hybrid_inventory_search import (
    flask_request_to_search_cars_kwargs,
    hybrid_search_with_kwargs,
)
from backend.knowledge_engine import prepare_car_detail_context
from backend.listings import listings_page
from backend.utils.car_serialize import format_display_value, serialize_car_for_api
from backend.utils.oem_links import mopar_vin_lookup_url
from backend.utils.client_ip import client_ip as _client_ip_from_request
from backend.utils.csrf import ensure_csrf_token, validate_csrf_form, validate_csrf_header
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
)

_MIN_PASSWORD_LEN = max(8, int(os.environ.get("MIN_PASSWORD_LENGTH", "8")))
_CHAT_MAX_MESSAGE = int(os.environ.get("CHAT_MAX_MESSAGE_CHARS", "4000"))
_CHAT_MAX_BODY = int(os.environ.get("CHAT_MAX_BODY_BYTES", "65536"))
_SMART_SEARCH_RPM = int(os.environ.get("RATE_LIMIT_SMART_SEARCH_PER_MIN", "90"))
_CHAT_RPM = int(os.environ.get("RATE_LIMIT_CAR_CHAT_PER_MIN", "40"))
_LOGIN_RPM = int(os.environ.get("RATE_LIMIT_LOGIN_PER_MIN", "30"))
_REGISTER_RPM = int(os.environ.get("RATE_LIMIT_REGISTER_PER_MIN", "10"))
_MFA_VERIFY_RPM = int(os.environ.get("RATE_LIMIT_MFA_VERIFY_PER_MIN", "20"))
_MFA_TOTP_ENROLL_RPM = int(os.environ.get("RATE_LIMIT_MFA_TOTP_ENROLL_PER_MIN", "10"))
_MFA_ISSUER = (os.environ.get("MFA_ISSUER") or "DealershipScanner").strip() or "DealershipScanner"


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
    return {
        "csrf_token": ensure_csrf_token(),
        "is_production": is_production_env(),
        "logged_in_user": session.get("username"),
        "nav_store_admin": nav_store_admin,
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
def _csrf_mutating_requests():
    if request.method != "POST":
        return
    ep = request.endpoint or ""
    if ep in (
        "login_page",
        "register_page",
        "mfa_choose",
        "mfa_setup",
        "mfa_verify",
        "mfa_qr.mfa_qr_complete",
    ):
        validate_csrf_form()
    elif ep and str(ep).startswith("dealer_portal."):
        validate_csrf_form()
    elif ep and str(ep).startswith("store_admin."):
        validate_csrf_form()
    elif ep in ("api_search_smart", "api_car_chat"):
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


# Report-only CSP (SEC-032): opt-in via CSP_REPORT_ONLY=1 after public pages avoid inline script/style.
_CSP_REPORT_ONLY = (
    "default-src 'self'; "
    "base-uri 'self'; "
    "form-action 'self'; "
    "frame-ancestors 'none'; "
    "object-src 'none'; "
    "img-src 'self' data: https: http: blob:; "
    "font-src 'self' https://fonts.gstatic.com data:; "
    "style-src 'self' https://fonts.googleapis.com; "
    "script-src 'self' https://esm.sh; "
    "connect-src 'self' https://esm.sh https://fonts.googleapis.com; "
    "worker-src 'self'; "
)


@app.after_request
def _csp_report_only_header(response):
    if (os.environ.get("CSP_REPORT_ONLY") or "").strip().lower() not in ("1", "true", "yes", "on"):
        return response
    if response.headers.get("Content-Security-Policy-Report-Only"):
        return response
    response.headers["Content-Security-Policy-Report-Only"] = _CSP_REPORT_ONLY
    return response


@app.template_filter("fmt_spec")
def _jinja_fmt_spec(value):
    return format_display_value(value)


@app.route("/favicon.ico")
def favicon():
    return send_from_directory(app.static_folder, "favicon.svg", mimetype="image/svg+xml")


@app.route("/")
def home():
    return redirect("/login")


@app.route("/login", methods=["GET", "POST"])
def login_page():
    if request.method == "POST":
        ip = _client_ip()
        if not allow_request(f"login:{ip}", max_events=_LOGIN_RPM, window_seconds=60.0):
            return render_template("login.html", error="Too many login attempts. Try again in a minute."), 429
        login_input = request.form["login"]
        password = request.form["password"]
        if check_user(login_input, password):
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
        is_admin = email_is_admin(email)
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


@app.route("/logout", methods=["GET", "POST"])
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


@app.route("/dashboard")
def dashboard():
    if session.get("mfa_pending_user_id") and not session.get("mfa_ok"):
        return redirect(url_for("mfa_verify"))
    return render_template("dashboard.html", saved_cars=[])


@app.route("/search")
def search():
    g = request.args.getlist

    def scalar(key):
        vals = [v.strip() for v in request.args.getlist(key) if v.strip()]
        return vals[-1] if vals else ""

    zip_code = scalar("zip_code")
    radius = scalar("radius")
    max_price = scalar("max_price")
    max_mileage = scalar("max_mileage")
    reg_id_raw = scalar("dealership_registry_id")

    dealership_registry_id = None
    if reg_id_raw:
        try:
            dealership_registry_id = int(reg_id_raw)
        except ValueError:
            dealership_registry_id = None

    q_text = scalar("q") or scalar("search")
    sql_kwargs = flask_request_to_search_cars_kwargs(request)
    if q_text:
        results, _ = hybrid_search_with_kwargs(q_text, sql_kwargs, vector_top_k=100)
    else:
        results = search_cars(**sql_kwargs)

    initial_grid_cars = []
    if q_text:
        initial_grid_cars = [
            serialize_car_for_api(c, include_verified=False) for c in results
        ]

    active = {
        "make": g("make"),
        "model": g("model"),
        "trim": g("trim"),
        "fuel_type": g("fuel_type"),
        "cylinders": g("cylinders"),
        "transmission": g("transmission"),
        "drivetrain": g("drivetrain"),
        "body_style": g("body_style"),
        "exterior_color": g("exterior_color"),
        "interior_color": g("interior_color"),
        "country": g("country"),
        "max_price": max_price,
        "max_mileage": max_mileage,
        "engine_l_min": scalar("engine_l_min") or scalar("engine_displacement_l_min"),
        "engine_l_max": scalar("engine_l_max") or scalar("engine_displacement_l_max"),
        "zip_code": zip_code,
        "radius": radius,
        "dealership_registry_id": reg_id_raw,
        "q": q_text,
    }

    return render_template(
        "listings.html",
        active=active,
        options=get_filter_options(),
        initial_grid_cars=initial_grid_cars,
    )


@app.route("/car/<int:car_id>")
def car_detail(car_id):
    car_raw = get_car_by_id(car_id, include_inactive=False)
    if not car_raw:
        abort(404)
    ctx = prepare_car_detail_context(car_raw)
    car = serialize_car_for_api(
        car_raw,
        include_verified=False,
        verified_specs=ctx.get("verified_specs") or {},
    )
    return render_template(
        "car.html",
        car=car,
        gallery_images=ctx.get("gallery_images") or [],
        verified_specs=ctx.get("verified_specs") or {},
        listing_packages_sections=ctx.get("listing_packages_sections") or [],
        listing_standalone_features=ctx.get("listing_standalone_features") or [],
        listing_observed_features=ctx.get("listing_observed_features") or [],
        interior_from_listing_description=bool(ctx.get("interior_from_listing_description")),
        interior_from_llava_vision=bool(ctx.get("interior_from_llava_vision")),
        packages_panel_has_content=bool(ctx.get("packages_panel_has_content")),
        llava_interior_section=ctx.get("llava_interior_section"),
        mopar_vin_lookup_url=mopar_vin_lookup_url(make=car.get("make"), vin=car.get("vin")),
    )


@app.route("/listings")
def listings():
    return listings_page()


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

    data = request.get_json() or {}
    q = (data.get("query") or data.get("q") or "").strip()
    filters = parse_natural_query(q)
    from backend.hybrid_inventory_search import hybrid_smart_search

    results, search_meta = hybrid_smart_search(q, filters, vector_top_k=100)
    safe_results = [serialize_car_for_api(c, include_verified=False) for c in results]
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

_cors = (os.environ.get("SOCKETIO_CORS_ORIGINS") or "*").strip() or "*"
socketio = SocketIO(
    app,
    async_mode="threading",
    cors_allowed_origins=_cors,
    manage_session=True,
)
if getattr(app, "extensions", None) is None:
    app.extensions = {}
app.extensions["socketio"] = socketio
register_mfa_qr_socketio(socketio)
