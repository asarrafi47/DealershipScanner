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
from werkzeug.exceptions import HTTPException

from backend.intelligence.ai.agent import run_car_page_chat
from backend.billing.routes import bp as billing_bp
from backend.dealer.admin import store_admin_bp
from backend.dealer.routes import bp as dealer_portal_bp
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
    init_users_db,
    save_user,
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
from backend.utils.car_chat_policy import car_chat_listing_daily_limit, car_chat_rate_limits, web_research_playwright_allowed
from backend.utils.client_ip import client_ip as _client_ip_from_request
from backend.utils.csrf import ensure_csrf_token, validate_csrf_form, validate_csrf_header

if not (validate_csrf_header.__code__.co_flags & inspect.CO_VARARGS):
    raise ImportError(
        "backend.utils.csrf.validate_csrf_header must be defined with *args (see repo csrf.py). "
        "Restart the server after git pull; check PYTHONPATH is not shadowing backend/utils/csrf.py."
    )
from backend.utils.ip_rate_limit import allow_request
from backend.utils.query_parser import ai_parse_natural_query, parse_natural_query
from backend.utils.registration_validation import registration_form_error
from backend.utils.runtime_env import is_production_env, session_cookie_secure_default
from backend.utils.roles import (
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
_LOGIN_RPM = int(os.environ.get("RATE_LIMIT_LOGIN_PER_MIN", "30"))
_REGISTER_RPM = int(os.environ.get("RATE_LIMIT_REGISTER_PER_MIN", "10"))


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
    return redirect(url_for("dashboard"))


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

init_users_db()
init_admin_db()
init_inventory_db()
init_dealer_portal_db()
app.register_blueprint(dev_bp, url_prefix="/dev")
app.register_blueprint(store_admin_bp)
app.register_blueprint(dealer_portal_bp)
app.register_blueprint(billing_bp)
register_dev_console(app)


@app.context_processor
def inject_csrf_and_flags():
    role = (session.get("user_role") or "").strip().lower()
    static_ver = "1"
    try:
        static_ver = str(int(Path(app.static_folder).resolve().joinpath("style.css").stat().st_mtime))
    except OSError:
        pass
    _is_admin = is_admin_role(role)
    return {
        "csrf_token": ensure_csrf_token(),
        "csp_nonce": getattr(g, "csp_nonce", "") or "",
        "is_production": is_production_env(),
        "logged_in_user": session.get("username"),
        "is_admin": _is_admin,
        "has_paid_access": _session_has_paid_access(),
        "billing_stripe_enabled": _billing_enabled(),
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
        "api_toggle_save",
        "api_session_listings_geo",
        "api_car_packages_ensure",
    ):
        validate_csrf_header()
    return None


def _session_has_paid_access() -> bool:
    """Premium, active org subscription, or app admin (matches context_processor ``has_paid_access``)."""
    if is_admin_role(session.get("user_role")):
        return True
    if bool(session.get("user_is_premium")):
        return True
    return bool(_org_subscription_active(session.get("org_subscription_status")))


def _car_window_sticker_preview_url(car_id: int) -> str:
    return f"/car/{car_id}/window-sticker-preview.png"


def _serve_car_window_sticker_preview(car_id: int):
    """Render page 1 of stored Monroney PDF as PNG (same access as car detail packages)."""
    if _billing_enabled() and not _session_has_paid_access():
        abort(403)
    car = get_car_by_id(car_id, include_inactive=False)
    if not car:
        abort(404)
    from backend.enrichment.window_sticker_service import (
        ensure_sticker_preview_png,
        window_sticker_local_path,
    )

    path = window_sticker_local_path(
        car.get("vin"),
        dealer_id=car.get("dealer_id"),
        dealership_registry_id=car.get("dealership_registry_id"),
    )
    if not path:
        abort(404)
    png = ensure_sticker_preview_png(path)
    if not png or not png.is_file():
        abort(503)
    return send_from_directory(
        str(png.parent),
        png.name,
        mimetype="image/png",
        as_attachment=False,
        download_name=f"window-sticker-{car_id}.png",
    )


@app.route("/car/<int:car_id>/window-sticker-preview.png")
def car_window_sticker_preview_png(car_id: int):
    return _serve_car_window_sticker_preview(car_id)


def _require_premium_feature() -> tuple[bool, str]:
    """
    When Stripe billing is enabled, paid surfaces require premium/subscription/admin.
    Returns (ok, error_code).
    """
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
        "object-src 'self'; "
        "frame-src 'self'; "
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


@app.errorhandler(404)
def page_not_found(_exc):
    if request.path.startswith("/api/") or (
        request.accept_mimetypes.best_match(["application/json", "text/html"]) == "application/json"
        and request.accept_mimetypes["application/json"] > request.accept_mimetypes["text/html"]
    ):
        return jsonify({"error": "not_found"}), 404
    return render_template("not_found.html"), 404


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
def serve_car_image(filename: str):
    # Block path traversal: reject any component that starts with '.' or contains separators
    parts = Path(filename).parts
    if not parts or any(p.startswith(".") or p in ("/", "\\") for p in parts):
        abort(400)
    safe_path = _CAR_IMAGES_DIR.joinpath(*parts).resolve()
    if not safe_path.is_relative_to(_CAR_IMAGES_DIR.resolve()):
        abort(400)
    return send_from_directory(str(_CAR_IMAGES_DIR), str(Path(*parts)))


@app.route("/")
def home():
    if session.get("user_id"):
        return redirect("/dashboard")
    from datetime import datetime
    return render_template("landing.html", now=datetime.utcnow())


@app.route("/login", methods=["GET", "POST"])
def login_page():
    if request.method == "GET":
        err = request.args.get("_error", "")
        if err == "session_expired":
            return render_template("login.html", error="Your session expired — please log in again.")
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
        username = request.form.get("username", "")
        email = request.form.get("email", "")
        password = request.form.get("password", "")
        plan = request.form.get("plan", "free").strip().lower()
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
        session.clear()
        if plan == "premium":
            session["post_auth_intent"] = "premium"
        if not _finalize_app_session(int(uid)):
            return render_template("register.html", error="Registration failed. Try again.")
        return _post_login_redirect()
    return render_template("register.html")


@app.route("/logout", methods=["POST"])
def logout_page():
    session.clear()
    return redirect(url_for("login_page"))




def _app_mfa_gone():
    """2FA removed: old bookmarks and session redirects land here."""
    if session.get("user_id"):
        return redirect(url_for("dashboard"))
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
    session["user_id"] = int(u["id"])
    session["username"] = u["username"]
    session["user_email"] = (u.get("email") or "").strip()
    session["user_role"] = normalize_role(u.get("role"))
    session["user_dealer_id"] = (u.get("dealer_id") or "").strip()
    rid = u.get("dealership_registry_id")
    session["user_dealership_registry_id"] = str(int(rid)) if rid is not None else ""
    session["org_id"] = int(u.get("org_id") or 0) if u.get("org_id") else 0
    session["user_is_premium"] = bool(u.get("is_premium"))
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
    if not session.get("user_id"):
        return redirect(url_for("login_page"))
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


@app.route("/api/listings/market-stats")
def api_listings_market_stats():
    """Trim-level average prices for premium listings grid (cached server-side)."""
    ok, err = _require_premium_feature()
    if not ok:
        return jsonify({"ok": False, "error": err}), 403
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
    from backend.enrichment.window_sticker_service import (
        ensure_sticker_preview_png,
        window_sticker_available,
        window_sticker_local_path,
    )
    from backend.scanner.window_sticker import get_window_sticker_url

    sticker_ready = window_sticker_available(car_raw)
    if sticker_ready and _session_has_paid_access():
        local_pdf = window_sticker_local_path(
            car_raw.get("vin"),
            dealer_id=car_raw.get("dealer_id"),
            dealership_registry_id=car_raw.get("dealership_registry_id"),
        )
        if local_pdf:
            ensure_sticker_preview_png(local_pdf)

    window_sticker_oem_url = None
    if _session_has_paid_access():
        window_sticker_oem_url = get_window_sticker_url(str(car_raw.get("vin") or ""))

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
    dealer_info = None
    reg_id = car_raw.get("dealership_registry_id")
    if reg_id:
        try:
            from backend.db.dealerships_db import get_dealership_by_id
            dealer_info = get_dealership_by_id(int(reg_id))
        except Exception:
            pass
    market_intel = None
    if _session_has_paid_access():
        from backend.listings.geo_session import listings_geo_kwargs_from_session
        from backend.utils.market_price import market_price_for_car

        geo = listings_geo_kwargs_from_session(session)
        market_intel = market_price_for_car(
            car_raw,
            zip_code=geo.get("zip_code"),
            radius_miles=geo.get("radius_miles"),
        )
    return render_template(
        "car.html",
        car=car,
        market_intel=market_intel,
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
        listing_sticker_options=ctx.get("listing_sticker_options") or [],
        listing_possible_packages=ctx.get("listing_possible_packages") or [],
        sticker_exterior_color=ctx.get("sticker_exterior_color"),
        sticker_interior_color=ctx.get("sticker_interior_color"),
        sticker_interior_material=ctx.get("sticker_interior_material"),
        sticker_spec_lines=ctx.get("sticker_spec_lines") or [],
        interior_from_listing_description=bool(ctx.get("interior_from_listing_description")),
        interior_from_llava_vision=bool(ctx.get("interior_from_llava_vision")),
        packages_panel_has_content=bool(ctx.get("packages_panel_has_content")),
        llava_interior_section=ctx.get("llava_interior_section"),
        window_sticker_available=sticker_ready,
        window_sticker_preview_api_url=_car_window_sticker_preview_url(car_id),
        window_sticker_preview_url=(
            _car_window_sticker_preview_url(car_id)
            if sticker_ready and _session_has_paid_access()
            else None
        ),
        hide_photo_analysis=bool(ctx.get("hide_photo_analysis")),
        window_sticker_oem_url=window_sticker_oem_url,
        dealer_info=dealer_info,
    )


@app.route("/api/cars/<int:car_id>/window-sticker")
def api_car_window_sticker(car_id: int):
    """Serve stored OEM window sticker PDF (premium only)."""
    ok, err = _require_premium_feature()
    if not ok:
        return jsonify({"ok": False, "error": err}), 403
    car = get_car_by_id(car_id, include_inactive=False)
    if not car:
        return jsonify({"ok": False, "error": "not_found"}), 404
    from backend.enrichment.window_sticker_service import window_sticker_local_path

    path = window_sticker_local_path(
        car.get("vin"),
        dealer_id=car.get("dealer_id"),
        dealership_registry_id=car.get("dealership_registry_id"),
    )
    if not path:
        from backend.enrichment.window_sticker_service import ensure_window_sticker_for_car

        ensure_window_sticker_for_car(car_id, allow_vision_fallback=False)
        path = window_sticker_local_path(
            car.get("vin"),
            dealer_id=car.get("dealer_id"),
            dealership_registry_id=car.get("dealership_registry_id"),
        )
    if not path:
        return jsonify({"ok": False, "error": "sticker_not_available"}), 404
    return send_from_directory(
        str(path.parent),
        path.name,
        mimetype="application/pdf",
        as_attachment=False,
        download_name=f"window-sticker-{car_id}.pdf",
    )


@app.route("/api/cars/<int:car_id>/window-sticker-preview")
def api_car_window_sticker_preview(car_id: int):
    """PNG preview of page 1 (legacy API path; prefer /car/<id>/window-sticker-preview.png)."""
    return _serve_car_window_sticker_preview(car_id)


@app.route("/api/cars/<int:car_id>/packages/ensure", methods=["POST"])
def api_car_packages_ensure(car_id: int):
    """Fetch/analyze window sticker and merge packages (premium only)."""
    ok, err = _require_premium_feature()
    if not ok:
        return jsonify({"ok": False, "error": err}), 403
    try:
        validate_csrf_header()
    except HTTPException:
        return jsonify({"ok": False, "error": "csrf_required"}), 403
    car = get_car_by_id(car_id, include_inactive=False)
    if not car:
        return jsonify({"ok": False, "error": "not_found"}), 404
    from backend.enrichment.window_sticker_service import (
        ensure_window_sticker_for_car,
        window_sticker_available,
    )

    allow_vision = request.args.get("vision", "0").strip().lower() in ("1", "true", "yes")
    status = ensure_window_sticker_for_car(car_id, allow_vision_fallback=allow_vision)
    car2 = get_car_by_id(car_id, include_inactive=False) or car
    ctx = prepare_car_detail_context(car2)
    status["packages_panel_has_content"] = bool(ctx.get("packages_panel_has_content"))
    status["window_sticker_available"] = bool(status.get("window_sticker_available"))
    from backend.enrichment.window_sticker_service import sticker_panel_payload
    from backend.scanner.window_sticker import get_window_sticker_url

    status.update(sticker_panel_payload(ctx, car2))
    status["window_sticker_available"] = bool(
        status.get("window_sticker_available")
        or window_sticker_available(car2)
    )
    vin = str(car2.get("vin") or "")
    if vin:
        status["window_sticker_oem_url"] = get_window_sticker_url(vin)
    if status.get("window_sticker_available"):
        status["window_sticker_view_url"] = url_for(
            "api_car_window_sticker", car_id=car_id
        )
        status["window_sticker_preview_url"] = _car_window_sticker_preview_url(car_id)
    return jsonify(status)


@app.route("/api/nearby-dealers")
def api_nearby_dealers():
    """Return dealerships within radius of a ZIP code (max 50 mi). Premium when billing enabled."""
    ok, err = _require_premium_feature()
    if not ok:
        return jsonify({"ok": False, "error": err, "dealers": []}), 403
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
    return render_template("premium.html")


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
    filters = ai_parse_natural_query(q)
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
    ok, err = _require_premium_feature()
    if not ok:
        return jsonify({"ok": False, "error": err}), 403

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

    daily_limit = car_chat_listing_daily_limit()
    if daily_limit > 0 and not allow_request(
        f"chat:daily:listing:{car_id}",
        max_events=daily_limit,
        window_seconds=86400.0,
    ):
        return jsonify({"ok": False, "error": "listing_chat_limit_reached"}), 429

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
