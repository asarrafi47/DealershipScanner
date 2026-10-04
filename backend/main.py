"""Sarrafi Collection — Flask web application."""

from backend.utils.project_env import load_project_dotenv

load_project_dotenv()

from backend.utils.kmac_vault import load_kmac_vault_secrets

load_kmac_vault_secrets()

# Central env access (single read point; see backend/config.py).
from backend.config import Config

import json
import logging
import os
import re
import sqlite3
import time
from datetime import timedelta

from flask import Flask, abort, jsonify, make_response, redirect, render_template, request, session, url_for
from werkzeug.exceptions import HTTPException

from backend.intelligence.ai.agent import run_car_page_chat, run_compare_chat
from backend.auth.apple_oauth import bp as apple_oauth_bp
from backend.auth.google_oauth import bp as google_oauth_bp
from backend.billing.routes import bp as billing_bp
from backend.billing.catalog import (
    FEATURE_AI_CAR_CHAT,
    FEATURE_AI_COMPARE_CHAT,
    FEATURE_MARKET_INTEL,
    FEATURE_NEARBY_DEALERS,
    FEATURE_PACKAGES_ENSURE,
    FEATURE_SAVED_SEARCHES,
    FEATURE_VEHICLE_HISTORY,
    FEATURE_WINDOW_STICKER,
    get_plan,
    minimum_plan_for_feature,
)
from backend.billing import access as paid_access
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
    clear_search_history,
    create_saved_search,
    delete_saved_search,
    delete_search_history_entry,
    get_car_by_id,
    get_cars_by_ids,
    get_filter_options,
    get_saved_car_ids,
    hidden_dealer_ids_for_user,
    hide_dealer,
    init_inventory_db,
    is_car_saved,
    is_dealer_hidden,
    list_hidden_dealers,
    list_saved_searches,
    list_search_history,
    listings_geo_coords_maps,
    record_search_history,
    save_car,
    search_cars,
    search_cars_by_make_model_pairs,
    serialize_car_for_listings_grid,
    serialize_cars_for_listings_grid,
    unhide_dealer,
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
    submitted_password_attempts,
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
from backend.utils.car_serialize import serialize_car_for_api
from backend.utils.listing_completeness import INCOMPLETE_FIELD_LABELS
from backend.utils.car_chat_policy import car_chat_rate_limits, car_chat_user_daily_limit, web_research_playwright_allowed
from backend.utils.client_ip import client_ip as _client_ip_from_request
from backend.utils.client_ip import trust_proxy_headers
from backend.utils.csrf import ensure_csrf_token
from backend.utils.ip_rate_limit import allow_request
from backend.utils.query_parser import parse_natural_query
from backend.utils.runtime_env import is_production_env, session_cookie_secure_default
from backend.utils.roles import (
    is_admin_role,
    normalize_role,
)

_MIN_PASSWORD_LEN = Config.MIN_PASSWORD_LENGTH
_logger = logging.getLogger(__name__)


# Backward-compatible re-import: shared with the extracted route modules.
from backend.routes._shared import _client_ip  # noqa: E402
from backend.auth.session import (  # noqa: E402
    finalize_app_session as _finalize_app_session,
    post_login_redirect as _post_login_redirect,
)
from backend.web.security import register_security  # noqa: E402
from backend.web.static import register_static  # noqa: E402
from backend.web.templating import register_templating  # noqa: E402

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

# Cap JSON POST bodies (smart search, chat) and allow dealer multipart uploads (8 MiB+).
app.config["MAX_CONTENT_LENGTH"] = Config.MAX_REQUEST_BODY_BYTES

if trust_proxy_headers():
    # Railway's edge terminates TLS and sets X-Forwarded-Proto; without this the app
    # thinks it is served over http and builds http:// redirects and absolute URLs.
    # x_for stays 0: client_ip() reads X-Forwarded-For itself (TRUSTED_PROXY_HOPS).
    from werkzeug.middleware.proxy_fix import ProxyFix

    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=0, x_proto=1, x_host=0)

app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.config["SESSION_COOKIE_SECURE"] = session_cookie_secure_default()
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(days=14)
app.config["SESSION_REFRESH_EACH_REQUEST"] = True

# Static assets: year-long cache, ?v= cache-buster, precompressed siblings.
register_static(app)



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
    """
    Background-warm the EPA dictionary index a CAR page needs (well under a second).

    It used to go on to build the whole-fleet listings grid (214k cars, tens of
    seconds of Python, several GB resident). Owner decision 2026-09-28: the listings
    page asks for the shopper's radius only, served from the persisted card store
    (``grid_cards_repo``), so no process holds the fleet and there is nothing to
    prewarm. Deliberately left OFF -- do not add a grid prewarm back.
    """
    import threading

    def _run() -> None:
        try:
            from backend.enrichment.dictionary_catalog import _epa_paths_by_make_norm

            t0 = time.perf_counter()
            makes = len(_epa_paths_by_make_norm())
            _logger.info(
                "EPA dictionary index prewarmed (%d makes, %.1fs)",
                makes,
                time.perf_counter() - t0,
            )
        except Exception:
            _logger.exception("EPA dictionary index prewarm failed")

    threading.Thread(target=_run, name="listings-prewarm", daemon=True).start()


# Under gunicorn this runs in the ARBITER (--preload imports the app there), which
# would warm an index that serves nothing -- see gunicorn.conf.py, whose
# post_fork hook starts the prewarm in each worker instead. Everything else
# (run.py dev server, tests, one-off scripts) keeps the import-time behaviour.
if not os.environ.get("DS_GUNICORN_ARBITER"):
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
from backend.routes import community_api as _community_api_routes  # noqa: E402
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
_community_api_routes.register(app)
_dealers_recalls_routes.register(app)
_dealership_page_routes.register(app)
_dealer_reviews_routes.register(app)
_fuel_api_routes.register(app)
_admin_dealer_api_routes.register(app)

# App-wide hooks, in the original order (pinned by test_app_surface_golden):
# gzip after_request + context processor + filters + 404, then the CSP nonce,
# CSRF and billing-gate before_request hooks and the CSP headers after_request.
register_templating(app)
register_security(app)


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
        if not allow_request(f"login:{ip}", max_events=Config.RATE_LIMIT_LOGIN_PER_MIN, window_seconds=60.0):
            return render_template("login.html", error="Too many login attempts. Try again in a minute."), 429
        login_input = (request.form.get("login") or "").strip()
        attempts = submitted_password_attempts(request.form.get("password"))
        if not login_input or not attempts:
            return render_template("login.html", error="Enter username/email and password.")
        u = next((x for x in (authenticate_app_user(login_input, pw) for pw in attempts) if x), None)
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
        if not allow_request(f"register:{ip}", max_events=Config.RATE_LIMIT_REGISTER_PER_MIN, window_seconds=60.0):
            return render_template("register.html", error="Too many registration attempts. Try again later."), 429
        from backend.auth.app_registration import register_general_app_user

        plan = request.form.get("plan", "free").strip().lower()
        uid, err_code, err_msg, wants_premium = register_general_app_user(
            request.form.get("username", ""),
            request.form.get("email", ""),
            (request.form.get("password") or "").strip(),
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
        "hidden_dealers": _hidden_dealers_for_profile(uid),
        "recent_searches": _recent_searches_for_profile(uid),
        "saved_searches": _saved_searches_for_profile(uid),
        "saved_searches_enabled": paid_access.check_feature(FEATURE_SAVED_SEARCHES)[0],
        "recent_searches_limit": _PROFILE_RECENT_SEARCHES,
    }
    ctx.update(extra)
    return ctx


def _hidden_dealers_for_profile(uid: int) -> list[dict]:
    """Server-rendered rows for the profile's Hidden dealerships section ([] on any error)."""
    try:
        return list_hidden_dealers(int(uid))
    except Exception:
        _logger.debug("hidden dealers lookup failed for user %s", uid, exc_info=True)
        return []


_PROFILE_RECENT_SEARCHES = 20


def _recent_searches_for_profile(uid: int) -> list[dict]:
    """Server-rendered rows for the profile's Recent searches section ([] on any error).

    Each row carries ``label`` / ``url`` / ``when`` from search_history_format on top
    of the repo fields, so the template and account_profile.js render the same text."""
    try:
        from backend.utils.search_history_format import decorate_search_rows

        return decorate_search_rows(list_search_history(int(uid), _PROFILE_RECENT_SEARCHES))
    except Exception:
        _logger.debug("search history lookup failed for user %s", uid, exc_info=True)
        return []


def _saved_searches_for_profile(uid: int) -> list[dict]:
    """Server-rendered rows for the profile's Saved searches section ([] on any error)."""
    try:
        from backend.utils.search_history_format import decorate_search_rows

        return decorate_search_rows(list_saved_searches(int(uid)))
    except Exception:
        _logger.debug("saved searches lookup failed for user %s", uid, exc_info=True)
        return []


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
    from backend.billing.entitlements import FEATURE_LABELS
    from backend.billing.stripe_billing import billing_enabled
    from backend.db.users_db import get_user_billing_snapshot

    billing = get_user_billing_snapshot(uid) or {}
    plan_id = (billing.get("subscription_plan_id") or "").strip().lower()
    if not plan_id:
        plan_id = "complete" if billing.get("is_premium") else "free"
    plan = get_plan(plan_id)
    feats = sorted(paid_access.current_access().entitlements())
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
        "has_paid_access": paid_access.sees_paid_ui(),
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
    if not allow_request(f"login:{ip}", max_events=Config.RATE_LIMIT_LOGIN_PER_MIN, window_seconds=60.0):
        return jsonify({"ok": False, "error": "rate_limited"}), 429
    data = request.get_json(silent=True) or {}
    login_input = (data.get("login") or "").strip()
    attempts = submitted_password_attempts(data.get("password"))
    if not login_input or not attempts:
        return jsonify({"ok": False, "error": "missing_credentials"}), 400
    u = next((x for x in (authenticate_app_user(login_input, pw) for pw in attempts) if x), None)
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
    if not allow_request(f"register:{ip}", max_events=Config.RATE_LIMIT_REGISTER_PER_MIN, window_seconds=60.0):
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
