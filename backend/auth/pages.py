"""Auth pages: HTML login/register/verify/reset/logout, ``/api/auth/*`` JSON API, MFA "gone" stubs.

Moved out of ``backend/main.py`` (monolith audit W1). ``register(app)`` keeps the
original bare endpoint names: templates call ``url_for("login_page")`` and the CSRF
hook in ``backend/web/security.py`` matches endpoints by bare name.
"""

from __future__ import annotations

from flask import jsonify, redirect, render_template, request, session, url_for

from backend.auth.session import finalize_app_session as _finalize_app_session
from backend.auth.session import post_login_redirect as _post_login_redirect
from backend.billing import access as paid_access
from backend.config import Config
from backend.db.users_db import (
    authenticate_app_user,
    get_user_profile,
    submitted_password_attempts,
    sync_env_admin_user_row,
)
from backend.routes._shared import _client_ip
from backend.utils.csrf import ensure_csrf_token
from backend.utils.ip_rate_limit import allow_request
from backend.utils.roles import normalize_role


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
            min_password_len=Config.MIN_PASSWORD_LENGTH,
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


def verify_email_page():
    from backend.auth.email_verification import verify_email_token

    token = (request.args.get("token") or "").strip()
    ok, message = verify_email_token(token)
    return render_template("verify_email.html", ok=ok, message=message)


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


def logout_page():
    session.clear()
    return redirect(url_for("login_page"))


def _auth_user_payload(u: dict) -> dict:
    return {
        "id": int(u["id"]),
        "username": u.get("username"),
        "email": u.get("email"),
        "role": normalize_role(u.get("role")),
        "is_premium": bool(u.get("is_premium")),
        "has_paid_access": paid_access.sees_paid_ui(),
    }


def api_auth_csrf():
    return jsonify({"ok": True, "csrf_token": ensure_csrf_token()})


def api_auth_me():
    uid = session.get("user_id")
    if not uid:
        return jsonify({"ok": False, "error": "not_logged_in"}), 401
    u = get_user_profile(int(uid))
    if not u:
        return jsonify({"ok": False, "error": "not_logged_in"}), 401
    return jsonify({"ok": True, "user": _auth_user_payload(u)})


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
        min_password_len=Config.MIN_PASSWORD_LENGTH,
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


def api_auth_logout():
    session.clear()
    return jsonify({"ok": True})




def _app_mfa_gone():
    """2FA removed: old bookmarks and session redirects land here."""
    if session.get("user_id"):
        return redirect(url_for("app_home"))
    return redirect(url_for("login_page"))


def mfa_qr_complete_gone():
    return _app_mfa_gone()


def mfa_qr_confirm_gone(token):
    return _app_mfa_gone()


def register(app) -> None:
    """Attach the auth routes to ``app`` keeping the original bare endpoint names."""
    app.add_url_rule("/login", endpoint="login_page", view_func=login_page, methods=["GET", "POST"])
    app.add_url_rule("/register", endpoint="register_page", view_func=register_page, methods=["GET", "POST"])
    app.add_url_rule("/verify-email", endpoint="verify_email_page", view_func=verify_email_page)
    app.add_url_rule("/resend-verification", endpoint="resend_verification", view_func=resend_verification, methods=["POST"])
    app.add_url_rule("/forgot-password", endpoint="forgot_password_page", view_func=forgot_password_page, methods=["GET", "POST"])
    app.add_url_rule("/reset-password", endpoint="reset_password_page", view_func=reset_password_page, methods=["GET", "POST"])
    app.add_url_rule("/logout", endpoint="logout_page", view_func=logout_page, methods=["POST"])
    app.add_url_rule("/api/auth/csrf", endpoint="api_auth_csrf", view_func=api_auth_csrf, methods=["GET"])
    app.add_url_rule("/api/auth/me", endpoint="api_auth_me", view_func=api_auth_me, methods=["GET"])
    app.add_url_rule("/api/auth/login", endpoint="api_auth_login", view_func=api_auth_login, methods=["POST"])
    app.add_url_rule("/api/auth/register", endpoint="api_auth_register", view_func=api_auth_register, methods=["POST"])
    app.add_url_rule("/api/auth/logout", endpoint="api_auth_logout", view_func=api_auth_logout, methods=["POST"])

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

    app.add_url_rule("/mfa/qr/complete", endpoint="mfa_qr_complete_gone", view_func=mfa_qr_complete_gone, methods=["POST"])
    app.add_url_rule("/mfa/qr-confirm/<path:token>", endpoint="mfa_qr_confirm_gone", view_func=mfa_qr_confirm_gone, methods=["GET", "POST"])
