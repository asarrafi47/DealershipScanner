"""Optional Google OAuth 2.0 sign-in for app users (server-side authorization code flow)."""

from __future__ import annotations

import logging
import os
import secrets
import sqlite3
from urllib.parse import urlencode

import requests
from flask import Blueprint, redirect, request, session, url_for

from backend.db.users_db import (
    get_user_by_google_sub,
    get_user_by_login,
    link_user_google_sub,
    save_oauth_user,
    sync_env_admin_user_row,
    user_exists_by_username,
)
from backend.utils.ip_rate_limit import allow_request
from backend.utils.registration_validation import MAX_USERNAME_LEN, normalize_registration_email
from backend.utils.roles import REGISTRATION_ADMIN_BLOCKED_MSG, ROLE_GENERAL, registration_blocked_by_env_admin

_log = logging.getLogger(__name__)

GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_USERINFO_URL = "https://openidconnect.googleapis.com/v1/userinfo"

bp = Blueprint("google_oauth", __name__, url_prefix="/auth/google")


def google_oauth_configured() -> bool:
    cid = (os.environ.get("GOOGLE_OAUTH_CLIENT_ID") or "").strip()
    secret = (os.environ.get("GOOGLE_OAUTH_CLIENT_SECRET") or "").strip()
    return bool(cid and secret)


def google_signin_visible() -> bool:
    if not google_oauth_configured():
        return False
    return (os.environ.get("GOOGLE_OAUTH_SHOW_BUTTON") or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def _client_id() -> str:
    return (os.environ.get("GOOGLE_OAUTH_CLIENT_ID") or "").strip()


def _client_secret() -> str:
    return (os.environ.get("GOOGLE_OAUTH_CLIENT_SECRET") or "").strip()


def _redirect_uri() -> str:
    explicit = (os.environ.get("GOOGLE_OAUTH_REDIRECT_URI") or "").strip()
    if explicit:
        return explicit
    return url_for("google_oauth.callback", _external=True)


def _login_rpm() -> int:
    return int(os.environ.get("RATE_LIMIT_LOGIN_PER_MIN", "30"))


def _client_ip() -> str:
    from backend.utils.client_ip import client_ip as _client_ip_from_request

    return _client_ip_from_request(request)


def _oauth_login_error(message: str):
    session["oauth_error"] = message
    return redirect(url_for("login_page"))


def _pick_username(email: str) -> str:
    local = (email.split("@", 1)[0] if "@" in email else "user").strip()
    base = local[:MAX_USERNAME_LEN] if local else "user"
    if len(base) < 2:
        base = "user"
    if not user_exists_by_username(base):
        return base
    for i in range(2, 1000):
        suffix = str(i)
        candidate = f"{base[: max(2, MAX_USERNAME_LEN - len(suffix))]}{suffix}"
        if not user_exists_by_username(candidate):
            return candidate
    return f"user_{secrets.token_hex(4)}"


def _exchange_code(code: str) -> dict | None:
    try:
        resp = requests.post(
            GOOGLE_TOKEN_URL,
            data={
                "code": code,
                "client_id": _client_id(),
                "client_secret": _client_secret(),
                "redirect_uri": _redirect_uri(),
                "grant_type": "authorization_code",
            },
            timeout=15,
        )
        resp.raise_for_status()
        payload = resp.json()
        return payload if isinstance(payload, dict) else None
    except (requests.RequestException, ValueError) as ex:
        _log.warning("Google OAuth token exchange failed: %s", ex)
        return None


def _fetch_userinfo(access_token: str) -> dict | None:
    try:
        resp = requests.get(
            GOOGLE_USERINFO_URL,
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=15,
        )
        resp.raise_for_status()
        payload = resp.json()
        return payload if isinstance(payload, dict) else None
    except (requests.RequestException, ValueError) as ex:
        _log.warning("Google OAuth userinfo fetch failed: %s", ex)
        return None


def _resolve_or_create_user(info: dict) -> tuple[int | None, str | None, bool]:
    google_sub = (info.get("sub") or "").strip()
    email = normalize_registration_email(str(info.get("email") or ""))
    if not google_sub:
        return None, "Google sign-in did not return a user id.", False
    if not email or "@" not in email:
        return None, "Google sign-in did not return a valid email.", False

    existing = get_user_by_google_sub(google_sub)
    if existing:
        return int(existing["id"]), None, False

    by_email = get_user_by_login(email)
    if by_email:
        stored_sub = (by_email.get("google_sub") or "").strip()
        if stored_sub and stored_sub != google_sub:
            return None, "This email is linked to a different Google account.", False
        if not link_user_google_sub(int(by_email["id"]), google_sub):
            return None, "Could not link Google to your account.", False
        return int(by_email["id"]), None, False

    username = _pick_username(email)
    if registration_blocked_by_env_admin(email, username):
        return None, REGISTRATION_ADMIN_BLOCKED_MSG, False
    try:
        uid = save_oauth_user(username, email, google_sub, role=ROLE_GENERAL)
    except sqlite3.IntegrityError:
        retry = get_user_by_google_sub(google_sub) or get_user_by_login(email)
        if retry:
            return int(retry["id"]), None, False
        return None, "Could not create your account.", False
    return uid, None, True


def _should_offer_premium_after_google_login(*, is_new_user: bool) -> bool:
    if not is_new_user:
        offer_returning = (os.environ.get("GOOGLE_OAUTH_OFFER_PREMIUM_ON_LOGIN") or "").strip().lower() in (
            "1",
            "true",
            "yes",
            "on",
        )
        if not offer_returning:
            return False
    return True


def _complete_login(user_id: int, *, is_new_user: bool = False):
    from backend.main import _finalize_app_session, _post_login_redirect
    from backend.utils.roles import is_admin_role

    oauth_post_intent = (session.pop("google_oauth_post_intent", None) or "").strip().lower()
    sync_env_admin_user_row(user_id)
    session.clear()
    if not _finalize_app_session(user_id):
        return _oauth_login_error("Login failed. Try again.")

    if is_admin_role(session.get("user_role")) or bool(session.get("user_is_premium")):
        return _post_login_redirect()

    wants_premium = oauth_post_intent == "premium" or _should_offer_premium_after_google_login(
        is_new_user=is_new_user
    )
    if wants_premium:
        session["post_auth_intent"] = "premium"
        session["auth_welcome_source"] = "google_new" if is_new_user else "google"
    return _post_login_redirect()


@bp.route("")
def start():
    if not google_oauth_configured():
        return redirect(url_for("login_page"))
    intent = (request.args.get("intent") or "").strip().lower()
    if intent == "premium":
        session["google_oauth_post_intent"] = "premium"
    state = secrets.token_urlsafe(32)
    session["google_oauth_state"] = state
    params = {
        "client_id": _client_id(),
        "redirect_uri": _redirect_uri(),
        "response_type": "code",
        "scope": "openid email profile",
        "state": state,
        "access_type": "online",
        "prompt": "select_account",
    }
    return redirect(f"{GOOGLE_AUTH_URL}?{urlencode(params)}")


@bp.route("/callback")
def callback():
    if not google_oauth_configured():
        return redirect(url_for("login_page"))

    ip = _client_ip()
    if not allow_request(f"google_oauth:{ip}", max_events=_login_rpm(), window_seconds=60.0):
        return _oauth_login_error("Too many sign-in attempts. Try again in a minute.")

    err = (request.args.get("error") or "").strip()
    if err:
        _log.info("Google OAuth denied by user or provider: %s", err)
        return _oauth_login_error("Google sign-in was cancelled.")

    state = (request.args.get("state") or "").strip()
    expected = (session.pop("google_oauth_state", None) or "").strip()
    if not state or not expected or state != expected:
        return _oauth_login_error("Google sign-in session expired. Try again.")

    code = (request.args.get("code") or "").strip()
    if not code:
        return _oauth_login_error("Google sign-in did not complete.")

    token_payload = _exchange_code(code)
    if not token_payload:
        return _oauth_login_error("Google sign-in failed. Try again.")

    access_token = (token_payload.get("access_token") or "").strip()
    if not access_token:
        return _oauth_login_error("Google sign-in failed. Try again.")

    info = _fetch_userinfo(access_token)
    if not info:
        return _oauth_login_error("Google sign-in failed. Try again.")

    if info.get("email_verified") is False:
        return _oauth_login_error("Google account email is not verified.")

    user_id, user_err, is_new_user = _resolve_or_create_user(info)
    if user_err:
        return _oauth_login_error(user_err)
    if not user_id:
        return _oauth_login_error("Google sign-in failed. Try again.")

    return _complete_login(user_id, is_new_user=is_new_user)
