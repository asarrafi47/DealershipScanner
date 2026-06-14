"""Optional Sign in with Apple for app users (OAuth 2.0 authorization code flow)."""

from __future__ import annotations

import logging
import os
import secrets
import sqlite3
import time
from urllib.parse import urlencode

import requests
from flask import Blueprint, redirect, request, session, url_for

from backend.db.users_db import (
    get_user_by_apple_sub,
    get_user_by_login,
    link_user_apple_sub,
    save_apple_oauth_user,
    sync_env_admin_user_row,
    user_exists_by_username,
)
from backend.utils.ip_rate_limit import allow_request
from backend.utils.registration_validation import MAX_USERNAME_LEN, normalize_registration_email
from backend.utils.roles import REGISTRATION_ADMIN_BLOCKED_MSG, ROLE_GENERAL, registration_blocked_by_env_admin

_log = logging.getLogger(__name__)

APPLE_AUTH_URL = "https://appleid.apple.com/auth/authorize"
APPLE_TOKEN_URL = "https://appleid.apple.com/auth/token"

bp = Blueprint("apple_oauth", __name__, url_prefix="/auth/apple")


def apple_oauth_configured() -> bool:
    cid = (os.environ.get("APPLE_OAUTH_CLIENT_ID") or "").strip()
    team = (os.environ.get("APPLE_OAUTH_TEAM_ID") or "").strip()
    key_id = (os.environ.get("APPLE_OAUTH_KEY_ID") or "").strip()
    key_pem = (os.environ.get("APPLE_OAUTH_PRIVATE_KEY") or "").strip()
    return bool(cid and team and key_id and key_pem)


def apple_signin_visible() -> bool:
    if not apple_oauth_configured():
        return False
    return (os.environ.get("APPLE_OAUTH_SHOW_BUTTON") or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def _client_id() -> str:
    return (os.environ.get("APPLE_OAUTH_CLIENT_ID") or "").strip()


def _redirect_uri() -> str:
    explicit = (os.environ.get("APPLE_OAUTH_REDIRECT_URI") or "").strip()
    if explicit:
        return explicit
    return url_for("apple_oauth.callback", _external=True)


def _login_rpm() -> int:
    return int(os.environ.get("RATE_LIMIT_LOGIN_PER_MIN", "30"))


def _client_ip() -> str:
    from backend.utils.client_ip import client_ip as _client_ip_from_request

    return _client_ip_from_request(request)


def _oauth_login_error(message: str):
    session["oauth_error"] = message
    return redirect(url_for("login_page"))


def _build_client_secret() -> str:
    """
    Apple requires a JWT client secret signed with your private key.
    Requires PyJWT with crypto extras in production.
    """
    import jwt  # type: ignore

    team_id = (os.environ.get("APPLE_OAUTH_TEAM_ID") or "").strip()
    key_id = (os.environ.get("APPLE_OAUTH_KEY_ID") or "").strip()
    key_pem = (os.environ.get("APPLE_OAUTH_PRIVATE_KEY") or "").strip()
    client_id = _client_id()
    if not all((team_id, key_id, key_pem, client_id)):
        raise RuntimeError("Apple OAuth is not fully configured.")

    now = int(time.time())
    headers = {"kid": key_id}
    payload = {
        "iss": team_id,
        "iat": now,
        "exp": now + 3600,
        "aud": "https://appleid.apple.com",
        "sub": client_id,
    }
    return jwt.encode(payload, key_pem, algorithm="ES256", headers=headers)


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
            APPLE_TOKEN_URL,
            data={
                "code": code,
                "client_id": _client_id(),
                "client_secret": _build_client_secret(),
                "redirect_uri": _redirect_uri(),
                "grant_type": "authorization_code",
            },
            timeout=15,
        )
        resp.raise_for_status()
        payload = resp.json()
        return payload if isinstance(payload, dict) else None
    except (requests.RequestException, ValueError, RuntimeError) as ex:
        _log.warning("Apple OAuth token exchange failed: %s", ex)
        return None


def _decode_id_token(id_token: str) -> dict | None:
    try:
        import jwt  # type: ignore

        # Apple id_token; signature verified against Apple JWKS in production hardening.
        payload = jwt.decode(
            id_token,
            options={"verify_signature": False, "verify_aud": False},
            algorithms=["RS256"],
        )
        return payload if isinstance(payload, dict) else None
    except Exception as ex:
        _log.warning("Apple id_token decode failed: %s", ex)
        return None


def _resolve_or_create_user(info: dict) -> tuple[int | None, str | None, bool]:
    apple_sub = (info.get("sub") or "").strip()
    email = normalize_registration_email(str(info.get("email") or ""))
    if not apple_sub:
        return None, "Apple sign-in did not return a user id.", False

    existing = get_user_by_apple_sub(apple_sub)
    if existing:
        return int(existing["id"]), None, False

    if email and "@" in email:
        by_email = get_user_by_login(email)
        if by_email:
            stored_sub = (by_email.get("apple_sub") or "").strip()
            if stored_sub and stored_sub != apple_sub:
                return None, "This email is linked to a different Apple account.", False
            if not link_user_apple_sub(int(by_email["id"]), apple_sub):
                return None, "Could not link Apple to your account.", False
            from backend.db.users_db import mark_user_email_verified

            mark_user_email_verified(int(by_email["id"]))
            return int(by_email["id"]), None, False

    if not email or "@" not in email:
        # Apple may hide email on subsequent logins; username from sub suffix
        email = f"{apple_sub[:12]}@privaterelay.appleid.com"

    username = _pick_username(email)
    if registration_blocked_by_env_admin(email, username):
        return None, REGISTRATION_ADMIN_BLOCKED_MSG, False
    try:
        uid = save_apple_oauth_user(username, email, apple_sub, role=ROLE_GENERAL)
    except sqlite3.IntegrityError:
        retry = get_user_by_apple_sub(apple_sub) or get_user_by_login(email)
        if retry:
            return int(retry["id"]), None, False
        return None, "Could not create your account.", False
    from backend.db.users_db import mark_user_email_verified

    mark_user_email_verified(int(uid))
    return uid, None, True


def _complete_login(user_id: int, *, is_new_user: bool = False):
    from backend.main import _finalize_app_session, _post_login_redirect
    from backend.utils.roles import is_admin_role

    oauth_post_intent = (session.pop("apple_oauth_post_intent", None) or "").strip().lower()
    sync_env_admin_user_row(user_id)
    session.clear()
    if not _finalize_app_session(user_id):
        return _oauth_login_error("Login failed. Try again.")

    if is_admin_role(session.get("user_role")) or bool(session.get("user_is_premium")):
        return _post_login_redirect()

    if oauth_post_intent == "premium" or is_new_user:
        session["post_auth_intent"] = "premium"
        session["auth_welcome_source"] = "apple_new" if is_new_user else "apple"
    return _post_login_redirect()


@bp.route("")
def start():
    if not apple_oauth_configured():
        return redirect(url_for("login_page"))
    intent = (request.args.get("intent") or "").strip().lower()
    if intent == "premium":
        session["apple_oauth_post_intent"] = "premium"
    state = secrets.token_urlsafe(32)
    session["apple_oauth_state"] = state
    params = {
        "client_id": _client_id(),
        "redirect_uri": _redirect_uri(),
        "response_type": "code",
        "response_mode": "query",
        "scope": "name email",
        "state": state,
    }
    return redirect(f"{APPLE_AUTH_URL}?{urlencode(params)}")


@bp.route("/callback")
def callback():
    if not apple_oauth_configured():
        return redirect(url_for("login_page"))

    ip = _client_ip()
    if not allow_request(f"apple_oauth:{ip}", max_events=_login_rpm(), window_seconds=60.0):
        return _oauth_login_error("Too many sign-in attempts. Try again in a minute.")

    err = (request.args.get("error") or "").strip()
    if err:
        _log.info("Apple OAuth denied: %s", err)
        return _oauth_login_error("Apple sign-in was cancelled.")

    state = (request.args.get("state") or "").strip()
    expected = (session.pop("apple_oauth_state", None) or "").strip()
    if not state or not expected or not secrets.compare_digest(state, expected):
        return _oauth_login_error("Apple sign-in session expired. Try again.")

    code = (request.args.get("code") or "").strip()
    if not code:
        return _oauth_login_error("Apple sign-in did not complete.")

    token_payload = _exchange_code(code)
    if not token_payload:
        return _oauth_login_error("Apple sign-in failed. Try again.")

    id_token = (token_payload.get("id_token") or "").strip()
    if not id_token:
        return _oauth_login_error("Apple sign-in failed. Try again.")

    info = _decode_id_token(id_token)
    if not info:
        return _oauth_login_error("Apple sign-in failed. Try again.")

    user_id, user_err, is_new_user = _resolve_or_create_user(info)
    if user_err:
        return _oauth_login_error(user_err)
    if not user_id:
        return _oauth_login_error("Apple sign-in failed. Try again.")

    return _complete_login(user_id, is_new_user=is_new_user)
