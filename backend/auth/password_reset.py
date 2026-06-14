"""Password reset flow (Resend scaffold, B2)."""

from __future__ import annotations

import hashlib
import logging
import os
import secrets
import time
from urllib.parse import urlencode

from backend.db.users_db import (
    clear_user_password_reset_token,
    get_user_id_by_password_reset_token_hash,
    get_user_by_login,
    get_user_profile,
    reset_user_password,
    set_user_password_reset_token,
)
from backend.utils.mfa_delivery import send_transactional_email
from backend.utils.runtime_env import is_production_env

_log = logging.getLogger(__name__)

_RESET_TTL_SECONDS = 3600


def password_reset_enabled() -> bool:
    return (os.environ.get("PASSWORD_RESET_ENABLED") or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def _reset_pepper() -> str:
    return (
        (os.environ.get("PASSWORD_RESET_PEPPER") or "").strip()
        or (os.environ.get("EMAIL_VERIFY_PEPPER") or "").strip()
        or (os.environ.get("FLASK_SECRET_KEY") or "").strip()
        or "dev-password-reset-pepper"
    )


def hash_reset_token(token: str) -> str:
    raw = (token or "").strip()
    return hashlib.sha256(f"{_reset_pepper()}:password_reset:{raw}".encode("utf-8")).hexdigest()


def _public_base_url() -> str:
    base = (
        (os.environ.get("PUBLIC_BASE_URL") or os.environ.get("MFA_QR_BASE_URL") or "").strip().rstrip("/")
    )
    if base:
        return base
    if is_production_env():
        return ""
    return "http://127.0.0.1:8000"


def build_reset_password_url(token: str) -> str:
    base = _public_base_url()
    q = urlencode({"token": token})
    if not base:
        return f"/reset-password?{q}"
    return f"{base}/reset-password?{q}"


def request_password_reset(*, login_input: str) -> bool:
    """
    Issue reset token and send email when account exists.
    Always returns True to avoid email enumeration.
    """
    if not password_reset_enabled():
        return True
    li = (login_input or "").strip()
    if not li:
        return True
    user = get_user_by_login(li)
    if not user:
        return True
    if (user.get("google_sub") or "").strip() or (user.get("apple_sub") or "").strip():
        return True
    uid = int(user["id"])
    email = (user.get("email") or "").strip().lower()
    if not email or "@" not in email:
        return True
    token = secrets.token_urlsafe(32)
    expires_at = int(time.time()) + _RESET_TTL_SECONDS
    if not set_user_password_reset_token(uid, hash_reset_token(token), expires_at):
        return True
    link = build_reset_password_url(token)
    subject = "Reset your Sarrafi Collection password"
    text = (
        "We received a request to reset your password.\n\n"
        f"Open this link to choose a new password (expires in 1 hour):\n\n"
        f"{link}\n\n"
        "If you did not request this, you can ignore this message.\n"
    )
    try:
        send_transactional_email(
            to_email=email,
            subject=subject,
            text=text,
            log_surface="password_reset",
        )
    except Exception as e:  # noqa: BLE001
        _log.warning("password reset send failed user_id=%s: %s", uid, type(e).__name__)
    return True


def complete_password_reset(*, token: str, new_password: str) -> tuple[bool, str]:
    raw = (token or "").strip()
    if not raw or len(raw) > 256:
        return False, "Invalid or expired reset link."
    uid = get_user_id_by_password_reset_token_hash(hash_reset_token(raw))
    if not uid:
        return False, "Invalid or expired reset link."
    from backend.utils.registration_validation import registration_form_error

    profile = get_user_profile(uid) or {}
    fmt_err = registration_form_error(
        profile.get("username") or "user",
        profile.get("email") or "user@local",
        new_password,
        min_password_len=8,
    )
    if fmt_err:
        return False, fmt_err
    err = reset_user_password(uid, new_password)
    if err:
        return False, err
    clear_user_password_reset_token(uid)
    return True, "Your password has been updated. You can log in now."
