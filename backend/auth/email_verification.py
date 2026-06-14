"""Email verification for password-registered app users (Resend scaffold)."""

from __future__ import annotations

import hashlib
import logging
import os
import secrets
from urllib.parse import urlencode

from backend.db.users_db import (
    clear_user_email_verify_token,
    get_user_email_verification_state,
    set_user_email_verify_token,
    mark_user_email_verified,
    get_user_id_by_email_verify_token_hash,
)
from backend.utils.mfa_delivery import send_transactional_email
from backend.utils.runtime_env import is_production_env

_log = logging.getLogger(__name__)

_VERIFY_TTL_HOURS = 48


def email_verification_enabled() -> bool:
    return (os.environ.get("EMAIL_VERIFICATION_ENABLED") or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def _verify_pepper() -> str:
    return (
        (os.environ.get("EMAIL_VERIFY_PEPPER") or "").strip()
        or (os.environ.get("FLASK_SECRET_KEY") or "").strip()
        or "dev-email-verify-pepper"
    )


def hash_verify_token(token: str) -> str:
    raw = (token or "").strip()
    return hashlib.sha256(f"{_verify_pepper()}:email_verify:{raw}".encode("utf-8")).hexdigest()


def user_needs_email_verification(user_id: int) -> bool:
    if not email_verification_enabled():
        return False
    state = get_user_email_verification_state(user_id)
    if not state:
        return False
    if state.get("email_verified_at"):
        return False
    if (state.get("google_sub") or "").strip() or (state.get("apple_sub") or "").strip():
        return False
    return bool((state.get("email_verify_token_hash") or "").strip())


def _public_base_url() -> str:
    base = (
        (os.environ.get("PUBLIC_BASE_URL") or os.environ.get("MFA_QR_BASE_URL") or "").strip().rstrip("/")
    )
    if base:
        return base
    if is_production_env():
        return ""
    return "http://127.0.0.1:8000"


def build_verify_email_url(token: str) -> str:
    base = _public_base_url()
    q = urlencode({"token": token})
    if not base:
        return f"/verify-email?{q}"
    return f"{base}/verify-email?{q}"


def issue_and_send_verification_email(*, user_id: int, to_email: str) -> bool:
    """Create a verify token and send email. Returns True when send attempted/succeeded."""
    if not email_verification_enabled():
        return False
    email = (to_email or "").strip().lower()
    if not email or "@" not in email:
        return False
    token = secrets.token_urlsafe(32)
    token_hash = hash_verify_token(token)
    if not set_user_email_verify_token(int(user_id), token_hash):
        return False
    link = build_verify_email_url(token)
    subject = "Verify your Sarrafi Collection email"
    text = (
        "Thanks for signing up.\n\n"
        f"Verify your email by opening this link (expires in {_VERIFY_TTL_HOURS} hours):\n\n"
        f"{link}\n\n"
        "If you did not create this account, you can ignore this message.\n"
    )
    try:
        send_transactional_email(
            to_email=email,
            subject=subject,
            text=text,
            log_surface="email_verify",
        )
    except Exception as e:  # noqa: BLE001
        _log.warning("email verification send failed user_id=%s: %s", user_id, type(e).__name__)
        return False
    return True


def verify_email_token(token: str) -> tuple[bool, str]:
    """Returns (ok, message)."""
    raw = (token or "").strip()
    if not raw or len(raw) > 256:
        return False, "Invalid or expired verification link."
    uid = get_user_id_by_email_verify_token_hash(hash_verify_token(raw))
    if not uid:
        return False, "Invalid or expired verification link."
    if not mark_user_email_verified(uid):
        return False, "Could not verify your email. Try again or request a new link."
    clear_user_email_verify_token(uid)
    return True, "Your email is verified. You can close this page and continue using Sarrafi Collection."
