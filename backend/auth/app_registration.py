"""Shared app-user registration for HTML ``/register`` and ``POST /api/auth/register``."""

from __future__ import annotations

import sqlite3

from backend.db.users_db import save_user, user_exists_by_email, user_exists_by_username
from backend.utils.registration_validation import (
    normalize_registration_email,
    normalize_registration_username,
    registration_form_error,
)
from backend.utils.roles import (
    REGISTRATION_ADMIN_BLOCKED_MSG,
    ROLE_GENERAL,
    registration_blocked_by_env_admin,
)

RegisterResult = tuple[int | None, str | None, str | None, bool]


def register_general_app_user(
    username: str,
    email: str,
    password: str,
    *,
    plan: str = "free",
    min_password_len: int = 8,
) -> RegisterResult:
    """
    Create a row in ``users.db`` (same store as the website).

    Returns ``(user_id, None, None, wants_premium)`` on success,
    else ``(None, error_code, message, False)``.
    """
    err = registration_form_error(
        username, email, password, min_password_len=min_password_len
    )
    if err:
        return None, "validation_error", err, False

    username_n = normalize_registration_username(username)
    email_n = normalize_registration_email(email)
    if registration_blocked_by_env_admin(email_n, username_n):
        return None, "registration_blocked", REGISTRATION_ADMIN_BLOCKED_MSG, False

    if user_exists_by_email(email_n) or user_exists_by_username(username_n):
        return (
            None,
            "duplicate_user",
            "That username or email is already registered.",
            False,
        )

    plan_l = (plan or "free").strip().lower()
    if plan_l not in ("free", "premium"):
        plan_l = "free"

    try:
        uid = save_user(username_n, email_n, password, role=ROLE_GENERAL, org_id=None)
    except sqlite3.IntegrityError:
        return (
            None,
            "duplicate_user",
            "That username or email is already registered.",
            False,
        )

    from backend.auth.email_verification import issue_and_send_verification_email

    issue_and_send_verification_email(user_id=int(uid), to_email=email_n)

    return int(uid), None, None, plan_l == "premium"
