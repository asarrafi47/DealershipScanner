"""Shared username/email/password checks for app and dev registration forms."""

from __future__ import annotations

MAX_USERNAME_LEN = 128
MAX_EMAIL_LEN = 254
MAX_PASSWORD_LEN = 128


def normalize_registration_email(email: str) -> str:
    return (email or "").strip().lower()


def normalize_registration_username(username: str) -> str:
    return (username or "").strip()


def registration_form_error(
    username: str,
    email: str,
    password: str,
    *,
    min_password_len: int,
) -> str | None:
    u = normalize_registration_username(username)
    e = normalize_registration_email(email)
    p = password or ""
    if len(u) < 2:
        return "Username must be at least 2 characters."
    if len(u) > MAX_USERNAME_LEN:
        return f"Username must be at most {MAX_USERNAME_LEN} characters."
    if len(e) < 3 or "@" not in e:
        return "Enter a valid email address."
    if len(e) > MAX_EMAIL_LEN:
        return f"Email must be at most {MAX_EMAIL_LEN} characters."
    if len(p) < min_password_len:
        return f"Password must be at least {min_password_len} characters."
    if len(p) > MAX_PASSWORD_LEN:
        return f"Password must be at most {MAX_PASSWORD_LEN} characters."
    return None
