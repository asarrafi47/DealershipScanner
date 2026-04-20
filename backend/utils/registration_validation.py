"""Shared username/email/password checks for app and dev registration forms."""

from __future__ import annotations


def registration_form_error(
    username: str,
    email: str,
    password: str,
    *,
    min_password_len: int,
) -> str | None:
    u = (username or "").strip()
    e = (email or "").strip()
    p = password or ""
    if len(u) < 2:
        return "Username must be at least 2 characters."
    if len(e) < 3 or "@" not in e:
        return "Enter a valid email address."
    if len(p) < min_password_len:
        return f"Password must be at least {min_password_len} characters."
    return None
