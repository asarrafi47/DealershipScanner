"""Process environment helpers for security-sensitive behavior."""

from __future__ import annotations

import os


def is_production_env() -> bool:
    """True when the app is configured for production deployment."""
    v = (os.environ.get("FLASK_ENV") or os.environ.get("ENV") or "").strip().lower()
    return v == "production"


def session_cookie_secure_default() -> bool:
    """
    Secure session cookies when in production, or when explicitly forced.
    Set SESSION_COOKIE_SECURE=0 to allow cookies over HTTP (e.g. local prod testing).
    """
    o = (os.environ.get("SESSION_COOKIE_SECURE") or "").strip().lower()
    if o in ("0", "false", "no", "off"):
        return False
    if o in ("1", "true", "yes", "on"):
        return True
    return is_production_env()
