"""Process environment helpers for security-sensitive behavior."""

from __future__ import annotations

import os


def is_production_env() -> bool:
    """True when the app is configured for production deployment.

    Fails safe on hosted deploys: when FLASK_ENV/ENV is unset but the process is
    running on Railway, assume production — dev-only affordances (plaintext
    legacy passwords, seeded admin credentials) must not activate just because
    an env var was forgotten in the dashboard.
    """
    v = (os.environ.get("FLASK_ENV") or os.environ.get("ENV") or "").strip().lower()
    if v:
        return v == "production"
    return bool(
        os.environ.get("RAILWAY_PROJECT_ID")
        or os.environ.get("RAILWAY_ENVIRONMENT_NAME")
        or os.environ.get("RAILWAY_ENVIRONMENT")
    )


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
