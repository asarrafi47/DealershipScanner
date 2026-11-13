"""Device cookie ``sc_dev`` and keyed hashes for chat and phone limits (card D1).

``sc_dev`` is a random 128-bit value in a signed, HttpOnly, SameSite=Lax cookie kept 400
days. It is set only on phone and chat routes (not on every page). Clearing cookies gives a
new device, which is why IP, phone and the site-wide cap exist too. Hashes are
HMAC-SHA256 with ``PHONE_HASH_PEPPER``; the pepper's value is never logged.
"""

from __future__ import annotations

import hashlib
import hmac
import os
import re
import secrets

from flask import Flask, Request, Response, g, request

from backend.utils.runtime_env import is_production_env, session_cookie_secure_default

COOKIE_NAME = "sc_dev"
COOKIE_MAX_AGE_SECONDS = 400 * 24 * 60 * 60
_SALT = "sc-dev-cookie-v1"
_DEV_PEPPER = "dev-only-pepper-not-for-production"

# Paths that get the cookie: the phone login pages and the four chat routes.
_PATH_PATTERNS = (
    re.compile(r"^/auth/phone(/|$)"),
    re.compile(r"^/api/auth/phone(/|$)"),
    re.compile(r"^/api/car/[^/]+/chat/?$"),
    re.compile(r"^/api/car/[^/]+/narrate/?$"),
    re.compile(r"^/api/compare/chat/?$"),
    re.compile(r"^/api/ai/chat/?$"),
)

_TRUTHY = ("1", "true", "yes", "on")


class PepperMissing(RuntimeError):
    """PHONE_HASH_PEPPER is unset in production."""


def wants_device_cookie(path: str) -> bool:
    return any(p.search(path or "") for p in _PATH_PATTERNS)


def pepper() -> bytes:
    """The hash pepper. Production without it raises; development uses a fixed dev value."""
    value = (os.environ.get("PHONE_HASH_PEPPER") or "").strip()
    if value:
        return value.encode("utf-8")
    if is_production_env():
        raise PepperMissing("PHONE_HASH_PEPPER must be set in production")
    return _DEV_PEPPER.encode("utf-8")


def assert_pepper_if_needed() -> None:
    """Boot check: fail only when a feature that needs the pepper is on (phone login)."""
    if (os.environ.get("AUTH_PHONE_LOGIN_ENABLED") or "").strip().lower() in _TRUTHY:
        pepper()


def hmac_hash(kind: str, value: str) -> str:
    """Keyed hash of ``value`` in the namespace ``kind`` (hex SHA-256 HMAC)."""
    msg = f"{kind}:{value}".encode("utf-8")
    return hmac.new(pepper(), msg, hashlib.sha256).hexdigest()


def device_hash(device_id: str) -> str:
    return hmac_hash("device", device_id)


def _sign(device_id: str) -> str:
    from flask import current_app

    key = (current_app.secret_key or "").encode("utf-8")
    mac = hmac.new(key, f"{_SALT}:{device_id}".encode("utf-8"), hashlib.sha256).hexdigest()
    return f"{device_id}.{mac}"


def read_device_id(req: Request) -> str | None:
    """The device id in a valid signed cookie, or None (missing, malformed or tampered)."""
    raw = req.cookies.get(COOKIE_NAME) or ""
    device_id, _, _mac = raw.partition(".")
    if not re.fullmatch(r"[0-9a-f]{32}", device_id):
        return None
    if not hmac.compare_digest(_sign(device_id), raw):
        return None
    return device_id


def ensure_device_cookie(req: Request | None = None) -> str:
    """The request's device id; makes a new one (to be set on the response) if needed."""
    req = req or request
    existing = getattr(g, "device_id", None)
    if existing:
        return existing
    device_id = read_device_id(req)
    if device_id is None:
        device_id = secrets.token_hex(16)
        g.device_cookie_new = True
    g.device_id = device_id
    return device_id


def _device_cookie_before() -> None:
    if wants_device_cookie(request.path):
        ensure_device_cookie(request)


def _device_cookie_after(response: Response) -> Response:
    if getattr(g, "device_cookie_new", False) and getattr(g, "device_id", None):
        response.set_cookie(
            COOKIE_NAME,
            _sign(g.device_id),
            max_age=COOKIE_MAX_AGE_SECONDS,
            httponly=True,
            samesite="Lax",
            secure=session_cookie_secure_default(),
            path="/",
        )
    return response


def register_device_cookie(app: Flask) -> None:
    app.before_request(_device_cookie_before)
    app.after_request(_device_cookie_after)
