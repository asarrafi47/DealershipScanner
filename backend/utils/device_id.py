"""Device cookie ``sc_dev`` and keyed hashes for chat and phone limits (card D1).

``sc_dev`` is a random 128-bit value in a signed, HttpOnly, SameSite=Lax cookie kept 400
days. It is set only on phone and chat routes (not on every page). Clearing cookies gives a
new device, which is why IP, phone and the site-wide cap exist too. Hashes are
HMAC-SHA256 with ``PHONE_HASH_PEPPER``; the pepper's value is never logged.

``sc_dev`` is a rate-limit key ONLY: it never skips a code, never marks a browser trusted and
never enters a session or authorization decision. Missing or tampered means "new device" and
the phone, address and site caps still apply.

The pepper, the dev pepper, the minimum length, ``PepperMissing`` and every hash live in
``backend/auth/phone/numbers.py`` (card S1); this module only delegates to them.
"""

from __future__ import annotations

import hashlib
import hmac
import os
import re
import secrets

from flask import Flask, Request, Response, g, request

from backend.auth.phone import numbers
from backend.utils.runtime_env import session_cookie_secure_default

COOKIE_NAME = "sc_dev"
COOKIE_MAX_AGE_SECONDS = 400 * 24 * 60 * 60
_SALT = "sc-dev-cookie-v1"

# Paths that get the cookie: the phone login pages and the four chat routes.
_PATH_PATTERNS = (
    re.compile(r"^/login(/|$)"),  # includes /login/phone and /login/claim
    re.compile(r"^/staff/login/?$"),
    re.compile(r"^/account/phone(/|$)"),
    re.compile(r"^/auth/phone(/|$)"),
    re.compile(r"^/api/auth/phone(/|$)"),
    re.compile(r"^/api/car/[^/]+/chat/?$"),
    re.compile(r"^/api/car/[^/]+/narrate/?$"),
    re.compile(r"^/api/compare/chat/?$"),
    re.compile(r"^/api/ai/chat/?$"),
)

_TRUTHY = ("1", "true", "yes", "on")


PepperMissing = numbers.PepperMissing  # the one class (backend/auth/phone/numbers.py)


def wants_device_cookie(path: str) -> bool:
    return any(p.search(path or "") for p in _PATH_PATTERNS)


def pepper() -> bytes:
    """The hash pepper (delegates to the S1 reader: production without it raises)."""
    return numbers.pepper_bytes()


def assert_pepper_if_needed() -> None:
    """Boot check: fail only when a feature that needs the pepper is on (phone login).

    The chat gate also needs the pepper but does not stop the boot: it fails closed (chat
    answers "unavailable", the rest of the site runs) while the variable is unset, so
    production keeps booting until the owner sets it.
    """
    if (os.environ.get("AUTH_PHONE_LOGIN_ENABLED") or "").strip().lower() in _TRUTHY:
        pepper()


def hmac_hash(kind: str, value: str) -> str:
    """Keyed hash of ``value`` in the namespace ``kind`` (the S1 helper)."""
    return numbers.hash_value(kind, value)


def device_hash(device_id: str) -> str:
    return numbers.hash_device(device_id)


def _sign(device_id: str) -> str:
    from flask import current_app

    key = (current_app.secret_key or "").encode("utf-8")
    mac = hmac.new(key, f"{_SALT}:{device_id}".encode("utf-8"), hashlib.sha256).hexdigest()
    return f"{device_id}.{mac}"


def read_device_id(req: Request) -> str | None:
    """The device id in a valid signed cookie, or None (missing, malformed or tampered)."""
    raw = req.cookies.get(COOKIE_NAME) or ""
    if not re.fullmatch(r"[0-9a-f]{32}\.[0-9a-f]{64}", raw):
        return None  # also keeps non-ASCII text away from compare_digest (it raises TypeError)
    device_id = raw.partition(".")[0]
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
