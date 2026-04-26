from __future__ import annotations

import hashlib
import hmac
import secrets
import time


def new_otp_code() -> str:
    return f"{secrets.randbelow(1_000_000):06d}"


def hash_code(code: str, *, salt: str) -> str:
    raw = (code or "").strip()
    s = (salt or "").strip()
    return hashlib.sha256((s + ":" + raw).encode("utf-8")).hexdigest()


def issue_session_otp(session: dict, *, kind: str, ttl_seconds: int = 600) -> str:
    """
    Store a 6-digit OTP in the session (hashed) with an expiry.
    Returns the plain code so the caller can deliver it (e.g. email).
    """
    code = new_otp_code()
    salt = secrets.token_urlsafe(16)
    now = int(time.time())
    session[f"{kind}_otp_salt"] = salt
    session[f"{kind}_otp_hash"] = hash_code(code, salt=salt)
    session[f"{kind}_otp_exp"] = now + int(ttl_seconds)
    return code


def verify_session_otp(session: dict, *, kind: str, code: str) -> bool:
    try:
        exp = int(session.get(f"{kind}_otp_exp") or 0)
    except (TypeError, ValueError):
        exp = 0
    if exp <= 0 or int(time.time()) > exp:
        return False
    salt = str(session.get(f"{kind}_otp_salt") or "")
    expected = str(session.get(f"{kind}_otp_hash") or "")
    got = hash_code(code, salt=salt)
    return bool(expected) and hmac.compare_digest(expected, got)


def clear_session_otp(session: dict, *, kind: str) -> None:
    for k in (f"{kind}_otp_salt", f"{kind}_otp_hash", f"{kind}_otp_exp"):
        session.pop(k, None)

