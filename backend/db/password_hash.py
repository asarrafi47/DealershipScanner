"""bcrypt password hashing for users.db and admin_users (SEC-082, SEC-088)."""

from __future__ import annotations

import hashlib
import os

import bcrypt

_BCRYPT_MAX_BYTES = 72


def bcrypt_rounds() -> int:
    """Work factor for new hashes (default 13). Tunable via BCRYPT_ROUNDS (12–15)."""
    raw = (os.environ.get("BCRYPT_ROUNDS") or "13").strip()
    try:
        n = int(raw)
    except ValueError:
        n = 13
    return max(12, min(n, 15))


def _bcrypt_input(plain: str) -> bytes:
    """Encode `plain` for bcrypt, pre-hashing if it exceeds bcrypt's 72-byte limit.

    bcrypt>=4.1 raises ValueError instead of truncating, so passwords whose
    UTF-8 encoding is longer than 72 bytes are first collapsed with SHA-256
    (hex digest, 64 bytes) before being handed to bcrypt. This mirrors
    Django's BCryptSHA256PasswordHasher and must be applied identically when
    hashing and when verifying.
    """
    encoded = plain.encode("utf-8")
    if len(encoded) <= _BCRYPT_MAX_BYTES:
        return encoded
    return hashlib.sha256(encoded).hexdigest().encode("utf-8")


def hash_password(plain: str) -> str:
    return bcrypt.hashpw(
        _bcrypt_input(plain),
        bcrypt.gensalt(rounds=bcrypt_rounds()),
    ).decode("utf-8")


def verify_password(plain: str, stored: str) -> bool:
    if not stored:
        return False
    s = stored.encode("utf-8")
    if stored.startswith("$2"):
        try:
            return bcrypt.checkpw(_bcrypt_input(plain), s)
        except ValueError:
            return False
    return False


def is_bcrypt_hash(stored: str) -> bool:
    return bool(stored) and stored.startswith("$2")


def bcrypt_cost(stored: str) -> int | None:
    if not is_bcrypt_hash(stored):
        return None
    try:
        return int(stored.split("$")[2])
    except (IndexError, ValueError):
        return None


def password_needs_rehash(stored: str) -> bool:
    """True for legacy plaintext or bcrypt below the configured work factor."""
    if not is_bcrypt_hash(stored):
        return True
    cost = bcrypt_cost(stored)
    if cost is None:
        return True
    return cost < bcrypt_rounds()


def verify_or_legacy(plain: str, stored: str) -> bool:
    """True if plain matches bcrypt hash (SEC-082).

    Legacy plaintext rows verify only outside production AND with
    ALLOW_LEGACY_PLAINTEXT_PASSWORDS=1 set explicitly; one successful login
    rehashes them to bcrypt.
    """
    if not stored:
        return False
    if is_bcrypt_hash(stored):
        return verify_password(plain, stored)
    from backend.utils.runtime_env import is_production_env

    if is_production_env():
        return False
    optin = (os.environ.get("ALLOW_LEGACY_PLAINTEXT_PASSWORDS") or "").strip().lower()
    if optin not in ("1", "true", "yes", "on"):
        return False
    import hmac

    return hmac.compare_digest(plain.encode("utf-8"), stored.encode("utf-8"))
