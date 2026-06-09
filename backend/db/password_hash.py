"""bcrypt password hashing for users.db and admin_users (SEC-082, SEC-088)."""

from __future__ import annotations

import os

import bcrypt


def bcrypt_rounds() -> int:
    """Work factor for new hashes (default 13). Tunable via BCRYPT_ROUNDS (12–15)."""
    raw = (os.environ.get("BCRYPT_ROUNDS") or "13").strip()
    try:
        n = int(raw)
    except ValueError:
        n = 13
    return max(12, min(n, 15))


def hash_password(plain: str) -> str:
    return bcrypt.hashpw(
        plain.encode("utf-8"),
        bcrypt.gensalt(rounds=bcrypt_rounds()),
    ).decode("utf-8")


def verify_password(plain: str, stored: str) -> bool:
    if not stored:
        return False
    s = stored.encode("utf-8")
    if stored.startswith("$2"):
        try:
            return bcrypt.checkpw(plain.encode("utf-8"), s)
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
    """True if plain matches bcrypt hash; legacy plaintext only outside production (SEC-082)."""
    if not stored:
        return False
    if is_bcrypt_hash(stored):
        return verify_password(plain, stored)
    from backend.utils.runtime_env import is_production_env

    if is_production_env():
        return False
    return plain == stored
