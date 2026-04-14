"""bcrypt password hashing for users.db and admin_users."""

from __future__ import annotations

import bcrypt


def hash_password(plain: str) -> str:
    return bcrypt.hashpw(plain.encode("utf-8"), bcrypt.gensalt(rounds=12)).decode("utf-8")


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


def verify_or_legacy(plain: str, stored: str) -> bool:
    """True if plain matches bcrypt hash or legacy plaintext."""
    if not stored:
        return False
    if is_bcrypt_hash(stored):
        return verify_password(plain, stored)
    return plain == stored
