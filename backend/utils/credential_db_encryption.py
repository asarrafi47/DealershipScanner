"""At-rest encryption policy for user credential SQLite databases (SEC-088)."""

from __future__ import annotations

import os

from backend.utils.runtime_env import is_production_env

_MIN_KEY_LEN = 32


def users_db_encryption_key() -> str:
    return (os.environ.get("USERS_DB_ENCRYPTION_KEY") or "").strip()


def dev_users_db_encryption_key() -> str:
    return (os.environ.get("DEV_USERS_DB_ENCRYPTION_KEY") or "").strip()


def allow_unencrypted_user_db() -> bool:
    """Dev/test only — forbidden when ``FLASK_ENV=production``."""
    v = (os.environ.get("ALLOW_UNENCRYPTED_USER_DB") or "").strip().lower()
    return v in ("1", "true", "yes", "on")


def sqlcipher_available() -> bool:
    try:
        import sqlcipher3  # noqa: F401

        return True
    except ImportError:
        return False


def assert_credential_db_encryption_config() -> None:
    """Fail fast in production if credential DBs are not configured for SQLCipher."""
    if not is_production_env():
        return
    if allow_unencrypted_user_db():
        raise RuntimeError(
            "ALLOW_UNENCRYPTED_USER_DB=1 is forbidden when FLASK_ENV=production (SEC-088)."
        )
    for env_name, key in (
        ("USERS_DB_ENCRYPTION_KEY", users_db_encryption_key()),
        ("DEV_USERS_DB_ENCRYPTION_KEY", dev_users_db_encryption_key()),
    ):
        if len(key) < _MIN_KEY_LEN:
            raise RuntimeError(
                f"{env_name} must be set to at least {_MIN_KEY_LEN} characters in production "
                "(SEC-088). Use a long random secret (e.g. openssl rand -hex 32)."
            )
    if not sqlcipher_available():
        raise RuntimeError(
            "sqlcipher3 must be installed in production to encrypt users.db and dev_users.db "
            "(SEC-088). See requirements.txt and install libsqlcipher / brew install sqlcipher."
        )


def require_encrypted_users_db_in_production() -> bool:
    """True when production must open users.db via SQLCipher (no plain SQLite)."""
    return is_production_env() and not allow_unencrypted_user_db()


def require_encrypted_dev_users_db_in_production() -> bool:
    return is_production_env() and not allow_unencrypted_user_db()
