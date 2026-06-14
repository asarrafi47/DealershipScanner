"""
SQLite for /dev operator accounts only (separate file from public users.db).

Production (SEC-088): requires DEV_USERS_DB_ENCRYPTION_KEY (≥32 chars) and sqlcipher3.
Set ALLOW_UNENCRYPTED_USER_DB=1 only for local dev/test (forbidden in production).
"""

from __future__ import annotations

import logging
import os
import sqlite3
from typing import Any

from backend.utils.credential_db_encryption import (
    dev_users_db_encryption_key,
    require_encrypted_dev_users_db_in_production,
    sqlcipher_available,
)

logger = logging.getLogger(__name__)

DB_PATH = os.environ.get("DEV_USERS_DB_PATH", "dev_users.db")

_PLAIN_SQLITE_FALLBACK_WARNED = False


def dev_users_db_path() -> str:
    return os.environ.get("DEV_USERS_DB_PATH", DB_PATH)


def _connect_sqlcipher(key: str) -> Any:
    import sqlcipher3 as sc  # type: ignore[import-not-found]

    conn = sc.connect(dev_users_db_path())
    esc = key.replace("'", "''")
    conn.execute(f"PRAGMA key = '{esc}'")
    try:
        conn.execute("SELECT count(*) FROM sqlite_master")
    except Exception as e:
        conn.close()
        raise RuntimeError(
            "Failed to open encrypted dev_users.db (wrong DEV_USERS_DB_ENCRYPTION_KEY or file is not SQLCipher)?"
        ) from e
    return conn


def _plain_sqlite_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(dev_users_db_path(), timeout=15.0)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=5000")
    except sqlite3.Error:
        pass
    return conn


def get_dev_users_conn() -> sqlite3.Connection:
    key = dev_users_db_encryption_key()
    if require_encrypted_dev_users_db_in_production():
        if not key:
            raise RuntimeError(
                "DEV_USERS_DB_ENCRYPTION_KEY is required in production (SEC-088)."
            )
        if not sqlcipher_available():
            raise RuntimeError(
                "sqlcipher3 is required in production to encrypt dev_users.db (SEC-088)."
            )
        return _connect_sqlcipher(key)

    if key and sqlcipher_available():
        try:
            return _connect_sqlcipher(key)
        except RuntimeError:
            logger.warning(
                "DEV_USERS_DB_ENCRYPTION_KEY set but dev_users.db is not SQLCipher (or wrong key); "
                "using plain SQLite for local dev."
            )

    if key and not sqlcipher_available():
        global _PLAIN_SQLITE_FALLBACK_WARNED
        if not _PLAIN_SQLITE_FALLBACK_WARNED:
            _PLAIN_SQLITE_FALLBACK_WARNED = True
            logger.warning(
                "DEV_USERS_DB_ENCRYPTION_KEY is set but sqlcipher3 is not installed; using plain SQLite for "
                "dev_users.db. Install sqlcipher3 for encryption, or unset the key for local dev."
            )
    return _plain_sqlite_conn()
