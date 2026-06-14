"""
SQLite connection for users.db (public app users only).

Dev operator accounts live in dev_users.db (see dev_users_sqlite / admin_users_db).

Production (SEC-088): requires USERS_DB_ENCRYPTION_KEY (≥32 chars) and sqlcipher3.
Set ALLOW_UNENCRYPTED_USER_DB=1 only for local dev/test (forbidden in production).
"""

from __future__ import annotations

import logging
import os
import sqlite3
from typing import Any

from backend.utils.credential_db_encryption import (
    require_encrypted_users_db_in_production,
    sqlcipher_available,
    users_db_encryption_key,
)

logger = logging.getLogger(__name__)

DB_PATH = os.environ.get("USERS_DB_PATH", "users.db")

_PLAIN_SQLITE_FALLBACK_WARNED = False


def users_db_path() -> str:
    return os.environ.get("USERS_DB_PATH", DB_PATH)


def _connect_sqlcipher(key: str) -> Any:
    import sqlcipher3 as sc  # type: ignore[import-not-found]

    conn = sc.connect(users_db_path())
    esc = key.replace("'", "''")
    conn.execute(f"PRAGMA key = '{esc}'")
    try:
        conn.execute("SELECT count(*) FROM sqlite_master")
    except Exception as e:
        conn.close()
        raise RuntimeError(
            "Failed to open encrypted users.db (wrong USERS_DB_ENCRYPTION_KEY or file is not SQLCipher)?"
        ) from e
    try:
        conn.execute("PRAGMA busy_timeout=30000")
    except Exception:
        pass
    return conn


def _plain_sqlite_conn() -> sqlite3.Connection:
    connect_timeout = float(
        (os.environ.get("USERS_DB_CONNECT_TIMEOUT_S") or "30.0").strip() or "30.0"
    )
    busy_ms = int(
        (os.environ.get("USERS_DB_BUSY_TIMEOUT_MS") or "30000").strip() or "30000"
    )
    conn = sqlite3.connect(
        users_db_path(),
        timeout=connect_timeout,
        check_same_thread=False,
    )
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute(f"PRAGMA busy_timeout={busy_ms}")
    except sqlite3.Error:
        pass
    return conn


def get_users_conn() -> sqlite3.Connection:
    key = users_db_encryption_key()
    if require_encrypted_users_db_in_production():
        if not key:
            raise RuntimeError(
                "USERS_DB_ENCRYPTION_KEY is required in production (SEC-088)."
            )
        if not sqlcipher_available():
            raise RuntimeError(
                "sqlcipher3 is required in production to encrypt users.db (SEC-088)."
            )
        return _connect_sqlcipher(key)

    if key and sqlcipher_available():
        try:
            return _connect_sqlcipher(key)
        except RuntimeError:
            logger.warning(
                "USERS_DB_ENCRYPTION_KEY set but users.db is not SQLCipher (or wrong key); "
                "using plain SQLite for local dev."
            )

    if key and not sqlcipher_available():
        global _PLAIN_SQLITE_FALLBACK_WARNED
        if not _PLAIN_SQLITE_FALLBACK_WARNED:
            _PLAIN_SQLITE_FALLBACK_WARNED = True
            logger.warning(
                "USERS_DB_ENCRYPTION_KEY is set but sqlcipher3 is not installed; using plain SQLite for "
                "users.db. For encrypted DBs install sqlcipher3 (see requirements.txt), or unset the key "
                "for local dev with a non-encrypted file."
            )
    return _plain_sqlite_conn()
