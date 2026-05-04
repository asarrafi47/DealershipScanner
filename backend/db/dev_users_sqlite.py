"""
SQLite for /dev operator accounts only (separate file from public users.db).

Optional encryption: DEV_USERS_DB_ENCRYPTION_KEY + sqlcipher3 (same pattern as users_sqlite).
If the key is set but sqlcipher3 is missing, falls back to plain SQLite with a warning.
"""

from __future__ import annotations

import logging
import os
import sqlite3
from typing import Any

logger = logging.getLogger(__name__)

DB_PATH = os.environ.get("DEV_USERS_DB_PATH", "dev_users.db")

_PLAIN_SQLITE_FALLBACK_WARNED = False


def _sqlcipher_available() -> bool:
    try:
        import sqlcipher3  # noqa: F401
        return True
    except ImportError:
        return False


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


def get_dev_users_conn() -> sqlite3.Connection:
    key = (os.environ.get("DEV_USERS_DB_ENCRYPTION_KEY") or "").strip()
    if key and _sqlcipher_available():
        return _connect_sqlcipher(key)
    if key and not _sqlcipher_available():
        global _PLAIN_SQLITE_FALLBACK_WARNED
        if not _PLAIN_SQLITE_FALLBACK_WARNED:
            _PLAIN_SQLITE_FALLBACK_WARNED = True
            logger.warning(
                "DEV_USERS_DB_ENCRYPTION_KEY is set but sqlcipher3 is not installed; using plain SQLite for "
                "dev_users.db. Install sqlcipher3 for encryption, or unset the key for local dev."
            )
    conn = sqlite3.connect(dev_users_db_path(), timeout=15.0)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=5000")
    except sqlite3.Error:
        pass
    return conn
