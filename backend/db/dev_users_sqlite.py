"""
SQLite for /dev operator accounts only (separate file from public users.db).

Optional encryption: DEV_USERS_DB_ENCRYPTION_KEY + sqlcipher3 (same pattern as users_sqlite).
"""

from __future__ import annotations

import os
import sqlite3
from typing import Any

DB_PATH = os.environ.get("DEV_USERS_DB_PATH", "dev_users.db")


def dev_users_db_path() -> str:
    return os.environ.get("DEV_USERS_DB_PATH", DB_PATH)


def _connect_sqlcipher(key: str) -> Any:
    try:
        import sqlcipher3 as sc  # type: ignore[import-not-found]
    except ImportError as e:  # pragma: no cover - optional dep
        raise RuntimeError(
            "DEV_USERS_DB_ENCRYPTION_KEY is set but the sqlcipher3 package is not installed. "
            "Install with: pip install sqlcipher3"
        ) from e
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
    if key:
        return _connect_sqlcipher(key)
    conn = sqlite3.connect(dev_users_db_path(), timeout=15.0)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=5000")
    except sqlite3.Error:
        pass
    return conn
