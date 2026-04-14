"""
SQLite connection for users.db (public users + admin_users).

Optional encryption: set USERS_DB_ENCRYPTION_KEY to enable SQLCipher on this file.
Requires: pip install sqlcipher3

Plain SQLite is used when the env var is unset (default dev).
"""

from __future__ import annotations

import os
import sqlite3
from typing import Any

DB_PATH = os.environ.get("USERS_DB_PATH", "users.db")


def _connect_sqlcipher(key: str) -> Any:
    try:
        import sqlcipher3 as sc  # type: ignore[import-not-found]
    except ImportError as e:  # pragma: no cover - optional dep
        raise RuntimeError(
            "USERS_DB_ENCRYPTION_KEY is set but the sqlcipher3 package is not installed. "
            "Install with: pip install sqlcipher3"
        ) from e
    conn = sc.connect(DB_PATH)
    # SQLCipher expects passphrase via PRAGMA key
    esc = key.replace("'", "''")
    conn.execute(f"PRAGMA key = '{esc}'")
    try:
        conn.execute("SELECT count(*) FROM sqlite_master")
    except Exception as e:
        conn.close()
        raise RuntimeError(
            "Failed to open encrypted users.db (wrong USERS_DB_ENCRYPTION_KEY or file is not SQLCipher)?"
        ) from e
    return conn


def get_users_conn() -> sqlite3.Connection:
    key = (os.environ.get("USERS_DB_ENCRYPTION_KEY") or "").strip()
    if key:
        return _connect_sqlcipher(key)
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
    except sqlite3.Error:
        pass
    return conn
