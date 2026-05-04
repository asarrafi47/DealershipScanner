"""
SQLite connection for users.db (public app users only).

Dev operator accounts live in dev_users.db (see dev_users_sqlite / admin_users_db).

Optional encryption: set USERS_DB_ENCRYPTION_KEY to enable SQLCipher on this file.
Requires: pip install sqlcipher3 (also in requirements.txt).

If the key is set but sqlcipher3 is not installed, connections fall back to plain SQLite
with a warning (typical local dev). Production deployments using encryption must install sqlcipher3.
"""

from __future__ import annotations

import logging
import os
import sqlite3
from typing import Any

logger = logging.getLogger(__name__)

DB_PATH = os.environ.get("USERS_DB_PATH", "users.db")

_PLAIN_SQLITE_FALLBACK_WARNED = False


def _sqlcipher_available() -> bool:
    try:
        import sqlcipher3  # noqa: F401
        return True
    except ImportError:
        return False


def users_db_path() -> str:
    # Read env at call time so tests and operators can swap DB paths without
    # needing to reload every importing module.
    return os.environ.get("USERS_DB_PATH", DB_PATH)


def _connect_sqlcipher(key: str) -> Any:
    import sqlcipher3 as sc  # type: ignore[import-not-found]

    conn = sc.connect(users_db_path())
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
    if key and _sqlcipher_available():
        c = _connect_sqlcipher(key)
        try:
            c.execute("PRAGMA busy_timeout=30000")
        except Exception:
            pass
        return c
    if key and not _sqlcipher_available():
        global _PLAIN_SQLITE_FALLBACK_WARNED
        if not _PLAIN_SQLITE_FALLBACK_WARNED:
            _PLAIN_SQLITE_FALLBACK_WARNED = True
            logger.warning(
                "USERS_DB_ENCRYPTION_KEY is set but sqlcipher3 is not installed; using plain SQLite for "
                "users.db. For encrypted DBs install sqlcipher3 (see requirements.txt), or unset the key "
                "for local dev with a non-encrypted file."
            )
    # timeout: seconds to wait on connect lock; busy_timeout: ms to wait on each statement
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
