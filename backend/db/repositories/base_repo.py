"""Inventory DB connection + path resolution (repository base).

``DB_PATH`` is initialized here, but the *writable source of truth* is the facade
module ``backend.db.inventory_db`` (tests monkeypatch ``inventory_db.DB_PATH``).
Every internal reader must go through :func:`_resolve_db_path`.
"""
import os
import sqlite3
import sys
from contextlib import contextmanager
from typing import Any, Iterator

# Default SQLite location for the public scanned inventory. Prefer ``backend/inventory.db``
# when that file exists (common dev layout next to ``backend/incomplete_listings.db``); otherwise
# ``<repo>/inventory.db``. Always set ``INVENTORY_DB_PATH`` in production if ambiguous.
# NOTE: this module lives at backend/db/repositories/, one level deeper than the original
# backend/db/inventory_db.py, so three ``os.pardir`` hops are needed to reach the repo root.
_REPO_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), os.pardir, os.pardir, os.pardir)
)


def _default_inventory_db_path() -> str:
    backend_p = os.path.join(_REPO_ROOT, "backend", "inventory.db")
    root_p = os.path.join(_REPO_ROOT, "inventory.db")
    try:
        if os.path.isfile(backend_p):
            import sqlite3 as _sqlite3
            _conn = _sqlite3.connect(backend_p)
            _has_cars = bool(_conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='cars'"
            ).fetchone())
            _conn.close()
            if _has_cars:
                return backend_p
    except OSError:
        pass
    return root_p


DB_PATH = os.environ.get("INVENTORY_DB_PATH", _default_inventory_db_path())


def _resolve_db_path() -> str:
    """Honor tests that monkeypatch ``backend.db.inventory_db.DB_PATH`` (the facade)."""
    facade = sys.modules.get("backend.db.inventory_db")
    if facade is not None:
        path = getattr(facade, "DB_PATH", None)
        if path is not None:
            return path
    return DB_PATH


def _inventory_sqlite_lock_wait_sec() -> float:
    """Connect ``timeout=`` and basis for ``busy_timeout``; default 60s, min 5s."""
    raw = (os.environ.get("INVENTORY_SQLITE_LOCK_TIMEOUT_SEC") or "60").strip() or "60"
    try:
        return max(5.0, float(raw.split()[0]))
    except (ValueError, IndexError):
        return 60.0


def _sqlite_connect_raw() -> sqlite3.Connection:
    """SQLite inventory connection (WAL + busy_timeout); used only when not on PostgreSQL."""
    lock_s = _inventory_sqlite_lock_wait_sec()
    conn = sqlite3.connect(_resolve_db_path(), timeout=lock_s)
    try:
        conn.execute("PRAGMA busy_timeout=?", (int(max(5000, round(lock_s * 1000))),))
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
    except sqlite3.Error:
        pass
    return conn


def get_conn():
    """
    Inventory DB connection: PostgreSQL (``DATABASE_URL`` / ``INVENTORY_DATABASE_URL``) via
    psycopg3 when configured; otherwise SQLite with WAL and extended lock wait.
    """
    from backend.db.inventory_compat import open_inventory_connection

    return open_inventory_connection()


@contextmanager
def db_conn(*, row_factory: Any = None) -> Iterator[Any]:
    """
    Open an inventory connection and always close it (avoids leaks on error paths).
    When *row_factory* is set, assign ``conn.row_factory = row_factory`` before *yield*.
    """
    conn = get_conn()
    if row_factory is not None:
        conn.row_factory = row_factory
    try:
        yield conn
    finally:
        conn.close()


def _placeholders(lst):
    return ", ".join("?" * len(lst))
