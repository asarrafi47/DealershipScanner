"""The one Postgres data-access layer (N37.4, docs/ALL_POSTGRES_PLAN_2026_10.md 3.1).

Today every store opens its own ``psycopg.connect`` (or goes through the
SQLite-translating adapter in ``inventory_pg``) and reads rows positionally
(``fetchone()[0]``). This module is the target those call sites move to; it
migrates none of them. It holds:

* one :class:`psycopg_pool.ConnectionPool` per process and DSN (rebuilt after a
  ``fork``, so a gunicorn worker never inherits its parent's sockets),
* ``dict_row`` rows, so a column is read by name and a tuple-vs-dict mix-up
  fails loudly,
* ``%s`` placeholders only (psycopg native; no ``?`` translation),
* the connection string from :func:`backend.db.connect.inventory_dsn`, the same
  precedence every script already uses.

``configure`` gives each pooled connection ``statement_timeout``,
``lock_timeout`` and ``timezone=UTC`` (env ``PG_STATEMENT_TIMEOUT_MS`` /
``PG_LOCK_TIMEOUT_MS``; ``0`` turns one off).

Helpers take ``conn=None``. With a connection they run on it and leave the
transaction to the caller; without one they borrow a pooled connection, run in
their own transaction and commit on success.
"""

from __future__ import annotations

import os
import threading
from contextlib import contextmanager
from typing import Any, Iterable, Iterator, Sequence

from backend.db.connect import inventory_dsn

DEFAULT_STATEMENT_TIMEOUT_MS = 60_000
DEFAULT_LOCK_TIMEOUT_MS = 10_000
DEFAULT_POOL_MIN = 1
DEFAULT_POOL_MAX = 8
DEFAULT_CONNECT_TIMEOUT = 10

_lock = threading.Lock()
_pool: Any = None
_pool_key: tuple[int, str] | None = None  # (pid, dsn) the live pool was built for


def _env_int(name: str, default: int, *, minimum: int = 0) -> int:
    try:
        val = int(float((os.environ.get(name) or "").strip()))
    except ValueError:
        return default
    return val if val >= minimum else default


def _configure(conn: Any) -> None:
    """Per-connection session settings; runs once when the pool opens a connection."""
    stmt = _env_int("PG_STATEMENT_TIMEOUT_MS", DEFAULT_STATEMENT_TIMEOUT_MS)
    lock = _env_int("PG_LOCK_TIMEOUT_MS", DEFAULT_LOCK_TIMEOUT_MS)
    # SET cannot take bind parameters; both values are ints from _env_int.
    conn.execute(f"SET statement_timeout = {int(stmt)}")
    conn.execute(f"SET lock_timeout = {int(lock)}")
    conn.execute("SET timezone = 'UTC'")
    # Leave the connection idle so the first borrower starts a clean transaction.
    conn.commit()


def _build_pool(dsn: str) -> Any:
    from psycopg.rows import dict_row
    from psycopg_pool import ConnectionPool

    kwargs: dict[str, Any] = {"row_factory": dict_row}
    if "connect_timeout" not in dsn.lower():
        kwargs["connect_timeout"] = _env_int("PG_CONNECT_TIMEOUT", DEFAULT_CONNECT_TIMEOUT, minimum=1)
    pool_max = _env_int("PG_POOL_MAX", DEFAULT_POOL_MAX, minimum=1)
    pool_min = min(_env_int("PG_POOL_MIN", DEFAULT_POOL_MIN, minimum=0), pool_max)
    # open=True without wait: building the pool never blocks on the server; the
    # first connection() does, bounded by its timeout.
    return ConnectionPool(
        dsn,
        min_size=pool_min,
        max_size=pool_max,
        kwargs=kwargs,
        configure=_configure,
        open=True,
        name="inventory-pg",
    )


def _close(pool: Any, *, forked: bool = False) -> None:
    try:
        # A pool inherited across fork() shares its parent's sockets; do not
        # close them politely, just stop its workers.
        pool.close(timeout=0 if forked else 5)
    except Exception:
        pass


def get_pool() -> Any:
    """The process-wide pool for the current inventory DSN (created on first use).

    A changed DSN (tests re-point ``INVENTORY_DATABASE_URL`` per database) or a
    new PID closes the old pool and builds another.
    """
    global _pool, _pool_key
    dsn = str(inventory_dsn())
    key = (os.getpid(), dsn)
    with _lock:
        if _pool is not None and _pool_key == key:
            return _pool
        old, old_key = _pool, _pool_key
        _pool = _pool_key = None
        if old is not None:
            _close(old, forked=old_key is not None and old_key[0] != os.getpid())
        _pool = _build_pool(dsn)
        _pool_key = key
        return _pool


def close_pool() -> None:
    """Close the process pool (shutdown, and tests that drop their database)."""
    global _pool, _pool_key
    with _lock:
        old, old_key = _pool, _pool_key
        _pool = _pool_key = None
    if old is not None:
        _close(old, forked=old_key is not None and old_key[0] != os.getpid())


@contextmanager
def tx(*, timeout: float = 30.0) -> Iterator[Any]:
    """A pooled connection in one transaction: commit on success, rollback on error."""
    with get_pool().connection(timeout=timeout) as conn:
        yield conn


@contextmanager
def readonly(*, timeout: float = 30.0) -> Iterator[Any]:
    """A pooled connection whose transaction the server holds READ ONLY."""
    with get_pool().connection(timeout=timeout) as conn:
        conn.execute("SET TRANSACTION READ ONLY")
        yield conn


@contextmanager
def _use(conn: Any | None) -> Iterator[Any]:
    if conn is not None:
        yield conn
    else:
        with tx() as c:
            yield c


Params = Sequence[Any] | dict[str, Any] | None


def one(sql: str, params: Params = None, *, conn: Any | None = None) -> dict[str, Any] | None:
    """First row as a dict, or None when there is none."""
    with _use(conn) as c:
        return c.execute(sql, params).fetchone()


def all(sql: str, params: Params = None, *, conn: Any | None = None) -> list[dict[str, Any]]:  # noqa: A001
    """Every row as a list of dicts."""
    with _use(conn) as c:
        return c.execute(sql, params).fetchall()


def scalar(sql: str, params: Params = None, *, conn: Any | None = None, default: Any = None) -> Any:
    """First column of the first row; *default* when there is no row (a NULL comes back as None)."""
    row = one(sql, params, conn=conn)
    if row is None:
        return default
    return next(iter(row.values()))


def execute(sql: str, params: Params = None, *, conn: Any | None = None) -> int:
    """Run a statement; returns the affected row count."""
    with _use(conn) as c:
        return c.execute(sql, params).rowcount


def execute_many(sql: str, rows: Iterable[Sequence[Any]], *, conn: Any | None = None) -> int:
    """Run *sql* once per parameter row; returns the total affected row count."""
    rows = list(rows)
    if not rows:
        return 0
    with _use(conn) as c:
        with c.cursor() as cur:
            cur.executemany(sql, rows)
            return cur.rowcount


def execute_returning_id(sql: str, params: Params = None, *, conn: Any | None = None) -> Any:
    """Run an ``INSERT ... RETURNING id`` and return that id (the replacement for ``lastrowid``)."""
    if "returning" not in sql.lower():
        raise ValueError("execute_returning_id needs a RETURNING clause")
    row = one(sql, params, conn=conn)
    if row is None:
        return None
    return next(iter(row.values()))


def in_list(column: str, values: Sequence[Any]) -> tuple[str, list[Any]]:
    """``("col = ANY(%s)", [values])`` -- one placeholder however long the list is.

    *column* is code, never user input. An empty list matches nothing, where
    ``IN ()`` would be a syntax error.
    """
    return f"{column} = ANY(%s)", [list(values)]


__all__ = [
    "all",
    "close_pool",
    "execute",
    "execute_many",
    "execute_returning_id",
    "get_pool",
    "in_list",
    "one",
    "readonly",
    "scalar",
    "tx",
]
