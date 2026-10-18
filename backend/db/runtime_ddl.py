"""
Gate for the runtime DDL that hot-path ``ensure_*`` helpers still carry (P4.6).

``migrations/`` is the inventory schema. A few modules still create their tables
on first use, and on Postgres that DDL is not free even when the objects exist:

* ``CREATE INDEX IF NOT EXISTS`` takes a SHARE lock on its table before it sees
  the index is already there. The dealership page ran ``CREATE TABLE IF NOT
  EXISTS dealer_reviews`` and two such indexes on every request, so one long
  transaction on ``dealer_reviews`` queued every later page view behind it (the
  2026-07-30 lock pile-up pattern).
* ``ALTER TABLE ... ADD COLUMN IF NOT EXISTS`` takes ACCESS EXCLUSIVE even when
  the column exists (``recipe_store`` ran one per process on ``dealer_recipes``).

The rules for those helpers on Postgres:

* :func:`pg_legacy_ddl_needed` is False when the database records every
  migration (:func:`backend.db.schema_version.schema_is_current`, one check per
  process). The helper then issues no statement at all.
* Behind the chain (``INVENTORY_SCHEMA_CHECK=warn``, the default),
  :func:`pg_missing_relations` asks the catalog which objects are absent
  (``to_regclass``: a catalog read, no lock on the relation), and only those are
  created, through :func:`run_pg_legacy_ddl`. It issues
  ``SET LOCAL lock_timeout = '3s'`` before the first statement, so a blocked
  CREATE gives up after 3 s instead of parking every later reader behind it.
  ``SET LOCAL`` ends with the transaction: the connection does not keep the
  short timeout for the work that follows the commit.
* ``INVENTORY_SCHEMA_CHECK=strict``: the gate raises ``SchemaNotMigratedError``
  instead of running anything.

SQLite (pytest only) is unchanged: callers keep running their SQLite DDL.

Wired into ``reviews.store``, ``scanner.recipe_store`` and
``scanner.job_queue``; the remaining per-module helpers wait for P13B.3.
"""
from __future__ import annotations

import re
import sqlite3
from collections.abc import Iterable, Sequence
from typing import Any

LEGACY_DDL_LOCK_TIMEOUT = "3s"
SET_LOCAL_LOCK_TIMEOUT_SQL = f"SET LOCAL lock_timeout = '{LEGACY_DDL_LOCK_TIMEOUT}'"

# Relation and column names are inlined into catalog queries (a raw psycopg
# cursor wants %s, the inventory compat cursor wants ?), so only plain
# lower-case identifiers are accepted.
_IDENT_RE = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")


def _ident(name: str) -> str:
    if not isinstance(name, str) or not _IDENT_RE.match(name):
        raise ValueError(f"not a plain lower-case identifier: {name!r}")
    return name


def is_sqlite_connection(conn: Any) -> bool:
    """True only for a connection known to be SQLite: a raw ``sqlite3.Connection``
    or an inventory compat connection on the sqlite backend (pytest).

    Anything else (the Postgres compat connection, a raw psycopg connection, an
    unrecognised object) reads as Postgres, which takes the gated path.
    """
    if isinstance(conn, sqlite3.Connection):
        return True
    return getattr(conn, "_backend", None) == "sqlite"


def pg_legacy_ddl_needed() -> bool:
    """False when inventory is Postgres and at the ``migrations/`` version.

    True on SQLite (its callers keep their DDL) and on a Postgres database behind
    the chain in warn mode. Strict mode on a behind database raises
    :class:`backend.db.schema_version.SchemaNotMigratedError`.
    """
    from backend.db import inventory_pg

    if not inventory_pg.is_inventory_postgres():
        return True
    from backend.db.schema_version import schema_is_current

    return not schema_is_current()


def _row_values(row: Any) -> list[Any]:
    if row is None:
        return []
    if isinstance(row, dict):  # a dict_row cursor (callers that set row_factory)
        return list(row.values())
    return list(row)


def pg_missing_relations(cur: Any, names: Iterable[str]) -> list[str]:
    """The ``public`` relations (tables, indexes, sequences) in ``names`` that
    ``to_regclass`` cannot find, in the order given. One catalog query; it takes
    no lock on the relations themselves."""
    wanted = [_ident(n) for n in names]
    if not wanted:
        return []
    cols = ", ".join(f"to_regclass('public.{n}') AS r{i}" for i, n in enumerate(wanted))
    cur.execute(f"SELECT {cols}")
    found = _row_values(cur.fetchone())
    if len(found) != len(wanted):
        # An answer of the wrong shape proves nothing: treat every name as absent,
        # and the caller's IF NOT EXISTS DDL decides.
        return list(wanted)
    return [n for n, v in zip(wanted, found) if not v]


def pg_column_exists(cur: Any, table: str, column: str) -> bool:
    """True when ``public.<table>`` has ``<column>`` (``information_schema``)."""
    cur.execute(
        "SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' "
        f"AND table_name = '{_ident(table)}' AND column_name = '{_ident(column)}'"
    )
    return bool(_row_values(cur.fetchone()))


def run_pg_legacy_ddl(cur: Any, stmts: Sequence[str]) -> None:
    """Run legacy DDL on Postgres with ``SET LOCAL lock_timeout`` first.

    The caller owns the transaction (commit or rollback); the timeout ends with it.
    On an autocommit connection ``SET LOCAL`` has no effect, so callers pass a
    regular (transactional) connection. Issues nothing when ``stmts`` is empty.
    """
    todo = [s for s in stmts if s and s.strip()]
    if not todo:
        return
    cur.execute(SET_LOCAL_LOCK_TIMEOUT_SQL)
    for sql in todo:
        cur.execute(sql)
