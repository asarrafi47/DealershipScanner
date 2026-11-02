"""
Is this inventory Postgres at the schema the code expects?

The versioned chain in ``migrations/`` is the one schema source for inventory
Postgres. Runtime code used to re-create that schema on every process start
(``inventory_pg.init_postgres_inventory`` and ~8 per-module ``ensure_*``
helpers): 17+ tables of hand-mirrored ``CREATE TABLE IF NOT EXISTS`` that had to
be kept in sync with the migrations by hand, and whose ``CREATE INDEX`` passes
caused the 2026-07-30 lock pile-up. This module replaces that with one cheap
question -- "is ``schema_migrations`` at the highest ``V<NNN>`` file on disk?" --
asked once per process.

Behaviour (env):

* ``INVENTORY_SCHEMA_CHECK=warn`` (default): a database behind the chain logs a
  loud warning naming the operator command, and callers fall back to the legacy
  runtime DDL, so behaviour on a behind-the-chain database is exactly what it was
  before. Default until strict is switched on for every service (P13A.4). Each
  new migration leaves every live database behind until an operator applies it:
  the P0A.2 census (2026-10-08, ``docs/db_layer/SCHEMA_LEDGER_2026_10.md``) found
  prod and local ``cars`` both recording V001-V025 with no checksum drift, and
  prod gets each later migration by hand before the deploy that needs it.
* ``INVENTORY_SCHEMA_CHECK=strict``: a behind-the-chain database raises
  :class:`SchemaNotMigratedError` instead (opt in once every database is migrated).
  The error propagates: ``schema_repo.init_inventory_db`` (web, scanner and
  worker start-up) and the scanner's per-upsert ``_ensure_schema`` re-raise it
  rather than logging it as a failed re-assert.
* ``INVENTORY_AUTO_MIGRATE=1``: dev convenience -- run
  ``python -m backend.scripts.migrate --apply`` in-process first, then check.

A database AT the expected version skips all runtime DDL: the check is two
catalog reads.
"""

from __future__ import annotations

import logging
import os
import re
import threading
from pathlib import Path
from typing import Any

_log = logging.getLogger(__name__)

MIGRATIONS_DIR = Path(__file__).resolve().parents[2] / "migrations"
_FILENAME_RE = re.compile(r"^V(\d+)__.+\.sql$")

MIGRATE_COMMAND = "python -m backend.scripts.migrate --apply"
# A populated database that predates the chain (it has ``cars`` but no
# ``schema_migrations``): record V001 without running it, then apply the rest.
BASELINE_V001_COMMAND = "python -m backend.scripts.migrate --baseline 1 --apply"
# Kept as an alias. V019 re-creates objects V001 already has; since P4.2 a plain
# ``migrate --apply`` records it without running it when that is safe (a fresh
# build, or all 48 shared objects present) and refuses otherwise, so the old
# unconditional ``--baseline 19`` hint is gone. See migrations/README.md.
BASELINE_V019_COMMAND = MIGRATE_COMMAND

_state_lock = threading.Lock()
_current: bool | None = None  # None = not checked yet in this process


class SchemaNotMigratedError(RuntimeError):
    """Inventory Postgres is behind ``migrations/`` and strict checking is on."""


def expected_schema_version(directory: Path | None = None) -> int:
    """Highest ``V<NNN>`` in ``migrations/`` (0 when the directory is empty)."""
    d = directory or MIGRATIONS_DIR
    best = 0
    try:
        names = os.listdir(d)
    except OSError:
        return 0
    for name in names:
        m = _FILENAME_RE.match(name)
        if m:
            best = max(best, int(m.group(1)))
    return best


def schema_check_mode() -> str:
    """``warn`` (default) or ``strict``; anything unrecognised reads as ``warn``."""
    raw = (os.environ.get("INVENTORY_SCHEMA_CHECK") or "").strip().lower()
    return "strict" if raw == "strict" else "warn"


def auto_migrate_enabled() -> bool:
    return (os.environ.get("INVENTORY_AUTO_MIGRATE") or "").strip().lower() in (
        "1", "true", "yes", "on",
    )


def applied_schema_versions(cur: Any) -> set[int] | None:
    """Versions recorded in ``schema_migrations``; None when the table is absent."""
    cur.execute("SELECT to_regclass('public.schema_migrations')")
    row = cur.fetchone()
    val = row[0] if row is not None and not isinstance(row, dict) else (row or {}).get("to_regclass")
    if not val:
        return None
    cur.execute("SELECT version FROM public.schema_migrations")
    out: set[int] = set()
    for r in cur.fetchall():
        out.add(int(r["version"] if isinstance(r, dict) else r[0]))
    return out


def inventory_tables_present(cur: Any) -> bool:
    """True when ``public.cars`` exists: a populated database, not an empty one."""
    cur.execute("SELECT to_regclass('public.cars')")
    row = cur.fetchone()
    val = row[0] if row is not None and not isinstance(row, dict) else (row or {}).get("to_regclass")
    return bool(val)


def describe_gap(
    applied: set[int] | None, expected: int, *, has_cars: bool | None = None
) -> str | None:
    """
    None when ``applied`` covers 1..expected; else a one-line operator message.

    ``has_cars`` (``to_regclass('public.cars')``, read only when there is no
    ``schema_migrations``) picks the hint: an empty database gets the fresh
    build (``migrate --apply``, which records V019 itself), a populated legacy
    database gets ``--baseline 1``. None means it was not looked up.
    """
    if applied is None:
        if has_cars is None:
            return (
                "inventory Postgres has no schema_migrations table (never migrated). "
                f"Run: {MIGRATE_COMMAND} on an empty database, or "
                f"{BASELINE_V001_COMMAND} on a populated one"
            )
        if has_cars:
            return (
                "inventory Postgres has tables but no schema_migrations table (a legacy "
                f"database never baselined). Run: {BASELINE_V001_COMMAND}  (records V001 "
                "without running it, then applies V002+)"
            )
        return (
            "inventory Postgres has no schema_migrations table and no cars table (an empty "
            f"database). Run: {MIGRATE_COMMAND}  (fresh build: V001 runs, V019 is recorded)"
        )
    missing = sorted(set(range(1, expected + 1)) - applied)
    if not missing:
        return None
    hint = MIGRATE_COMMAND
    if 19 in missing and 1 in applied:
        hint = (
            f"{MIGRATE_COMMAND}  (it records V019 without running it when the 48 objects "
            "V019 shares with V001 exist, and refuses and names them otherwise)"
        )
    shown = ", ".join(f"V{v:03d}" for v in missing[:8]) + (" ..." if len(missing) > 8 else "")
    return (
        f"inventory Postgres schema is behind migrations/ (at V{max(applied, default=0):03d}, "
        f"expected V{expected:03d}; missing {shown}). Run: {hint}"
    )


def _run_auto_migrate() -> None:
    from backend.scripts import migrate

    # Plain --apply is also the fresh path: on an empty database V001 runs and
    # V019 is recorded, not run (migrate.RECORD_ONLY).
    _log.info("INVENTORY_AUTO_MIGRATE=1: applying migrations/ before the schema check")
    rc = migrate.main(["--apply"])
    if rc != 0:
        _log.error("INVENTORY_AUTO_MIGRATE: migrate --apply exited %s", rc)


def ensure_schema_current(conn: Any) -> bool:
    """
    True when the database is at the expected migration version (callers skip DDL).

    False (warn mode) when it is behind: a warning naming the operator command is
    logged once per process and callers keep running their legacy DDL. Strict mode
    raises :class:`SchemaNotMigratedError` instead. Leaves no transaction open.
    Cached per process once True.
    """
    global _current
    if _current:
        return True
    with _state_lock:
        if _current:
            return True
        if auto_migrate_enabled() and _current is None:
            _run_auto_migrate()
        expected = expected_schema_version()
        cur = conn.cursor()
        has_cars: bool | None = None
        try:
            applied = applied_schema_versions(cur)
            if applied is None:
                has_cars = inventory_tables_present(cur)
        finally:
            try:
                conn.rollback()
            except Exception:
                pass
            try:
                cur.close()
            except Exception:
                pass
        gap = describe_gap(applied, expected, has_cars=has_cars)
        if gap is None:
            _current = True
            return True
        if schema_check_mode() == "strict":
            raise SchemaNotMigratedError(gap + "  (INVENTORY_SCHEMA_CHECK=strict)")
        if _current is None:
            _log.warning(
                "SCHEMA CHECK: %s -- falling back to the legacy runtime DDL "
                "(set INVENTORY_SCHEMA_CHECK=strict to refuse instead).",
                gap,
            )
        _current = False
        return False


def schema_is_current() -> bool:
    """
    Cheap per-process answer for per-module ``ensure_*`` helpers: True only when
    an earlier :func:`ensure_schema_current` (or this call's own check) confirmed
    the chain. Opens its own connection on first use; any failure reads as False
    so the caller keeps its legacy DDL.
    """
    if _current is not None:
        return bool(_current)
    from backend.db import inventory_pg

    if not inventory_pg.is_inventory_postgres():
        return False
    try:
        conn = inventory_pg.pg_connect()
    except Exception:
        return False
    try:
        return ensure_schema_current(conn)
    except SchemaNotMigratedError:
        raise
    except Exception:
        _log.debug("schema version check failed; keeping legacy DDL", exc_info=True)
        return False
    finally:
        try:
            conn.close()
        except Exception:
            pass


def reset_schema_check_cache() -> None:
    """Tests / tooling: forget this process's answer."""
    global _current
    with _state_lock:
        _current = None
