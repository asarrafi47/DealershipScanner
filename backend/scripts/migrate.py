#!/usr/bin/env python3
"""
Apply the versioned schema migrations in ``migrations/`` to inventory Postgres.

Rebuilding this database used to mean replaying a July snapshot dump and hoping
the ALTER TABLEs that landed since were all reproduced by the runtime's
CREATE-TABLE-IF-NOT-EXISTS pass. This walks ``migrations/V<NNN>__*.sql`` in
version order instead, recording each file it applies in ``schema_migrations``
with a checksum, so "what schema is this database at" has an answer you can read.

The checksum is the point, not decoration: if an already-applied file's text
changes, two databases that both report the same version are no longer the same
schema, and the runner stops rather than paper over it. Applied files are
immutable; fix forward with a new higher-numbered file. See migrations/README.md.

Usage:
  python -m backend.scripts.migrate                 # dry run (default), no writes
  python -m backend.scripts.migrate --apply
  python -m backend.scripts.migrate --baseline 1 --apply   # record V001 without running it
"""
from __future__ import annotations

import argparse
import hashlib
import logging
import re
import sys
from dataclasses import dataclass
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_REPO_ROOT))

from backend.utils.project_env import load_project_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("migrate")

MIGRATIONS_DIR = _REPO_ROOT / "migrations"

_FILENAME_RE = re.compile(r"^V(\d+)__(.+)\.sql$")

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version    INTEGER PRIMARY KEY,
    name       TEXT NOT NULL,
    checksum   TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    path: Path
    checksum: str


@dataclass(frozen=True)
class Plan:
    pending: list[Migration]
    drifted: list[tuple[Migration, str]]  # (file, checksum recorded in the DB)
    missing: list[int]                    # applied versions with no file on disk
    out_of_order: list[Migration]         # pending, but older than something applied

    @property
    def blocked(self) -> bool:
        return bool(self.drifted or self.out_of_order)


def compute_checksum(sql: str) -> str:
    """
    SHA-256 over the migration text, CRLF-normalized.

    Line endings are normalized because a Windows checkout or an editor that
    rewrites them would otherwise look identical to someone having edited an
    applied migration, and that is a hard stop.
    """
    normalized = sql.replace("\r\n", "\n").replace("\r", "\n")
    return "sha256:" + hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def parse_filename(filename: str) -> tuple[int, str] | None:
    """``V007__add_foo.sql`` -> ``(7, 'add_foo')``; None for anything else."""
    m = _FILENAME_RE.match(filename)
    if not m:
        return None
    return int(m.group(1)), m.group(2)


def discover_migrations(directory: Path) -> list[Migration]:
    """
    Every migration file in ``directory``, ordered by version number.

    Sorted numerically, not lexically -- V010 sorts before V9 as text, which
    would apply the chain in an order no database has ever seen.
    """
    found: dict[int, Migration] = {}
    for path in sorted(directory.glob("*.sql")):
        parsed = parse_filename(path.name)
        if not parsed:
            log.warning("ignoring %s (not V<NNN>__<description>.sql)", path.name)
            continue
        version, name = parsed
        if version in found:
            raise SystemExit(
                f"duplicate migration version {version}: "
                f"{found[version].path.name} and {path.name}. "
                "Two files cannot claim the same version -- renumber one."
            )
        found[version] = Migration(
            version=version,
            name=name,
            path=path,
            checksum=compute_checksum(path.read_text(encoding="utf-8")),
        )
    return [found[v] for v in sorted(found)]


def plan_migrations(available: list[Migration], applied: dict[int, str]) -> Plan:
    """
    Compare files on disk against the ``schema_migrations`` rows.

    ``applied`` maps version -> recorded checksum. Pure: no DB, no filesystem,
    so the interesting cases are testable without a live Postgres.
    """
    drifted = [
        (mig, applied[mig.version])
        for mig in available
        if mig.version in applied and applied[mig.version] != mig.checksum
    ]
    pending = [mig for mig in available if mig.version not in applied]
    missing = sorted(set(applied) - {mig.version for mig in available})

    # A file numbered below the high-water mark showing up as pending means two
    # databases would run the chain in different orders -- the rebuilt one runs
    # it in numeric order, production ran it interleaved. Refuse and renumber.
    high_water = max(applied, default=0)
    out_of_order = [mig for mig in pending if mig.version < high_water]

    return Plan(
        pending=pending, drifted=drifted, missing=missing, out_of_order=out_of_order
    )


def _connect():
    """
    Raw psycopg connection from the repo's own Postgres opener.

    Deliberately NOT ``inventory_db.db_conn()``: that wraps every statement in
    ``adapt_sql_for_postgres_execute``, which rewrites ``?`` to ``%s``, doubles
    bare ``%``, and rewrites IFNULL/datetime() for the SQLite-shaped call sites.
    Migration files are already Postgres SQL and must reach the server verbatim.
    """
    from backend.db import inventory_pg

    if not inventory_pg.is_inventory_postgres():
        raise SystemExit(
            "INVENTORY_DATABASE_URL (or DATABASE_URL) must point at postgresql:// "
            "to run migrations. Refusing to guess."
        )
    return inventory_pg.pg_connect()


def _migrations_table_exists(cur) -> bool:
    cur.execute("SELECT to_regclass('public.schema_migrations')")
    row = cur.fetchone()
    return bool(row and row[0])


def read_applied(cur) -> dict[int, str]:
    """version -> checksum for everything already recorded, {} before first run."""
    if not _migrations_table_exists(cur):
        return {}
    cur.execute("SELECT version, checksum FROM schema_migrations")
    return {int(v): str(c) for v, c in cur.fetchall()}


def _record(cur, mig: Migration) -> None:
    cur.execute(
        "INSERT INTO schema_migrations (version, name, checksum) VALUES (%s, %s, %s)",
        (mig.version, mig.name, mig.checksum),
    )


def apply_migration(conn, mig: Migration) -> None:
    """
    Run one file and record it, in a single transaction.

    The DDL and its bookkeeping row commit together or not at all; a crash
    halfway can never leave a migration applied but unrecorded (it would be
    re-applied on the next run) or recorded but unapplied (silently skipped).
    """
    sql = mig.path.read_text(encoding="utf-8")
    cur = conn.cursor()
    try:
        cur.execute(sql)
        _record(cur, mig)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def _report_blocked(plan: Plan, next_version: int) -> None:
    for mig, recorded in plan.drifted:
        log.error(
            "CHECKSUM DRIFT %s: applied as %s, file is now %s",
            mig.path.name, recorded, mig.checksum,
        )
    if plan.drifted:
        log.error(
            "An applied migration was edited. Every database that already ran it "
            "has the OLD schema, so re-running would silently diverge. Restore the "
            "file to its applied contents and put the change in a new V%03d__*.sql.",
            next_version,
        )
    for mig in plan.out_of_order:
        log.error(
            "OUT OF ORDER %s: version %d is below the highest applied version. "
            "Renumber it above the high-water mark.",
            mig.path.name, mig.version,
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true",
                        help="write (default: dry run, no statement is executed)")
    parser.add_argument("--dry-run", action="store_true",
                        help="explicit no-op default; report the plan and exit")
    parser.add_argument("--dir", default=str(MIGRATIONS_DIR),
                        help=f"migrations directory (default: {MIGRATIONS_DIR})")
    parser.add_argument("--baseline", type=int, metavar="VERSION",
                        help="record migrations up to VERSION as applied WITHOUT "
                             "running them -- for a database that already has that "
                             "schema (production). Everything above still runs.")
    args = parser.parse_args(argv)

    load_project_dotenv()
    apply = args.apply and not args.dry_run

    directory = Path(args.dir)
    if not directory.is_dir():
        log.error("no migrations directory at %s", directory)
        return 2
    available = discover_migrations(directory)
    if not available:
        log.info("no migration files in %s", directory)
        return 0

    try:
        conn = _connect()
    except Exception as exc:  # SystemExit (not a Postgres DSN) passes through
        log.error("could not connect to inventory Postgres: %s", exc)
        return 2
    try:
        cur = conn.cursor()
        table_exists = _migrations_table_exists(cur)
        applied = read_applied(cur)
        cur.close()

        if not table_exists:
            # Creating it in a dry run would make the "no writes" promise a lie.
            log.info("schema_migrations does not exist yet%s",
                     "" if apply else " (would be created by --apply)")

        plan = plan_migrations(available, applied)
        log.info("%d migration file(s), %d already applied, %d pending",
                 len(available), len(applied), len(plan.pending))
        for version in plan.missing:
            # Not fatal: the DB is still the sum of what ran. But the chain can no
            # longer rebuild that database from scratch, which is the whole point.
            log.warning("version %d is recorded as applied but has no file on disk", version)
        if plan.blocked:
            _report_blocked(plan, max(max(applied, default=0), available[-1].version) + 1)
            return 1

        if not plan.pending:
            log.info("database is up to date")
            return 0

        if apply and not table_exists:
            cur = conn.cursor()
            cur.execute(_CREATE_TABLE_SQL)
            conn.commit()
            cur.close()

        baseline_through = args.baseline or 0
        for mig in plan.pending:
            recorded_only = mig.version <= baseline_through
            verb = "RECORD (baseline)" if recorded_only else "APPLY"
            if not apply:
                log.info("would %s  V%03d__%s.sql", verb.lower(), mig.version, mig.name)
                continue
            log.info("%s V%03d__%s.sql", verb, mig.version, mig.name)
            try:
                if recorded_only:
                    cur = conn.cursor()
                    _record(cur, mig)
                    conn.commit()
                    cur.close()
                else:
                    apply_migration(conn, mig)
            except Exception as exc:
                log.error("FAILED V%03d__%s.sql: %s", mig.version, mig.name, exc)
                log.error("rolled back; nothing above this version was applied")
                return 1

        log.info("=== Summary (%s) ===", "applied" if apply else "dry run")
        log.info("  %s %d migration(s) through V%03d",
                 "applied" if apply else "would apply",
                 len(plan.pending), plan.pending[-1].version)
        if not apply:
            log.info("  re-run with --apply to write")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
