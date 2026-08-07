#!/usr/bin/env python3
"""
One-shot copy of the users domain from users.db (SQLCipher/SQLite) to Postgres.

Opens the source through ``backend.db.users_sqlite.get_users_conn()`` so the
SQLCipher key handling (USERS_DB_ENCRYPTION_KEY / ALLOW_UNENCRYPTED_USER_DB,
SEC-088) works exactly as the running app's does -- if the app can read the
file, so can this script. The target is the inventory Postgres from
``INVENTORY_DATABASE_URL`` (or ``--dsn``), whose tables must already exist:
run ``python -m backend.scripts.migrate --apply`` through V013__users.sql first.

Primary keys are preserved verbatim (``OVERRIDING SYSTEM VALUE`` on the
identity columns) and every identity sequence is reset to MAX(id) afterwards,
so rows created after the cutover continue the same id space -- users.id is
referenced from outside this domain (saved_cars.user_id, dealer portal rows).

Safety model:
  * Dry run by default: prints per-table source/target row counts and the
    planned action; writes nothing.
  * ``--apply`` copies, committing per table. A non-empty target table is
    SKIPPED (idempotent re-runs pick up where a partial run stopped).
  * ``--force`` (with ``--apply``) TRUNCATEs non-empty targets first
    (RESTART IDENTITY CASCADE -- note truncating users/orgs cascades into
    org_invites, which is copied after both).

Usage:
  python -m backend.scripts.migrate_users_sqlite_to_postgres            # dry run
  python -m backend.scripts.migrate_users_sqlite_to_postgres --apply
  python -m backend.scripts.migrate_users_sqlite_to_postgres --apply --force
  python -m backend.scripts.migrate_users_sqlite_to_postgres --dsn postgresql://...
"""
from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_REPO_ROOT))

from backend.utils.project_env import load_project_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("migrate_users")

# Copy order matters: org_invites carries enforced FKs to orgs and users in
# Postgres (V013), so both must be populated first. The remaining tables are
# unconstrained.
TABLES: tuple[str, ...] = (
    "orgs",
    "users",
    "org_invites",
    "car_view_history",
    "car_compare_history",
    "compare_sessions",
    "search_events",
)

BATCH_SIZE = 500


@dataclass
class TableReport:
    table: str
    source_rows: int | None      # None = table absent in sqlite
    target_rows: int | None      # None = table absent in Postgres
    action: str                  # "copy" | "skip-empty-source" | ...
    copied: int = 0


def _sqlite_table_names(conn) -> set[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table'"
    ).fetchall()
    return {str(r[0]) for r in rows}


def _sqlite_columns(conn, table: str) -> list[str]:
    return [str(r[1]) for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]


def _pg_column_types(cur, table: str) -> dict[str, str]:
    """column name -> data_type (e.g. 'timestamp with time zone'), {} if absent."""
    cur.execute(
        """
        SELECT column_name, data_type FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        """,
        (table,),
    )
    return {str(r[0]): str(r[1]) for r in cur.fetchall()}


def _pg_identity_columns(cur, table: str) -> set[str]:
    cur.execute(
        """
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
          AND is_identity = 'YES'
        """,
        (table,),
    )
    return {str(r[0]) for r in cur.fetchall()}


def _convert_timestamptz(value):
    """sqlite datetime('now') text (naive UTC) -> aware datetime for TIMESTAMPTZ."""
    if value is None:
        return None
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        try:
            dt = datetime.fromisoformat(s)
        except ValueError:
            # Leave it to Postgres to parse (or reject loudly) -- silently
            # nulling a timestamp would be data loss.
            return s
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    return value


def _quoted(cols: list[str]) -> str:
    return ", ".join(f'"{c}"' for c in cols)


def copy_table(sconn, pconn, table: str, *, apply: bool, force: bool) -> TableReport:
    scur_tables = _sqlite_table_names(sconn)
    pcur = pconn.cursor()
    try:
        pg_types = _pg_column_types(pcur, table)
        if not pg_types:
            return TableReport(table, None, None,
                               "ERROR: missing in Postgres (run backend.scripts.migrate --apply first)")
        pcur.execute(f'SELECT COUNT(*) FROM "{table}"')
        target_rows = int(pcur.fetchone()[0])

        if table not in scur_tables:
            return TableReport(table, None, target_rows,
                               "skip: table never created in users.db")

        source_rows = int(sconn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])

        src_cols = _sqlite_columns(sconn, table)
        cols = [c for c in src_cols if c in pg_types]
        dropped = [c for c in src_cols if c not in pg_types]
        if dropped:
            log.warning("%s: sqlite column(s) %s have no Postgres counterpart; NOT copied",
                        table, ", ".join(dropped))

        if source_rows == 0:
            return TableReport(table, source_rows, target_rows, "skip: source empty")
        if target_rows > 0 and not force:
            return TableReport(table, source_rows, target_rows,
                               "skip: target non-empty (use --force to truncate and recopy)")

        action = "copy" if target_rows == 0 else "truncate + copy (--force)"
        if not apply:
            return TableReport(table, source_rows, target_rows, f"would {action}")

        if target_rows > 0:
            log.warning("%s: --force TRUNCATE ... RESTART IDENTITY CASCADE", table)
            pcur.execute(f'TRUNCATE TABLE "{table}" RESTART IDENTITY CASCADE')

        identity_cols = _pg_identity_columns(pcur, table)
        overriding = " OVERRIDING SYSTEM VALUE" if identity_cols & set(cols) else ""
        tstz_idx = [i for i, c in enumerate(cols)
                    if pg_types.get(c) == "timestamp with time zone"]
        placeholders = ", ".join(["%s"] * len(cols))
        insert_sql = (
            f'INSERT INTO "{table}" ({_quoted(cols)}){overriding} '
            f"VALUES ({placeholders})"
        )

        copied = 0
        read_cur = sconn.execute(f"SELECT {', '.join(cols)} FROM {table}")
        while True:
            rows = read_cur.fetchmany(BATCH_SIZE)
            if not rows:
                break
            batch = []
            for row in rows:
                vals = list(row)
                for i in tstz_idx:
                    vals[i] = _convert_timestamptz(vals[i])
                batch.append(tuple(vals))
            pcur.executemany(insert_sql, batch)
            copied += len(batch)

        for col in sorted(identity_cols & set(cols)):
            pcur.execute("SELECT pg_get_serial_sequence(%s, %s)", (table, col))
            seq_row = pcur.fetchone()
            seq = seq_row[0] if seq_row else None
            if not seq:
                continue
            pcur.execute(f'SELECT MAX("{col}") FROM "{table}"')
            max_id = pcur.fetchone()[0]
            if max_id is None:
                pcur.execute("SELECT setval(%s, 1, false)", (seq,))
            else:
                pcur.execute("SELECT setval(%s, %s)", (seq, int(max_id)))

        pcur.execute(f'SELECT COUNT(*) FROM "{table}"')
        final = int(pcur.fetchone()[0])
        if final != source_rows:
            raise RuntimeError(
                f"{table}: copied {copied} but target holds {final} of {source_rows} source rows"
            )
        pconn.commit()
        return TableReport(table, source_rows, final, action, copied=copied)
    except Exception:
        pconn.rollback()
        raise
    finally:
        pcur.close()


def _pg_dsn(cli_dsn: str | None) -> str:
    if cli_dsn:
        dsn = cli_dsn.strip()
        if not dsn.startswith(("postgresql://", "postgres://")):
            raise SystemExit("--dsn must be a postgresql:// or postgres:// URL")
        return dsn
    from backend.db import inventory_pg

    dsn = inventory_pg.inventory_postgres_dsn()
    if not dsn:
        raise SystemExit(
            "INVENTORY_DATABASE_URL (or DATABASE_URL) must point at postgresql:// "
            "or pass --dsn explicitly. Refusing to guess."
        )
    return dsn


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--apply", action="store_true",
                        help="write (default: dry run, prints counts only)")
    parser.add_argument("--force", action="store_true",
                        help="with --apply: TRUNCATE non-empty target tables before copying")
    parser.add_argument("--dsn", default=None,
                        help="target Postgres DSN (default: INVENTORY_DATABASE_URL)")
    args = parser.parse_args(argv)

    load_project_dotenv()

    if args.force and not args.apply:
        log.warning("--force has no effect without --apply (dry run)")

    dsn = _pg_dsn(args.dsn)

    # Same opener as the app: SQLCipher key handling included (SEC-088).
    from backend.db.users_sqlite import get_users_conn, users_db_path

    log.info("source: %s", users_db_path())
    sconn = get_users_conn()

    import psycopg

    pconn = psycopg.connect(dsn, autocommit=False)
    try:
        cur = pconn.cursor()
        # sqlite datetime('now') text is naive UTC; make the session parse any
        # string that slips through _convert_timestamptz as UTC, not server-local.
        cur.execute("SET TimeZone = 'UTC'")
        pconn.commit()
        cur.close()

        reports: list[TableReport] = []
        failed = False
        for table in TABLES:
            try:
                rep = copy_table(sconn, pconn, table, apply=args.apply, force=args.force)
            except Exception as exc:
                log.error("FAILED %s: %s", table, exc)
                log.error("rolled back %s; earlier tables remain committed -- "
                          "re-running skips them (non-empty targets)", table)
                failed = True
                break
            reports.append(rep)

        log.info("=== Summary (%s) ===", "applied" if args.apply else "dry run")
        for rep in reports:
            src = "-" if rep.source_rows is None else str(rep.source_rows)
            tgt = "-" if rep.target_rows is None else str(rep.target_rows)
            log.info("  %-22s source=%-7s target=%-7s %s", rep.table, src, tgt, rep.action)
        if any(r.action.startswith("ERROR") for r in reports):
            return 1
        if failed:
            return 1
        if not args.apply:
            log.info("  dry run: nothing written; re-run with --apply to copy")
        return 0
    finally:
        pconn.close()
        try:
            sconn.close()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
