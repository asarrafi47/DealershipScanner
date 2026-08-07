#!/usr/bin/env python3
"""
One-shot copy of the dealer portal sidecar SQLite into the inventory Postgres.

Source:  DEALER_PORTAL_DB_PATH (default: repo dealer_portal.db) — or --sqlite-path
Target:  INVENTORY_DATABASE_URL / DATABASE_URL (postgresql:// DSN), table dealer_vehicles
         (schema of record: migrations/V012__dealer_portal.sql)

Dry-run by default (counts only, nothing written); pass --apply to write.

Idempotent: rows are inserted with their original SQLite ids (upload directories
on disk are keyed by vehicle id, so ids must survive the move) using
``ON CONFLICT DO NOTHING`` — a re-run, or a run against a database that already
has the rows, inserts nothing and exits cleanly. After inserting, the id
sequence is bumped past MAX(id) so the next portal insert cannot collide.

Usage:
  PYTHONPATH=. python backend/scripts/migrate_dealer_portal_sqlite_to_postgres.py
  PYTHONPATH=. python backend/scripts/migrate_dealer_portal_sqlite_to_postgres.py --apply
"""
from __future__ import annotations

import argparse
import logging
import os
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

try:
    from backend.utils.project_env import load_project_dotenv

    load_project_dotenv()
except ImportError:
    pass

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_TABLE = "dealer_vehicles"


def _sqlite_path(override: str | None) -> Path:
    raw = (override or os.environ.get("DEALER_PORTAL_DB_PATH") or "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return (ROOT / "dealer_portal.db").resolve()


def _table_columns_sqlite(conn: sqlite3.Connection, table: str) -> list[str]:
    cur = conn.execute(f"PRAGMA table_info({table})")
    return [row[1] for row in cur.fetchall()]


def _table_columns_pg(cur, table: str) -> set[str]:
    cur.execute(
        """
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        """,
        (table,),
    )
    return {str(r[0]) for r in cur.fetchall()}


def migrate(*, apply: bool = False, sqlite_path: str | None = None) -> dict[str, int]:
    from backend.db.inventory_pg import inventory_postgres_dsn, pg_connect

    dsn = inventory_postgres_dsn()
    if not dsn:
        raise SystemExit("Set INVENTORY_DATABASE_URL or DATABASE_URL to a postgresql:// DSN")

    src = _sqlite_path(sqlite_path)
    if not src.is_file():
        raise SystemExit(f"SQLite source not found: {src}")

    logger.info("Source SQLite: %s", src)
    logger.info("Target Postgres: %s", dsn.split("@")[-1] if "@" in dsn else "(dsn)")
    if not apply:
        logger.info("DRY RUN (no writes). Pass --apply to copy rows.")

    sq = sqlite3.connect(str(src))
    sq.row_factory = sqlite3.Row
    pg = pg_connect()
    stats: dict[str, int] = {"source_rows": 0, "already_in_pg": 0, "inserted": 0}

    try:
        try:
            sq_cols = _table_columns_sqlite(sq, _TABLE)
        except sqlite3.OperationalError:
            sq_cols = []
        if not sq_cols:
            logger.info("No %s table in %s; nothing to copy.", _TABLE, src)
            return stats

        pg_cur = pg.cursor()
        # Ensure the target table exists (same DDL the app re-asserts at startup;
        # schema of record is migrations/V012__dealer_portal.sql).
        from backend.db import dealer_portal_db

        pg_cur.execute(dealer_portal_db._DDL_DEALER_VEHICLES_PG)
        for stmt in dealer_portal_db._INDEX_STATEMENTS:
            pg_cur.execute(stmt)

        pg_cols = _table_columns_pg(pg_cur, _TABLE)
        use_cols = [c for c in sq_cols if c in pg_cols]
        skipped = [c for c in sq_cols if c not in pg_cols]
        if skipped:
            logger.warning("SQLite columns absent in Postgres, not copied: %s", skipped)

        rows = sq.execute(f"SELECT {', '.join(use_cols)} FROM {_TABLE}").fetchall()
        stats["source_rows"] = len(rows)

        pg_cur.execute(f"SELECT COUNT(*) FROM {_TABLE}")
        stats["already_in_pg"] = int(pg_cur.fetchone()[0])
        logger.info(
            "%d row(s) in SQLite, %d already in Postgres.",
            stats["source_rows"],
            stats["already_in_pg"],
        )

        if not rows:
            if apply:
                pg.commit()
            else:
                pg.rollback()
            return stats

        col_list = ", ".join(f'"{c}"' for c in use_cols)
        placeholders = ", ".join("%s" for _ in use_cols)
        # Bare ON CONFLICT DO NOTHING (no target): a re-run conflicts on the id
        # primary key, a hand-re-entered vehicle conflicts on (user_id, vin) —
        # both must skip, not abort the batch.
        sql = f'INSERT INTO "{_TABLE}" ({col_list}) VALUES ({placeholders}) ON CONFLICT DO NOTHING'
        batch = [tuple(row[c] for c in use_cols) for row in rows]
        pg_cur.executemany(sql, batch)

        pg_cur.execute(f"SELECT COUNT(*) FROM {_TABLE}")
        stats["inserted"] = int(pg_cur.fetchone()[0]) - stats["already_in_pg"]

        if "id" in use_cols:
            # Explicit ids bypass the sequence; advance it so the next INSERT
            # from the portal does not collide with a migrated row.
            pg_cur.execute(
                f"SELECT setval(pg_get_serial_sequence('{_TABLE}', 'id'), "
                f"(SELECT COALESCE(MAX(id), 1) FROM {_TABLE}))"
            )

        if apply:
            pg.commit()
            logger.info("Inserted %d new row(s) into %s.", stats["inserted"], _TABLE)
        else:
            pg.rollback()
            logger.info("[dry-run] Would insert %d new row(s) into %s.", stats["inserted"], _TABLE)
    finally:
        sq.close()
        pg.close()

    return stats


def main() -> int:
    ap = argparse.ArgumentParser(description="Migrate dealer_portal.db SQLite -> Postgres")
    ap.add_argument("--apply", action="store_true", help="Write rows (default is dry-run)")
    ap.add_argument("--sqlite-path", help="Override source SQLite file path")
    args = ap.parse_args()
    stats = migrate(apply=args.apply, sqlite_path=args.sqlite_path)
    logger.info("Done: %s", stats)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
