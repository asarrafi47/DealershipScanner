#!/usr/bin/env python3
"""
One-time copy of core inventory tables from SQLite (INVENTORY_DB_PATH) to Postgres.

Requires:
  INVENTORY_DB_PATH — source SQLite file (default: repo inventory.db)
  INVENTORY_DATABASE_URL or DATABASE_URL — target Postgres DSN

Usage:
  PYTHONPATH=. python backend/scripts/migrate_inventory_sqlite_to_postgres.py
  PYTHONPATH=. python backend/scripts/migrate_inventory_sqlite_to_postgres.py --dry-run
"""
from __future__ import annotations

import argparse
import json
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

# Tables copied in order (respect FKs where present).
_TABLES = (
    "cars",
    "epa_master",
    "model_specs",
    "dealer_scan_profile",
    "scan_runs",
    "dealer_geopoints",
    "nhtsa_vpic_cache",
    "saved_cars",
)


def _sqlite_path() -> Path:
    raw = (os.environ.get("INVENTORY_DB_PATH") or "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return (ROOT / "inventory.db").resolve()


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


def migrate(*, dry_run: bool = False) -> dict[str, int]:
    from backend.db.inventory_pg import init_postgres_inventory, inventory_postgres_dsn, pg_connect

    dsn = inventory_postgres_dsn()
    if not dsn:
        raise SystemExit("Set INVENTORY_DATABASE_URL or DATABASE_URL to a postgresql:// DSN")

    src = _sqlite_path()
    if not src.is_file():
        raise SystemExit(f"SQLite source not found: {src}")

    logger.info("Source SQLite: %s", src)
    logger.info("Target Postgres: %s", dsn.split("@")[-1] if "@" in dsn else "(dsn)")

    sq = sqlite3.connect(str(src))
    sq.row_factory = sqlite3.Row
    pg = pg_connect()
    stats: dict[str, int] = {}

    try:
        init_postgres_inventory(pg)
        pg_cur = pg.cursor()
        for table in _TABLES:
            try:
                sq_cols = _table_columns_sqlite(sq, table)
            except sqlite3.OperationalError:
                logger.info("Skip %s (not in SQLite)", table)
                continue
            if not sq_cols:
                continue
            pg_cols = _table_columns_pg(pg_cur, table)
            if not pg_cols:
                logger.warning("Skip %s (not in Postgres schema)", table)
                continue
            use_cols = [c for c in sq_cols if c in pg_cols]
            if table == "cars":
                use_cols = [c for c in use_cols if c != "id"]
            if not use_cols:
                logger.warning("Skip %s (no overlapping columns)", table)
                continue
            rows = sq.execute(f"SELECT {', '.join(use_cols)} FROM {table}").fetchall()
            if not rows:
                stats[table] = 0
                continue
            if dry_run:
                stats[table] = len(rows)
                logger.info("[dry-run] Would copy %d row(s) into %s", len(rows), table)
                continue

            col_list = ", ".join(f'"{c}"' for c in use_cols)
            placeholders = ", ".join("%s" for _ in use_cols)
            if table == "cars":
                conflict = "ON CONFLICT (vin) DO NOTHING"
            elif table == "dealer_scan_profile":
                conflict = "ON CONFLICT (dealer_id) DO NOTHING"
            elif table == "dealer_geopoints":
                conflict = "ON CONFLICT (dealer_url) DO NOTHING"
            elif table == "model_specs":
                conflict = "ON CONFLICT (make, model) DO NOTHING"
            elif table == "saved_cars":
                conflict = "ON CONFLICT (user_id, car_id) DO NOTHING"
            elif table == "nhtsa_vpic_cache":
                conflict = "ON CONFLICT (vin) DO NOTHING"
            elif table == "incomplete_listings_meta":
                conflict = "ON CONFLICT (k) DO NOTHING"
            else:
                conflict = ""

            sql = f'INSERT INTO "{table}" ({col_list}) VALUES ({placeholders}) {conflict}'.strip()
            batch = [tuple(row[c] for c in use_cols) for row in rows]
            pg_cur.executemany(sql, batch)
            stats[table] = len(batch)
            logger.info("Copied %d row(s) into %s", len(batch), table)

        if dry_run:
            pg.rollback()
        else:
            pg.commit()
    finally:
        sq.close()
        pg.close()

    return stats


def main() -> int:
    ap = argparse.ArgumentParser(description="Migrate inventory SQLite → Postgres")
    ap.add_argument("--dry-run", action="store_true", help="Count rows only; no writes")
    args = ap.parse_args()
    stats = migrate(dry_run=args.dry_run)
    logger.info("Done: %s", json.dumps(stats))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
