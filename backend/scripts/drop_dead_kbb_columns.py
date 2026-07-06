#!/usr/bin/env python3
"""
Drop the unused KBB valuation columns from ``cars`` (Tier 1 schema cleanup).

These columns were scaffolded for a KBB valuation feature that was never built: no code
anywhere fetches or writes them, and they are 100% NULL across all rows. Confirmed no live
writer before authoring this script (see git history / conversation this was authored in).

Columns dropped:
  kbb_fetched_at, kbb_snapshot_json, kbb_fair_purchase, kbb_range_low,
  kbb_range_high, kbb_private_party, kbb_trade_in

Requires INVENTORY_DATABASE_URL or DATABASE_URL (postgresql://).

Usage:
  PYTHONPATH=. python backend/scripts/drop_dead_kbb_columns.py --dry-run
  PYTHONPATH=. python backend/scripts/drop_dead_kbb_columns.py
"""
from __future__ import annotations

import argparse
import logging
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

_DEAD_COLUMNS: tuple[str, ...] = (
    "kbb_fetched_at",
    "kbb_snapshot_json",
    "kbb_fair_purchase",
    "kbb_range_low",
    "kbb_range_high",
    "kbb_private_party",
    "kbb_trade_in",
)


def _column_exists(cur, table: str, column: str) -> bool:
    cur.execute(
        """
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s AND column_name = %s
        """,
        (table, column),
    )
    return cur.fetchone() is not None


def _non_null_count(cur, table: str, column: str) -> int:
    cur.execute(f'SELECT count("{column}") FROM "{table}" WHERE "{column}" IS NOT NULL')
    return int(cur.fetchone()[0])


def drop_dead_kbb_columns(*, dry_run: bool = False) -> list[str]:
    from backend.db.inventory_pg import inventory_postgres_dsn, pg_connect

    dsn = inventory_postgres_dsn()
    if not dsn:
        raise SystemExit("Set INVENTORY_DATABASE_URL or DATABASE_URL to a postgresql:// DSN")

    dropped: list[str] = []
    conn = pg_connect()
    try:
        cur = conn.cursor()
        for col in _DEAD_COLUMNS:
            if not _column_exists(cur, "cars", col):
                logger.info("skip (already dropped): %s", col)
                continue
            n = _non_null_count(cur, "cars", col)
            if n:
                raise SystemExit(
                    f"Refusing to drop cars.{col}: found {n} non-NULL value(s). "
                    "Something is writing to it now — investigate before re-running."
                )
            sql = f'ALTER TABLE cars DROP COLUMN IF EXISTS "{col}"'
            logger.info("%s", sql)
            if not dry_run:
                cur.execute(sql)
            dropped.append(col)
        if not dry_run:
            conn.commit()
    finally:
        conn.close()
    return dropped


def main() -> int:
    parser = argparse.ArgumentParser(description="Drop unused KBB valuation columns from cars.")
    parser.add_argument("--dry-run", action="store_true", help="Print planned drops only.")
    parser.add_argument(
        "--force", action="store_true",
        help="Run even while a scanner process is alive (breaks its remaining upserts).",
    )
    args = parser.parse_args()
    if not args.dry_run:
        from backend.scripts.scanner_liveness import abort_if_scanner_running

        abort_if_scanner_running(force=bool(args.force), action="DROP COLUMN (KBB columns)")
    dropped = drop_dead_kbb_columns(dry_run=bool(args.dry_run))
    if args.dry_run:
        logger.info("dry-run: would drop %d column(s)", len(dropped))
    else:
        logger.info("dropped %d column(s)", len(dropped))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
