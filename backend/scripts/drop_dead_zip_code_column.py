#!/usr/bin/env python3
"""
Drop ``cars.zip_code`` (Tier 1 schema cleanup).

Superseded by the dealer-level geo mechanism (``backend/db/dealer_geo.py`` +
the ``dealerships``/``dealer_geopoints`` tables), which is what the live
"nearby dealers" / radius-search feature actually uses
(``backend/listings/nearby_dealers.py``). ``cars.zip_code`` was 99.99% NULL
(1/16,567 rows — a "Test Dealer" seed row) and the ~150 lines of per-car
distance-filtering code built on it were dead in practice. The scanner write
path, schema definitions, and dead fallback code were removed in the same
change that authored this script.

Requires INVENTORY_DATABASE_URL or DATABASE_URL (postgresql://).

Usage:
  PYTHONPATH=. python backend/scripts/drop_dead_zip_code_column.py --dry-run
  PYTHONPATH=. python backend/scripts/drop_dead_zip_code_column.py
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

_COLUMN = "zip_code"
_MAX_ALLOWED_NON_NULL = 5  # near-zero tolerance; abort if this looks like live data, not test noise


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


def drop_dead_zip_code_column(*, dry_run: bool = False) -> bool:
    from backend.db.inventory_pg import inventory_postgres_dsn, pg_connect

    dsn = inventory_postgres_dsn()
    if not dsn:
        raise SystemExit("Set INVENTORY_DATABASE_URL or DATABASE_URL to a postgresql:// DSN")

    conn = pg_connect()
    try:
        cur = conn.cursor()
        if not _column_exists(cur, "cars", _COLUMN):
            logger.info("skip (already dropped): %s", _COLUMN)
            return False
        n = _non_null_count(cur, "cars", _COLUMN)
        if n > _MAX_ALLOWED_NON_NULL:
            raise SystemExit(
                f"Refusing to drop cars.{_COLUMN}: found {n} non-NULL value(s), "
                f"more than the expected test-noise threshold ({_MAX_ALLOWED_NON_NULL}). "
                "Something may be writing to it — investigate before re-running."
            )
        if n:
            logger.info("cars.%s has %d non-NULL value(s) (expected: prior test/seed noise)", _COLUMN, n)
        sql = f'ALTER TABLE cars DROP COLUMN IF EXISTS "{_COLUMN}"'
        logger.info("%s", sql)
        if not dry_run:
            cur.execute(sql)
            conn.commit()
    finally:
        conn.close()
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description="Drop the unused cars.zip_code column.")
    parser.add_argument("--dry-run", action="store_true", help="Print planned drop only.")
    parser.add_argument(
        "--force", action="store_true",
        help="Run even while a scanner process is alive (breaks its remaining upserts).",
    )
    args = parser.parse_args()
    if not args.dry_run:
        from backend.scripts.scanner_liveness import abort_if_scanner_running

        abort_if_scanner_running(force=bool(args.force), action="DROP COLUMN cars.zip_code")
    dropped = drop_dead_zip_code_column(dry_run=bool(args.dry_run))
    if args.dry_run:
        logger.info("dry-run: would drop cars.zip_code" if dropped else "dry-run: nothing to do")
    else:
        logger.info("dropped cars.zip_code" if dropped else "nothing to do")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
