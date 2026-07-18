#!/usr/bin/env python3
"""Recompute the market price bands in ``market_price_stats``.

Nightly-friendly: reads the live ``cars`` inventory, aggregates per
(year, make, model, trim, condition, mileage-band) and persists every band
that clears the minimum-sample gate. Safe to re-run; each run replaces the
table contents inside one transaction.

Requires INVENTORY_DATABASE_URL or DATABASE_URL (postgresql://).

Examples
--------
    python -m backend.scripts.compute_market_stats
    python -m backend.scripts.compute_market_stats --dry-run
    python -m backend.scripts.compute_market_stats --top 15
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

from backend.db.inventory_pg import inventory_postgres_dsn, pg_connect  # noqa: E402
from backend.intelligence.market_pricing import (  # noqa: E402
    MIN_DEALERS,
    MIN_LISTINGS,
    STATS_TABLE,
    rebuild_stats_table,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger("compute_market_stats")


def _print_top(conn, limit: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT year, make, model, trim, condition,
                   mileage_band_low, median_price, p25_price, p75_price,
                   sample_count, dealer_count
            FROM {STATS_TABLE}
            ORDER BY sample_count DESC, dealer_count DESC
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()
    logger.info("Top %d bands by sample size:", len(rows))
    for r in rows:
        (year, make, model, trim, cond, band_low, med, p25, p75, n, dealers) = r
        logger.info(
            "  %s %s %s %-14s %-4s %6dmi  med=$%-8.0f p25=$%-8.0f p75=$%-8.0f  n=%d dealers=%d",
            year, make, model, (trim or "-"), cond, band_low, med, p25, p75, n, dealers,
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="compute but roll back (no persist)")
    parser.add_argument("--min-listings", type=int, default=MIN_LISTINGS)
    parser.add_argument("--min-dealers", type=int, default=MIN_DEALERS)
    parser.add_argument("--top", type=int, default=10, help="print N largest bands after compute")
    args = parser.parse_args(argv)

    if not inventory_postgres_dsn():
        raise SystemExit("Set INVENTORY_DATABASE_URL or DATABASE_URL to a postgresql:// DSN")

    conn = pg_connect()
    try:
        count = rebuild_stats_table(
            conn,
            min_listings=args.min_listings,
            min_dealers=args.min_dealers,
        )
        if args.top:
            _print_top(conn, args.top)
        if args.dry_run:
            conn.rollback()
            logger.info("DRY RUN: computed %d bands, rolled back (nothing persisted)", count)
        else:
            conn.commit()
            logger.info("Persisted %d market price bands to %s", count, STATS_TABLE)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
