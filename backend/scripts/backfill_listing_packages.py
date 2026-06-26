#!/usr/bin/env python3
"""
Backfill every active listing: fetch/parse dealer description + download/analyze window sticker.

Usage:
  PYTHONPATH=. python backend/scripts/backfill_listing_packages.py
  PYTHONPATH=. python backend/scripts/backfill_listing_packages.py --limit 50 --missing-only
  PYTHONPATH=. python backend/scripts/backfill_listing_packages.py --no-vision
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
import os

os.chdir(ROOT)
sys.path.insert(0, str(ROOT))

try:
    from backend.utils.project_env import load_project_dotenv

    load_project_dotenv()
except ImportError:
    pass

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("backfill_listing_packages")


def main() -> int:
    ap = argparse.ArgumentParser(description="Backfill listing descriptions + window stickers for inventory.")
    ap.add_argument("--limit", type=int, default=0, help="Max cars to process (0 = all active)")
    ap.add_argument("--no-vision", action="store_true", help="Skip photo vision merge")
    ap.add_argument("--sleep", type=float, default=1.0, help="Seconds between cars")
    ap.add_argument(
        "--missing-only",
        action="store_true",
        help="Only process rows where packages is NULL or empty",
    )
    args = ap.parse_args()

    from backend.db.inventory_db import get_car_by_id, get_conn, init_inventory_db
    from backend.enrichment.listing_packages_service import ensure_listing_packages_for_car

    init_inventory_db()
    conn = get_conn()
    cur = conn.cursor()
    sql = "SELECT id FROM cars WHERE COALESCE(listing_active, 1) = 1 "
    if args.missing_only:
        sql += "AND (packages IS NULL OR TRIM(packages) IN ('', '{}', '[]', 'null')) "
    sql += "ORDER BY id ASC"
    if args.limit > 0:
        sql += f" LIMIT {max(1, int(args.limit))}"
    cur.execute(sql)
    rows = cur.fetchall()
    conn.close()

    ids = [int(r[0]) for r in rows]
    stats = {
        "total": len(ids),
        "description_parsed": 0,
        "description_fetched": 0,
        "sticker_stored": 0,
        "sticker_analyzed": 0,
        "listing_options_applied": 0,
        "errors": 0,
    }

    for idx, car_id in enumerate(ids, start=1):
        car = get_car_by_id(car_id, include_inactive=False)
        if not car:
            continue
        label = f"{car.get('year')} {car.get('make')} {car.get('model')} ({car_id})"
        logger.info("[%d/%d] %s", idx, len(ids), label)
        try:
            out = ensure_listing_packages_for_car(
                car_id,
                allow_vision_fallback=not args.no_vision,
                refetch_description=True,
            )
            if out.get("listing_description_parsed"):
                stats["description_parsed"] += 1
            if out.get("listing_description_fetched"):
                stats["description_fetched"] += 1
            if out.get("stored"):
                stats["sticker_stored"] += 1
            if out.get("listing_options_applied"):
                stats["listing_options_applied"] += 1
            if out.get("analyzed"):
                stats["sticker_analyzed"] += 1
            if out.get("fetch_error"):
                logger.info("  sticker: %s", out.get("fetch_error"))
        except Exception as e:
            stats["errors"] += 1
            logger.warning("  failed: %s", e)
        if args.sleep > 0:
            time.sleep(args.sleep)

    logger.info("Done: %s", stats)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
