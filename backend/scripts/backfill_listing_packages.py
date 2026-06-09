#!/usr/bin/env python3
"""
Backfill every active listing: fetch/parse dealer description + download/analyze window sticker.

Usage:
  INVENTORY_DB_PATH=inventory.db python backend/scripts/backfill_listing_packages.py
  INVENTORY_DB_PATH=inventory.db python backend/scripts/backfill_listing_packages.py --limit 50
  INVENTORY_DB_PATH=inventory.db python backend/scripts/backfill_listing_packages.py --no-vision
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
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
    args = ap.parse_args()

    from backend.db.inventory_db import get_car_by_id, init_inventory_db
    from backend.enrichment.listing_packages_service import ensure_listing_packages_for_car
    import sqlite3

    init_inventory_db()
    db_path = os.environ.get("INVENTORY_DB_PATH", "inventory.db")
    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        "SELECT id FROM cars WHERE active = 1 ORDER BY id ASC"
    ).fetchall()
    conn.close()

    ids = [int(r[0]) for r in rows]
    if args.limit > 0:
        ids = ids[: args.limit]

    stats = {
        "total": len(ids),
        "description_parsed": 0,
        "description_fetched": 0,
        "sticker_stored": 0,
        "sticker_analyzed": 0,
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
