#!/usr/bin/env python3
"""
Backfill window stickers for non-CDJR inventory, grouped by dealership.

Learns dealer sticker_provider (iPacket / listing embed / none) while fetching.

Usage:
  INVENTORY_DB_PATH=inventory.db python backend/scripts/backfill_window_stickers.py
  INVENTORY_DB_PATH=inventory.db python backend/scripts/backfill_window_stickers.py --limit 100
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from collections import defaultdict
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
logger = logging.getLogger("backfill_window_stickers")


def main() -> int:
    ap = argparse.ArgumentParser(description="Backfill listing/OEM window stickers by dealership.")
    ap.add_argument("--limit", type=int, default=0, help="Max cars to process (0 = all active)")
    ap.add_argument("--sleep", type=float, default=1.5, help="Seconds between cars")
    args = ap.parse_args()

    from backend.db.inventory_db import get_car_by_id, init_inventory_db
    from backend.enrichment.window_sticker_service import (
        car_sticker_packages_need_analysis,
        ensure_window_sticker_for_car,
        window_sticker_available,
    )
    from backend.scanner.dealer_sticker_provider import get_dealer_sticker_provider
    from backend.scanner.window_sticker import is_cdjr_stellantis_car
    import sqlite3

    init_inventory_db()
    db_path = os.environ.get("INVENTORY_DB_PATH", "inventory.db")
    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        """
        SELECT id, dealership_registry_id, vin
        FROM cars
        WHERE active = 1
        ORDER BY dealership_registry_id ASC, id ASC
        """
    ).fetchall()
    conn.close()

    by_dealer: dict[int | None, list[int]] = defaultdict(list)
    for cid, reg_id, _vin in rows:
        by_dealer[int(reg_id) if reg_id else None].append(int(cid))

    ids: list[int] = []
    for _reg, car_ids in sorted(by_dealer.items(), key=lambda x: (x[0] is None, x[0] or 0)):
        ids.extend(car_ids)
    if args.limit > 0:
        ids = ids[: args.limit]

    stats = {
        "total": len(ids),
        "skipped_cdjr": 0,
        "skipped_cached": 0,
        "skipped_none_dealer": 0,
        "attempted": 0,
        "stored": 0,
        "failed": 0,
    }

    for car_id in ids:
        car = get_car_by_id(car_id, include_inactive=True)
        if not car:
            continue
        if is_cdjr_stellantis_car(car):
            stats["skipped_cdjr"] += 1
            continue
        reg = car.get("dealership_registry_id")
        try:
            reg_i = int(reg) if reg is not None else None
        except (TypeError, ValueError):
            reg_i = None
        if reg_i and get_dealer_sticker_provider(reg_i) == "none":
            if window_sticker_available(car) and not car_sticker_packages_need_analysis(car):
                stats["skipped_cached"] += 1
                continue
            stats["skipped_none_dealer"] += 1
            continue
        if window_sticker_available(car) and not car_sticker_packages_need_analysis(car):
            stats["skipped_cached"] += 1
            continue
        stats["attempted"] += 1
        try:
            out = ensure_window_sticker_for_car(int(car_id), allow_vision_fallback=False)
            if out.get("window_sticker_available") and out.get("stored"):
                stats["stored"] += 1
            elif out.get("fetch_error"):
                stats["failed"] += 1
        except Exception as e:
            stats["failed"] += 1
            logger.debug("backfill failed car_id=%s: %s", car_id, e)
        time.sleep(max(0.0, float(args.sleep)))

    logger.info("backfill_window_stickers done: %s", stats)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
