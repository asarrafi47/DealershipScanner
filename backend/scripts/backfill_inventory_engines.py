#!/usr/bin/env python3
"""
Backfill engine columns on all inventory listings from DICTIONARY EPA data.

Updates ``engine_l``, ``cylinders``, ``engine_description``, and ``forced_induction``
using year/make/model/trim matching plus listing-signal disambiguation.

Usage:
  python backend/scripts/backfill_inventory_engines.py
  python backend/scripts/backfill_inventory_engines.py --dry-run
  INVENTORY_DB_PATH=/path/to/inventory.db python backend/scripts/backfill_inventory_engines.py
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT))

from backend.utils.project_env import load_project_dotenv

load_project_dotenv()

from backend.db.inventory_db import DB_PATH, update_car_row_partial  # noqa: E402
from backend.dictionary.epa_engine import enrich_car_engine_from_dictionary  # noqa: E402
from backend.utils.forced_induction import classify_forced_induction_from_car_row  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser(description="Backfill inventory engine fields from DICTIONARY")
    ap.add_argument("--dry-run", action="store_true", help="Print changes without writing")
    ap.add_argument(
        "--only-active",
        action="store_true",
        default=True,
        help="Skip inactive listings (default: true)",
    )
    args = ap.parse_args()

    import sqlite3

    db_path = os.environ.get("INVENTORY_DB_PATH", DB_PATH)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("PRAGMA table_info(cars)")
    cols = {r[1] for r in cur.fetchall()}
    if "forced_induction" not in cols:
        cur.execute("ALTER TABLE cars ADD COLUMN forced_induction TEXT")
        conn.commit()

    where = "WHERE listing_active IS NULL OR listing_active != 0" if args.only_active else ""
    cur.execute(
        f"SELECT id, year, make, model, trim, title, description, engine_l, cylinders, "
        f"engine_description, forced_induction, fuel_type FROM cars {where}"
    )
    rows = cur.fetchall()
    print(f"Processing {len(rows)} cars from {db_path}...")

    updated = 0
    dict_hits = 0
    fi_only = 0

    for row in rows:
        car = dict(row)
        updates = enrich_car_engine_from_dictionary(car, overwrite=True)
        if updates:
            dict_hits += 1
        merged = {**car, **updates}
        fi = classify_forced_induction_from_car_row(merged)
        if fi:
            updates["forced_induction"] = fi
        elif not updates.get("forced_induction"):
            updates["forced_induction"] = None

        if not updates:
            continue

        if args.dry_run:
            if updated < 8:
                print(f"  id={car['id']} {car.get('year')} {car.get('make')} {car.get('model')} -> {updates}")
            updated += 1
            continue

        update_car_row_partial(int(car["id"]), updates)
        updated += 1

    if not args.dry_run:
        conn.commit()
        try:
            from backend.db.inventory_db import clear_inventory_listings_cache

            clear_inventory_listings_cache()
        except Exception:
            pass
    conn.close()

    print(f"Updated {updated} cars ({dict_hits} dictionary matches)")


if __name__ == "__main__":
    main()
