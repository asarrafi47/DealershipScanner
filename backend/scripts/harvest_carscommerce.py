#!/usr/bin/env python3
"""
Fill inventory gaps from the shared CarsCommerce group API — no browser.

Replays every dealer's captured CarsCommerce recipe at full page size, matches
harvested rows to ``cars`` by VIN, and fills ONLY empty columns (never
overwrites a stored value; never touches price — see
backend/scanner/carscommerce_harvest.py). Patched rows are re-synced in the
incomplete-listings index.

Usage (repo root)::

  python3 backend/scripts/harvest_carscommerce.py --dry-run   # report only
  python3 backend/scripts/harvest_carscommerce.py             # all CarsCommerce dealers
  python3 backend/scripts/harvest_carscommerce.py --dealer-id normreeves-com
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
os.chdir(_REPO_ROOT)
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from backend.utils.project_env import load_project_dotenv

load_project_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("harvest_carscommerce")

from backend.scanner.carscommerce_harvest import HARVEST_FIELDS, harvest_dealer, is_carscommerce_recipe

# Columns we fill (drop image_url's synthetic handling — treat like the rest).
_FILL = tuple(f for f in HARVEST_FIELDS)


def _carscommerce_dealer_ids() -> list[str]:
    from backend.scanner.recipes import load_recipes

    out = []
    for path in sorted((_REPO_ROOT / "workspace" / "recipes").glob("*.json")):
        did = path.stem
        if any(is_carscommerce_recipe(r.url) and not r.stale for r in load_recipes(did)):
            out.append(did)
    return out


def patch_dealer(dealer_id: str, *, dry_run: bool, include_new: bool) -> dict:
    from backend.db.incomplete_listings_db import sync_incomplete_listing_for_car_id
    from backend.db.inventory_db import db_conn

    by_vin = harvest_dealer(dealer_id, include_new=include_new)
    stats = {"harvested": len(by_vin), "rows_patched": 0, "fields": {}}
    if not by_vin:
        return stats

    patched_ids: list[int] = []
    with db_conn() as conn:
        cur = conn.cursor()
        for vin, fields in by_vin.items():
            cur.execute(
                f"SELECT id, {', '.join(_FILL)} FROM cars"
                " WHERE UPPER(vin) = ? AND (COALESCE(listing_active, 1) = 1)",
                (vin,),
            )
            for row in cur.fetchall():
                car_id, *current = row
                updates = {
                    f: fields[f]
                    for f, cur_val in zip(_FILL, current)
                    if fields.get(f) is not None
                    and (cur_val is None or str(cur_val).strip() == "")
                }
                if not updates:
                    continue
                stats["rows_patched"] += 1
                for f in updates:
                    stats["fields"][f] = stats["fields"].get(f, 0) + 1
                if dry_run:
                    continue
                set_sql = ", ".join(f"{f} = ?" for f in updates)
                cur.execute(f"UPDATE cars SET {set_sql} WHERE id = ?", (*updates.values(), car_id))
                patched_ids.append(car_id)
        if not dry_run:
            conn.commit()

    for car_id in patched_ids:
        try:
            sync_incomplete_listing_for_car_id(car_id)
        except Exception:
            log.exception("index sync failed for car %s", car_id)
    return stats


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dealer-id", action="append", default=None)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--used-only", action="store_true", help="keep the recipe's Used/CPO filter")
    ns = ap.parse_args()

    dealer_ids = ns.dealer_id or _carscommerce_dealer_ids()
    log.info("CarsCommerce dealers: %d", len(dealer_ids))

    grand = {"harvested": 0, "rows_patched": 0, "fields": {}}
    for did in dealer_ids:
        stats = patch_dealer(did, dry_run=ns.dry_run, include_new=not ns.used_only)
        if stats["harvested"] or stats["rows_patched"]:
            log.info("[%s] %s", did, json.dumps(stats))
        grand["harvested"] += stats["harvested"]
        grand["rows_patched"] += stats["rows_patched"]
        for f, n in stats["fields"].items():
            grand["fields"][f] = grand["fields"].get(f, 0) + n
    log.info("TOTAL%s: %s", " (dry-run)" if ns.dry_run else "", json.dumps(grand))


if __name__ == "__main__":
    main()
