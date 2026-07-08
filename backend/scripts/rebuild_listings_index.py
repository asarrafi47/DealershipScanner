#!/usr/bin/env python3
"""
Rebuild the incomplete/complete listings index.

This scans all cars in inventory.db and moves those with missing critical specs
(transmission, drivetrain, cylinders, price, etc.) into incomplete_listings.db.

After running:
  - Cars with all specs → appear on /listings (complete listings)
  - Cars with gaps → appear on /dev incomplete listings page only

Usage (from repo root)::

  python3 backend/scripts/rebuild_listings_index.py
  python3 backend/scripts/rebuild_listings_index.py --fast    # Faster, no knowledge engine
"""
import argparse
import logging
import os
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
os.chdir(_REPO_ROOT)
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

try:
    from backend.utils.project_env import load_project_dotenv
    load_project_dotenv()
except ImportError:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("rebuild_index")


def main():
    ap = argparse.ArgumentParser(
        description="Rebuild incomplete/complete listings index.",
    )
    ap.add_argument(
        "--fast",
        action="store_true",
        help="Use fast rebuild (no knowledge engine, direct DB scan)",
    )
    args = ap.parse_args()

    from backend.db import incomplete_listings_db as _ild
    from backend.db.incomplete_listings_db import (
        rebuild_incomplete_listings_index,
        fast_rebuild_incomplete_listings_index,
    )

    from backend.db.inventory_db import get_conn as _inventory_get_conn

    conn_inv = _inventory_get_conn()
    try:
        total_cars = int(conn_inv.execute("SELECT COUNT(*) FROM cars").fetchone()[0])
    finally:
        conn_inv.close()

    logger.info("=" * 60)
    logger.info("Rebuilding listings index...")
    logger.info(f"Inventory snapshot before rebuild: cars table rows={total_cars}")
    logger.info("=" * 60)

    if args.fast:
        logger.info("Using fast rebuild (direct DB scan)...")
        incomplete_returned = fast_rebuild_incomplete_listings_index()
    else:
        logger.info("Using full rebuild (with knowledge engine)...")
        incomplete_returned = rebuild_incomplete_listings_index()

    conn_inc = _ild.get_conn()
    try:
        incomplete_table_rows = int(
            conn_inc.execute("SELECT COUNT(*) FROM incomplete_listings").fetchone()[0]
        )
    finally:
        conn_inc.close()

    if int(incomplete_returned) != incomplete_table_rows:
        logger.warning(
            "Incomplete count mismatch: rebuild returned %s but incomplete_listings has %s rows",
            incomplete_returned,
            incomplete_table_rows,
        )

    print(
        f"[rebuild_listings_index] debug: cars={total_cars} "
        f"incomplete_listings={incomplete_table_rows}",
        flush=True,
    )

    incomplete = incomplete_table_rows
    complete = max(0, total_cars - incomplete)

    logger.info("=" * 60)
    logger.info(f"✅ Rebuild complete!")
    logger.info(f"   Total cars (inventory): {total_cars}")
    logger.info(f"   Incomplete listings: {incomplete}")
    logger.info(f"   Complete listings: {complete}")
    logger.info("=" * 60)
    logger.info("")
    logger.info("Next steps:")
    logger.info("  - Run: python backfill_specs.py  (to fill more spec gaps)")
    logger.info("  - Check: http://127.0.0.1:5000/dev for incomplete listings")
    logger.info("  - Check: http://127.0.0.1:5000/listings for complete listings")


if __name__ == "__main__":
    main()
