#!/usr/bin/env python3
"""
Rebuild the incomplete/complete listings index.

This scans all cars in inventory.db and moves those with missing critical specs
(transmission, drivetrain, cylinders, price, etc.) into incomplete_listings.db.

After running:
  - Cars with all specs → appear on /listings (complete listings)
  - Cars with gaps → appear on /dev incomplete listings page only

Usage:
  python rebuild_listings_index.py
  python rebuild_listings_index.py --fast    # Faster, no knowledge engine
"""
import argparse
import logging
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))

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

    from backend.db.incomplete_listings_db import (
        rebuild_incomplete_listings_index,
        fast_rebuild_incomplete_listings_index,
    )

    logger.info("=" * 60)
    logger.info("Rebuilding listings index...")
    logger.info("=" * 60)

    if args.fast:
        logger.info("Using fast rebuild (direct DB scan)...")
        count = fast_rebuild_incomplete_listings_index()
    else:
        logger.info("Using full rebuild (with knowledge engine)...")
        count = rebuild_incomplete_listings_index()

    logger.info("=" * 60)
    logger.info(f"✅ Rebuild complete!")
    logger.info(f"   Incomplete listings: {count}")
    logger.info(f"   Complete listings: {205 - count}")
    logger.info("=" * 60)
    logger.info("")
    logger.info("Next steps:")
    logger.info("  - Run: python backfill_specs.py  (to fill more spec gaps)")
    logger.info("  - Check: http://127.0.0.1:5000/dev for incomplete listings")
    logger.info("  - Check: http://127.0.0.1:5000/listings for complete listings")


if __name__ == "__main__":
    main()
