#!/usr/bin/env python3
"""
Apply the model_specs dictionary + static fallbacks to fill missing cylinders,
transmission, drivetrain, body_style, and fuel_type on ``cars`` rows.

Delegates to :func:`backend.scanner.database.apply_model_specs_corrections` (same logic as
post-upsert scanner correction and ``lookup_model_specs_dictionary``).

Usage:
  python apply_model_specs.py                  # Full inventory; apply updates
  python apply_model_specs.py --dry-run        # Count rows that would change (no DB writes)
  python apply_model_specs.py --all              # Same as default (all rows scanned)

Environment:
  INVENTORY_DB_PATH — SQLite path (default: inventory.db in cwd)

See also:
  backend/model_specs_dictionary.py — fuzzy lookup + fallbacks
  populate_model_specs.py — seed ``model_specs`` table
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
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
logger = logging.getLogger("apply_model_specs")


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Fill missing specs from model_specs dictionary + fallbacks.",
    )
    ap.add_argument(
        "--all",
        action="store_true",
        help="Scan all inventory rows (default; kept for backward compatibility)",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Report how many rows would be updated without writing to the database",
    )
    args = ap.parse_args()

    db_path = os.environ.get("INVENTORY_DB_PATH", "inventory.db")
    if not os.path.isfile(os.path.abspath(db_path)):
        logger.error("Database not found: %s", db_path)
        sys.exit(1)

    from backend.scanner.database import apply_model_specs_corrections

    if args.all:
        logger.info("Scanning full inventory (same as default).")

    logger.info("Database: %s", os.path.abspath(db_path))
    if args.dry_run:
        logger.info("Dry-run mode — no rows will be modified.")

    n = apply_model_specs_corrections(vins=None, dry_run=args.dry_run)

    logger.info("Done: %s row(s).", n)
    if not args.dry_run and n > 0:
        logger.info("Next (optional): python rebuild_listings_index.py --fast")


if __name__ == "__main__":
    main()
