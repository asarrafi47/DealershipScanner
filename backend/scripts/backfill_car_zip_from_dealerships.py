#!/usr/bin/env python3
"""
Backfill empty ``cars.zip_code`` from dealership registry / geopoints.

Usage (from repo root):
  PYTHONPATH=. python3 backend/scripts/backfill_car_zip_from_dealerships.py --dry-run
  PYTHONPATH=. python3 backend/scripts/backfill_car_zip_from_dealerships.py
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from backend.utils.project_env import bootstrap_inventory_script

    bootstrap_inventory_script()
except ImportError:
    pass

from backend.db.inventory_db import backfill_car_zip_from_dealerships  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")


def main() -> int:
    p = argparse.ArgumentParser(description="Backfill cars.zip_code from dealerships.")
    p.add_argument("--dry-run", action="store_true", help="Count rows only; do not write.")
    args = p.parse_args()
    stats = backfill_car_zip_from_dealerships(dry_run=args.dry_run)
    mode = "dry-run" if args.dry_run else "applied"
    print(f"[{mode}] by_registry={stats['by_registry']} by_lookup={stats['by_lookup']} remaining={stats['remaining']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
