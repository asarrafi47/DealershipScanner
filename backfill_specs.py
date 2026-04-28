#!/usr/bin/env python3
"""
Backfill missing vehicle specs (transmission, drivetrain, cylinders, engine).

Uses:
  1. Title parsing + regex patterns (fast, local)
  2. NHTSA vPIC API (DecodeVinValuesExtended) for remaining gaps
  3. EPA master lookups when available
  4. Local model_specs table overrides

Fills gaps in: transmission, drivetrain, cylinders, engine_l, fuel_type, body_style.

Usage:
  python backfill_specs.py                   # All cars with gaps
  python backfill_specs.py --vin <VIN>       # Single car
  python backfill_specs.py --dealer-id <ID>  # Dealer's cars
  python backfill_specs.py --limit 100       # First 100 with gaps
  python backfill_specs.py --all              # All cars (reprocess)
  python backfill_specs.py --dry-run          # Show what would update

Environment:
  INVENTORY_DB_PATH   Path to inventory.db (default: ./inventory.db)
  SPEC_VPIC_OVERWRITE When 1, allow vPIC to overwrite non-empty dealer fields
"""
import argparse
import json
import logging
import os
import sqlite3
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))

try:
    from backend.utils.project_env import load_project_dotenv
    load_project_dotenv()
except ImportError:
    pass

from backend.db.inventory_db import get_conn, get_car_by_id, update_car_row_partial
from backend.knowledge_engine import decode_trim_logic, lookup_epa_aggregate
from backend.nhtsa_vpic import (
    fetch_decode_vin_values_extended,
    looks_like_decode_vin,
    flat_vpic_result_to_car_patch,
)
from backend.utils.field_clean import is_effectively_empty
from backend.utils.inventory_repair import collect_row_storage_repairs

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("backfill_specs")


def needs_spec_backfill(car: dict[str, Any]) -> bool:
    """Check if car has significant gaps in critical fields."""
    # Missing: transmission OR drivetrain OR cylinders
    trans = str(car.get("transmission") or "").strip().lower()
    drive = str(car.get("drivetrain") or "").strip().lower()
    cyl = car.get("cylinders")

    trans_missing = not trans or trans in ("n/a", "na", "unknown", "—", "-")
    drive_missing = not drive or drive in ("n/a", "na", "unknown", "—", "-")
    cyl_missing = not cyl or cyl == 0

    return trans_missing or drive_missing or cyl_missing


def get_cars_to_backfill(
    db_path: str,
    vin: str | None = None,
    dealer_id: str | None = None,
    limit: int = 1000,
    all_cars: bool = False,
) -> list[dict[str, Any]]:
    """Query for cars needing spec backfill."""
    conn = get_conn()
    cursor = conn.cursor()

    if vin:
        cursor.execute("SELECT id, vin, year, make, model, trim, title, transmission, drivetrain, cylinders FROM cars WHERE vin = ?", (vin,))
    elif dealer_id:
        cursor.execute(
            "SELECT id, vin, year, make, model, trim, title, transmission, drivetrain, cylinders "
            "FROM cars WHERE dealer_id = ? ORDER BY scraped_at DESC LIMIT ?",
            (dealer_id, limit),
        )
    elif all_cars:
        cursor.execute(
            "SELECT id, vin, year, make, model, trim, title, transmission, drivetrain, cylinders "
            "FROM cars ORDER BY scraped_at DESC LIMIT ?",
            (limit,),
        )
    else:
        # Default: cars with gaps
        cursor.execute(
            "SELECT id, vin, year, make, model, trim, title, transmission, drivetrain, cylinders "
            "FROM cars WHERE (transmission IS NULL OR transmission = '' OR transmission = 'N/A') "
            "OR (drivetrain IS NULL OR drivetrain = '' OR drivetrain = 'N/A') "
            "OR (cylinders IS NULL OR cylinders = 0) "
            "ORDER BY scraped_at DESC LIMIT ?",
            (limit,),
        )

    rows = cursor.fetchall()
    conn.close()

    return [
        {
            "id": r[0],
            "vin": r[1],
            "year": r[2],
            "make": r[3],
            "model": r[4],
            "trim": r[5],
            "title": r[6],
            "transmission": r[7],
            "drivetrain": r[8],
            "cylinders": r[9],
        }
        for r in rows
    ]


def backfill_single_car(
    car: dict[str, Any],
    db_path: str,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Backfill specs for one car using available sources."""
    vin = car["vin"]
    result = {
        "vin": vin,
        "updated": False,
        "updates": {},
        "sources": [],
        "errors": [],
    }

    try:
        # 1. Title parsing (fast, local)
        logger.info(f"[{vin}] Parsing title...")
        if car.get("title"):
            title_hints = _extract_specs_from_title(car["title"])
            if title_hints:
                for k, v in title_hints.items():
                    if v and not is_effectively_empty(car.get(k)):
                        result["updates"][k] = v
                        result["sources"].append(f"title:{k}")
                logger.debug(f"[{vin}] Title hints: {title_hints}")

        # 2. Knowledge engine (trim decoder)
        logger.info(f"[{vin}] Knowledge engine lookup...")
        if car.get("make") and car.get("model"):
            ke_result = decode_trim_logic(
                make=car["make"],
                model=car["model"],
                trim=car.get("trim"),
                title=car.get("title"),
            )
            if ke_result:
                for k, v in ke_result.items():
                    if k in ("cylinders", "transmission", "drivetrain") and v:
                        if not is_effectively_empty(car.get(k)):
                            result["updates"][k] = v
                            result["sources"].append(f"knowledge_engine:{k}")
                logger.debug(f"[{vin}] Knowledge engine: {ke_result}")

        # 3. NHTSA vPIC (if still missing)
        if looks_like_decode_vin(vin):
            if (
                (is_effectively_empty(car.get("transmission")))
                or (is_effectively_empty(car.get("drivetrain")))
                or (not car.get("cylinders") or car["cylinders"] == 0)
            ):
                logger.info(f"[{vin}] Querying NHTSA vPIC...")
                try:
                    raw_body, flat_row, error_msg = fetch_decode_vin_values_extended(vin)
                    if flat_row and not error_msg:
                        patch = flat_vpic_result_to_car_patch(flat_row)
                        if patch:
                            for k, v in patch.items():
                                if k in ("cylinders", "transmission", "drivetrain", "fuel_type", "body_style", "engine_description"):
                                    if v and is_effectively_empty(car.get(k)):
                                        result["updates"][k] = v
                                        result["sources"].append(f"vpic:{k}")
                            logger.debug(f"[{vin}] vPIC: {patch}")
                        else:
                            logger.debug(f"[{vin}] vPIC: no patch generated")
                    else:
                        logger.debug(f"[{vin}] vPIC: {error_msg or 'no data'}")
                except Exception as e:
                    result["errors"].append(f"vPIC lookup failed: {e}")
                    logger.warning(f"[{vin}] vPIC error: {e}")

        # 4. EPA master lookup (if available)
        if car.get("year") and car.get("make") and car.get("model"):
            logger.info(f"[{vin}] EPA master lookup...")
            try:
                epa = lookup_epa_aggregate(
                    year=car["year"],
                    make=car["make"],
                    model=car["model"],
                )
                if epa:
                    for k in ("cylinders", "fuel_type", "drivetrain"):
                        if epa.get(k) and is_effectively_empty(car.get(k)):
                            result["updates"][k] = epa[k]
                            result["sources"].append(f"epa:{k}")
                    logger.debug(f"[{vin}] EPA: {epa}")
            except Exception as e:
                logger.debug(f"[{vin}] EPA lookup: {e}")

    except Exception as e:
        result["errors"].append(str(e))
        logger.error(f"[{vin}] Backfill failed: {e}")

    # Update database
    if result["updates"]:
        if dry_run:
            logger.info(f"[{vin}] DRY-RUN would update: {result['updates']}")
        else:
            try:
                update_car_row_partial(car["id"], result["updates"])
                result["updated"] = True
                logger.info(f"[{vin}] ✅ Updated: {list(result['updates'].keys())}")
            except Exception as e:
                result["errors"].append(f"DB update failed: {e}")
                logger.error(f"[{vin}] DB update failed: {e}")

    return result


def _extract_specs_from_title(title: str) -> dict[str, Any]:
    """Extract transmission, drivetrain, engine hints from title string."""
    hints = {}
    if not title:
        return hints

    u = title.upper()

    # Transmission patterns
    if "CVT" in u:
        hints["transmission"] = "Continuously Variable"
    elif "MANUAL" in u:
        hints["transmission"] = "Manual"
    elif re.search(r"\b(\d)-SPEED\s+AUTO", u):
        match = re.search(r"\b(\d+)-SPEED\s+AUTO", u)
        if match:
            hints["transmission"] = f"{match.group(1)}-Speed Automatic"
    elif "AUTOMATIC" in u or "AUTO" in u:
        hints["transmission"] = "Automatic"

    # Drivetrain patterns
    if "AWD" in u:
        hints["drivetrain"] = "All-Wheel Drive"
    elif "4WD" in u or "4X4" in u:
        hints["drivetrain"] = "Four-Wheel Drive"
    elif "FWD" in u or "FRONT WHEEL" in u:
        hints["drivetrain"] = "Front-Wheel Drive"
    elif "RWD" in u or "REAR WHEEL" in u:
        hints["drivetrain"] = "Rear-Wheel Drive"

    return hints


def main():
    ap = argparse.ArgumentParser(
        description="Backfill missing vehicle specs (transmission, drivetrain, cylinders).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    ap.add_argument("--vin", help="Backfill only this VIN")
    ap.add_argument("--dealer-id", help="Backfill only this dealer's vehicles")
    ap.add_argument("--limit", type=int, default=1000, help="Max cars to process (default: 1000)")
    ap.add_argument("--all", action="store_true", help="Reprocess all cars")
    ap.add_argument("--dry-run", action="store_true", help="Show what would update without saving")
    args = ap.parse_args()

    db_path = os.environ.get("INVENTORY_DB_PATH", "inventory.db")
    logger.info(f"Specs Backfill — DB: {db_path}")

    cars = get_cars_to_backfill(
        db_path,
        vin=args.vin,
        dealer_id=args.dealer_id,
        limit=args.limit,
        all_cars=args.all,
    )

    if not cars:
        logger.info("No cars need backfill")
        return

    cars_to_process = [c for c in cars if needs_spec_backfill(c)] if not args.all else cars

    logger.info(f"Found {len(cars)} total cars, {len(cars_to_process)} need backfill")

    stats = {
        "total": len(cars_to_process),
        "updated": 0,
        "skipped": 0,
        "errors": 0,
    }

    for car in cars_to_process:
        result = backfill_single_car(car, db_path, dry_run=args.dry_run)

        if result["updated"]:
            stats["updated"] += 1
        elif result["errors"]:
            stats["errors"] += 1
        else:
            stats["skipped"] += 1

    logger.info("\n" + "=" * 60)
    logger.info(f"Backfill Complete:")
    logger.info(f"  Total processed: {stats['total']}")
    logger.info(f"  Updated: {stats['updated']}")
    logger.info(f"  Skipped: {stats['skipped']}")
    logger.info(f"  Errors: {stats['errors']}")
    logger.info("=" * 60)


import re

if __name__ == "__main__":
    main()
