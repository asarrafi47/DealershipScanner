#!/usr/bin/env python3
"""
Apply model_specs dictionary to fill missing transmission/drivetrain/cylinders in cars table.

References the model_specs table (dictionary) created by populate_model_specs.py
and fills in missing fields for cars that match make/model.

Usage:
  python apply_model_specs.py                # Apply to all cars with gaps
  python apply_model_specs.py --all          # Reapply to all cars
  python apply_model_specs.py --dry-run      # Show what would be filled
"""
import argparse
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("apply_model_specs")


def is_empty(val: Any) -> bool:
    """Check if a field is empty/missing."""
    if val is None:
        return True
    s = str(val).strip()
    if not s:
        return True
    return s.lower() in ("n/a", "na", "unknown", "—", "-", "null")


def get_cars_to_fill(db_path: str, fill_all: bool = False) -> list[dict[str, Any]]:
    """Get cars with gaps that could be filled from model_specs."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    if fill_all:
        # All cars
        cursor.execute(
            "SELECT id, vin, make, model, transmission, drivetrain, cylinders FROM cars "
            "WHERE make IS NOT NULL AND model IS NOT NULL ORDER BY vin"
        )
    else:
        # Cars with gaps
        cursor.execute(
            "SELECT id, vin, make, model, transmission, drivetrain, cylinders FROM cars "
            "WHERE make IS NOT NULL AND model IS NOT NULL "
            "AND (transmission IS NULL OR transmission = '' OR transmission LIKE '%N/A%' "
            "OR drivetrain IS NULL OR drivetrain = '' OR drivetrain LIKE '%N/A%' "
            "OR cylinders IS NULL OR cylinders = 0) "
            "ORDER BY vin"
        )

    rows = cursor.fetchall()
    conn.close()

    return [
        {
            "id": r[0],
            "vin": r[1],
            "make": r[2],
            "model": r[3],
            "transmission": r[4],
            "drivetrain": r[5],
            "cylinders": r[6],
        }
        for r in rows
    ]


def lookup_model_specs(
    db_path: str, make: str, model: str
) -> dict[str, Any] | None:
    """Look up specs in model_specs table."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute(
        "SELECT transmission, drivetrain, cylinders FROM model_specs WHERE make = ? AND model = ?",
        (make, model),
    )
    row = cursor.fetchone()
    conn.close()

    if not row:
        return None

    return {
        "transmission": row[0],
        "drivetrain": row[1],
        "cylinders": row[2],
    }


def apply_specs_to_car(
    db_path: str,
    car_id: int,
    car: dict[str, Any],
    dry_run: bool = False,
) -> dict[str, Any]:
    """Apply model_specs to a single car."""
    make = car.get("make", "").strip()
    model = car.get("model", "").strip()
    vin = car.get("vin", "")

    result = {
        "vin": vin,
        "updated": False,
        "updates": {},
    }

    if not make or not model:
        return result

    specs = lookup_model_specs(db_path, make, model)
    if not specs:
        return result

    updates = {}

    # Fill transmission if missing
    if specs["transmission"] and is_empty(car.get("transmission")):
        updates["transmission"] = specs["transmission"]

    # Fill drivetrain if missing
    if specs["drivetrain"] and is_empty(car.get("drivetrain")):
        updates["drivetrain"] = specs["drivetrain"]

    # Fill cylinders if missing
    if specs["cylinders"] and (not car.get("cylinders") or car["cylinders"] == 0):
        updates["cylinders"] = specs["cylinders"]

    if not updates:
        return result

    result["updates"] = updates

    if not dry_run:
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()

            set_clause = ", ".join(f"{k} = ?" for k in updates.keys())
            values = list(updates.values()) + [car_id]
            cursor.execute(f"UPDATE cars SET {set_clause} WHERE id = ?", values)
            conn.commit()
            conn.close()
            result["updated"] = True
        except Exception as e:
            logger.error(f"[{vin}] DB update failed: {e}")

    return result


def main():
    ap = argparse.ArgumentParser(
        description="Apply model_specs dictionary to fill missing car specs.",
    )
    ap.add_argument(
        "--all",
        action="store_true",
        help="Reapply to all cars (not just those with gaps)",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be filled without updating DB",
    )
    args = ap.parse_args()

    db_path = os.environ.get("INVENTORY_DB_PATH", "inventory.db")

    logger.info("=" * 60)
    logger.info("Applying Model Specs Dictionary")
    logger.info("=" * 60)

    cars = get_cars_to_fill(db_path, fill_all=args.all)

    if not cars:
        logger.info("No cars to process")
        return

    logger.info(f"Found {len(cars)} cars to process")
    if args.dry_run:
        logger.info("(DRY-RUN mode)")

    stats = {
        "total": len(cars),
        "updated": 0,
        "no_match": 0,
    }

    for car in cars:
        result = apply_specs_to_car(db_path, car["id"], car, dry_run=args.dry_run)

        if result["updated"]:
            stats["updated"] += 1
            updates_str = ", ".join(
                f"{k}={v}" for k, v in result["updates"].items()
            )
            logger.info(f"[{result['vin']}] Updated: {updates_str}")
        elif not result["updates"]:
            stats["no_match"] += 1

    logger.info("=" * 60)
    logger.info("Apply Complete:")
    logger.info(f"  Total: {stats['total']}")
    logger.info(f"  Updated: {stats['updated']}")
    logger.info(f"  No match in dictionary: {stats['no_match']}")
    logger.info("=" * 60)

    if stats["updated"] > 0:
        logger.info("")
        logger.info("Next: python rebuild_listings_index.py --fast")


if __name__ == "__main__":
    main()
