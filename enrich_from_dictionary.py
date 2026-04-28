#!/usr/bin/env python3
"""
Enrich cars in inventory.db using the /DICTIONARY EPA CSV files.

For each car missing transmission, drivetrain, fuel_type, cylinders,
displacement, mpg_city, mpg_highway, or body_style — looks up the
matching EPA CSV (year+make+model) and fills in what's available.

Uses trim matching when possible; falls back to the most common value
for that year+make+model when no trim match is found.

Usage:
  python enrich_from_dictionary.py             # fill only gaps
  python enrich_from_dictionary.py --all       # reapply to all cars
  python enrich_from_dictionary.py --dry-run   # show changes without writing
"""
from __future__ import annotations

import argparse
import csv
import logging
import os
import re
import sqlite3
import sys
from collections import Counter
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger("enrich_dict")

DICTIONARY = ROOT / "DICTIONARY"
DB_PATH = os.environ.get("INVENTORY_DB_PATH", "inventory.db")

FILLABLE_FIELDS = {
    "transmission":   "transmissionOptions",
    "drivetrain":     "drivetrainOptions",
    "fuel_type":      "fuelType",
    "cylinders":      "cylinders",
    "engine_l":       "displacement",
    "mpg_city":       "mpg_city",
    "mpg_highway":    "mpg_highway",
    "body_style":     "bodyStyle",
    "engine_description": "engineOptions",
}

_epa_cache: dict[str, list[dict]] = {}


def _normalize(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())


def _load_epa_csv(year: int, make: str, model: str) -> list[dict]:
    key = f"{year}_{make}_{model}"
    if key in _epa_cache:
        return _epa_cache[key]

    make_safe = re.sub(r"[^\w\-. ]", "_", make.strip())
    model_safe = re.sub(r"[^\w\-. ]", "_", model.strip())
    fname = DICTIONARY / f"{year}_{make_safe}_{model_safe}_EPA.csv"

    rows = []
    if fname.exists():
        with open(fname, newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
    _epa_cache[key] = rows
    return rows


def _best_row(epa_rows: list[dict], trim: str) -> dict | None:
    if not epa_rows:
        return None

    norm_trim = _normalize(trim or "")

    # Exact trim match
    for row in epa_rows:
        if _normalize(row.get("Trim", "")) == norm_trim:
            return row

    # Partial trim match — car trim is substring of EPA trim or vice versa
    if norm_trim:
        for row in epa_rows:
            epa_t = _normalize(row.get("Trim", ""))
            if norm_trim in epa_t or epa_t in norm_trim:
                return row

    # Fallback: most common transmission (proxy for "base/most popular trim")
    trans_counter: Counter = Counter()
    for row in epa_rows:
        t = (row.get("transmissionOptions") or "").strip()
        if t:
            trans_counter[t] += 1
    if trans_counter:
        most_common_trans = trans_counter.most_common(1)[0][0]
        for row in epa_rows:
            if (row.get("transmissionOptions") or "").strip() == most_common_trans:
                return row

    return epa_rows[0]


def _is_empty(val: Any) -> bool:
    if val is None:
        return True
    s = str(val).strip()
    return not s or s.lower() in ("n/a", "na", "unknown", "null", "0", "none", "-")


def enrich_car(car: dict, dry_run: bool = False, fill_all: bool = False) -> dict:
    year = car.get("year")
    make = (car.get("make") or "").strip()
    model = (car.get("model") or "").strip()
    trim = car.get("trim") or ""
    vin = car.get("vin", "")

    updates: dict[str, Any] = {}

    if not year or not make or not model:
        return updates

    epa_rows = _load_epa_csv(int(year), make, model)
    if not epa_rows:
        return updates

    best = _best_row(epa_rows, trim)
    if not best:
        return updates

    for db_col, csv_col in FILLABLE_FIELDS.items():
        current = car.get(db_col)
        if not fill_all and not _is_empty(current):
            continue
        epa_val = (best.get(csv_col) or "").strip()
        if not epa_val or epa_val in ("0", "0.0"):
            continue
        # Type coercions
        if db_col == "cylinders":
            try:
                epa_val = int(float(epa_val))
                if epa_val == 0:
                    continue
            except (ValueError, TypeError):
                continue
        elif db_col in ("mpg_city", "mpg_highway"):
            try:
                epa_val = int(float(epa_val))
                if epa_val <= 0:
                    continue
            except (ValueError, TypeError):
                continue
        elif db_col == "engine_l":
            try:
                epa_val = float(epa_val)
                if epa_val <= 0:
                    continue
            except (ValueError, TypeError):
                continue
        updates[db_col] = epa_val

    return updates


def main():
    ap = argparse.ArgumentParser(description="Enrich cars from DICTIONARY EPA data")
    ap.add_argument("--all", action="store_true", help="Reapply to all cars (not just those with gaps)")
    ap.add_argument("--dry-run", action="store_true", help="Show what would be filled without writing")
    args = ap.parse_args()

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    if args.all:
        query = "SELECT * FROM cars WHERE make IS NOT NULL AND model IS NOT NULL ORDER BY id"
    else:
        query = (
            "SELECT * FROM cars WHERE make IS NOT NULL AND model IS NOT NULL "
            "AND (transmission IS NULL OR transmission='' "
            "OR drivetrain IS NULL OR drivetrain='' "
            "OR fuel_type IS NULL OR fuel_type='' "
            "OR cylinders IS NULL OR cylinders=0 "
            "OR mpg_city IS NULL OR mpg_city=0) "
            "ORDER BY id"
        )

    cars = [dict(r) for r in conn.execute(query).fetchall()]
    logger.info("Cars to process: %d", len(cars))
    if args.dry_run:
        logger.info("DRY-RUN mode — no DB writes")

    stats = {"total": len(cars), "updated": 0, "no_epa": 0}

    for car in cars:
        updates = enrich_car(car, dry_run=args.dry_run, fill_all=args.all)
        if not updates:
            stats["no_epa"] += 1
            continue

        if not args.dry_run:
            set_clause = ", ".join(f"{k}=?" for k in updates)
            vals = list(updates.values()) + [car["id"]]
            conn.execute(f"UPDATE cars SET {set_clause} WHERE id=?", vals)

        stats["updated"] += 1
        fields_str = ", ".join(f"{k}={v}" for k, v in updates.items())
        logger.info("[%s] %s %s %s → %s", car["vin"], car["year"], car["make"], car["model"], fields_str)

    if not args.dry_run:
        conn.commit()
    conn.close()

    logger.info("=" * 55)
    logger.info("Complete: total=%d updated=%d no_epa_match=%d", stats["total"], stats["updated"], stats["no_epa"])
    if stats["updated"]:
        logger.info("Next: python rebuild_listings_index.py --fast")


if __name__ == "__main__":
    main()
