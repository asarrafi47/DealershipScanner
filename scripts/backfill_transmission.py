"""
Backfill missing transmission AND cylinders in the cars table.

Priority order for each field:
  1. model_specs dictionary  (canonical make/model lookup)
  2. EPA aggregate lookup    (fuzzy-matched, formatted)
  3. Trim decoder hint       (regex brand rules)

Also seeds model_specs so the Node.js scanner self-correction and the
apply_model_specs_corrections() post-upsert hook have a full reference
dictionary to work from for both new and existing listings.

Usage:
    python scripts/backfill_transmission.py [--dry-run] [--db PATH]
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.knowledge_engine import (
    decode_trim_logic,
    format_transmission_display,
    lookup_epa_aggregate,
)
from backend.database import apply_model_specs_corrections

DB_PATH = os.environ.get("INVENTORY_DB_PATH", "inventory.db")

# ---------------------------------------------------------------------------
# Model specs seed data (make, model, cylinders, gears, transmission)
# These are the "dictionary" entries the scanner self-correction uses.
# ---------------------------------------------------------------------------
MODEL_SPECS_SEED = [
    # BMW — all modern (2019+) use 8-speed Steptronic
    ("BMW", "X1",       4, 7,  "7-Speed DCT"),
    ("BMW", "X2",       4, 7,  "7-Speed DCT"),
    ("BMW", "X3",       4, 8,  "8-Speed Automatic"),
    ("BMW", "X3 M",     6, 8,  "8-Speed Automatic"),
    ("BMW", "X4",       4, 8,  "8-Speed Automatic"),
    ("BMW", "X5",       6, 8,  "8-Speed Automatic"),
    ("BMW", "X5 M",     8, 8,  "8-Speed Automatic"),
    ("BMW", "X5 PHEV",  4, 8,  "8-Speed Automatic"),
    ("BMW", "X6",       6, 8,  "8-Speed Automatic"),
    ("BMW", "X6 M",     8, 8,  "8-Speed Automatic"),
    ("BMW", "X7",       6, 8,  "8-Speed Automatic"),
    ("BMW", "3 Series", 4, 8,  "8-Speed Automatic"),
    ("BMW", "5 Series", 6, 8,  "8-Speed Automatic"),
    ("BMW", "7 Series", 6, 8,  "8-Speed Automatic"),
    ("BMW", "M2",       6, 8,  "8-Speed Automatic"),
    ("BMW", "M3",       6, 8,  "8-Speed Automatic"),
    ("BMW", "M4",       6, 8,  "8-Speed Automatic"),
    ("BMW", "M5",       8, 8,  "8-Speed Automatic"),
    ("BMW", "M8",       8, 8,  "8-Speed Automatic"),
    ("BMW", "550e",     6, 8,  "8-Speed Automatic"),
    ("BMW", "530e",     4, 8,  "8-Speed Automatic"),
    ("BMW", "330e",     4, 8,  "8-Speed Automatic"),
    ("BMW", "M340",     6, 8,  "8-Speed Automatic"),
    ("BMW", "M440",     6, 8,  "8-Speed Automatic"),
    ("BMW", "228i",     4, 8,  "8-Speed Automatic"),
    ("BMW", "750e",     6, 8,  "8-Speed Automatic"),
    ("BMW", "Z4",       4, 8,  "8-Speed Automatic"),
    # Jeep (stored with correct make in model_specs even if DB has wrong make)
    ("Jeep", "Wrangler",         6, 8,  "8-Speed Automatic"),
    ("Jeep", "Gladiator",        6, 8,  "8-Speed Automatic"),
    ("Jeep", "Grand Cherokee",   6, 8,  "8-Speed Automatic"),
    ("Jeep", "Grand Cherokee L", 6, 8,  "8-Speed Automatic"),
    ("Jeep", "Grand Wagoneer",   8, 8,  "8-Speed Automatic"),
    ("Jeep", "Grand Wagoneer L", 8, 8,  "8-Speed Automatic"),
    ("Jeep", "Cherokee",         4, 9,  "9-Speed Automatic"),
    ("Jeep", "Compass",          4, 7,  "7-Speed DCT"),
    # RAM
    ("RAM", "1500",     8, 8,  "8-Speed Automatic"),
    ("RAM", "2500",     6, 6,  "6-Speed Automatic"),
    ("RAM", "3500",     6, 6,  "6-Speed Automatic"),
    ("RAM", "ProMaster 1500", 4, 9, "9-Speed Automatic"),
    ("RAM", "ProMaster 2500", 4, 9, "9-Speed Automatic"),
    # Dodge
    ("Dodge", "Durango",   6, 8, "8-Speed Automatic"),
    ("Dodge", "Charger",   6, 8, "8-Speed Automatic"),
    ("Dodge", "Challenger",6, 8, "8-Speed Automatic"),
    ("Dodge", "Journey",   4, 4, "4-Speed Automatic"),
    ("Dodge", "Grand Caravan", 6, 6, "6-Speed Automatic"),
    ("Dodge", "Pacifica",  6, 9, "9-Speed Automatic"),
    # Ford
    ("Ford", "F-150",        8, 10, "10-Speed Automatic"),
    ("Ford", "F-250",        8, 10, "10-Speed Automatic"),
    ("Ford", "F-350",        8, 10, "10-Speed Automatic"),
    ("Ford", "Ranger",       4, 10, "10-Speed Automatic"),
    ("Ford", "Explorer",     4, 10, "10-Speed Automatic"),
    ("Ford", "Bronco Sport", 4, 8,  "8-Speed Automatic"),
    ("Ford", "Escape",       4, 8,  "8-Speed Automatic"),
    # GMC/Chevy
    ("GMC",  "Sierra 1500",      8, 10, "10-Speed Automatic"),
    ("Chevrolet", "Silverado 1500", 8, 10, "10-Speed Automatic"),
    ("Chevrolet", "Camaro",         6, 10, "10-Speed Automatic"),
    ("Chevrolet", "Tahoe",          8, 10, "10-Speed Automatic"),
    # Toyota
    ("Toyota", "Tacoma",  4, 8, "8-Speed Automatic"),
    ("Toyota", "RAV4",    4, 8, "8-Speed Automatic"),
    # Hyundai / Kia
    ("Hyundai", "Ioniq 5", 0, 1, "Single-speed automatic"),
    ("Hyundai", "Kona",    4, 8, "8-Speed DCT"),
    ("Kia",     "Telluride", 6, 8, "8-Speed Automatic"),
    # Others
    ("Mazda", "CX-50", 4, 6, "6-Speed Automatic"),
    ("Cadillac", "Escalade ESV", 8, 10, "10-Speed Automatic"),
    ("Lincoln", "Aviator", 6, 10, "10-Speed Automatic"),
    ("Acura", "MDX", 6, 10, "10-Speed Automatic"),
    ("Acura", "RDX", 4, 10, "10-Speed Automatic"),
    ("Honda", "Accord Sedan", 4, 10, "10-Speed Automatic"),
    ("VW",    "Tiguan",  4, 8, "8-Speed Automatic"),
    ("VW",    "Golf GTI", 4, 7, "7-Speed DCT"),
    ("Mini",  "Cooper S Countryman", 4, 8, "8-Speed Automatic"),
    ("Lexus", "GX",       8, 6, "6-Speed Automatic"),
    ("Buick", "LaCrosse", 4, 6, "6-Speed Automatic"),
    ("Kia",   "Forte",    4, 7, "7-Speed DCT"),
    # BMW EVs (0 cylinders = electric)
    ("BMW",     "i4",  0, 1, "Single-speed automatic"),
    ("BMW",     "i5",  0, 1, "Single-speed automatic"),
    ("BMW",     "i7",  0, 1, "Single-speed automatic"),
    ("BMW",     "iX",  0, 1, "Single-speed automatic"),
    # Tesla (all electric)
    ("Tesla",   "Model 3", 0, 1, "Single-speed automatic"),
    ("Tesla",   "Model Y", 0, 1, "Single-speed automatic"),
    ("Tesla",   "Model S", 0, 1, "Single-speed automatic"),
    ("Tesla",   "Model X", 0, 1, "Single-speed automatic"),
    # Hyundai EVs
    ("Hyundai", "IONIQ 5", 0, 1, "Single-speed automatic"),
    ("Hyundai", "IONIQ 6", 0, 1, "Single-speed automatic"),
    ("Hyundai", "Ioniq 5", 0, 1, "Single-speed automatic"),
    # Ford EV
    ("Ford",    "Mustang Mach-E", 0, 1, "Single-speed automatic"),
    # Kia
    ("Kia",     "Sorento",  6, 8, "8-Speed Automatic"),
]

# Mapping for cars in the DB where make/model got swapped by scraper
# (e.g. make='Wrangler' model='Wrangler' → should be make='Jeep' model='Wrangler')
MAKE_FIX_MAP = {
    # (db_make, db_model) → canonical_make
    ("Wrangler",  "Wrangler"):          "Jeep",
    ("Gladiator", "Gladiator"):         "Jeep",
    ("Grand",     "Grand Cherokee"):    "Jeep",
    ("Grand",     "Grand Cherokee L"):  "Jeep",
    ("Grand",     "Grand Wagoneer"):    "Jeep",
    ("Grand",     "Grand Wagoneer L"):  "Jeep",
    ("Cherokee",  "Cherokee"):          "Jeep",
    ("Compass",   "Compass"):           "Jeep",
    ("1500",      "1500"):              "RAM",
    ("2500",      "2500"):              "RAM",
    ("3500",      "3500"):              "RAM",
    ("Classic",   "1500 Classic"):      "RAM",
    ("Promaster", "ProMaster 1500"):    "RAM",
    ("Promaster", "ProMaster 2500"):    "RAM",
    ("Durango",   "Durango"):           "Dodge",
    ("Charger",   "Charger"):           "Dodge",
    ("Challenger","Challenger"):        "Dodge",
    ("Journey",   "Journey"):           "Dodge",
    ("Pacifica",  "Pacifica"):          "Dodge",
    ("Sierra",    "Sierra 1500"):       "GMC",
    ("Silverado", "Silverado 1500"):    "Chevrolet",
    ("Silverado", "Silverado 1500 LTD"):"Chevrolet",
    ("Tacoma",    "Tacoma"):            "Toyota",
    ("Telluride", "Telluride"):         "Kia",
    ("Santa",     "Santa Fe"):          "Hyundai",
    ("Santa",     "Santa Fe Sport"):    "Hyundai",
    ("Ioniq",     "IONIQ 5"):           "Hyundai",
    ("F-150",     "F-150"):             "Ford",
    ("Explorer",  "Explorer"):          "Ford",
    ("Mustang",   "Mustang"):           "Ford",
    ("Camaro",    "Camaro"):            "Chevrolet",
    ("Escalade",  "Escalade ESV"):      "Cadillac",
    ("Tahoe",     "Tahoe"):             "Chevrolet",
    ("Mdx",       "MDX"):               "Acura",
    ("Rdx",       "RDX"):               "Acura",
    ("Gx",        "GX"):                "Lexus",
    ("5500hd",    "5500HD"):            "RAM",
    ("Cx-50",     "CX-50"):             "Mazda",
    ("Rav4",      "RAV4"):              "Toyota",
    ("Lacrosse",  "LaCrosse"):          "Buick",
    ("Tiguan",    "Tiguan"):            "VW",
    ("Forte",     "Forte"):             "Kia",
    ("Cooper",    "Cooper S Countryman"):"Mini",
    ("Golf",      "Golf GTI"):          "VW",
    ("Accord",    "Accord Sedan"):      "Honda",
    ("Hyundai",   "Ioniq 5"):           "Hyundai",
    ("Hyundai",   "Kona"):              "Hyundai",
    ("Lincoln",   "Aviator"):           "Lincoln",
}


def seed_model_specs(conn: sqlite3.Connection, dry_run: bool) -> int:
    cur = conn.cursor()
    inserted = 0
    for make, model, cyl, gears, transmission in MODEL_SPECS_SEED:
        cur.execute(
            "SELECT COUNT(*) FROM model_specs WHERE lower(make)=lower(?) AND lower(model)=lower(?)",
            (make, model),
        )
        if cur.fetchone()[0] == 0:
            if not dry_run:
                cur.execute(
                    "INSERT INTO model_specs (make, model, cylinders, gears, transmission) VALUES (?,?,?,?,?)",
                    (make, model, cyl, gears, transmission),
                )
            inserted += 1
    if not dry_run:
        conn.commit()
    return inserted


def _lookup_model_specs(cur: sqlite3.Cursor, make: str, model: str) -> tuple[int | None, str | None]:
    """Return (cylinders, transmission) from model_specs for a (make, model) pair."""
    cur.execute(
        "SELECT cylinders, transmission FROM model_specs "
        "WHERE lower(make)=lower(?) AND lower(model)=lower(?) LIMIT 1",
        (make, model),
    )
    row = cur.fetchone()
    if row:
        return row[0], row[1]
    return None, None


def _resolve_canonical_make(make: str, model: str) -> str:
    """Canonical make for a (make, model) pair, handling scraper swaps."""
    fixed = MAKE_FIX_MAP.get((make, model)) or MAKE_FIX_MAP.get((make.capitalize(), model))
    return fixed if fixed else make


def _is_electric(make: str, model: str) -> bool:
    """True for known BEV make/models — 0 cylinders is correct for these."""
    mu, mo = make.upper(), model.upper()
    if mu == "TESLA":
        return True
    if mu == "BMW" and any(x in mo for x in ("I4", "I5", "I7", "IX")):
        return True
    if mu == "HYUNDAI" and "IONIQ" in mo:
        return True
    if "MACH-E" in mo or "MACHE" in mo:
        return True
    return False


def resolve_specs(car: dict, spec_cur: sqlite3.Cursor) -> tuple[str | None, int | None]:
    """
    Return (transmission, cylinders) inferred for a car row.
    Priority: model_specs → EPA → trim decoder.
    Returns (None, None) fields that cannot be inferred.
    """
    make = (car.get("make") or "").strip()
    model = (car.get("model") or "").strip()
    trim = (car.get("trim") or "").strip()
    title = (car.get("title") or "").strip()
    year = car.get("year")

    canonical_make = _resolve_canonical_make(make, model)

    # 1. model_specs dictionary
    spec_cyl, spec_trans = _lookup_model_specs(spec_cur, canonical_make, model)

    try:
        y = int(year) if year is not None else None
    except (TypeError, ValueError):
        y = None

    # 2. EPA aggregate (fuzzy BMW match included)
    epa = lookup_epa_aggregate(y, canonical_make, model)

    # 3. Trim decoder
    regex = decode_trim_logic(canonical_make, model, trim, title)

    # --- Resolve transmission ---
    trans = spec_trans
    if not trans:
        if epa.get("transmission"):
            trans = format_transmission_display(epa["transmission"]) or epa["transmission"]
    if not trans and regex.get("transmission_hint"):
        trans = str(regex["transmission_hint"]).strip()
    if not trans:
        gears = regex.get("gears") or epa.get("gears")
        if gears and gears > 0:
            trans = f"{gears}-Speed Automatic"

    # --- Resolve cylinders ---
    cyl: int | None = None
    if spec_cyl is not None:
        cyl = int(spec_cyl)
    elif epa.get("cylinders") is not None:
        cyl = int(epa["cylinders"])
    elif regex.get("cylinders") is not None:
        cyl = int(regex["cylinders"])

    # For known EVs, cylinders=0 is valid; don't return it as "unknown"
    if cyl == 0 and not _is_electric(canonical_make, model):
        cyl = None

    return trans, cyl


def backfill(conn: sqlite3.Connection, dry_run: bool) -> dict:
    cur = conn.cursor()
    spec_cur = conn.cursor()  # separate cursor for model_specs lookups

    # Fetch all cars missing transmission OR cylinders
    cur.execute(
        """SELECT vin, year, make, model, trim, title, transmission, cylinders FROM cars
           WHERE (transmission IS NULL OR transmission = '')
              OR (cylinders IS NULL OR cylinders = 0)"""
    )
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]

    trans_updated = 0
    cyl_updated = 0
    skipped = 0
    results = []

    for row in rows:
        car = dict(zip(cols, row))
        vin = car["vin"]

        needs_trans = not car.get("transmission") or str(car["transmission"]).strip() == ""
        cur_cyl = car.get("cylinders")
        needs_cyl = (
            cur_cyl is None
            or (isinstance(cur_cyl, (int, float)) and int(cur_cyl) == 0
                and not _is_electric(car.get("make") or "", car.get("model") or ""))
        )

        if not needs_trans and not needs_cyl:
            continue

        inferred_trans, inferred_cyl = resolve_specs(car, spec_cur)

        patch: dict = {}
        if needs_trans and inferred_trans:
            patch["transmission"] = inferred_trans
            trans_updated += 1
        if needs_cyl and inferred_cyl is not None:
            patch["cylinders"] = inferred_cyl
            cyl_updated += 1

        if not patch:
            skipped += 1
            continue

        results.append((vin, car["make"], car["model"], car["trim"], patch))
        if not dry_run:
            sets = ", ".join(f"{k}=?" for k in patch)
            cur.execute(
                f"UPDATE cars SET {sets} WHERE vin=?",
                (*patch.values(), vin),
            )

    if not dry_run:
        conn.commit()

    return {
        "trans_updated": trans_updated,
        "cyl_updated": cyl_updated,
        "skipped": skipped,
        "results": results,
    }


def main():
    parser = argparse.ArgumentParser(description="Backfill missing transmission + cylinders")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing")
    parser.add_argument("--db", default=DB_PATH, help="Path to inventory.db")
    args = parser.parse_args()

    if args.db != DB_PATH:
        import backend.knowledge_engine as _ke
        _ke.DB_PATH = args.db
        import backend.database as _db
        _db.DB_PATH = args.db

    conn = sqlite3.connect(args.db)

    print(f"{'[DRY RUN] ' if args.dry_run else ''}Seeding model_specs...")
    seeded = seed_model_specs(conn, args.dry_run)
    print(f"  → {seeded} new entries added to model_specs")

    print(f"\n{'[DRY RUN] ' if args.dry_run else ''}Backfilling transmission + cylinders...")
    result = backfill(conn, args.dry_run)

    print(
        f"  → transmission: {result['trans_updated']} filled | "
        f"cylinders: {result['cyl_updated']} filled | "
        f"skipped (no data): {result['skipped']}\n"
    )

    if result["results"]:
        label = "Sample u" if len(result["results"]) > 20 else "U"
        print(f"{label}pdates:")
        for vin, make, model, trim, patch in result["results"][:40]:
            parts = ", ".join(f"{k}={v}" for k, v in patch.items())
            print(f"  [{make} {model} {trim or ''}] VIN {vin} → {parts}")
        if len(result["results"]) > 40:
            print(f"  ... and {len(result['results']) - 40} more")

    conn.close()


if __name__ == "__main__":
    main()
