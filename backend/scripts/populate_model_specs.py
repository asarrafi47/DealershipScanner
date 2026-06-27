#!/usr/bin/env python3
"""
Populate model_specs dictionary with transmission, cylinders, drivetrain data.

Sources (in order):
  1. ``epa_master`` aggregates (make+model mode row)
  2. Static ``MODEL_SPECS`` fallback for gaps

Usage:
  PYTHONPATH=. python backend/scripts/populate_model_specs.py
  PYTHONPATH=. python backend/scripts/populate_model_specs.py --from-epa --overwrite
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
os.chdir(ROOT)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.db.inventory_db import get_conn  # noqa: E402
from backend.db.inventory_pg import is_inventory_postgres, qmarks_to_percent_s  # noqa: E402
from backend.enrichment.dictionary_catalog import canonical_make  # noqa: E402
from backend.enrichment.knowledge_engine import _gears_from_trany, _norm_drive_epa  # noqa: E402

# Model specs dictionary: make -> model -> {transmission, cylinders, drivetrain}
MODEL_SPECS = {
    # Mazda
    "Mazda": {
        "CX-30": {"transmission": "Automatic", "cylinders": 4, "drivetrain": "Front-Wheel Drive"},
        "CX-5": {"transmission": "Automatic", "cylinders": 4, "drivetrain": "All-Wheel Drive"},
        "CX-9": {"transmission": "Automatic", "cylinders": 6, "drivetrain": "All-Wheel Drive"},
        "Mazda3": {"transmission": "Automatic", "cylinders": 4, "drivetrain": "Front-Wheel Drive"},
    },
    # BMW
    "BMW": {
        "330i": {"transmission": "8-Speed Automatic", "cylinders": 4, "drivetrain": "Rear-Wheel Drive"},
        "330e": {"transmission": "8-Speed Automatic", "cylinders": 4, "drivetrain": "Rear-Wheel Drive"},
        "340i": {"transmission": "8-Speed Automatic", "cylinders": 6, "drivetrain": "Rear-Wheel Drive"},
        "430i": {"transmission": "8-Speed Automatic", "cylinders": 4, "drivetrain": "Rear-Wheel Drive"},
        "430I": {"transmission": "8-Speed Automatic", "cylinders": 4, "drivetrain": "Rear-Wheel Drive"},
        "530i": {"transmission": "8-Speed Automatic", "cylinders": 4, "drivetrain": "Rear-Wheel Drive"},
        "530e": {"transmission": "8-Speed Automatic", "cylinders": 4, "drivetrain": "Rear-Wheel Drive"},
        "540i": {"transmission": "8-Speed Automatic", "cylinders": 6, "drivetrain": "Rear-Wheel Drive"},
        "740i": {"transmission": "8-Speed Automatic", "cylinders": 6, "drivetrain": "Rear-Wheel Drive"},
        "840i": {"transmission": "8-Speed Automatic", "cylinders": 6, "drivetrain": "Rear-Wheel Drive"},
        "X1": {"transmission": "8-Speed Automatic", "cylinders": 4, "drivetrain": "All-Wheel Drive"},
        "X3": {"transmission": "8-Speed Automatic", "cylinders": 4, "drivetrain": "All-Wheel Drive"},
        "X5": {"transmission": "8-Speed Automatic", "cylinders": 6, "drivetrain": "All-Wheel Drive"},
        "X5 PHEV": {"transmission": "8-Speed Automatic", "cylinders": 6, "drivetrain": "All-Wheel Drive"},
        "X7": {"transmission": "8-Speed Automatic", "cylinders": 6, "drivetrain": "All-Wheel Drive"},
        "i4": {"transmission": "Single-speed automatic", "cylinders": 0, "drivetrain": "Rear-Wheel Drive"},
        "i7": {"transmission": "Single-speed automatic", "cylinders": 0, "drivetrain": "Rear-Wheel Drive"},
        "iX": {"transmission": "Single-speed automatic", "cylinders": 0, "drivetrain": "All-Wheel Drive"},
        "M4": {"transmission": "8-Speed Automatic", "cylinders": 6, "drivetrain": "Rear-Wheel Drive"},
        "M235i": {"transmission": "8-Speed Automatic", "cylinders": 4, "drivetrain": "All-Wheel Drive"},
    },
    # Jeep
    "Jeep": {
        "Wrangler": {"transmission": "8-Speed Automatic", "cylinders": 6, "drivetrain": "Four-Wheel Drive"},
        "Grand Cherokee": {"transmission": "8-Speed Automatic", "cylinders": 6, "drivetrain": "All-Wheel Drive"},
        "Grand Cherokee L": {"transmission": "8-Speed Automatic", "cylinders": 6, "drivetrain": "All-Wheel Drive"},
        "Compass": {"transmission": "Continuously Variable Transmission (CVT)", "cylinders": 4, "drivetrain": "Front-Wheel Drive"},
        "Cherokee": {"transmission": "9-Speed Automatic", "cylinders": 4, "drivetrain": "Front-Wheel Drive"},
        "Gladiator": {"transmission": "8-Speed Automatic", "cylinders": 6, "drivetrain": "Four-Wheel Drive"},
    },
    # Ford
    "Ford": {
        "Maverick": {"transmission": "10-Speed Automatic", "cylinders": 4, "drivetrain": "Rear-Wheel Drive"},
        "Bronco Sport": {"transmission": "8-Speed Automatic", "cylinders": 4, "drivetrain": "All-Wheel Drive"},
        "Ranger": {"transmission": "10-Speed Automatic", "cylinders": 4, "drivetrain": "Four-Wheel Drive"},
        "F-150": {"transmission": "10-Speed Automatic", "cylinders": 6, "drivetrain": "Four-Wheel Drive"},
        "Escape": {"transmission": "8-Speed Automatic", "cylinders": 4, "drivetrain": "All-Wheel Drive"},
        "Expedition MAX": {"transmission": "10-Speed Automatic", "cylinders": 6, "drivetrain": "Four-Wheel Drive"},
        "Explorer": {"transmission": "10-Speed Automatic", "cylinders": 6, "drivetrain": "All-Wheel Drive"},
        "Mustang": {"transmission": "10-Speed Automatic", "cylinders": 6, "drivetrain": "Rear-Wheel Drive"},
        "Bronco": {"transmission": "7-Speed Manual/Standard", "cylinders": 6, "drivetrain": "Four-Wheel Drive"},
        "Fusion Energi": {"transmission": "Continuously Variable Transmission (CVT)", "cylinders": 4, "drivetrain": "Front-Wheel Drive"},
    },
    # Dodge
    "Dodge": {
        "Durango": {"transmission": "8-Speed Automatic", "cylinders": 8, "drivetrain": "All-Wheel Drive"},
        "Charger": {"transmission": "8-Speed Automatic", "cylinders": 8, "drivetrain": "Rear-Wheel Drive"},
        "Journey": {"transmission": "6-Speed Automatic", "cylinders": 6, "drivetrain": "Front-Wheel Drive"},
    },
    # Ram
    "Ram": {
        "1500": {"transmission": "10-Speed Automatic", "cylinders": 6, "drivetrain": "Four-Wheel Drive"},
        "2500": {"transmission": "6-Speed Automatic", "cylinders": 6, "drivetrain": "Four-Wheel Drive"},
        "3500": {"transmission": "6-Speed Automatic", "cylinders": 8, "drivetrain": "Four-Wheel Drive"},
    },
    # Lincoln
    "Lincoln": {
        "Navigator": {"transmission": "10-Speed Automatic", "cylinders": 6, "drivetrain": "Four-Wheel Drive"},
        "Nautilus": {"transmission": "8-Speed Automatic", "cylinders": 6, "drivetrain": "All-Wheel Drive"},
    },
    # Honda
    "Honda": {
        "Civic": {"transmission": "Continuously Variable Transmission (CVT)", "cylinders": 4, "drivetrain": "Front-Wheel Drive"},
        "Accord": {"transmission": "10-Speed Automatic", "cylinders": 4, "drivetrain": "Front-Wheel Drive"},
    },
    # Hyundai
    "Hyundai": {
        "Kona": {"transmission": "Continuously Variable Transmission (CVT)", "cylinders": 4, "drivetrain": "Front-Wheel Drive"},
        "Elantra Gt": {"transmission": "Continuously Variable Transmission (CVT)", "cylinders": 4, "drivetrain": "Front-Wheel Drive"},
        "Sonata": {"transmission": "8-Speed Automatic", "cylinders": 4, "drivetrain": "Front-Wheel Drive"},
    },
    # Chevrolet
    "Chevrolet": {
        "Camaro": {"transmission": "10-Speed Automatic", "cylinders": 6, "drivetrain": "Rear-Wheel Drive"},
        "Silverado": {"transmission": "10-Speed Automatic", "cylinders": 6, "drivetrain": "Four-Wheel Drive"},
    },
    # Buick
    "Buick": {
        "Lacrosse": {"transmission": "9-Speed Automatic", "cylinders": 6, "drivetrain": "Front-Wheel Drive"},
    },
    # Chrysler
    "Chrysler": {
        "Pacifica": {"transmission": "9-Speed Automatic", "cylinders": 6, "drivetrain": "Front-Wheel Drive"},
    },
    # GMC
    "GMC": {
        "Acadia": {"transmission": "9-Speed Automatic", "cylinders": 6, "drivetrain": "All-Wheel Drive"},
        "Sierra": {"transmission": "10-Speed Automatic", "cylinders": 6, "drivetrain": "Four-Wheel Drive"},
    },
}


def _upsert_model_spec(
    cursor,
    *,
    make: str,
    model: str,
    transmission: str | None,
    cylinders: int | None,
    drivetrain: str | None,
    gears: int | None = None,
    fuel_type: str | None = None,
    body_style: str | None = None,
    overwrite: bool = False,
) -> None:
    sql_ignore = qmarks_to_percent_s(
        """
        INSERT INTO model_specs (make, model, transmission, cylinders, drivetrain, gears, fuel_type, body_style)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (make, model) DO NOTHING
        """
    )
    sql_replace = qmarks_to_percent_s(
        """
        INSERT INTO model_specs (make, model, transmission, cylinders, drivetrain, gears, fuel_type, body_style)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (make, model) DO UPDATE SET
            transmission = excluded.transmission,
            cylinders = excluded.cylinders,
            drivetrain = excluded.drivetrain,
            gears = excluded.gears,
            fuel_type = excluded.fuel_type,
            body_style = excluded.body_style
        """
    )
    cursor.execute(
        sql_replace if overwrite else sql_ignore,
        (make, model, transmission, cylinders, drivetrain, gears, fuel_type, body_style),
    )


def populate_from_epa(*, overwrite: bool = False) -> int:
    """Aggregate ``epa_master`` by make/model; insert mode row into ``model_specs``."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT make, model, cylinders, trany, drive, fuel_type, body_style, COUNT(*) AS n
        FROM epa_master
        WHERE make IS NOT NULL AND trim(make) != ''
          AND model IS NOT NULL AND trim(model) != ''
        GROUP BY make, model, cylinders, trany, drive, fuel_type, body_style
        """
    )
    best: dict[tuple[str, str], tuple] = {}
    for row in cur.fetchall():
        if isinstance(row, dict):
            mk, md, cyl, trany, drive, fuel, body, n = (
                row["make"],
                row["model"],
                row["cylinders"],
                row["trany"],
                row["drive"],
                row["fuel_type"],
                row["body_style"],
                row["n"],
            )
        else:
            mk, md, cyl, trany, drive, fuel, body, n = row
        key = (canonical_make(str(mk)).strip(), str(md).strip())
        if key not in best or int(n) > int(best[key][-1]):
            best[key] = (mk, md, cyl, trany, drive, fuel, body, n)
    count = 0
    for mk, md, cyl, trany, drive, fuel, body, _ in best.values():
        gears = _gears_from_trany(str(trany or ""))
        _upsert_model_spec(
            cur,
            make=str(mk).strip(),
            model=str(md).strip(),
            transmission=(str(trany).strip() if trany else None),
            cylinders=int(cyl) if cyl is not None else None,
            drivetrain=_norm_drive_epa(str(drive or "")) or None,
            gears=gears,
            fuel_type=(str(fuel).strip() if fuel else None),
            body_style=(str(body).strip() if body else None),
            overwrite=overwrite,
        )
        count += 1
    conn.commit()
    conn.close()
    return count


def populate_static_fallback(*, overwrite: bool = False) -> int:
    conn = get_conn()
    cur = conn.cursor()
    count = 0
    for make, models in MODEL_SPECS.items():
        for model, specs in models.items():
            _upsert_model_spec(
                cur,
                make=make,
                model=model,
                transmission=specs.get("transmission"),
                cylinders=specs.get("cylinders"),
                drivetrain=specs.get("drivetrain"),
                overwrite=overwrite,
            )
            count += 1
    conn.commit()
    conn.close()
    return count


def populate_model_specs(*, from_epa: bool = True, overwrite: bool = False) -> None:
    """Populate model_specs from EPA aggregates plus static fallback."""
    epa_n = populate_from_epa(overwrite=overwrite) if from_epa else 0
    static_n = populate_static_fallback(overwrite=False)

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM model_specs")
    row = cur.fetchone()
    total = int(row[0] if not isinstance(row, dict) else list(row.values())[0])

    print("\n" + "=" * 60)
    print("Model Specs Dictionary Populated")
    print("=" * 60)
    print(f"EPA aggregates written: {epa_n}")
    print(f"Static fallback attempts: {static_n}")
    print(f"Total entries: {total}")
    print(f"Postgres: {is_inventory_postgres()}")
    print("=" * 60)
    conn.close()


def main() -> int:
    from backend.utils.project_env import bootstrap_inventory_script

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from-epa", action="store_true", default=True, help="Load from epa_master (default)")
    parser.add_argument("--static-only", action="store_true", help="Skip EPA; use MODEL_SPECS dict only")
    parser.add_argument("--overwrite", action="store_true", help="Replace existing make/model rows")
    args = parser.parse_args()
    bootstrap_inventory_script()
    populate_model_specs(from_epa=not args.static_only, overwrite=args.overwrite)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
