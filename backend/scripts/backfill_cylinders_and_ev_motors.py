#!/usr/bin/env python3
"""
Backfill missing ``cars.cylinders`` for ICE rows and EV motor counts.

Uses heuristic spec normalization (``collect_raw_spec_heuristic_updates``) plus
direct ``infer_ev_motor_count`` for electric vehicles.

Requires Postgres via ``INVENTORY_DATABASE_URL``.

Usage (from repo root)::

  INVENTORY_DATABASE_URL=postgresql://user@localhost/dealership_scanner \\
    PYTHONPATH=. python3 backend/scripts/backfill_cylinders_and_ev_motors.py --dry-run
  INVENTORY_DATABASE_URL=postgresql://user@localhost/dealership_scanner \\
    PYTHONPATH=. python3 backend/scripts/backfill_cylinders_and_ev_motors.py
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from backend.utils.project_env import load_project_dotenv

    load_project_dotenv()
except ImportError:
    pass

from backend.db.inventory_db import get_conn, init_inventory_db  # noqa: E402
from backend.db.inventory_pg import is_inventory_postgres  # noqa: E402
from backend.utils.ev_motor_count import infer_ev_motor_count  # noqa: E402
from backend.utils.spec_field_normalize import collect_raw_spec_heuristic_updates  # noqa: E402

_GAP_SQL = """
SELECT vin, make, model, trim, title, description, engine_description,
       fuel_type, drivetrain, cylinders
FROM cars
WHERE cylinders IS NULL
   OR (cylinders = 0 AND LOWER(TRIM(COALESCE(fuel_type, ''))) = 'electric')
ORDER BY vin
"""


def _row_dict(row: object) -> dict:
    if isinstance(row, dict):
        return row
    return {
        "vin": row[0],
        "make": row[1],
        "model": row[2],
        "trim": row[3],
        "title": row[4],
        "description": row[5],
        "engine_description": row[6],
        "fuel_type": row[7],
        "drivetrain": row[8],
        "cylinders": row[9],
    }


def _is_electric(fuel_type: object) -> bool:
    return str(fuel_type or "").strip().lower() == "electric"


def _resolve_cylinders(row: dict) -> int | None:
    """Return new cylinders value, or None when no fill is possible."""
    patch = collect_raw_spec_heuristic_updates(row)
    patch_cyl = patch.get("cylinders")

    if _is_electric(row.get("fuel_type")):
        motor = infer_ev_motor_count(
            make=row.get("make"),
            model=row.get("model"),
            trim=row.get("trim"),
            title=row.get("title"),
            description=row.get("description"),
            drivetrain=row.get("drivetrain"),
            fuel_type=row.get("fuel_type"),
        )
        if motor is not None and motor > 0:
            return motor
        if patch_cyl is not None:
            try:
                n = int(patch_cyl)
                if n > 0:
                    return n
            except (TypeError, ValueError):
                pass
        return None

    if patch_cyl is not None:
        try:
            n = int(patch_cyl)
            if n > 0:
                return n
        except (TypeError, ValueError):
            pass
    return None


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true", help="Print changes without writing.")
    args = ap.parse_args()

    if not is_inventory_postgres():
        print(
            "INVENTORY_DATABASE_URL must be set to a postgresql:// or postgres:// URL.",
            file=sys.stderr,
        )
        return 1

    init_inventory_db()
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(_GAP_SQL)
        rows = [_row_dict(r) for r in cur.fetchall()]

        ice_filled = 0
        ev_motor_filled = 0
        skipped = 0

        for row in rows:
            vin = str(row.get("vin") or "").strip()
            new_cyl = _resolve_cylinders(row)
            if new_cyl is None:
                skipped += 1
                if args.dry_run:
                    print(f"  skip {vin} (no inference)")
                continue

            if _is_electric(row.get("fuel_type")):
                ev_motor_filled += 1
                label = "ev_motor"
            else:
                ice_filled += 1
                label = "ice"

            if args.dry_run:
                print(
                    f"  {label} {vin} {row.get('make')} {row.get('model')} "
                    f"cylinders {row.get('cylinders')} -> {new_cyl}"
                )
                continue

            cur.execute("UPDATE cars SET cylinders = ? WHERE vin = ?", (new_cyl, vin))

        if not args.dry_run:
            conn.commit()
    finally:
        conn.close()

    mode = "dry-run" if args.dry_run else "applied"
    print(
        f"[{mode}] ice_filled={ice_filled} ev_motor_filled={ev_motor_filled} skipped={skipped}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
