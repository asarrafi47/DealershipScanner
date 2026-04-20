#!/usr/bin/env python3
"""
Normalize placeholder strings and backfill missing spec/condition fields on ``cars``.

  - Applies ``clean_car_row_dict`` diffs (``--`` → NULL, etc.)
  - Fills transmission, drivetrain, cylinders, fuel_type, body_style from EPA + trim decoder
    when the dealer column is empty or junk
  - Sets ``condition`` from the same heuristics as ``serialize_car_for_api`` / storage inference

Run from repo root::

  PYTHONPATH=. python3 scripts/repair_inventory_fields.py --dry-run
  PYTHONPATH=. python3 scripts/repair_inventory_fields.py
  PYTHONPATH=. python3 scripts/repair_inventory_fields.py --limit 100
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

DB_PATH = os.environ.get("INVENTORY_DB_PATH", str(ROOT / "inventory.db"))


def main() -> int:
    from backend.db.inventory_db import get_car_by_id, refresh_car_data_quality_score, update_car_row_partial
    from backend.utils.inventory_repair import collect_row_storage_repairs

    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="Print counts only; no writes.")
    ap.add_argument("--limit", type=int, default=None, help="Max rows to process (ordered by id).")
    args = ap.parse_args()

    if not os.path.isfile(DB_PATH):
        print(f"Database not found: {DB_PATH}", file=sys.stderr)
        return 1

    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute("SELECT id FROM cars ORDER BY id")
        ids = [int(r[0]) for r in cur.fetchall()]
    finally:
        conn.close()

    if args.limit is not None:
        ids = ids[: max(0, int(args.limit))]

    would_update = 0
    fields_touched: dict[str, int] = {}

    for cid in ids:
        raw = get_car_by_id(cid)
        if not raw:
            continue
        patch = collect_row_storage_repairs(raw)
        if not patch:
            continue
        would_update += 1
        for k in patch:
            fields_touched[k] = fields_touched.get(k, 0) + 1
        if args.dry_run:
            continue
        update_car_row_partial(cid, patch)
        refresh_car_data_quality_score(cid)

    print(f"cars_scanned={len(ids)} rows_with_patch={would_update} dry_run={args.dry_run}")
    for k, n in sorted(fields_touched.items(), key=lambda x: (-x[1], x[0])):
        print(f"  {k}: {n}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
