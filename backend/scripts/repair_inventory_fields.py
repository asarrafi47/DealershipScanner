#!/usr/bin/env python3
"""
Normalize placeholder strings and backfill missing spec/condition fields on ``cars``.

  - Applies ``clean_car_row_dict`` diffs (``--`` → NULL, etc.)
  - Fills transmission, drivetrain, cylinders, fuel_type, body_style from EPA + trim decoder
    when the dealer column is empty or junk
  - Applies regex/heuristic spec normalization (``backend.utils.spec_field_normalize``): drivetrain
    abbreviations, cylinder/engine parsing, trim-from-title, ``transmission_type`` bucket
  - Sets ``condition`` from the same heuristics as ``serialize_car_for_api`` / storage inference

Run from repo root::

  PYTHONPATH=. python3 backend/scripts/repair_inventory_fields.py --dry-run
  PYTHONPATH=. python3 backend/scripts/repair_inventory_fields.py
  PYTHONPATH=. python3 backend/scripts/repair_inventory_fields.py --limit 100
  PYTHONPATH=. python3 backend/scripts/repair_inventory_fields.py --spec-gaps-only --dry-run

One-off: infer ``transmission_type`` from ``transmission`` when the bucket is empty or placeholder
(non-weak normalization only)::

  PYTHONPATH=. python3 backend/scripts/repair_inventory_fields.py --backfill-unknown-transmission-type --dry-run
  PYTHONPATH=. python3 backend/scripts/repair_inventory_fields.py --backfill-unknown-transmission-type
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def run_backfill_unknown_transmission_type(*, dry_run: bool, limit: int | None) -> int:
    """Set ``transmission_type`` when it is unset/unknown but ``transmission`` normalizes confidently."""
    from backend.db.inventory_db import (
        ensure_cars_table_columns,
        get_car_by_id,
        get_conn,
        refresh_car_data_quality_score,
        update_car_row_partial,
    )
    from backend.utils.transmission_normalize import normalize_transmission_standard

    conn = get_conn()
    try:
        cur = conn.cursor()
        ensure_cars_table_columns(cur)
        conn.commit()
        cur.execute(
            """
            SELECT id FROM cars
            WHERE transmission IS NOT NULL AND LENGTH(TRIM(transmission)) > 0
              AND (
                transmission_type IS NULL
                OR LENGTH(TRIM(COALESCE(transmission_type,''))) = 0
                OR LOWER(TRIM(transmission_type)) IN ('unknown','--','n/a','na','null')
              )
            ORDER BY id
            """
        )
        ids = [int(r[0]) for r in cur.fetchall()]
    finally:
        conn.close()

    if limit is not None:
        ids = ids[: max(0, int(limit))]

    updated = 0
    for cid in ids:
        raw = get_car_by_id(cid)
        if not raw:
            continue
        trans = raw.get("transmission")
        if trans is None or not str(trans).strip():
            continue

        year_raw = raw.get("year")
        y_int = year_raw if isinstance(year_raw, int) else None
        if y_int is None and year_raw is not None:
            try:
                y_int = int(year_raw)
            except (TypeError, ValueError):
                y_int = None

        label, weak = normalize_transmission_standard(
            trans,
            make=raw.get("make"),
            model=raw.get("model"),
            trim=raw.get("trim"),
            title=raw.get("title"),
            year=y_int,
            vin=raw.get("vin"),
            log_weak=False,
        )
        if not label or weak:
            continue
        updated += 1
        if dry_run:
            continue
        update_car_row_partial(cid, {"transmission_type": label})
        refresh_car_data_quality_score(cid)

    print(
        "backfill_unknown_transmission_type "
        f"candidates={len(ids)} rows_updated_or_would_update={updated} dry_run={dry_run}"
    )
    return 0


def main() -> int:
    from backend.db.inventory_db import (
        ensure_cars_table_columns,
        get_car_by_id,
        get_conn,
        refresh_car_data_quality_score,
        update_car_row_partial,
    )
    from backend.utils.inventory_repair import collect_row_storage_repairs

    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="Print counts only; no writes.")
    ap.add_argument("--limit", type=int, default=None, help="Max rows to process (ordered by id).")
    ap.add_argument(
        "--spec-gaps-only",
        action="store_true",
        help="Only rows with missing/placeholder drivetrain, cylinders, trim, engine, or transmission fields.",
    )
    ap.add_argument(
        "--backfill-unknown-transmission-type",
        action="store_true",
        help=(
            "One-off: rows with empty/unknown transmission_type get a bucket "
            "from transmission text when normalization is confident (non-weak)."
        ),
    )
    args = ap.parse_args()

    if args.backfill_unknown_transmission_type:
        return run_backfill_unknown_transmission_type(dry_run=args.dry_run, limit=args.limit)

    conn = get_conn()
    try:
        cur = conn.cursor()
        ensure_cars_table_columns(cur)
        conn.commit()
        if args.spec_gaps_only:
            cur.execute(
                """
                SELECT id FROM cars WHERE
                  drivetrain IS NULL OR LENGTH(TRIM(COALESCE(drivetrain,''))) = 0
                  OR LOWER(TRIM(COALESCE(drivetrain,''))) IN ('unknown','--','n/a','na','null')
                  OR cylinders IS NULL
                  OR "trim" IS NULL OR LENGTH(TRIM(COALESCE("trim",''))) = 0
                  OR LOWER(TRIM(COALESCE("trim",''))) IN ('unknown','--','n/a','na')
                  OR engine_description IS NULL OR LENGTH(TRIM(COALESCE(engine_description,''))) = 0
                  OR transmission IS NULL OR LENGTH(TRIM(COALESCE(transmission,''))) = 0
                  OR transmission_type IS NULL OR LENGTH(TRIM(COALESCE(transmission_type,''))) = 0
                ORDER BY id
                """
            )
        else:
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
