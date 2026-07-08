#!/usr/bin/env python3
"""
One-off cleanup: set known placeholder strings to NULL on selected columns.

Run from repo root (so ``backend`` is importable):

  PYTHONPATH=. python3 backend/scripts/migrate_placeholder_nulls.py --dry-run
  PYTHONPATH=. python3 backend/scripts/migrate_placeholder_nulls.py
  PYTHONPATH=. python3 backend/scripts/migrate_placeholder_nulls.py --refresh-scores

Then re-index pgvector listing embeddings so vectors match cleaned text.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

_JUNK_LOWER = (
    "",
    "n/a",
    "na",
    "null",
    "none",
    "unknown",
    "undefined",
    "-",
    "—",
    "--",
    "---",
    "tbd",
    "not specified",
    "unspecified",
)

# NOTE: zip_code was dropped from cars (superseded by dealer-level geo lookup) — not a target here.
TARGET_COLS = (
    "trim",
    "transmission",
    "drivetrain",
    "interior_color",
    "exterior_color",
    "fuel_type",
    "body_style",
    "engine_description",
    "condition",
    "stock_number",
    "dealer_url",
    "carfax_url",
)


def _null_placeholders(conn: Any, col: str, *, dry_run: bool) -> int:
    cur = conn.cursor()
    marks = ",".join("?" * len(_JUNK_LOWER))
    if dry_run:
        cur.execute(
            f"""
            SELECT COUNT(*) FROM cars
            WHERE {col} IS NOT NULL
              AND LOWER(TRIM({col})) IN ({marks})
            """,
            _JUNK_LOWER,
        )
        return int(cur.fetchone()[0] or 0)
    cur.execute(
        f"""
        UPDATE cars
        SET {col} = NULL
        WHERE {col} IS NOT NULL
          AND LOWER(TRIM({col})) IN ({marks})
        """,
        _JUNK_LOWER,
    )
    return int(cur.rowcount or 0)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true", help="Count rows only; no writes.")
    p.add_argument(
        "--refresh-scores",
        action="store_true",
        help="Recompute data_quality_score for every row (slower).",
    )
    args = p.parse_args()

    from backend.db.inventory_db import get_conn

    conn = get_conn()
    try:
        total = 0
        for col in TARGET_COLS:
            n = _null_placeholders(conn, col, dry_run=args.dry_run)
            total += n
            verb = "would null" if args.dry_run else "nulled"
            print(f"  {col}: {verb} {n} rows")
        if not args.dry_run:
            conn.commit()
        print(f"Done. Total placeholder row-updates (sum per column): {total}")

        if args.refresh_scores and not args.dry_run:
            from backend.db.inventory_db import refresh_car_data_quality_score

            cur = conn.cursor()
            cur.execute("SELECT id FROM cars")
            ids = [int(r[0]) for r in cur.fetchall()]
            for i, cid in enumerate(ids, 1):
                refresh_car_data_quality_score(cid)
                if i % 200 == 0:
                    print(f"  refreshed scores: {i}/{len(ids)}")
            print(f"Refreshed data_quality_score for {len(ids)} cars.")
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
