#!/usr/bin/env python3
"""
One-off SQLite cleanup: set known placeholder strings to NULL on selected columns.

Run from repo root (so ``backend`` is importable):

  PYTHONPATH=. python3 scripts/migrate_placeholder_nulls.py
  PYTHONPATH=. python3 scripts/migrate_placeholder_nulls.py --refresh-scores

Then re-index pgvector listing embeddings so vectors match cleaned text.
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

TARGET_COLS = (
    "trim",
    "zip_code",
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


def _null_placeholders(conn: sqlite3.Connection, col: str) -> int:
    cur = conn.cursor()
    marks = ",".join("?" * len(_JUNK_LOWER))
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
    p.add_argument(
        "--refresh-scores",
        action="store_true",
        help="Recompute data_quality_score for every row (slower).",
    )
    args = p.parse_args()

    if not os.path.isfile(DB_PATH):
        print(f"Database not found: {DB_PATH}")
        return 1

    conn = sqlite3.connect(DB_PATH)
    try:
        total = 0
        for col in TARGET_COLS:
            n = _null_placeholders(conn, col)
            total += n
            print(f"  {col}: nulled {n} rows")
        conn.commit()
        print(f"Done. Total placeholder row-updates (sum per column): {total}")

        if args.refresh_scores:
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
