#!/usr/bin/env python3
"""
Backfill ``cars.history_highlights`` from stored ``description`` (dealer-published claims).

Sets NULL when no history phrases are found. Use ``--missing-only`` to skip rows that
already have non-empty highlights.

Usage:
  PYTHONPATH=. python backend/scripts/backfill_history_highlights.py --missing-only
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
import os

os.chdir(ROOT)
sys.path.insert(0, str(ROOT))

try:
    from backend.utils.project_env import load_project_dotenv

    load_project_dotenv()
except ImportError:
    pass

from backend.db.inventory_db import ensure_cars_table_columns, get_conn, init_inventory_db  # noqa: E402
from backend.utils.history_highlights import (  # noqa: E402
    coalesce_history_highlights_for_storage,
    history_highlights_json,
)


def _row_has_highlights(raw: str | None) -> bool:
    if not raw or not str(raw).strip() or str(raw).strip() == "[]":
        return False
    try:
        parsed = json.loads(raw)
        return isinstance(parsed, list) and len(parsed) > 0
    except (json.JSONDecodeError, TypeError):
        return bool(str(raw).strip())


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--limit", type=int, default=None, help="Max rows to scan (ORDER BY id).")
    ap.add_argument(
        "--missing-only",
        action="store_true",
        help="Only update rows with empty history_highlights.",
    )
    args = ap.parse_args()

    init_inventory_db()
    conn = get_conn()
    cur = conn.cursor()
    ensure_cars_table_columns(cur)
    conn.commit()

    sql = "SELECT id, vin, description, history_highlights FROM cars ORDER BY id"
    if args.limit is not None:
        sql += " LIMIT ?"
        cur.execute(sql, (max(1, int(args.limit)),))
    else:
        cur.execute(sql)
    rows = cur.fetchall()

    updated = 0
    populated = 0
    skipped_already = 0
    unchanged_empty = 0
    for car_id, _vin, description, existing in rows:
        if args.missing_only and _row_has_highlights(existing):
            skipped_already += 1
            continue
        vehicle = {"history_highlights": existing, "description": description}
        highlights = coalesce_history_highlights_for_storage(vehicle)
        payload = history_highlights_json(highlights)
        normalized_existing = existing if _row_has_highlights(existing) else None
        if payload == normalized_existing:
            if not payload:
                unchanged_empty += 1
            continue
        cur.execute("UPDATE cars SET history_highlights = ? WHERE id = ?", (payload, car_id))
        updated += 1
        if payload:
            populated += 1

    conn.commit()
    conn.close()
    print(
        {
            "scanned": len(rows),
            "updated": updated,
            "with_highlights": populated,
            "skipped_already_populated": skipped_already,
            "unchanged_still_empty": unchanged_empty,
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
