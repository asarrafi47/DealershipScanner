#!/usr/bin/env python3
"""
Seed ``cars.price_provenance_json`` from current ``price`` + ``first_seen_at``.

Use after wiring scan provenance for existing inventory. Does not fabricate drops;
only creates an initial ``listed`` event per row.

Usage:
  PYTHONPATH=. python backend/scripts/backfill_price_provenance.py
  PYTHONPATH=. python backend/scripts/backfill_price_provenance.py --missing-only
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
from backend.utils.price_provenance import merge_price_provenance_for_upsert, parse_price_provenance  # noqa: E402


def _has_provenance(raw: str | None) -> bool:
    return bool(parse_price_provenance(raw).get("history"))


def _needs_repair(raw: str | None) -> bool:
    if not raw or not str(raw).strip():
        return True
    return not _has_provenance(raw)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--missing-only", action="store_true")
    args = ap.parse_args()

    init_inventory_db()
    conn = get_conn()
    cur = conn.cursor()
    ensure_cars_table_columns(cur)
    conn.commit()

    sql = (
        "SELECT id, vin, price, first_seen_at, scraped_at, price_provenance_json "
        "FROM cars ORDER BY id"
    )
    if args.limit is not None:
        sql += " LIMIT ?"
        cur.execute(sql, (max(1, int(args.limit)),))
    else:
        cur.execute(sql)
    rows = cur.fetchall()

    updated = 0
    skipped = 0
    for row in rows:
        if isinstance(row, dict):
            car_id = row["id"]
            price = row.get("price")
            first_seen = row.get("first_seen_at") or row.get("scraped_at")
            existing = row.get("price_provenance_json")
        else:
            car_id, _vin, price, first_seen, scraped_at, existing = row
            first_seen = first_seen or scraped_at

        if args.missing_only and _has_provenance(existing) and not _needs_repair(existing):
            skipped += 1
            continue
        if price is None or float(price) <= 0:
            continue
        recorded_at = str(first_seen or "").strip()
        if not recorded_at:
            continue
        payload = merge_price_provenance_for_upsert(
            existing_provenance_json=existing,
            existing_price=price,
            existing_first_seen_at=recorded_at,
            incoming_price=price,
            recorded_at=recorded_at,
            is_new_row=False,
        )
        if not payload or payload == existing:
            continue
        cur.execute("UPDATE cars SET price_provenance_json = ? WHERE id = ?", (payload, car_id))
        updated += 1

    conn.commit()
    conn.close()
    print({"scanned": len(rows), "updated": updated, "skipped_already": skipped})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
