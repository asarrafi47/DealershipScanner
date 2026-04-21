#!/usr/bin/env python3
"""
Recompute ``cars.interior_color_buckets`` from ``interior_color`` + ``make``.

Uses ``ensure_cars_table_columns`` so the column exists. Optional ``--limit`` for dry runs.
"""
from __future__ import annotations

import argparse
import os
import sys

# Allow running from repo root without PYTHONPATH tweaks
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from backend.db.inventory_db import ensure_cars_table_columns, get_conn, init_inventory_db  # noqa: E402
from backend.utils.interior_color_buckets import interior_color_buckets_json  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--limit", type=int, default=None, help="Max rows to update (ORDER BY id).")
    args = ap.parse_args()

    init_inventory_db()
    conn = get_conn()
    cur = conn.cursor()
    ensure_cars_table_columns(cur)
    conn.commit()

    if args.limit is not None:
        cur.execute(
            "SELECT id, interior_color, make FROM cars ORDER BY id LIMIT ?",
            (max(1, int(args.limit)),),
        )
    else:
        cur.execute("SELECT id, interior_color, make FROM cars ORDER BY id")
    rows = cur.fetchall()
    n = 0
    for car_id, interior, make in rows:
        payload = interior_color_buckets_json(interior, make)
        cur.execute("UPDATE cars SET interior_color_buckets = ? WHERE id = ?", (payload, car_id))
        n += 1
    conn.commit()
    conn.close()
    print(f"Updated interior_color_buckets for {n} row(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
