#!/usr/bin/env python3
"""
Batch-refresh KBB IDWS columns for vehicles in SQLite (operator script).

Requires ``KBB_API_KEY`` (and usually ``KBB_DEFAULT_ZIP`` when rows lack ``zip_code``).

Usage (from repo root)::

    python scripts/fetch_kbb_for_inventory.py --limit 50
    python scripts/fetch_kbb_for_inventory.py --car-id 123
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))

try:
    from backend.utils.project_env import load_project_dotenv

    load_project_dotenv()
except ImportError:
    pass


def main() -> int:
    from backend.db.inventory_db import get_car_by_id, get_conn, refresh_car_data_quality_score, update_car_row_partial
    from backend.kbb_idws import kbb_api_configured, patch_from_refresh_result, refresh_kbb_for_vehicle_row

    ap = argparse.ArgumentParser(description="Refresh KBB IDWS valuations in inventory.db")
    ap.add_argument("--limit", type=int, default=30, help="Max rows when scanning by id (default 30)")
    ap.add_argument("--car-id", type=int, default=None, help="Refresh a single cars.id")
    ap.add_argument(
        "--zip-override",
        default=None,
        help="ZIP (5 digits) used when a row has no zip_code (also set KBB_DEFAULT_ZIP in .env)",
    )
    args = ap.parse_args()

    if not kbb_api_configured():
        print("KBB_API_KEY is not set; aborting.", file=sys.stderr)
        return 2

    if args.car_id is not None:
        row = get_car_by_id(int(args.car_id), include_inactive=True)
        if not row:
            print(f"No row for id={args.car_id}", file=sys.stderr)
            return 1
        rows = [row]
    else:
        conn = get_conn()
        cur = conn.cursor()
        lim = max(1, int(args.limit))
        cur.execute(
            """
            SELECT * FROM cars
            WHERE (COALESCE(listing_active, 1) = 1)
              AND vin IS NOT NULL AND length(trim(vin)) = 17
            ORDER BY id ASC
            LIMIT ?
            """,
            (lim,),
        )
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        conn.close()

    ok_n = 0
    for row in rows:
        cid = int(row["id"])
        res = refresh_kbb_for_vehicle_row(row, zip_override=args.zip_override)
        if not res.ok:
            print(f"id={cid} vin={row.get('vin')!r} -> skip: {res.message}")
            continue
        patch = patch_from_refresh_result(res)
        if patch:
            update_car_row_partial(cid, patch)
            refresh_car_data_quality_score(cid)
        print(f"id={cid} vin={row.get('vin')!r} -> ok")
        ok_n += 1

    print(f"Done. Updated {ok_n} / {len(rows)} row(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
