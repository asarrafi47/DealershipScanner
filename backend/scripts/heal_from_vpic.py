#!/usr/bin/env python3
"""Store NHTSA vPIC drivetrain / electrification on active rows where the feed
disagrees (policy 2026-09-23: the VIN decode outranks the dealer label).

  .venv/bin/python backend/scripts/heal_from_vpic.py --dealers a,b --dry-run
  .venv/bin/python backend/scripts/heal_from_vpic.py --all
  .venv/bin/python backend/scripts/heal_from_vpic.py --all --decode   # also decode VINs missing from the cache
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT))

from backend.utils.project_env import load_project_dotenv  # noqa: E402

load_project_dotenv()

from backend.db.inventory_db import get_conn  # noqa: E402
from backend.enrichment.vpic_facts import decode_missing_vins, heal_rows  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--dealers", help="comma-separated dealer_ids")
    g.add_argument("--all", action="store_true", help="every active row")
    ap.add_argument("--decode", action="store_true", help="decode VINs missing from nhtsa_vpic_cache first (network)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    dealers = [d.strip() for d in (args.dealers or "").split(",") if d.strip()]
    conn = get_conn()
    try:
        if args.decode:
            cur = conn.cursor()
            where = "COALESCE(listing_active,1)=1 AND vin IS NOT NULL AND length(vin)=17"
            params: tuple = ()
            if dealers:
                where += " AND dealer_id IN (" + ",".join("?" * len(dealers)) + ")"
                params = tuple(dealers)
            cur.execute(f"SELECT vin FROM cars WHERE {where}", params)
            vins = [r[0] for r in cur.fetchall()]
            print("decode:", json.dumps(decode_missing_vins(conn, vins)))
        stats = heal_rows(conn, dealers=dealers or None, dry_run=args.dry_run)
        ex = stats.pop("examples", [])
        print(("DRY RUN " if args.dry_run else "") + json.dumps(stats))
        for e in ex:
            print("  ", e)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
