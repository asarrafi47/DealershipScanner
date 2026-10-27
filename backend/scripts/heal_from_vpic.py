#!/usr/bin/env python3
"""Store NHTSA vPIC drivetrain / electrification on active rows where the feed
disagrees (policy 2026-09-23: the VIN decode outranks the dealer label).

  .venv/bin/python backend/scripts/heal_from_vpic.py --dealers a,b --dry-run
  .venv/bin/python backend/scripts/heal_from_vpic.py --all
  .venv/bin/python backend/scripts/heal_from_vpic.py --all --decode   # also decode VINs missing from the cache
  .venv/bin/python backend/scripts/heal_from_vpic.py --all --refresh-partial --refresh-min-year 2026 --dry-run
A dry run decodes nothing and writes nothing (it prints the counts a real run would act on).
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
from backend.enrichment.vpic_facts import decode_missing_vins, heal_rows, refresh_decodes  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--dealers", help="comma-separated dealer_ids")
    g.add_argument("--all", action="store_true", help="every active row")
    ap.add_argument("--decode", action="store_true", help="decode VINs missing from nhtsa_vpic_cache first (network)")
    ap.add_argument("--refresh-partial", action="store_true", help="decode again cached VINs whose ErrorCode is partial (network)")
    ap.add_argument("--refresh-days", type=float, help="decode again cached VINs fetched more than this many days ago (network)")
    ap.add_argument("--refresh-min-year", type=int, help="limit the refresh to model years >= this")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--fields", help="comma-separated columns to heal (drivetrain,fuel_type,cylinders,engine_description,body_style)")
    args = ap.parse_args()
    dealers = [d.strip() for d in (args.dealers or "").split(",") if d.strip()]
    conn = get_conn()
    try:
        if args.decode or args.refresh_partial or args.refresh_days:
            cur = conn.cursor()
            where = "COALESCE(listing_active,1)=1 AND vin IS NOT NULL AND length(vin)=17"
            params: tuple = ()
            if dealers:
                where += " AND dealer_id IN (" + ",".join("?" * len(dealers)) + ")"
                params = tuple(dealers)
            cur.execute(f"SELECT vin FROM cars WHERE {where}", params)
            vins = [r[0] for r in cur.fetchall()]
            if args.decode:
                print("decode:", json.dumps(decode_missing_vins(conn, vins, dry_run=args.dry_run)))
            if args.refresh_partial or args.refresh_days:
                print("refresh:", json.dumps(refresh_decodes(
                    conn, vins, max_age_days=args.refresh_days, partial=args.refresh_partial,
                    min_year=args.refresh_min_year, dry_run=args.dry_run)))
        fields = [f.strip() for f in (args.fields or "").split(",") if f.strip()] or None
        stats = heal_rows(conn, dealers=dealers or None, dry_run=args.dry_run, fields=fields)
        ex = stats.pop("examples", [])
        print(("DRY RUN " if args.dry_run else "") + json.dumps(stats))
        for e in ex:
            print("  ", e)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
