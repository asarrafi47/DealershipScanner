#!/usr/bin/env python3
"""
Backfill missing ``cylinders`` / ``mpg_city`` / ``mpg_highway`` on SQLite ``cars``.

Pipeline (see ``backend.spec_backfill``): EPA aggregate + trim decoder → optional
MasterCatalog (Postgres) → Playwright VDP → Google Programmable Search →
fueleconomy.gov HTML parse. Provenance stored in ``spec_source_json``.

Environment (optional tier C):
  GOOGLE_CSE_API_KEY, GOOGLE_CSE_ID — Google Custom Search JSON API only (no SERP HTML).
  SPEC_SEARCH_EXTRA_ALLOWED_HOSTS — comma-separated extra hostnames allowed for GET.
  SPEC_BACKFILL_USE_MASTER_CATALOG=0 — skip pgvector catalog tier.

Examples::

    python scripts/backfill_vehicle_specs.py --limit 50 --dry-run
    python scripts/backfill_vehicle_specs.py --car-id 123
    python scripts/backfill_vehicle_specs.py --limit 200 --no-search
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def main() -> int:
    from dotenv import load_dotenv

    load_dotenv(ROOT / ".env")

    from backend.db.inventory_db import init_inventory_db
    from backend.spec_backfill import iter_candidate_car_ids, run_spec_backfill_for_car

    init_inventory_db()

    ap = argparse.ArgumentParser(description="Backfill vehicle spec columns on cars.")
    ap.add_argument("--car-id", type=int, default=None, help="Process a single car id.")
    ap.add_argument("--limit", type=int, default=None, help="Max cars when scanning ids.")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--no-vdp", action="store_true", help="Skip Playwright VDP tier.")
    ap.add_argument("--no-search", action="store_true", help="Skip Google CSE tier.")
    ap.add_argument(
        "--search-pause",
        type=float,
        default=1.2,
        help="Seconds to sleep between Google CSE calls (default 1.2).",
    )
    args = ap.parse_args()

    use_vdp = not args.no_vdp
    keys_ok = bool(
        (os.environ.get("GOOGLE_CSE_API_KEY") or "").strip()
        and (os.environ.get("GOOGLE_CSE_ID") or "").strip()
    )
    use_search = not args.no_search and keys_ok
    if not args.no_search and not keys_ok:
        print(
            "[backfill_vehicle_specs] GOOGLE_CSE_API_KEY / GOOGLE_CSE_ID not set — "
            "search tier skipped.",
            file=sys.stderr,
        )

    if args.car_id is not None:
        ids = [int(args.car_id)]
    else:
        ids = iter_candidate_car_ids(limit=args.limit)

    ok = 0
    skipped = 0
    for cid in ids:
        r = run_spec_backfill_for_car(
            cid,
            use_vdp=use_vdp,
            use_search=use_search,
            dry_run=args.dry_run,
            search_pause_s=max(0.0, float(args.search_pause)),
        )
        if r.message in ("already_complete", "no_fillable_fields"):
            skipped += 1
        elif r.ok:
            ok += 1
        print(f"id={cid} ok={r.ok} message={r.message!r} tiers={r.tiers} fields={r.updated_fields}")
    print(f"done processed={len(ids)} updated_ok={ok} skipped_or_noop={skipped}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
