#!/usr/bin/env python3
"""
Backfill missing / placeholder ``cars`` spec fields using existing inventory repair logic,
then NHTSA vPIC ``DecodeVinValuesExtended`` for remaining gaps (US-market decode).

Run from repo root::

  PYTHONPATH=. python3 scripts/backfill_specs_from_structured_sources.py --dry-run --limit 50
  PYTHONPATH=. python3 scripts/backfill_specs_from_structured_sources.py --vin 1HGBH41JXMN109185
  PYTHONPATH=. python3 scripts/backfill_specs_from_structured_sources.py --limit 200 --no-vpic-cache

Environment: ``SPEC_STRUCTURED_VPIC_OVERWRITE_DEALER`` — allow vPIC to replace non-empty dealer text.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

DB_PATH = os.environ.get("INVENTORY_DB_PATH", str(ROOT / "inventory.db"))

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("backfill_specs")


def main() -> int:
    from backend.db import inventory_db
    from backend.spec_structured_backfill import apply_structured_spec_backfill_for_car, iter_candidate_car_ids

    ap = argparse.ArgumentParser(description="Structured spec backfill (inventory_repair + NHTSA vPIC).")
    ap.add_argument("--dry-run", action="store_true", help="Compute patches but do not write cars or vPIC cache.")
    ap.add_argument("--limit", type=int, default=None, metavar="N", help="Max candidate rows to process.")
    ap.add_argument("--vin", type=str, default=None, help="Restrict to this VIN (any case).")
    ap.add_argument(
        "--no-vpic-cache",
        action="store_true",
        help="Do not read/write SQLite nhtsa_vpic_cache (always hit vPIC when needed).",
    )
    args = ap.parse_args()

    if not os.path.isfile(DB_PATH):
        log.error("Database not found: %s", DB_PATH)
        return 1

    conn = inventory_db.get_conn()
    try:
        inventory_db.ensure_nhtsa_vpic_cache_table(conn)
        conn.commit()
    finally:
        conn.close()

    ids = iter_candidate_car_ids(limit=args.limit, vin=args.vin)
    use_cache = not args.no_vpic_cache

    scanned = 0
    patched = 0
    would_patch = 0
    skipped_complete = 0
    skipped_no_fillable = 0
    skipped_not_found = 0
    vpic_failures = 0
    skip_reasons: Counter[str] = Counter()

    for cid in ids:
        scanned += 1
        r = apply_structured_spec_backfill_for_car(
            cid,
            dry_run=bool(args.dry_run),
            use_vpic_cache=use_cache,
        )
        if r.skip_reason == "already_complete":
            skipped_complete += 1
        elif r.skip_reason == "no_fillable_fields":
            skipped_no_fillable += 1
        elif r.skip_reason == "not_found":
            skipped_not_found += 1
        elif r.skip_reason:
            skip_reasons[r.skip_reason] += 1

        if r.vpic_error:
            vpic_failures += 1

        if r.applied:
            patched += 1
        elif args.dry_run and r.skip_reason == "dry_run" and r.has_pending_patch:
            would_patch += 1

    log.info(
        "rows_scanned=%s patches_applied=%s dry_run_would_patch=%s skipped_complete=%s "
        "skipped_no_fillable=%s skipped_not_found=%s vpic_failures=%s dry_run=%s use_vpic_cache=%s",
        scanned,
        patched,
        would_patch,
        skipped_complete,
        skipped_no_fillable,
        skipped_not_found,
        vpic_failures,
        bool(args.dry_run),
        use_cache,
    )
    if skip_reasons:
        log.info("other_skip_reasons=%s", dict(skip_reasons))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
