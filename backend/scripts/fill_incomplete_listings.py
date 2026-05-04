#!/usr/bin/env python3
"""
Fill spec gaps in incomplete listings using the knowledge engine.

Run this AFTER build_epa_master.py so the engine has real EPA data.

Steps:
  1. Query all incomplete car IDs from incomplete_listings.db
  2. Run the knowledge engine on each and persist any fillable fields back to cars table
  3. Print a summary of what was filled

Usage:
  python -m backend.scripts.fill_incomplete_listings
  python -m backend.scripts.fill_incomplete_listings --all    # run on every active listing
  python -m backend.scripts.fill_incomplete_listings --dry-run
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_REPO_ROOT))

from backend.enrichment.persist_enrichment import enrich_car_and_persist
from backend.utils.listing_completeness import (
    INCOMPLETE_FIELD_LABELS,
    listing_missing_field_codes,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("fill_incomplete")


def _get_incomplete_car_ids() -> list[int]:
    try:
        from backend.db import incomplete_listings_db as ild
        cars = ild.get_incomplete_cars_for_dev()
        return [c["id"] for c in cars if c.get("id")]
    except Exception:
        log.warning("incomplete_listings.db unavailable; falling back to inline scan")
        return []


def _get_all_active_car_ids() -> list[int]:
    from backend.db.inventory_db import db_conn
    with db_conn() as conn:
        rows = conn.execute(
            "SELECT id FROM cars WHERE COALESCE(listing_active, 1) = 1"
        ).fetchall()
    return [r[0] for r in rows]


def _missing_before_after(car_id: int) -> tuple[list[str], list[str]]:
    from backend.db.inventory_db import get_car_by_id
    car_before = get_car_by_id(car_id)
    before = listing_missing_field_codes(car_before or {}, for_public_filter=True)
    return before, None  # after computed after enrichment


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--all", action="store_true", help="Run on all active listings, not just incomplete ones")
    parser.add_argument("--dry-run", action="store_true", help="Show what would change, don't write")
    args = parser.parse_args(argv)

    if args.all:
        car_ids = _get_all_active_car_ids()
        log.info("Running on all %d active listings", len(car_ids))
    else:
        car_ids = _get_incomplete_car_ids()
        log.info("Running on %d incomplete listings", len(car_ids))

    if not car_ids:
        log.info("No listings to process")
        return

    field_fill_counts: dict[str, int] = {}
    cars_improved = 0
    cars_now_complete = 0

    for car_id in car_ids:
        from backend.db.inventory_db import get_car_by_id
        car_before = get_car_by_id(car_id)
        if not car_before:
            continue

        before_missing = listing_missing_field_codes(car_before, for_public_filter=True)
        # In --all mode run enrichment even on "complete" listings to persist
        # render-time inferred values (e.g. condition derived from title) back to DB.
        if not before_missing and not args.all:
            continue

        if args.dry_run:
            # Show what the engine would fill without writing
            from backend.enrichment.persist_enrichment import _FILLABLE, _is_blank, _coerce
            from backend.enrichment.knowledge_engine import merge_verified_specs
            from backend.utils.field_clean import is_effectively_empty
            vs = merge_verified_specs(car_before)
            would_fill = []
            for db_col, primary_key, fallback_key in _FILLABLE:
                if not _is_blank(car_before, db_col):
                    continue
                value = vs.get(primary_key)
                if (value is None or (isinstance(value, str) and is_effectively_empty(value))) and fallback_key:
                    value = vs.get(fallback_key)
                value = _coerce(db_col, value)
                if value is not None and not (isinstance(value, str) and is_effectively_empty(value)):
                    would_fill.append(f"{db_col}={value!r}")
            if would_fill:
                log.info("car %d (%s %s %s): would fill %s",
                         car_id,
                         car_before.get("year"), car_before.get("make"), car_before.get("model"),
                         ", ".join(would_fill))
            continue

        filled = enrich_car_and_persist(car_id)
        if not filled:
            continue

        cars_improved += 1
        for field in filled:
            field_fill_counts[field] = field_fill_counts.get(field, 0) + 1

        # Check if listing is now complete
        car_after = get_car_by_id(car_id)
        after_missing = listing_missing_field_codes(car_after or {}, for_public_filter=True)
        if not after_missing:
            cars_now_complete += 1
            log.info("car %d (%s %s): NOW COMPLETE — filled %s",
                     car_id,
                     car_before.get("make"), car_before.get("model"),
                     list(filled.keys()))
        else:
            resolved = set(before_missing) - set(after_missing)
            if resolved:
                log.info("car %d (%s %s): filled %s, still missing %s",
                         car_id,
                         car_before.get("make"), car_before.get("model"),
                         sorted(resolved), sorted(after_missing))

    if args.dry_run:
        log.info("Dry-run complete — no changes written")
        return

    log.info("")
    log.info("=== Summary ===")
    log.info("Cars improved:      %d / %d", cars_improved, len(car_ids))
    log.info("Cars now complete:  %d", cars_now_complete)
    if field_fill_counts:
        log.info("Fields filled:")
        for field, count in sorted(field_fill_counts.items(), key=lambda x: -x[1]):
            label = INCOMPLETE_FIELD_LABELS.get(field, field)
            log.info("  %-22s %d cars", label, count)


if __name__ == "__main__":
    main()
