#!/usr/bin/env python3
"""
Rewrite ``cars.transmission`` to standard buckets (Automatic / Manual / CVT) for rows
indexed as incomplete due to a transmission gap.

The incomplete index lives in the **sidecar** DB ``incomplete_listings.db`` (same directory
as ``inventory.db`` by default), not inside ``inventory.db``. This script uses
:func:`backend.db.inventory_db.get_incomplete_cars` so both files are resolved the same
way as the rest of the app.

Uses :func:`backend.utils.transmission_normalize.normalize_transmission_standard` with
weak-match logging suppressed during the batch (see summary line at INFO).

Examples::

  INVENTORY_DB_PATH=/path/to/inventory.db python backend/scripts/normalize_transmission_inventory.py --dry-run
  python backend/scripts/normalize_transmission_inventory.py
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    log = logging.getLogger("normalize_transmission_inventory")

    p = argparse.ArgumentParser(description="Normalize transmission strings for incomplete-index rows.")
    p.add_argument(
        "--db",
        type=Path,
        default=None,
        help="inventory.db path (sets INVENTORY_DB_PATH for this process)",
    )
    p.add_argument(
        "--incomplete-db",
        type=Path,
        default=None,
        help="incomplete_listings.db path (sets INCOMPLETE_LISTINGS_DB_PATH)",
    )
    p.add_argument("--dry-run", action="store_true", help="Print counts only; do not UPDATE.")
    args = p.parse_args(argv)

    if args.db:
        os.environ["INVENTORY_DB_PATH"] = str(Path(args.db).expanduser().resolve())
    if args.incomplete_db:
        os.environ["INCOMPLETE_LISTINGS_DB_PATH"] = str(Path(args.incomplete_db).expanduser().resolve())

    from backend.db.incomplete_listings_db import DB_PATH as INCOMPLETE_DB_PATH
    from backend.db.inventory_db import DB_PATH, get_incomplete_cars, update_car_row_partial
    from backend.utils.transmission_normalize import normalize_transmission_standard

    inv_path = Path(DB_PATH).expanduser().resolve()
    if not inv_path.is_file():
        log.error("Inventory database not found: %s (set INVENTORY_DB_PATH or --db)", inv_path)
        return 2

    inc_path = Path(INCOMPLETE_DB_PATH).expanduser().resolve()
    if not inc_path.is_file():
        log.warning(
            "Incomplete index DB missing: %s — nothing indexed yet. "
            "Run the app once or: python backend/scripts/rebuild_listings_index.py",
            inc_path,
        )
        return 0

    cars = get_incomplete_cars()
    if not cars:
        log.info("Incomplete index is empty (%s). Nothing to do.", inc_path)
        return 0

    eligible = skipped_no_norm = unchanged = updated = weak_count = 0
    dry_skipped = 0

    for c in cars:
        fields = c.get("incomplete_missing_fields") or []
        if not isinstance(fields, list) or "transmission" not in fields:
            continue
        eligible += 1
        raw = c.get("transmission")
        norm, weak = normalize_transmission_standard(
            raw,
            make=c.get("make"),
            model=c.get("model"),
            trim=c.get("trim"),
            title=c.get("title"),
            year=c.get("year"),
            vin=c.get("vin"),
            log_weak=False,
        )
        if not norm:
            skipped_no_norm += 1
            continue
        prev = (raw or "").strip() if raw is not None else ""
        if prev == norm:
            unchanged += 1
            continue
        if weak:
            weak_count += 1
        if args.dry_run:
            dry_skipped += 1
            continue
        update_car_row_partial(int(c["id"]), {"transmission": norm})
        updated += 1

    log.info(
        "Inventory: %s | Incomplete index: %s",
        inv_path,
        inc_path,
    )
    log.info(
        "Eligible (transmission gap): %s | would update / updated: %s | "
        "unchanged: %s | no normalized value: %s | weak_fallback among updates: %s | dry_run=%s",
        eligible,
        dry_skipped if args.dry_run else updated,
        unchanged,
        skipped_no_norm,
        weak_count,
        args.dry_run,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
