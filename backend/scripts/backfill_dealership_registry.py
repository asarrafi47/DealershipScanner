#!/usr/bin/env python3
"""Backfill ``cars.dealership_registry_id`` from ``dealer_url`` host → registry mapping."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from backend.db.inventory_db import backfill_dealership_registry_ids, init_inventory_db  # noqa: E402
from backend.listings.nearby_dealers import (  # noqa: E402
    _mis_stamped_pairs,
    _registry_rows_by_id,
    _rooftop_inventory,
    repair_mis_stamped_registry_ids,
)


def _dry_run() -> int:
    from backend.db.inventory_db import db_conn
    from backend.listings.dealer_registry_match import (
        dealer_url_like_patterns,
        registry_id_by_dealer_host,
    )

    with db_conn() as conn:
        host_map = registry_id_by_dealer_host(conn)
        cur = conn.cursor()
        total = 0
        for host in host_map:
            # Same anchored patterns backfill_dealership_registry_ids uses. A bare
            # f"%{host}%" counted rows the live backfill will never touch: registry 170
            # is subaru.com, and the substring form matches every *subaru.com rooftop.
            patterns = dealer_url_like_patterns(host)
            if not patterns:
                continue
            where = " OR ".join("LOWER(IFNULL(dealer_url, '')) LIKE ?" for _ in patterns)
            cur.execute(
                f"""
                SELECT COUNT(*) FROM cars
                WHERE (COALESCE(listing_active, 1) = 1)
                  AND (dealership_registry_id IS NULL
                       OR CAST(dealership_registry_id AS INTEGER) <= 0)
                  AND ({where})
                """,
                tuple(patterns),
            )
            total += int(cur.fetchone()[0] or 0)

        rooftops = _rooftop_inventory(conn)
        registry_rows = _registry_rows_by_id(conn)

    pairs = _mis_stamped_pairs(rooftops, registry_rows)
    for host, rid, count in sorted(pairs, key=lambda p: -p[2]):
        print(f"  would clear {count} row(s): {host} stamped registry id {rid}")
    print(f"Would clear {sum(p[2] for p in pairs)} mis-stamped row(s).")
    print(f"Would update up to {total} unlinked row(s) across {len(host_map)} host(s).")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Count mis-stamped and unlinked rows only (no UPDATE).",
    )
    parser.add_argument(
        "--no-repair",
        action="store_true",
        help="Skip the mis-stamp repair pass and only fill unlinked rows.",
    )
    args = parser.parse_args()
    init_inventory_db()
    if args.dry_run:
        return _dry_run()

    # Repair first: it nulls out bad stamps, and the backfill below is what re-links the
    # freed rows once their rooftop has a registry row of its own.
    if not args.no_repair:
        cleared, pairs = repair_mis_stamped_registry_ids()
        for host, rid, count in sorted(pairs, key=lambda p: -p[2]):
            print(f"  cleared {host} stamped registry id {rid} ({count} row(s))")
        print(f"Cleared {cleared} mis-stamped row(s).")
    n = backfill_dealership_registry_ids()
    print(f"Updated {n} row(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
