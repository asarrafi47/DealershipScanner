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


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Count unlinked rows only (no UPDATE).",
    )
    args = parser.parse_args()
    init_inventory_db()
    if args.dry_run:
        from backend.db.inventory_db import db_conn
        from backend.listings.dealer_registry_match import registry_id_by_dealer_host

        with db_conn() as conn:
            host_map = registry_id_by_dealer_host(conn)
            cur = conn.cursor()
            total = 0
            for host in host_map:
                cur.execute(
                    """
                    SELECT COUNT(*) FROM cars
                    WHERE (COALESCE(listing_active, 1) = 1)
                      AND (dealership_registry_id IS NULL
                           OR CAST(dealership_registry_id AS INTEGER) <= 0)
                      AND LOWER(IFNULL(dealer_url, '')) LIKE ?
                    """,
                    (f"%{host.lower()}%",),
                )
                total += int(cur.fetchone()[0] or 0)
        print(f"Would update up to {total} row(s) across {len(host_map)} host(s).")
        return 0
    n = backfill_dealership_registry_ids()
    print(f"Updated {n} row(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
