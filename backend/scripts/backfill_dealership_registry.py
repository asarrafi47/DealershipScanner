#!/usr/bin/env python3
"""Backfill ``cars.dealership_registry_id`` from ``dealer_url`` host → registry mapping."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from backend.db.inventory_db import backfill_dealership_registry_ids, db_conn  # noqa: E402
from backend.listings.dealer_registry_match import (  # noqa: E402
    registry_id_by_dealer_host,
    registry_id_by_dealer_slug,
    resolve_car_dealership_registry_id,
)
from backend.utils.project_env import bootstrap_inventory_script  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Count unlinked rows only (no UPDATE).",
    )
    args = parser.parse_args()
    bootstrap_inventory_script()
    if args.dry_run:
        with db_conn() as conn:
            host_map = registry_id_by_dealer_host(conn)
            slug_map = registry_id_by_dealer_slug(host_map)
            cur = conn.cursor()
            cur.execute(
                """
                SELECT id, dealer_url, dealer_id FROM cars
                WHERE (COALESCE(listing_active, 1) = 1)
                  AND (dealership_registry_id IS NULL
                       OR CAST(dealership_registry_id AS INTEGER) <= 0)
                  AND (
                    (dealer_url IS NOT NULL AND TRIM(dealer_url) != '')
                    OR (dealer_id IS NOT NULL AND TRIM(dealer_id) != '')
                  )
                """
            )
            total = 0
            for row in cur.fetchall():
                if isinstance(row, dict):
                    url = row.get("dealer_url")
                    did = row.get("dealer_id")
                else:
                    url = row[1]
                    did = row[2]
                if resolve_car_dealership_registry_id(
                    {
                        "dealer_url": url,
                        "dealer_id": did,
                        "dealership_registry_id": None,
                    },
                    host_to_registry=host_map,
                    slug_to_registry=slug_map,
                ):
                    total += 1
        print(
            f"Would update {total} row(s) across {len(host_map)} host(s) "
            f"and {len(slug_map)} manifest slug(s)."
        )
        return 0
    n = backfill_dealership_registry_ids()
    print(f"Updated {n} row(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
