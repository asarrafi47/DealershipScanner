#!/usr/bin/env python3
"""Compute + cache LLM lease-offer matches for one or more dealers.

The dealership research page renders lease qualifying-cars from the
``lease_offer_matches`` cache; this script populates it out of band so page
views never block on the model.

Usage:
    python3 scripts/refresh_lease_matches.py --dealer fjmercedes-com
    python3 scripts/refresh_lease_matches.py --dealer fjmercedes-com --force
    python3 scripts/refresh_lease_matches.py --all           # every dealer with lease offers
"""
from __future__ import annotations

import argparse
import sqlite3
import sys

from backend.db.inventory_db import get_conn
from backend.intelligence.lease_matcher import refresh_dealer_lease_matches


def _dealers_with_leases(conn) -> list[str]:
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(
        "SELECT DISTINCT dealer_id FROM dealer_specials "
        "WHERE LOWER(COALESCE(type,'')) = 'lease' ORDER BY dealer_id"
    )
    return [r["dealer_id"] for r in cur.fetchall()]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dealer", action="append", default=[], help="dealer_id (repeatable)")
    ap.add_argument("--all", action="store_true", help="all dealers with lease offers")
    ap.add_argument("--force", action="store_true", help="recompute cached offers too")
    ap.add_argument("--provider", default=None, help="override LLM provider (claude|local)")
    args = ap.parse_args()

    conn = get_conn()
    try:
        dealers = list(args.dealer)
        if args.all:
            dealers = _dealers_with_leases(conn)
        if not dealers:
            print("no dealers given (use --dealer <id> or --all)", file=sys.stderr)
            return 2
        for d in dealers:
            stats = refresh_dealer_lease_matches(
                conn, d, provider=args.provider, force=args.force,
            )
            print(
                f"{d}: {stats['lease_offers']} lease offers, "
                f"computed={stats['computed']} skipped={stats['skipped']} "
                f"matches={stats['total_matches']}"
            )
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
