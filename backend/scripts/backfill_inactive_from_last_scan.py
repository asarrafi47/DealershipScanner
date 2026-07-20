#!/usr/bin/env python3
"""
One-time backfill for the reconcile bug (2026-07-19): mark rows that were NOT
refreshed by a dealer's most recent scan as inactive.

Because reconcile's ``deduped_rows`` gate was never satisfied, sold/removed
cars were never soft-unlisted. Every car captured by a scan gets a fresh
``scraped_at`` at upsert, so "active row whose scraped_at predates the
dealer's last refresh window" == "VIN absent from the latest feed".

Safety gates per dealer (mirrors ``inventory_reconcile``):
  - the dealer's fresh-row count must be >= --min-rows (default 8);
  - fresh rows must cover >= --min-coverage (default 0.8) of the dealer's
    active rows — partial feeds (e.g. new-only) skip rather than mass-retire;
  - only rows older than the dealer's newest scrape by > --grace-hours.

Usage::

  PYTHONPATH=. python backend/scripts/backfill_inactive_from_last_scan.py --dry-run
  PYTHONPATH=. python backend/scripts/backfill_inactive_from_last_scan.py
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT))

from backend.utils.project_env import load_project_dotenv

load_project_dotenv()

from backend.db.inventory_db import get_conn  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser(description="Backfill listing_active for rows missing from the latest scan")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--min-rows", type=int, default=8)
    ap.add_argument("--min-coverage", type=float, default=0.8)
    ap.add_argument("--grace-hours", type=float, default=6.0,
                    help="Rows within this many hours of the dealer's newest scrape are kept")
    args = ap.parse_args()

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT dealer_id, MAX(scraped_at) FROM cars "
        "WHERE COALESCE(listing_active,1)=1 AND dealer_id IS NOT NULL GROUP BY dealer_id"
    )
    dealers = [(d, mx) for d, mx in cur.fetchall() if d and mx]
    now_iso = datetime.now(timezone.utc).isoformat()

    total_marked = 0
    skipped: dict[str, int] = {"low_fresh": 0, "low_coverage": 0, "clean": 0}
    marked_dealers: list[tuple[str, int, int, int]] = []

    for dealer_id, max_scraped in dealers:
        try:
            newest = datetime.fromisoformat(str(max_scraped).replace("Z", "+00:00"))
        except ValueError:
            continue
        cutoff = (newest - timedelta(hours=args.grace_hours)).isoformat()

        cur.execute(
            "SELECT COUNT(*) FILTER (WHERE scraped_at >= ?) AS fresh, COUNT(*) AS total "
            "FROM cars WHERE COALESCE(listing_active,1)=1 AND dealer_id = ?",
            (cutoff, dealer_id),
        )
        fresh, total = cur.fetchone()
        fresh, total = int(fresh or 0), int(total or 0)
        stale = total - fresh
        if fresh < args.min_rows:
            skipped["low_fresh"] += 1
            continue
        if total and fresh / total < args.min_coverage:
            skipped["low_coverage"] += 1
            continue
        if stale <= 0:
            skipped["clean"] += 1
            continue

        marked_dealers.append((dealer_id, fresh, total, stale))
        total_marked += stale
        if args.dry_run:
            continue
        cur.execute(
            "UPDATE cars SET listing_active = 0, listing_removed_at = ? "
            "WHERE COALESCE(listing_active,1)=1 AND dealer_id = ? AND scraped_at < ?",
            (now_iso, dealer_id, cutoff),
        )
    if not args.dry_run:
        conn.commit()
        try:
            from backend.db.repositories.listings_repo import clear_inventory_listings_cache

            clear_inventory_listings_cache()
        except Exception:
            pass
    conn.close()

    mode = "DRY RUN" if args.dry_run else "live"
    print(f"[{mode}] dealers eligible: {len(marked_dealers)}, rows to retire: {total_marked}")
    print(f"skipped: {skipped}")
    for d, f, t, s in sorted(marked_dealers, key=lambda x: -x[3])[:15]:
        print(f"  {d}: retire {s} of {t} (fresh {f})")


if __name__ == "__main__":
    main()
