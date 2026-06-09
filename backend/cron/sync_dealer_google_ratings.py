#!/usr/bin/env python3
"""
Backfill missing Google ratings on registry dealerships (cache-only writes).

Ratings are never refreshed on car page views — only here, on discovery ingest,
or when the scanner cron hits every Nth batch (default 10).

Usage:
  python backend/cron/sync_dealer_google_ratings.py
  python backend/cron/sync_dealer_google_ratings.py --max 50
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from typing import Any

import requests

from backend.discovery.google_place_rating import (
    fetch_place_rating_by_id,
    google_maps_api_key,
    search_place_rating,
)
from backend.db.dealerships_db import (
    list_dealers_needing_google_rating,
    save_dealer_google_rating,
)

logger = logging.getLogger("sync_dealer_google_ratings")


def _backfill_max_per_run() -> int:
    raw = (os.environ.get("DEALER_GOOGLE_RATING_BACKFILL_MAX") or "25").strip()
    try:
        return max(1, int(raw))
    except ValueError:
        return 25


def _request_delay_s() -> float:
    raw = (os.environ.get("DEALER_GOOGLE_RATING_REQUEST_DELAY_S") or "0.25").strip()
    try:
        return max(0.0, float(raw))
    except ValueError:
        return 0.25


def backfill_dealer_google_ratings(*, max_dealers: int | None = None) -> dict[str, Any]:
    """
    Fetch Google ratings for active registry rows that have never been looked up.

    Existing cached ratings are left unchanged (no refresh).
    """
    cap = max_dealers if max_dealers is not None else _backfill_max_per_run()
    cap = max(1, int(cap))
    stats: dict[str, Any] = {
        "examined": 0,
        "resolved": 0,
        "missed": 0,
        "skipped_no_api_key": False,
        "max_dealers": cap,
    }
    key = google_maps_api_key()
    if not key:
        stats["skipped_no_api_key"] = True
        logger.warning("Skipping dealer Google rating backfill — no GOOGLE_MAPS_API_KEY")
        return stats

    rows = list_dealers_needing_google_rating(limit=cap)
    if not rows:
        return stats

    sess = requests.Session()
    delay = _request_delay_s()
    for row in rows:
        stats["examined"] += 1
        dealer_id = int(row["id"])
        place_id = row.get("google_place_id")
        resolved = None
        if place_id:
            resolved = fetch_place_rating_by_id(str(place_id), api_key=key, session=sess)
        if not resolved:
            resolved = search_place_rating(
                str(row.get("name") or ""),
                str(row.get("city") or ""),
                str(row.get("state") or ""),
                latitude=row.get("latitude"),
                longitude=row.get("longitude"),
                api_key=key,
                session=sess,
            )
        if resolved:
            save_dealer_google_rating(
                dealer_id,
                place_id=resolved.place_id,
                rating=resolved.rating,
                review_count=resolved.review_count,
            )
            stats["resolved"] += 1
        else:
            save_dealer_google_rating(
                dealer_id,
                place_id=str(place_id) if place_id else None,
                rating=None,
                review_count=None,
            )
            stats["missed"] += 1
        if delay > 0:
            time.sleep(delay)
    return stats


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    ap = argparse.ArgumentParser(description="Backfill missing dealer Google ratings.")
    ap.add_argument(
        "--max",
        type=int,
        default=None,
        help="Max dealerships to look up this run (default: DEALER_GOOGLE_RATING_BACKFILL_MAX or 25).",
    )
    args = ap.parse_args()
    summary = backfill_dealer_google_ratings(max_dealers=args.max)
    logger.info("Dealer Google rating backfill: %s", summary)
    if summary.get("skipped_no_api_key"):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
