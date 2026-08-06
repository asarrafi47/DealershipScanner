#!/usr/bin/env python3
"""
Import Google star ratings + review counts for registry dealerships.

Dry run by default — nothing is written until ``--apply``, matching
``backend/scripts/migrate.py``. Work is sequential and paced; the queue is
"dealerships with no rating row yet", so an interrupted run resumes rather than
starting over, and re-running a completed run is a no-op.

The Places API (New) has been disabled on this project before (403
SERVICE_DISABLED on GCP project 326691549936). If that happens again the run
stops on the first response and says so, with the exact console action needed,
instead of quietly recording two hundred dealers as "no rating".

Usage:
  python -m backend.scripts.import_dealer_google_ratings                  # dry run, 25 dealers
  python -m backend.scripts.import_dealer_google_ratings --limit 5        # smaller probe
  python -m backend.scripts.import_dealer_google_ratings --apply
  python -m backend.scripts.import_dealer_google_ratings --apply --refresh-days 30
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_REPO_ROOT))

from backend.utils.project_env import load_project_dotenv  # noqa: E402

log = logging.getLogger("import_dealer_google_ratings")

_REMEDY = {
    "no_api_key": "Set GOOGLE_MAPS_API_KEY in .env.",
    "service_disabled": (
        "Enable 'Places API (New)' for the GCP project that owns GOOGLE_MAPS_API_KEY "
        "(console.cloud.google.com > APIs & Services > Library), then re-run."
    ),
    "invalid_key": "GOOGLE_MAPS_API_KEY is rejected — reissue it in the GCP console.",
    "permission_denied": (
        "The key exists but may not call Places — check its API restrictions in the GCP console."
    ),
    "quota_exhausted": "Quota or rate limit hit — wait, then re-run with a smaller --limit.",
}


def _report(stats, *, apply: bool) -> int:
    d = stats.as_dict()
    unverified = int(d.get("unverified") or 0)
    log.info("=== Dealer Google ratings (%s) ===", "applied" if apply else "DRY RUN")
    log.info("  examined:  %d", d["examined"])
    log.info("  resolved:  %d  (verified %d, UNVERIFIED %d)",
             d["resolved"], d.get("verified", d["resolved"] - unverified), unverified)
    log.info("  no result: %d", d["no_result"])
    log.info("  failed:    %d", d["failed"])
    log.info("  written:   %d", d["written"])
    for s in d["samples"]:
        # The verified flag rides on the sample line so "resolved: 5" can never
        # be read as five confirmed rooftops when some were name-only guesses.
        log.info(
            "    [%s] %-40s %s stars (%s reviews)",
            "OK " if s.get("verified") else "??",
            str(s["name"])[:40], s["rating"], s["review_count"],
        )
    if d["errors"]:
        log.info("  errors:    %s", d["errors"])

    if unverified:
        # Loud, and after the counters, because attaching a rating to the wrong
        # rooftop is the failure this importer exists to avoid.
        log.warning(
            "  %d of %d rating(s) are NOT domain-confirmed — stored under source=%r, "
            "legacy dealerships.google_* left untouched. Grep the warnings above for "
            "'UNVERIFIED' to see which dealers.",
            unverified, d["resolved"], "google_places_unverified",
        )

    stopped = d["stopped_early"]
    if stopped:
        log.error("STOPPED EARLY: %s", stopped)
        remedy = _REMEDY.get(stopped)
        if remedy:
            log.error("  -> %s", remedy)
        return 2
    if not apply and d["examined"]:
        log.info("  nothing was written — re-run with --apply")
    return 0


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true",
                    help="write results (default: dry run, no row is stored)")
    ap.add_argument("--limit", type=int, default=25,
                    help="max dealerships to look up this run (default: 25)")
    ap.add_argument("--delay", type=float, default=None,
                    help="seconds between requests (default: 0.35; bursts get soft-blocked)")
    ap.add_argument("--refresh-days", type=int, default=None,
                    help="also refresh ratings older than N days (default: never refresh)")
    args = ap.parse_args(argv)

    load_project_dotenv()

    from backend.enrichment.dealer_ratings import DEFAULT_DELAY_SECONDS, sync_dealer_ratings

    stats = sync_dealer_ratings(
        limit=max(1, int(args.limit)),
        refresh_after_days=args.refresh_days,
        dry_run=not args.apply,
        delay_seconds=DEFAULT_DELAY_SECONDS if args.delay is None else max(0.0, args.delay),
    )
    return _report(stats, apply=bool(args.apply))


if __name__ == "__main__":
    raise SystemExit(main())
