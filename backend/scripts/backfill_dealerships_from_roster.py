#!/usr/bin/env python3
"""
Backfill the local ``dealerships`` table from an existing national-scan roster
JSON (default: ``workspace/national_dealers.json``).

This makes NO new Google Places / DDG calls — it only persists dealers the
discovery phase already collected, via ``upsert_discovery_row`` (fuzzy-deduped,
provenance-ORed). Non-dealer rows (``is_dealer == False``) are skipped by
default so salvage yards / rentals / misclassified places don't land in the
registry.

Usage:
    python -m backend.scripts.backfill_dealerships_from_roster
    python -m backend.scripts.backfill_dealerships_from_roster --roster workspace/national_dealers.json
    python -m backend.scripts.backfill_dealerships_from_roster --include-non-dealers
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
os.chdir(_ROOT)
sys.path.insert(0, str(_ROOT))

from backend.utils.project_env import load_project_dotenv

load_project_dotenv()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--roster",
        default=str(_ROOT / "workspace" / "national_dealers.json"),
        help="Path to the roster JSON to load (default: workspace/national_dealers.json).",
    )
    ap.add_argument(
        "--include-non-dealers",
        action="store_true",
        help="Also persist rows flagged is_dealer=False (default: skip them).",
    )
    args = ap.parse_args()

    roster_path = Path(args.roster)
    if not roster_path.is_absolute():
        roster_path = _ROOT / roster_path
    dealers = json.loads(roster_path.read_text())
    print(f"loaded {len(dealers)} roster rows from {roster_path}", flush=True)

    from backend.scripts.national_scan import persist_roster

    n = persist_roster(dealers, only_dealers=not args.include_non_dealers)
    print(f"upserted {n} dealers into the dealerships table", flush=True)


if __name__ == "__main__":
    main()
