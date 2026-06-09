#!/usr/bin/env python3
"""
Download EPA EV/PHEV range miles from fueleconomy.gov and cache locally.

Usage:
  python backend/cron/sync_epa_ev_ranges.py
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    from backend.intelligence.ev_range_estimates import build_epa_ev_range_cache, write_epa_ev_range_cache

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    payload = build_epa_ev_range_cache()
    path = write_epa_ev_range_cache(payload)
    rows = payload.get("rows") or []
    logging.info("Wrote %s (%s electrified rows)", path, len(rows))
    return 0


if __name__ == "__main__":
    sys.exit(main())
