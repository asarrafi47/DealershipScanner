#!/usr/bin/env python3
"""
Discover dealerships in the top 100 US cities using DMV + web search tiers.

This bypasses the Overpass API entirely, making discovery truly unlimited —
DMV records are static CSV files, and DuckDuckGo HTML search has no quotas.

Examples::

  python run_city_discovery.py --city Charlotte --persist -v
  python run_city_discovery.py --max-cities 10 --persist
  python run_city_discovery.py --max-cities 5 --dmv-state NC -v
  python run_city_discovery.py
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from backend.utils.project_env import load_project_dotenv
    load_project_dotenv()
except ImportError:
    pass


def load_cities(max_count: int = 0) -> list[dict]:
    """Load top 100 cities from JSON file."""
    cities_file = ROOT / "backend" / "data" / "top100_cities.json"
    if not cities_file.is_file():
        print(f"error: cities file not found: {cities_file}", file=sys.stderr)
        return []

    with cities_file.open() as f:
        cities = json.load(f)

    if max_count > 0:
        cities = cities[:max_count]
    return cities


def main() -> int:
    p = argparse.ArgumentParser(
        description="Discover dealerships in top 100 US cities (no Overpass API).",
    )
    p.add_argument(
        "--city",
        default=None,
        metavar="NAME",
        help="Single city name (e.g. 'Charlotte'); omit to process all top 100",
    )
    p.add_argument(
        "--max-cities",
        type=int,
        default=0,
        metavar="N",
        help="When using batch mode, stop after N cities (0 = no limit)",
    )
    p.add_argument(
        "--dmv-state",
        default=None,
        metavar="ST",
        help="Limit DMV tier to this state (e.g. NC); omit to auto-detect from city",
    )
    p.add_argument(
        "--radius",
        type=float,
        default=None,
        metavar="MILES",
        help="Override default radius per-city (only valid with --city)",
    )
    p.add_argument(
        "--no-ddg",
        action="store_true",
        help="Skip DuckDuckGo URL gap-fill",
    )
    p.add_argument(
        "--persist",
        action="store_true",
        help="Upsert results into inventory.db dealerships table",
    )
    p.add_argument(
        "--no-merge-manifest",
        action="store_true",
        help="Skip merging into dealers.json",
    )
    p.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Verbose logging",
    )
    args = p.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.WARNING,
        format="%(levelname)s %(name)s: %(message)s",
    )
    logger = logging.getLogger(__name__)

    from backend.discovery.pipeline import run_discovery, dmv_records_to_candidates
    from backend.discovery.web_city import search_city_dealerships
    from backend.discovery.merge import merge_and_dedupe
    from backend.discovery.coordinate_enrich import enrich_candidate_location_fields
    from backend.discovery.web import ddg_find_dealer_url
    from backend.db.dealerships_db import upsert_discovery_row
    from backend.discovery.normalize import normalize_zip

    # Load cities
    cities = load_cities(max_count=args.max_cities if args.max_cities > 0 else 0)
    if not cities:
        return 2

    # Filter by single city if specified
    if args.city:
        cities = [c for c in cities if c["name"].lower() == args.city.lower()]
        if not cities:
            print(f"error: city '{args.city}' not found in top 100", file=sys.stderr)
            return 2

    if len(cities) > 500:
        print(
            f"warning: batch discovery for {len(cities)} cities (large run, heavy on web search); "
            "use --max-cities to cap",
            file=sys.stderr,
        )

    merge_manifest = not args.no_merge_manifest
    ddg_fill = not args.no_ddg

    total_discovered = 0

    for city_data in cities:
        city_name = city_data["name"]
        state = city_data["state"]
        zip_code = normalize_zip(city_data["zip"])
        lat = city_data["lat"]
        lon = city_data["lon"]
        radius_miles = args.radius if args.radius is not None else city_data.get("radius_miles", 25.0)

        if not zip_code:
            logger.warning("Skipping %s %s: invalid zip in data", city_name, state)
            continue

        logger.info("=== City: %s, %s (zip %s, radius %.1f mi) ===", city_name, state, zip_code, radius_miles)

        candidates = []

        # Tier 1: DMV Records (optional, state-specific)
        dmv_state_to_use = args.dmv_state or state
        if dmv_state_to_use:
            try:
                from backend.discovery.dmv import fetch_dmv_records
                records = fetch_dmv_records(dmv_state_to_use, ROOT)
                dmv_cands = dmv_records_to_candidates(records, lat, lon, radius_miles)
                candidates.extend(dmv_cands)
                logger.info("  DMV tier: %d candidates", len(dmv_cands))
            except Exception as e:
                logger.debug("  DMV tier error: %s", e)

        # Tier 2: Web City Discovery (new)
        try:
            web_cands = search_city_dealerships(city_name, state)
            candidates.extend(web_cands)
            logger.info("  Web tier: %d candidates", len(web_cands))
        except Exception as e:
            logger.warning("  Web tier error: %s", e)

        # Merge & deduplicate
        merged = merge_and_dedupe(candidates)
        logger.info("  After merge: %d candidates", len(merged))

        # Enrich location fields
        for c in merged:
            try:
                enrich_candidate_location_fields(c)
            except Exception as e:
                logger.debug("  Enrichment error for %s: %s", c.name, e)

        # Tier 3: DDG URL gap-fill
        if ddg_fill:
            for c in merged:
                if not (c.dealer_website_url or c.website_url).strip():
                    try:
                        url = ddg_find_dealer_url(c.name, c.city, c.state)
                        if url:
                            c.dealer_website_url = url
                            c.website_url = url
                            c.source_web = True
                    except Exception as e:
                        logger.debug("  DDG lookup failed for %s: %s", c.name, e)

        # Persist
        if args.persist:
            for c in merged:
                try:
                    upsert_discovery_row(c.to_db_dict())
                except Exception as e:
                    logger.warning("  Upsert failed for %s: %s", c.name, e)

        # Merge manifest (dealers.json)
        if merge_manifest and merged:
            try:
                from backend.discovery.manifest_merge import merge_candidates_into_scanner_manifest
                manifest_path = (ROOT / "dealers.json").resolve()
                stats = merge_candidates_into_scanner_manifest(merged, manifest_path=manifest_path)
                logger.info(
                    "  Manifest merge: inserted=%d updated=%d skipped=%d",
                    stats["inserted"],
                    stats["updated"],
                    stats["skipped"],
                )
            except Exception as e:
                logger.warning("  Manifest merge failed: %s", e)

        total_discovered += len(merged)
        logger.info("  Total from this city: %d", len(merged))

    logger.info("=== DONE: %d dealerships discovered across %d cities ===", total_discovered, len(cities))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
