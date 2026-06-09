"""
City-based dealership discovery using Google Maps.

Geocodes a city to get its bounding box, auto-tiles it with overlapping circles,
runs Google Places searchNearby on each tile, persists all results to DB.

Usage:
    python scripts/discover_metro.py --city "Charlotte, NC"
    python scripts/discover_metro.py --city "Atlanta, GA"
    python scripts/discover_metro.py --city "Dallas, TX" --tile-radius 12
    python scripts/discover_metro.py --city "Charlotte, NC" --wipe-dealerships
"""
from __future__ import annotations

import argparse
import logging
import math
import os
import sys
import time
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

try:
    from backend.utils.project_env import load_project_dotenv
    load_project_dotenv()
except Exception:
    pass

logger = logging.getLogger(__name__)

GEOCODE_URL = "https://maps.googleapis.com/maps/api/geocode/json"


def geocode_city(city: str, api_key: str) -> dict[str, Any]:
    """
    Returns {lat, lon, viewport: {sw, ne}} for a city string like 'Charlotte, NC'.
    """
    r = requests.get(
        GEOCODE_URL,
        params={"address": city, "key": api_key},
        timeout=15,
    )
    r.raise_for_status()
    data = r.json()
    if data.get("status") != "OK" or not data.get("results"):
        raise ValueError(f"Geocoding failed for {city!r}: {data.get('status')}")
    result = data["results"][0]
    loc = result["geometry"]["location"]
    vp = result["geometry"]["viewport"]
    return {
        "lat": loc["lat"],
        "lon": loc["lng"],
        "sw": (vp["southwest"]["lat"], vp["southwest"]["lng"]),
        "ne": (vp["northeast"]["lat"], vp["northeast"]["lng"]),
    }


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    return R * 2 * math.asin(math.sqrt(a))


def tile_bounding_box(
    sw: tuple[float, float],
    ne: tuple[float, float],
    *,
    tile_radius_miles: float = 12.0,
) -> list[tuple[float, float]]:
    """
    Generate a grid of lat/lon centers covering the bounding box.
    Spacing = tile_radius_miles so adjacent tiles overlap by ~50%.
    """
    tile_km = tile_radius_miles * 1.60934
    # degrees per km
    lat_deg_per_km = 1.0 / 111.0
    mid_lat = (sw[0] + ne[0]) / 2
    lon_deg_per_km = 1.0 / (111.0 * math.cos(math.radians(mid_lat)))

    lat_step = tile_km * lat_deg_per_km
    lon_step = tile_km * lon_deg_per_km

    centers: list[tuple[float, float]] = []
    lat = sw[0]
    while lat <= ne[0] + lat_step * 0.5:
        lon = sw[1]
        while lon <= ne[1] + lon_step * 0.5:
            centers.append((round(lat, 6), round(lon, 6)))
            lon += lon_step
        lat += lat_step

    return centers


def discover_city(
    city: str,
    *,
    tile_radius_miles: float = 12.0,
    no_ddg: bool = False,
    wipe: bool = False,
    verbose: bool = False,
) -> None:
    api_key = os.environ.get("GOOGLE_MAPS_API_KEY", "")
    if not api_key:
        print("ERROR: GOOGLE_MAPS_API_KEY not set", file=sys.stderr)
        sys.exit(1)

    from backend.discovery.google_places import fetch_google_places_dealerships
    from backend.db.dealerships_db import ensure_dealerships_table, get_conn

    if wipe:
        print("Wiping dealerships table...")
        conn = get_conn()
        cur = conn.cursor()
        ensure_dealerships_table(cur)
        cur.execute("DELETE FROM dealerships")
        conn.commit()
        conn.close()
        print("Dealerships table cleared.")

    print(f"Geocoding {city!r}...")
    geo = geocode_city(city, api_key)
    print(f"  Center: {geo['lat']:.4f}, {geo['lon']:.4f}")
    print(f"  Viewport: SW={geo['sw']} NE={geo['ne']}")

    centers = tile_bounding_box(geo["sw"], geo["ne"], tile_radius_miles=tile_radius_miles)
    print(f"  Auto-tiled into {len(centers)} circles ({tile_radius_miles}mi radius each)")

    manifest_path = ROOT / "dealers.json"
    seen_place_ids: set[str] = set()
    all_rows: list[Any] = []
    total_raw = 0

    with requests.Session() as sess:
        for i, (lat, lon) in enumerate(centers):
            print(f"  [{i+1}/{len(centers)}] tile ({lat:.4f}, {lon:.4f})", end=" ... ", flush=True)
            try:
                results = fetch_google_places_dealerships(lat, lon, tile_radius_miles, session=sess)
            except Exception as e:
                print(f"ERROR: {e}")
                continue
            new = [r for r in results if (r.osm_id or r.name) not in seen_place_ids]
            for r in new:
                seen_place_ids.add(r.osm_id or r.name)
            total_raw += len(results)
            print(f"{len(new)} new ({len(results)} raw)")
            all_rows.extend(new)
            if i < len(centers) - 1:
                time.sleep(0.3)

    print(f"\nTotal unique dealers found: {len(all_rows)} (from {total_raw} raw results across {len(centers)} tiles)")

    if not all_rows:
        print("No dealers found. Check GOOGLE_MAPS_API_KEY and city name.")
        return

    # Persist via run_discovery-compatible path: upsert each candidate
    from backend.db.dealerships_db import upsert_discovery_row

    persisted = 0
    for candidate in all_rows:
        try:
            upsert_discovery_row(candidate.to_db_dict())
            persisted += 1
        except Exception as e:
            logger.debug("Upsert failed for %s: %s", candidate.name, e)

    print(f"Persisted {persisted} dealers to DB")
    print(f"\nRun scanner with: DEALERS_FROM_DB=1 python -m backend.scanner.cli")


def main() -> None:
    p = argparse.ArgumentParser(description="City-based dealership discovery via Google Maps")
    p.add_argument("--city", required=True, help='City to discover, e.g. "Charlotte, NC"')
    p.add_argument("--tile-radius", type=float, default=12.0, metavar="MILES",
                   help="Radius per tile in miles (default: 12)")
    p.add_argument("--wipe-dealerships", action="store_true",
                   help="Clear the dealerships table before running (fresh start)")
    p.add_argument("--no-ddg", action="store_true", help="Skip DDG URL gap-fill")
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.WARNING,
        format="%(levelname)s %(name)s: %(message)s",
    )

    discover_city(
        args.city,
        tile_radius_miles=args.tile_radius,
        no_ddg=args.no_ddg,
        wipe=args.wipe_dealerships,
        verbose=args.verbose,
    )


if __name__ == "__main__":
    main()
