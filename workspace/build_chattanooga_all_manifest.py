"""
Build manifest_chattanooga_all_20260619.json — ALL car dealers within 25mi of Chattanooga.

Sources:
  1. OSM/Overpass — 55+ dealers (franchise + independent + used lots)
  2. Existing manifest_chattanooga_25mi.json — 18 franchise dealers with known providers
  3. DDG URL gap-fill for OSM dealers without websites
"""
from __future__ import annotations

import json
import logging
import re
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

try:
    from backend.utils.project_env import load_project_dotenv
    load_project_dotenv()
except ImportError:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("manifest_builder")

LAT, LON = 35.0456, -85.3097
RADIUS = 25.0

WORKSPACE = ROOT / "workspace"


def slug_from_url(url: str) -> str:
    """Convert URL hostname to dealer_id slug."""
    url = url.strip().lower()
    url = re.sub(r"^https?://", "", url)
    url = re.sub(r"^www\.", "", url)
    url = url.split("/")[0].split("?")[0]
    url = re.sub(r"[^a-z0-9]", "-", url)
    url = re.sub(r"-+", "-", url).strip("-")
    return url


def normalize_https(url: str) -> str:
    url = url.strip()
    if not url:
        return ""
    if url.startswith("http://"):
        url = "https://" + url[7:]
    elif not url.startswith("https://"):
        url = "https://" + url
    return url.rstrip("/")


def main() -> None:
    from backend.discovery.osm import fetch_osm_dealerships
    from backend.discovery.web import ddg_find_dealer_url
    from backend.discovery.normalize import normalize_url, looks_like_dealer_website

    # ── Step 1: Load existing franchise manifest ──────────────────────────────
    existing_path = WORKSPACE / "manifest_chattanooga_25mi.json"
    with open(existing_path) as f:
        existing = json.load(f)
    logger.info("Loaded %d existing franchise dealers from %s", len(existing), existing_path.name)

    # Build URL → row index for fast lookup
    url_to_existing: dict[str, dict] = {}
    for row in existing:
        u = (row.get("url") or "").strip().lower().rstrip("/")
        if u:
            url_to_existing[u] = row

    # ── Step 2: OSM discovery ─────────────────────────────────────────────────
    logger.info("Querying OSM Overpass for all car dealers within %.0fmi...", RADIUS)
    osm_dealers = fetch_osm_dealerships(LAT, LON, RADIUS)
    logger.info("OSM returned %d dealers", len(osm_dealers))

    # ── Step 3: DDG URL gap-fill for OSM dealers without URLs ─────────────────
    logger.info("Running DDG URL gap-fill for OSM dealers without websites...")
    ddg_calls = 0
    for dealer in osm_dealers:
        raw = (dealer.dealer_website_url or dealer.website_url or "").strip()
        if raw and looks_like_dealer_website(raw):
            continue
        # Only gap-fill for dealers with a proper name (skip bare brand names with no city)
        name = (dealer.name or "").strip()
        city = (dealer.city or "").strip()
        state = (dealer.state or "").strip()
        if not name or len(name) < 4:
            continue
        if ddg_calls > 0:
            time.sleep(1.5)
        ddg_calls += 1
        logger.info("DDG: looking up URL for '%s' (%s, %s)", name, city or "?", state or "?")
        u = ddg_find_dealer_url(name, city, state, timeout_s=15.0)
        if u and looks_like_dealer_website(u):
            nu = normalize_url(u) or u
            dealer.dealer_website_url = nu
            dealer.website_url = nu
            dealer.source_web = True
            logger.info("  DDG found: %s", nu)
        else:
            logger.info("  DDG: no URL found")

    # ── Step 4: Build unified manifest ────────────────────────────────────────
    # Start with all existing franchise dealers (preserves skip/provider/dealer_id)
    manifest: list[dict] = list(existing)
    manifest_urls: set[str] = set()
    for row in existing:
        u = (row.get("url") or "").strip().lower().rstrip("/")
        if u:
            manifest_urls.add(u)

    added = 0
    skipped_no_url = 0
    skipped_duplicate = 0

    for dealer in osm_dealers:
        raw_url = (dealer.dealer_website_url or dealer.website_url or "").strip()
        if not raw_url:
            skipped_no_url += 1
            logger.info("SKIP (no url): %s", dealer.name)
            continue

        url = normalize_https(raw_url).rstrip("/")
        url_key = url.lower()

        if url_key in manifest_urls:
            skipped_duplicate += 1
            continue

        manifest_urls.add(url_key)
        dealer_id = slug_from_url(url)
        name = (dealer.name or "").strip()

        manifest.append({
            "name": name,
            "url": url,
            "provider": "unknown",
            "dealer_id": dealer_id,
        })
        added += 1
        logger.info("ADDED: %s | %s [%s]", name, url, dealer_id)

    logger.info(
        "Manifest: %d total | %d from existing franchise | %d new from OSM | "
        "%d OSM skipped (no url) | %d OSM skipped (duplicate)",
        len(manifest), len(existing), added, skipped_no_url, skipped_duplicate,
    )

    # ── Step 5: Save ──────────────────────────────────────────────────────────
    out_path = WORKSPACE / "manifest_chattanooga_all_20260619.json"
    with open(out_path, "w") as f:
        json.dump(manifest, f, indent=2)
    logger.info("Saved manifest → %s (%d dealers)", out_path, len(manifest))

    # Print summary
    active = [r for r in manifest if not r.get("skip")]
    skipped = [r for r in manifest if r.get("skip")]
    print(f"\n=== Manifest Summary: {len(manifest)} total ({len(active)} active, {len(skipped)} skipped) ===")
    for row in manifest:
        skip_tag = f" [SKIP: {row.get('skip_reason','')}]" if row.get("skip") else ""
        print(f"  {row['name']} | {row.get('provider','?')} | {row.get('url','')}{skip_tag}")


if __name__ == "__main__":
    main()
