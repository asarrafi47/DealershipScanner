#!/usr/bin/env python3
"""
Re-fetch dealer listing descriptions for cars that have a source_url but no description.

Uses Playwright (headless) to visit each listing page, extract the dealer description
text, store it in cars.description, then runs the package extractor to populate
cars.packages with identified packages, features, and badges.

Usage:
  python refetch_descriptions.py                # process all cars with source_url + no description
  python refetch_descriptions.py --all          # re-fetch even if description already set
  python refetch_descriptions.py --limit 20     # cap how many pages to visit
  python refetch_descriptions.py --workers 4    # concurrent tabs
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import re
import sqlite3
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))

try:
    from backend.utils.project_env import load_project_dotenv
    load_project_dotenv()
except ImportError:
    pass

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger("refetch_desc")

DESCRIPTION_SELECTORS = [
    ".vehicle-description", ".vehicleDescription", ".vehicle_description",
    ".dealer-comments", ".dealerComments", ".dealer_comments",
    ".seller-notes", ".sellerNotes", ".seller_notes",
    ".vehicle-overview", ".vehicleOverview", ".vehicle_overview",
    ".about-vehicle", ".aboutVehicle",
    "[class*='description'][class*='vehicle']",
    "[class*='dealer'][class*='comment']",
    "[class*='seller'][class*='note']",
    "#vehicle-description", "#vehicleDescription",
    ".listing-description", ".listingDescription",
    ".vdp-description", ".vdpDescription",
    "[data-test='vehicle-description']",
    "[data-testid='description']",
    # AutoTrader / Cars.com / CDK / DealerSocket common patterns
    ".atcui-vdp-about", ".vehicle-details-description", ".vdp-section-content",
    "[class*='sellerNotes']", "[class*='SellerNotes']",
    ".dt-dealer-description", ".dt-description",
]

BOILERPLATE_RE = re.compile(
    r"(call us|contact us|schedule a test drive|disclaimer|financing available|visit our|"
    r"taxes and fees|subject to change|msrp does not include|^[\s\*]+$)",
    re.IGNORECASE,
)


def _clean_description(raw: str) -> str:
    if not raw:
        return ""
    lines = [ln.strip() for ln in raw.replace("\r\n", "\n").split("\n")]
    cleaned = [ln for ln in lines if ln and not BOILERPLATE_RE.search(ln)]
    return " ".join(cleaned)[:6000]


async def _fetch_description_for_url(page, url: str) -> str:
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(1.5)

        desc = await page.evaluate("""
            (selectors) => {
                for (const sel of selectors) {
                    try {
                        const el = document.querySelector(sel);
                        if (el) {
                            const t = (el.innerText || el.textContent || '').trim();
                            if (t.length > 40) return t.slice(0, 6000);
                        }
                    } catch(e) {}
                }
                return '';
            }
        """, DESCRIPTION_SELECTORS)
        return _clean_description(desc or "")
    except Exception as e:
        logger.warning("Failed to fetch %s: %s", url, str(e)[:100])
        return ""


async def _run_batch(cars: list[dict], workers: int, dry_run: bool):
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        logger.error("Playwright not installed. Run: pip install playwright && playwright install chromium")
        return {}

    results: dict[str, str] = {}  # vin → description

    sem = asyncio.Semaphore(workers)

    async def fetch_one(car: dict, browser):
        vin = car["vin"]
        url = car["source_url"]
        async with sem:
            page = await browser.new_page()
            try:
                desc = await _fetch_description_for_url(page, url)
                results[vin] = desc
                status = f"{len(desc)} chars" if desc else "no description found"
                logger.info("[%s] %s %s %s → %s", vin, car["year"], car["make"], car["model"], status)
            finally:
                await page.close()

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        tasks = [fetch_one(car, browser) for car in cars]
        await asyncio.gather(*tasks)
        await browser.close()

    return results


def _apply_descriptions_and_parse(descriptions: dict[str, str], conn: Any, dry_run: bool):
    from backend.utils.listing_description_persist import process_listing_description_for_row

    conn.row_factory = sqlite3.Row
    applied = 0
    packages_found = 0

    for vin, desc in descriptions.items():
        if not desc:
            continue

        # Update description in DB first
        if not dry_run:
            conn.execute("UPDATE cars SET description=? WHERE vin=?", (desc, vin))
            conn.commit()

        # Fetch the full row to run package extraction
        row = conn.execute("SELECT * FROM cars WHERE vin=?", (vin,)).fetchone()
        if not row:
            continue
        row_dict = dict(row)
        row_dict["description"] = desc

        result = process_listing_description_for_row(row_dict, skip_if_unchanged=False, force=True)
        if result.get("applied") and result.get("updates"):
            updates = result["updates"]
            if not dry_run:
                set_clause = ", ".join(f"{k}=?" for k in updates)
                vals = list(updates.values()) + [vin]
                conn.execute(f"UPDATE cars SET {set_clause} WHERE vin=?", vals)
                conn.commit()
            applied += 1

            # Log what packages were found
            import json
            pkgs_json = updates.get("packages", "{}")
            try:
                pkgs = json.loads(pkgs_json) if isinstance(pkgs_json, str) else pkgs_json
                norm = pkgs.get("packages_normalized") or []
                feats = pkgs.get("standalone_features_from_description") or []
                if norm or feats:
                    packages_found += 1
                    pkg_names = [p.get("name") or p.get("canonical_name") for p in norm if p.get("name")]
                    logger.info("  → packages: %s | features: %d", pkg_names[:5], len(feats))
            except Exception:
                pass

    logger.info("Applied package parse to %d/%d cars with descriptions (%d had packages)", applied, len(descriptions), packages_found)


def main():
    ap = argparse.ArgumentParser(description="Re-fetch listing descriptions and extract packages")
    ap.add_argument("--all", action="store_true", help="Re-fetch even if description already set")
    ap.add_argument("--limit", type=int, default=0, help="Max cars to process (0=all)")
    ap.add_argument("--workers", type=int, default=3, help="Concurrent browser tabs")
    ap.add_argument("--dry-run", action="store_true", help="Fetch and parse but don't write to DB")
    ap.add_argument("--vin", help="Process a single VIN")
    args = ap.parse_args()

    from backend.db.inventory_db import get_conn

    conn = get_conn()
    conn.row_factory = sqlite3.Row

    if args.vin:
        cars = [dict(r) for r in conn.execute(
            "SELECT vin, year, make, model, source_url FROM cars WHERE vin=? AND source_url IS NOT NULL", (args.vin,)
        ).fetchall()]
    elif args.all:
        cars = [dict(r) for r in conn.execute(
            "SELECT vin, year, make, model, source_url FROM cars WHERE source_url IS NOT NULL ORDER BY id"
        ).fetchall()]
    else:
        cars = [dict(r) for r in conn.execute(
            "SELECT vin, year, make, model, source_url FROM cars "
            "WHERE source_url IS NOT NULL AND (description IS NULL OR description='') ORDER BY id"
        ).fetchall()]

    if args.limit > 0:
        cars = cars[:args.limit]

    if not cars:
        logger.info("No cars to process")
        return

    logger.info("Cars to process: %d (workers=%d)", len(cars), args.workers)
    if args.dry_run:
        logger.info("DRY-RUN mode")

    descriptions = asyncio.run(_run_batch(cars, args.workers, args.dry_run))

    found = sum(1 for v in descriptions.values() if v)
    logger.info("Fetched %d/%d descriptions", found, len(cars))

    if descriptions:
        _apply_descriptions_and_parse(descriptions, conn, args.dry_run)

    conn.close()
    logger.info("Done. Next: python rebuild_listings_index.py --fast")


if __name__ == "__main__":
    main()
