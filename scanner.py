#!/usr/bin/env python3
"""
Manifest-driven dealership inventory scanner. Uses playwright + stealth,
session warmup to avoid 403, network interception for JSON, and HTML fallback.
Run from project root: python scanner.py
"""
import argparse
import asyncio
import json
import logging
import os
import sys
from pathlib import Path

# Project root = directory containing this file
ROOT = Path(__file__).resolve().parent
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))

# Import BMW enhancement
from bmw_enhancer import bmw_optimized_browsing, is_bmw_dealership, enhance_scraping_for_bmw_dealerships

from backend.database import upsert_vehicles
from backend.parsers import parse
from backend.parsers.base import find_vehicle_list, get_total_count

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("scanner")

MANIFEST_PATH = ROOT / "dealers.json"
DEBUG_DIR = ROOT / "debug"
# Exhaustive category search: all three for every dealer
INVENTORY_PATHS = [
    "/new-inventory/index.htm",
    "/used-inventory/index.htm",
    "/certified-inventory/index.htm",
]
NEXT_SELECTORS = [
    'button:has-text("Next")',
    'button:has-text("Load More")',
    'a:has-text("Next")',
    '[data-action="next"]',
    '.pagination-next',
    '.load-more',
    'a:has-text("Load More")',
]
MAX_PAGINATION_CLICKS = 15


def load_manifest():
    with open(MANIFEST_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def filter_manifest_by_dealer_id(dealers: list, dealer_id: str) -> list:
    """Return rows whose dealer_id matches (exact string after strip)."""
    want = (dealer_id or "").strip()
    if not want:
        return []
    return [d for d in dealers if (d.get("dealer_id") or "").strip() == want]


def _is_valid_vehicle_list(body) -> bool:
    """True if JSON contains a list with at least 3 dicts that have vin/VIN (same as parser logic)."""
    return find_vehicle_list(body) is not None


def _is_playwright_shutdown_error(exc: BaseException) -> bool:
    """True when the browser was closed mid-operation (e.g. Ctrl+C / task cancellation)."""
    name = type(exc).__name__
    msg = str(exc).lower()
    if name == "TargetClosedError":
        return True
    if "targetclosed" in name.lower():
        return True
    if "target page, context or browser has been closed" in msg:
        return True
    if "browser has been closed" in msg:
        return True
    if "context or browser has been closed" in msg:
        return True
    return False


async def _safe_close_playwright(context, browser) -> None:
    if context is not None:
        try:
            await context.close()
        except BaseException:
            pass
    if browser is not None:
        try:
            await browser.close()
        except BaseException:
            pass


async def run_dealer(playwright, dealer: dict, browser_type="chromium"):
    name = dealer.get("name", "")
    url = dealer.get("url", "").rstrip("/")
    provider = dealer.get("provider", "dealer_dot_com")
    dealer_id = dealer.get("dealer_id", "")
    if not url or not dealer_id:
        logger.warning("Skipping dealer missing url or dealer_id: %s", dealer)
        return 0

    logger.info("Warmup: %s — navigating to base URL", name)
    launch = getattr(playwright, browser_type, playwright.chromium).launch
    browser = None
    context = None
    intercepted_json = []
    found_data = {"value": False}

    async def handle_response(response):
        try:
            ct = response.headers.get("content-type") or ""
            if "application/json" not in ct:
                return
            body = await response.json()
            if _is_valid_vehicle_list(body):
                intercepted_json.append(body)
                found_data["value"] = True
                logger.info("Intercepting: %s — got valid vehicle list (%s)", name, response.url[:80])
        except Exception:
            pass

    try:
        browser = await launch(headless=True)
        context = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        )
        page = await context.new_page()
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(4)
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight / 2)")
        await asyncio.sleep(1)
        logger.info("Warmup: %s — done (4s wait + scroll)", name)

        page.on("response", handle_response)
        # Accumulate intercepted JSON across all three category URLs
        intercepted_json.clear()

        for inv_path in INVENTORY_PATHS:
            found_data["value"] = False
            full_url = url + inv_path
            logger.info("Navigating: %s — %s", name, full_url)
            try:
                await page.goto(full_url, wait_until="domcontentloaded", timeout=20000)
                for _ in range(18):
                    await asyncio.sleep(1)
                    if found_data["value"]:
                        logger.info("Intercepting: %s — stopping wait (data found)", name)
                        break
                await asyncio.sleep(1)

                # Multi-page scrolling: trigger infinite scroll
                logger.info("Scrolling: %s — 5x scroll to bottom (2s between)", name)
                for _ in range(5):
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await asyncio.sleep(2)

                # API pagination: click Next/Load More until we have totalCount
                for _ in range(MAX_PAGINATION_CLICKS):
                    def _parsed_count():
                        by_vin = {}
                        for body in intercepted_json:
                            for v in parse(provider, body, base_url=url, dealer_id=dealer_id, dealer_name=name, dealer_url=url):
                                vin = (v.get("vin") or "").strip()
                                if vin:
                                    by_vin[vin] = v
                        return len(by_vin), by_vin

                    total_count = None
                    if intercepted_json:
                        total_count = get_total_count(intercepted_json[-1])
                    current_count, _ = _parsed_count()
                    if total_count is not None and total_count > current_count:
                        clicked = False
                        for sel in NEXT_SELECTORS:
                            try:
                                loc = page.locator(sel)
                                if await loc.count() > 0:
                                    first = loc.first
                                    if await first.is_visible():
                                        await first.click()
                                        logger.info("Pagination: %s — clicked %s", name, sel)
                                        await asyncio.sleep(3)
                                        clicked = True
                                        break
                            except Exception:
                                continue
                        if not clicked:
                            break
                    else:
                        break
            except Exception as e:
                logger.warning("Navigating: %s — %s: %s", name, full_url, e)
                continue

        # Parse all accumulated payloads and merge by VIN (upsert_vehicles dedupes).
        # Parser extracts carfax_url, history_report_url, vhr_url, and carfax_token from getInventory;
        # when vhr_url or carfax_token is present, uses partner link (vhr.carfax.com) for less iframe blocking.
        all_vehicles = []
        for body in intercepted_json:
            vehicles = parse(provider, body, base_url=url, dealer_id=dealer_id, dealer_name=name, dealer_url=url)
            for v in vehicles:
                v.setdefault("dealer_name", name)
                v.setdefault("dealer_url", url)
            all_vehicles.extend(vehicles)

        if not all_vehicles:
            logger.info("Extraction backup: %s — no JSON, trying page.content()", name)
            html = await page.content()
            all_vehicles = parse(provider, html, base_url=url, dealer_id=dealer_id, dealer_name=name, dealer_url=url)
            for v in all_vehicles:
                v.setdefault("dealer_name", name)
                v.setdefault("dealer_url", url)

        if all_vehicles:
            # Ensure gallery is always a list for DB (stored as json.dumps(gallery) in database.py)
            for v in all_vehicles:
                g = v.get("gallery")
                v["gallery"] = g if isinstance(g, list) else []
            count = upsert_vehicles(all_vehicles)
            logger.info("Parsing: %s — extracted %d vehicles (deduped by VIN), upserted %d", name, len(all_vehicles), count)
            return count
        # No path returned vehicles
        logger.warning("Parsing: %s — no vehicles from any inventory path", name)
        DEBUG_DIR.mkdir(parents=True, exist_ok=True)
        screenshot_path = DEBUG_DIR / f"fail_{dealer_id}.png"
        await page.screenshot(path=str(screenshot_path))
        logger.info("Debug: saved screenshot to %s", screenshot_path)
        return 0
    except asyncio.CancelledError:
        raise
    except Exception as e:
        if _is_playwright_shutdown_error(e):
            return 0
        raise
    finally:
        await _safe_close_playwright(context, browser)


async def main(dealers: list | None = None):
    if dealers is None:
        dealers = load_manifest()
    logger.info("Loading manifest: %s", MANIFEST_PATH)
    if not dealers:
        logger.error("No dealers to scan (manifest empty or filter matched nothing).")
        return
    logger.info("Found %d dealer(s) to run", len(dealers))

    # Enhance BMW dealerships with special optimization
    bmw_enhanced_dealers = enhance_scraping_for_bmw_dealerships(dealers)
    
    try:
        from playwright_stealth import Stealth
        from playwright.async_api import async_playwright
        async with Stealth().use_async(async_playwright()) as p:
            for dealer in bmw_enhanced_dealers:
                try:
                    # Apply BMW-specific optimization if needed
                    if dealer.get('optimize_for') == 'bmw':
                        logger.info("Applying BMW-specific optimization for %s", dealer.get('name'))
                        # This would be where we would call the BMW-specific enhanced browsing
                        pass
                    await run_dealer(p, dealer)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.exception("Dealer %s failed: %s", dealer.get("dealer_id"), e)
                    DEBUG_DIR.mkdir(parents=True, exist_ok=True)
                    try:
                        from playwright.async_api import async_playwright
                        async with async_playwright() as pw:
                            browser = await pw.chromium.launch(headless=True)
                            page = await browser.new_page()
                            await page.goto(dealer.get("url", "about:blank"), timeout=10000)
                            await page.screenshot(path=str(DEBUG_DIR / f"fail_{dealer.get('dealer_id', 'unknown')}.png"))
                            await browser.close()
                    except Exception:
                        pass
    except ImportError:
        logger.warning("playwright_stealth not found, using plain playwright")
        from playwright.async_api import async_playwright
        async with async_playwright() as p:
            for dealer in bmw_enhanced_dealers:
                try:
                    await run_dealer(p, dealer)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.exception("Dealer %s failed: %s", dealer.get("dealer_id"), e)
                    DEBUG_DIR.mkdir(parents=True, exist_ok=True)
                    try:
                        async with async_playwright() as pw:
                            browser = await pw.chromium.launch(headless=True)
                            page = await browser.new_page()
                            await page.goto(dealer.get("url", "about:blank"), timeout=10000)
                            await page.screenshot(path=str(DEBUG_DIR / f"fail_{dealer.get('dealer_id', 'unknown')}.png"))
                            await browser.close()
                    except Exception:
                        pass

    logger.info("Scanner finished.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description="Manifest-driven dealership inventory scanner (Playwright + stealth)."
    )
    ap.add_argument(
        "--dealer-id",
        metavar="ID",
        default=None,
        help="Scan only this dealer_id from dealers.json (e.g. from the /dev console).",
    )
    args = ap.parse_args()
    to_run = load_manifest()
    if args.dealer_id:
        to_run = filter_manifest_by_dealer_id(to_run, args.dealer_id)
        if not to_run:
            logger.error(
                "No dealer with dealer_id %r in %s — save the dealer in /dev first.",
                args.dealer_id.strip(),
                MANIFEST_PATH,
            )
            sys.exit(1)

    try:
        asyncio.run(main(to_run))
    except KeyboardInterrupt:
        logger.info("Scanner gracefully stopped by user.")
        sys.exit(0)
    except asyncio.CancelledError:
        logger.info("Scanner gracefully stopped by user.")
        sys.exit(0)
