"""
Scanner orchestration: browser pool, dealer concurrency, optional post-scan pipeline.

Inventory capture lives in ``backend.scanner.phases.dealer_run``; post-scan enrichment can run
inline or via ``backend.scanner.post_scan_job`` when ``scan_only=True``.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import random
import signal
import time
from typing import Any

from backend.scanner.bmw_enhancer import enhance_scraping_for_bmw_dealerships
from backend.scanner.constants import DEBUG_DIR, MANIFEST_PATH
from backend.scanner.manifest import filter_oem_manufacturers, load_manifest
from backend.scanner.inventory_write import InventoryWriteCoordinator, default_max_dealer_concurrency
from backend.scanner.phases.dealer_run import run_dealer
from backend.scanner.post_pipeline import (
    aggregate_vins_from_dealer_results,
    post_dealer_google_ratings_env_enabled,
    post_dict_enrich_env_enabled,
    post_enrich_env_enabled,
    post_enrich_vision_env_enabled,
    post_gas_prices_env_enabled,
    post_interior_vision_env_enabled,
    post_listing_description_env_enabled,
    post_listing_gap_fill_env_enabled,
    post_repair_env_enabled,
    post_window_sticker_env_enabled,
    run_dealer_google_rating_backfill_if_due,
    run_dictionary_enrich_for_vins,
    run_listing_gap_fill_stage,
    run_post_scan,
    run_sync_gas_prices,
)
from backend.scanner.scan_efficiency import (
    apply_fast_mode_env_defaults,
    apply_scan_only_env_defaults,
    scanner_fast_mode_enabled,
    scanner_scan_only_enabled,
)

logger = logging.getLogger("scanner")

_scanner_shutdown_requested = False


def on_scanner_shutdown_signal() -> None:
    global _scanner_shutdown_requested
    if _scanner_shutdown_requested:
        return
    _scanner_shutdown_requested = True
    logger.info(
        "Shutdown signal received; current dealer work may finish, then remaining dealers are skipped."
    )


def _max_vdp_concurrency() -> int:
    from backend.scanner.vdp import _max_vdp_concurrency as vdp_conc

    return vdp_conc()


async def _run_post_scan_tail(
    outcomes: list[Any],
    *,
    post_repair: bool,
    post_listing_description: bool,
    post_interior_vision: bool,
    post_enrich: bool,
    post_enrich_vision_only: bool,
    post_listing_gap_fill: bool,
    post_window_sticker: bool,
    post_gas_prices: bool,
    post_dealer_ratings: bool,
    post_gallery_vision: bool,
    enrichment_max_workers: int | None,
) -> None:
    scanned_vins = aggregate_vins_from_dealer_results(outcomes)
    try:
        from datetime import datetime, timezone

        from backend.db.inventory_db import record_scan_outcomes

        record_scan_outcomes(outcomes, finished_at=datetime.now(timezone.utc).isoformat())
    except Exception:
        logger.exception("record_scan_outcomes failed (inventory.db scan_runs)")

    if (
        post_repair
        or post_listing_description
        or post_interior_vision
        or post_enrich
        or post_enrich_vision_only
        or post_window_sticker
        or post_gallery_vision
    ):
        try:
            post_summary = run_post_scan(
                scanned_vins,
                post_repair=post_repair,
                post_listing_description=post_listing_description,
                post_interior_vision=post_interior_vision,
                post_enrich=post_enrich,
                post_enrich_vision_only=post_enrich_vision_only,
                post_window_sticker=post_window_sticker,
                post_gallery_vision=post_gallery_vision,
                enrichment_max_workers=enrichment_max_workers,
            )
            logger.info("Post-scan summary: %s", json.dumps(post_summary, default=str)[:1800])
        except Exception:
            logger.exception("Post-scan pipeline failed (inventory already saved)")

    if post_listing_gap_fill and scanned_vins:
        try:
            gap_summary = await asyncio.to_thread(run_listing_gap_fill_stage, scanned_vins)
            logger.info(
                "Listing gap fill: %s",
                json.dumps(gap_summary, default=str)[:1600],
            )
        except Exception:
            logger.exception("Listing gap fill failed (inventory already saved)")

    try:
        from backend.scanner.database import apply_model_specs_corrections

        corrected = await asyncio.to_thread(apply_model_specs_corrections)
        if corrected:
            logger.info("model_specs final pass: %d rows corrected", corrected)
    except Exception:
        logger.exception("model_specs final correction pass failed")

    if scanned_vins and post_dict_enrich_env_enabled():
        try:
            dict_stats = await asyncio.to_thread(run_dictionary_enrich_for_vins, scanned_vins)
            if dict_stats.get("updated"):
                logger.info(
                    "EPA dictionary enrichment: %d/%d VINs updated",
                    dict_stats["updated"],
                    dict_stats["vins"],
                )
        except Exception:
            logger.exception("EPA dictionary enrichment failed (inventory already saved)")

    try:
        from backend.db.incomplete_listings_db import fast_rebuild_incomplete_listings_index

        n_incomplete = await asyncio.to_thread(fast_rebuild_incomplete_listings_index)
        logger.info("incomplete_listings resynced: %d incomplete listings", n_incomplete)
    except Exception:
        logger.exception("incomplete_listings resync failed")

    if post_gas_prices:
        try:
            gas_summary = await asyncio.to_thread(run_sync_gas_prices)
            logger.info("Post-scan gas prices: %s", json.dumps(gas_summary, default=str)[:800])
        except Exception:
            logger.exception("AAA gas price sync failed (inventory already saved)")

    try:
        rating_summary = await asyncio.to_thread(
            run_dealer_google_rating_backfill_if_due,
            enabled=post_dealer_ratings,
        )
        logger.info("Post-scan dealer ratings: %s", json.dumps(rating_summary, default=str)[:800])
    except Exception:
        logger.exception("Dealer Google rating batch step failed (inventory already saved)")


async def main(
    dealers: list | None = None,
    *,
    scan_only: bool = False,
    post_repair: bool = True,
    post_listing_description: bool = True,
    post_interior_vision: bool = True,
    post_enrich: bool = False,
    post_enrich_vision_only: bool = False,
    post_listing_gap_fill: bool = False,
    post_window_sticker: bool = True,
    post_gas_prices: bool = True,
    post_dealer_ratings: bool = True,
    post_gallery_vision: bool = False,
    enrichment_max_workers: int | None = None,
    gallery_vision_filter: bool = False,
    monroney_vision: bool = True,
) -> None:
    if dealers is None:
        dealers = load_manifest()
    apply_fast_mode_env_defaults()
    apply_scan_only_env_defaults()
    if scanner_fast_mode_enabled():
        logger.info("Scanner: SCANNER_FAST_MODE=1 (core inventory paths, reduced VDP defaults)")
    if scan_only or scanner_scan_only_enabled():
        scan_only = True
        logger.info("Scanner: scan-only mode — inventory capture only; run post_scan.py for enrichment")
    logger.info("Loading manifest: %s (resolved %s)", MANIFEST_PATH, MANIFEST_PATH.resolve())

    original_count = len(dealers) if dealers else 0
    dealers = filter_oem_manufacturers(dealers or [])
    oem_filtered = original_count - len(dealers)
    if oem_filtered > 0:
        logger.info("Filtered out %d OEM manufacturer URL(s)", oem_filtered)

    if not dealers:
        logger.error("No dealers to scan (manifest empty or filter matched nothing).")
        return
    logger.info("Found %d dealer(s) to run", len(dealers))

    dealer_conc = default_max_dealer_concurrency()
    vdp_conc = _max_vdp_concurrency()
    write_coordinator = InventoryWriteCoordinator()
    logger.info("Scanner: dealer concurrency = %d", dealer_conc)
    logger.info("Scanner: VDP concurrency = %d", vdp_conc)

    bmw_enhanced_dealers = enhance_scraping_for_bmw_dealerships(dealers)
    scan_t0 = time.perf_counter()
    total_upserted = 0
    sem = asyncio.Semaphore(dealer_conc)
    outcomes: list[Any] = []

    async def run_dealers_with_browser(p) -> list[Any]:
        browser = await p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"],
        )

        async def one_dealer(dealer: dict) -> dict[str, Any]:
            did = dealer.get("dealer_id", "")
            if dealer.get("optimize_for") == "bmw":
                logger.info("Applying BMW-specific optimization for %s", dealer.get("name"))
            try:
                return await run_dealer(
                    browser,
                    dealer,
                    write_coordinator,
                    gallery_vision_filter=gallery_vision_filter,
                    monroney_vision=monroney_vision,
                )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.exception("Dealer %s failed: %s", did, e)
                DEBUG_DIR.mkdir(parents=True, exist_ok=True)
                try:
                    pg = await browser.new_page()
                    try:
                        await pg.goto(dealer.get("url", "about:blank"), timeout=10000)
                        await pg.screenshot(path=str(DEBUG_DIR / f"fail_{did or 'unknown'}.png"))
                    finally:
                        await pg.close()
                except Exception:
                    pass
                return {
                    "dealer_id": did,
                    "dealer_name": dealer.get("name", ""),
                    "upserted": 0,
                    "error": str(e),
                }

        async def bounded(dealer: dict) -> dict[str, Any]:
            if _scanner_shutdown_requested:
                return {
                    "dealer_id": dealer.get("dealer_id", ""),
                    "dealer_name": dealer.get("name", ""),
                    "upserted": 0,
                    "error": "shutdown_requested",
                }
            async with sem:
                await asyncio.sleep(random.uniform(0.5, 2.5))
                return await one_dealer(dealer)

        try:
            loop = asyncio.get_running_loop()
            for _sig in (signal.SIGTERM,):
                with contextlib.suppress(Exception):
                    loop.add_signal_handler(_sig, on_scanner_shutdown_signal)
            return await asyncio.gather(
                *[bounded(d) for d in bmw_enhanced_dealers],
                return_exceptions=True,
            )
        finally:
            with contextlib.suppress(Exception):
                await asyncio.shield(asyncio.wait_for(browser.close(), timeout=6.0))

    try:
        from playwright_stealth import Stealth
        from playwright.async_api import async_playwright

        async with Stealth().use_async(async_playwright()) as p:
            outcomes = await run_dealers_with_browser(p)
    except ImportError:
        logger.warning("playwright_stealth not found, using plain playwright")
        from playwright.async_api import async_playwright

        async with async_playwright() as p:
            outcomes = await run_dealers_with_browser(p)

    for o in outcomes:
        if isinstance(o, BaseException):
            logger.error("Dealer task ended with exception: %s", o)
            continue
        if isinstance(o, dict):
            total_upserted += int(o.get("upserted") or 0)

    elapsed = time.perf_counter() - scan_t0
    logger.info(
        "Scanner finished — total runtime %.1fs, dealer_concurrency=%d, vdp_concurrency=%d, total vehicles upserted=%d",
        elapsed,
        dealer_conc,
        vdp_conc,
        total_upserted,
    )

    if scan_only:
        try:
            from datetime import datetime, timezone

            from backend.db.inventory_db import record_scan_outcomes

            record_scan_outcomes(outcomes, finished_at=datetime.now(timezone.utc).isoformat())
        except Exception:
            logger.exception("record_scan_outcomes failed (inventory.db scan_runs)")
        return

    await _run_post_scan_tail(
        outcomes,
        post_repair=post_repair,
        post_listing_description=post_listing_description,
        post_interior_vision=post_interior_vision,
        post_enrich=post_enrich,
        post_enrich_vision_only=post_enrich_vision_only,
        post_listing_gap_fill=post_listing_gap_fill,
        post_window_sticker=post_window_sticker,
        post_gas_prices=post_gas_prices,
        post_dealer_ratings=post_dealer_ratings,
        post_gallery_vision=post_gallery_vision,
        enrichment_max_workers=enrichment_max_workers,
    )
