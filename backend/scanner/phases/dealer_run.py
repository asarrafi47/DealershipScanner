"""Per-dealer scan: recipe feed, attribution, recovery, enrichment, upsert, reconcile.

``run_dealer`` is a short orchestrator over the named steps in
``backend/scanner/phases/dealer_run_steps/`` (one module per stage).
"""
from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

from backend.scanner.inventory_write import InventoryWriteCoordinator
from backend.scanner.phases.dealer_run_steps import (
    after_write as _after_write,
    attribution as _attribution,
    enrich as _enrich,
    feed as _feed,
    recovery as _recovery,
)
from backend.scanner.phases.dealer_run_steps.enrich import (  # noqa: F401 - re-exported
    _CAPTURE_FIELDS,
    _capture_coverage,
    apply_monroney_vision_to_vehicles,
    log_gallery_bins,
)
from backend.scanner.phases.dealer_run_steps.persist import persist as _persist
from backend.scanner.phases.dealer_run_steps.session import _http_only, open_session  # noqa: F401 - _http_only re-exported
from backend.scanner.phases.dealer_run_steps.state import start_run
from backend.scanner.phases.dealer_run_steps.summary import emit_dealer_run_summary, log_dealer_complete
from backend.scanner.phases.nav import is_playwright_shutdown_error, safe_close_context

# One shared rooftop reconcile policy for the full scan and the delta path
# (re-exported: tests assert both paths hold the same objects).
from backend.scanner.rooftop_disown import (  # noqa: F401 - re-exported
    disown_foreign_rooftop_vins,
    split_refusals,
)

logger = logging.getLogger("scanner")


def _warmup_phase_timeout_sec() -> float:
    """Hard cap on the whole warmup phase (goto → settle → cookie banner); 0 disables."""
    raw = (os.environ.get("SCANNER_WARMUP_PHASE_TIMEOUT_SEC") or "240").strip()
    try:
        val = float(raw)
    except ValueError:
        return 240.0
    return val if val > 0 else 86400.0


def _vdp_phase_timeout_sec(n_vehicles: int) -> float:
    """
    Hard cap on the whole VDP enrichment phase, so a wedged renderer can't block the
    dealer's inventory upsert. Scales with lot size (VDP visits N pages) around a base
    budget; ``SCANNER_VDP_PHASE_TIMEOUT_SEC`` overrides the base, 0 disables.
    """
    raw = (os.environ.get("SCANNER_VDP_PHASE_TIMEOUT_SEC") or "").strip()
    if raw:
        try:
            v = float(raw)
            return v if v > 0 else 86400.0
        except ValueError:
            pass
    # ~4s/vehicle budget over a 300s floor, capped at 2h — generous for real VDP,
    # far below the 3h dealer timeout so a wedge is caught at the phase, not the dealer.
    return max(300.0, min(7200.0, 300.0 + 4.0 * max(0, n_vehicles)))


async def run_dealer(
    browser: Any,
    dealer: dict,
    write_coordinator: InventoryWriteCoordinator,
    *,
    gallery_vision_filter: bool = True,
    monroney_vision: bool = True,
) -> dict[str, Any]:
    run = start_run(dealer, write_coordinator)
    result, name = run.result, run.name
    if not run.url or not run.dealer_id:
        logger.warning("Skipping dealer missing url or dealer_id: %s", dealer)
        result["seconds"] = run.elapsed()
        emit_dealer_run_summary(result)
        return result

    logger.info("Dealer start: %s", name)
    logger.info("Warmup: %s — navigating to base URL", name)
    try:
        await open_session(run, browser)
        await _feed.fetch_recipe_feed(run)

        # Parse the feed (per-page rooftop gate), recover, then the all-rows pass.
        await _attribution.lookup_store_place(run)
        parse_raw = _attribution.inventory_parser(run)
        parsed = _attribution.parse_intercepts(run, parse_raw)
        feed_rows = parsed + run.rooftop_refused
        result["inventory_rows"] = len(feed_rows)
        feed_sufficient = _recovery.feed_is_sufficient(run, feed_rows)
        recovery = await _recovery.recover(run, feed_rows, parse_raw)
        _attribution.settle_attribution(run, recovery.vehicles)
        _recovery.note_recovery(run, recovery, feed_sufficient)

        if not run.all_vehicles:
            logger.warning("Parsing: %s — no vehicles from any inventory path", name)
            result["seconds"] = run.elapsed()
            log_dealer_complete(name, result)
            return result

        _enrich.dedupe_by_vin(run)
        _enrich.filter_sister_stores(run)
        await _enrich.prefetch_details(run)
        _enrich.drop_detail_page_sister_rows(run)
        _enrich.normalize_galleries(run)
        await _enrich.run_gallery_vision(run, gallery_vision_filter)
        await _enrich.run_monroney_vision(run, monroney_vision)
        _enrich.stamp_registry_and_source_urls(run)
        _enrich.apply_vin_facts(run)
        await _persist(run)
        await _after_write.after_write(run)

        result["seconds"] = run.elapsed()
        logger.info(
            "Parsing: %s — extracted %d vehicles (deduped by VIN), upserted %d",
            name,
            len(run.all_vehicles),
            result["upserted"],
        )
        log_dealer_complete(name, result)
        return result
    except asyncio.CancelledError:
        raise
    except Exception as e:
        result["error"] = str(e)
        result["seconds"] = run.elapsed()
        if is_playwright_shutdown_error(e):
            return result
        logger.exception("Dealer %s failed: %s", run.dealer_id, e)
        log_dealer_complete(name, result, " [error]")
        return result
    finally:
        result["intercept_count"] = len(run.intercept_records)
        # URL-gate denials were counted by the browser intercept, gone with the
        # scan-time browser stack; the summary key stays (always 0).
        result["filtered_count"] = 0
        if result.get("gallery_bins") is None:
            result["gallery_bins"] = {}
        result["seconds"] = run.elapsed()
        emit_dealer_run_summary(result)
        await safe_close_context(run.context)

__all__ = ['run_dealer', 'apply_monroney_vision_to_vehicles', 'emit_dealer_run_summary']
