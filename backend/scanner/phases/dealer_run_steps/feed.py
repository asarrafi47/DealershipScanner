"""Feed fetch: replay this dealer's stored recipes over plain HTTP."""
from __future__ import annotations

import asyncio
import logging
from typing import Any

from backend.scanner.phases.dealer_run_steps.session import _http_only
from backend.scanner.phases.dealer_run_steps.state import DealerRun

logger = logging.getLogger("scanner")


async def fetch_recipe_feed(run: DealerRun) -> None:
    """Recipe replay + coverage verdict + provider hint. The replayed pages are
    appended to ``run.intercept_records`` — the inventory feed of the scan.

    The yield is called "full" only when it is near the dealer's last-known lot
    size — a recipe saved from one listing config can be type-filtered and cover
    only part of the inventory; a "partial" yield is still used (VIN dedup
    downstream)."""
    result, name, dealer_id = run.result, run.name, run.dealer_id
    recipe_records: list[tuple[str, Any]] | None = None
    try:
        from backend.scanner.recipes import (
            last_known_vin_count,
            recipe_yield_replaces_browser,
            try_fetch_via_recipes,
        )

        _recipe_cov: dict[str, float] = {}
        # union=True: a dealer's recipes are per section (new / used / CPO).
        # First-hit replay returned Jordan Ford's used feed alone (160 VINs),
        # cleared 70% of a stale 176-row lot and skipped the browser without
        # ever pulling the 623-row new feed (pilot, 2026-09-22).
        _recipe_hit = await try_fetch_via_recipes(
            dealer_id, run.provider, run.url, name, union=True, coverage_out=_recipe_cov
        )
        if _recipe_hit:
            recipe_records, _recipe_vins = _recipe_hit
            if _http_only():
                # load_recipes reads the cache file and syncs with dealer_recipes.
                await asyncio.to_thread(_apply_recipe_provider_hint, run)
            _known = await asyncio.to_thread(last_known_vin_count, dealer_id)
            result["recipe_coverage"] = {
                k: round(float(v), 3) for k, v in _recipe_cov.items() if k != "n"
            }
            _ok, _why = recipe_yield_replaces_browser(_recipe_vins, _known, _recipe_cov)
            if _ok:
                result["recipe_fetch"] = "full"
                logger.info(
                    "Recipe fetch [%s]: %d/%d known VIN(s), price %.0f%% trim %.0f%% colour %.0f%% "
                    "— skipping browser inventory",
                    name, _recipe_vins, _known,
                    100 * _recipe_cov.get("price", 0.0), 100 * _recipe_cov.get("trim", 0.0),
                    100 * _recipe_cov.get("exterior_color", 0.0),
                )
            else:
                # A thin replay (VINs but no price/trim/colour) must never stand
                # in for the SRP scrape; merge what it found and scrape anyway.
                result["recipe_fetch"] = "partial"
                logger.info(
                    "Recipe fetch [%s]: %d VIN(s) vs %d known — %s; merging and still scraping",
                    name, _recipe_vins, _known, _why,
                )
    except Exception as _rec_e:
        logger.debug("Recipe fetch skipped [%s]: %s", name, str(_rec_e)[:200])

    if _http_only():
        result["recipe_fetch"] = f"{result.get('recipe_fetch') or 'none'}+http_only"
        if not recipe_records:
            logger.info("HTTP-only [%s]: NO RECIPE HIT: zero rows this run", name)
    # No browser SRP scrape: the recipe replay above is the inventory feed.
    if recipe_records:
        run.intercept_records.extend(recipe_records)


def _apply_recipe_provider_hint(run: DealerRun) -> None:
    """No site profile ran; the recipe's own provider hint is the truth.

    Blocking store I/O (load_recipes): ``fetch_recipe_feed`` runs it with
    ``asyncio.to_thread``."""
    try:
        from backend.scanner.recipes import load_recipes as _lr

        _hints = [r.provider_hint for r in _lr(run.dealer_id) if not r.stale and r.provider_hint]
        if _hints:
            run.result["provider"] = _hints[0]
    except Exception:
        pass
