"""Per-dealer scan: warmup, inventory, recovery, VDP, upsert."""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from typing import Any

from backend.parsers import parse
from backend.parsers.vdp_urls import apply_vehicle_source_url
from backend.scanner.constants import DEBUG_DIR, KNOWN_HAR_PROVIDERS
from backend.scanner.dealer_site_url import dealer_inventory_base_url
from backend.scanner.inventory_write import InventoryWriteCoordinator
from backend.scanner.phases.upsert import upsert_vehicles_for_dealer
from backend.scanner.phases.inventory_scrape import scrape_inventory_path
from backend.scanner.phases.nav import (
    capture_scanner_failure_har,
    get_rotating_ua,
    goto_with_retries,
    is_playwright_shutdown_error,
    playwright_inventory_json_predicate,
    safe_close_context,
    warmup_delays,
    warmup_settle_after_base_goto,
    inventory_wait_ms,
    pagination_response_wait_ms,
)
from backend.scanner.post_pipeline import (
    apply_gallery_vision_filter_to_vehicles,
    gallery_vision_filter_env_enabled,
)
from backend.scanner.scan_efficiency import (
    effective_vdp_ep_max,
    effective_vdp_price_max,
    gallery_vision_inline_enabled,
    inventory_paths_for_dealer,
    intercept_feed_is_sufficient,
)
from backend.scanner.scrapers.inventory_vin_merge import merge_inventory_rows_same_vin
from backend.scanner.vdp import enrich_vehicles_vdp
from backend.utils.gallery_merge import gallery_https_bin_histogram

logger = logging.getLogger("scanner")


def log_gallery_bins(dealer_name: str, phase: str, vehicles: list[dict[str, Any]]) -> None:
    bins = gallery_https_bin_histogram(vehicles)
    if bins:
        logger.info("Gallery bins [%s] %s: %s", dealer_name, phase, bins)

def apply_monroney_vision_to_vehicles(vehicles: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Monroney/window-sticker vision is disabled. Pops ``_monroney_page_texts`` from each vehicle
    to avoid leaking internal keys downstream, then returns a no-op stats dict.
    """
    logger.info("Monroney vision disabled — skipping sticker image parsing")
    for v in vehicles:
        v.pop("_monroney_page_texts", None)
    return {"skipped": "monroney_vision_removed"}


def emit_dealer_run_summary(result: dict[str, Any]) -> None:
    """One parseable INFO line per dealer (JSON), capped ~2KB for log pipelines."""
    payload: dict[str, Any] = {
        "dealer_id": result.get("dealer_id"),
        "intercept_count": result.get("intercept_count"),
        "filtered_count": result.get("filtered_count"),
        "inventory_rows": result.get("inventory_rows"),
        "deduped_rows": result.get("deduped_rows"),
        "vdps_visited": result.get("vdps_visited"),
        "gallery_bins": result.get("gallery_bins"),
        "gallery_vision": result.get("gallery_vision"),
        "monroney_vision": result.get("monroney_vision"),
        "seconds": round(float(result.get("seconds") or 0.0), 2),
        "upserted": result.get("upserted"),
    }
    err = result.get("error")
    if err:
        payload["error"] = str(err)[:400]
    rec = result.get("reconcile")
    if isinstance(rec, dict):
        payload["reconcile"] = {
            "ran": rec.get("ran"),
            "scraped_candidates": rec.get("scraped_candidates"),
            "marked_inactive": rec.get("marked_inactive"),
            "skipped_reason": rec.get("skipped_reason"),
        }
    phases = result.get("phase_secs")
    if isinstance(phases, dict) and phases:
        payload["phase_secs"] = phases
    line = json.dumps(payload, separators=(",", ":"), default=str, ensure_ascii=False)
    if len(line) > 2048:
        line = line[:2045] + "..."
    logger.info("dealer_run_summary %s", line)


async def run_dealer(
    browser: Any,
    dealer: dict,
    write_coordinator: InventoryWriteCoordinator,
    *,
    gallery_vision_filter: bool = True,
    monroney_vision: bool = True,
) -> dict[str, Any]:
    name = dealer.get("name", "")
    raw_manifest_url = (dealer.get("url") or "").strip()
    url, inv_doc_normalized = dealer_inventory_base_url(raw_manifest_url)
    url = url.rstrip("/")
    if inv_doc_normalized:
        logger.info(
            "Inventory base URL: %s — using site origin (manifest pointed at document: %s)",
            url,
            raw_manifest_url,
        )
    provider = dealer.get("provider", "dealer_dot_com")
    dealer_id = dealer.get("dealer_id", "")
    result: dict[str, Any] = {
        "dealer_id": dealer_id,
        "dealer_name": name,
        "upserted": 0,
        "inventory_rows": 0,
        "deduped_rows": 0,
        "vdps_visited": 0,
        "vehicles_vdp_enriched": 0,
        "gallery_vdp_urls_added": 0,
        "intercept_count": 0,
        "filtered_count": 0,
        "gallery_bins": None,
        "seconds": 0.0,
        "error": None,
        "vins": [],
        "reconcile": None,
        "gallery_vision": None,
        "monroney_vision": None,
        "phase_secs": {},
    }
    t0 = time.perf_counter()
    if not url or not dealer_id:
        logger.warning("Skipping dealer missing url or dealer_id: %s", dealer)
        result["seconds"] = time.perf_counter() - t0
        emit_dealer_run_summary(result)
        return result

    logger.info("Dealer start: %s", name)
    logger.info("Warmup: %s — navigating to base URL", name)
    context = None
    intercept_records: list[tuple[str, Any]] = []
    gate_stats = {"url_denied": 0}

    try:
        ctx_opts: dict[str, Any] = {"viewport": {"width": 1920, "height": 1080}}
        _ua = (os.environ.get("SCANNER_USER_AGENT") or "").strip() or get_rotating_ua()
        ctx_opts["user_agent"] = _ua
        context = await browser.new_context(**ctx_opts)
        page = await context.new_page()
        warm_pred = playwright_inventory_json_predicate(url)
        await goto_with_retries(page, url, log_label=f"Warmup:{name}", timeout_ms=30000)
        w_post, w_scroll = warmup_delays()
        await warmup_settle_after_base_goto(
            page,
            warm_pred,
            dealer_name=name,
            max_idle_sec=w_post,
            scroll_sec=w_scroll,
        )
        logger.info("Warmup: %s — done (signal race cap=%.1fs + scroll %.1fs)", name, w_post, w_scroll)

        inv_wait_ms = inventory_wait_ms()
        pag_wait_ms = pagination_response_wait_ms()

        # Scrape all three inventory paths in parallel — each on its own page within the
        # same browser context so session cookies from warmup are shared automatically.
        inv_paths = inventory_paths_for_dealer(dealer)
        logger.info("Inventory paths: %s — launching %d parallel scrapers", name, len(inv_paths))
        t_inv0 = time.perf_counter()
        dealer_city = str(dealer.get("city") or "").strip()
        dealer_state = str(dealer.get("state") or "").strip()
        try:
            from backend.scanner.dealer_location import build_dealer_site_profile

            _loc_prof = build_dealer_site_profile(dealer)
            dealer_city = dealer_city or _loc_prof.city
            dealer_state = dealer_state or _loc_prof.state
        except Exception:
            pass
        path_results = await asyncio.gather(
            *[
                scrape_inventory_path(
                    context,
                    path,
                    url,
                    provider,
                    dealer_id,
                    name,
                    inv_wait_ms,
                    pag_wait_ms,
                    dealer_city=dealer_city,
                    dealer_state=dealer_state,
                )
                for path in inv_paths
            ],
            return_exceptions=False,
        )
        result["phase_secs"]["inventory"] = round(time.perf_counter() - t_inv0, 2)

        # Merge results from all paths
        path_htmls: list[str | None] = []
        merged_card_locations: dict[str, str] = {}
        for path_records, path_html, path_denied, path_card_locs in path_results:
            intercept_records.extend(path_records)
            gate_stats["url_denied"] += path_denied
            path_htmls.append(path_html)
            if isinstance(path_card_locs, dict):
                merged_card_locations.update(path_card_locs)

        body_parse_cache: dict[int, list[dict[str, Any]]] = {}

        def _vehicles_for_body(body: Any) -> list[dict[str, Any]]:
            bid = id(body)
            cached = body_parse_cache.get(bid)
            if cached is not None:
                return cached
            vehicles = list(
                parse(provider, body, base_url=url, dealer_id=dealer_id, dealer_name=name, dealer_url=url)
            )
            for v in vehicles:
                v.setdefault("dealer_name", name)
                v.setdefault("dealer_url", url)
            body_parse_cache[bid] = vehicles
            return vehicles

        # Parse all accumulated payloads and merge by VIN (upsert_vehicles dedupes).
        # Parser sets carfax_url from explicit feed links first, then vhr.carfax.com. VDP capture
        # overwrites weak links with anchor/data URLs scraped from the live detail page when better.
        all_vehicles: list[dict[str, Any]] = []
        for _resp_url, body in intercept_records:
            all_vehicles.extend(_vehicles_for_body(body))

        result["inventory_rows"] = len(all_vehicles)

        from backend.scanner.inventory_recovery import unique_vin_count as _unique_vin_count

        merged_unique = _unique_vin_count(all_vehicles)
        feed_sufficient = (
            bool(all_vehicles)
            and bool(intercept_records)
            and intercept_feed_is_sufficient(
                intercept_records, url, len(all_vehicles), unique_vin_count=merged_unique
            )
        )

        def _parse_inventory_raw(raw: Any) -> list[dict[str, Any]]:
            rows = list(
                parse(provider, raw, base_url=url, dealer_id=dealer_id, dealer_name=name, dealer_url=url)
            )
            for v in rows:
                v.setdefault("dealer_name", name)
                v.setdefault("dealer_url", url)
            return rows

        from backend.scanner.inventory_recovery import RecoveryContext, recover_inventory

        if not all_vehicles:
            logger.info(
                "Extraction backup: %s — no vehicles from %d JSON intercept(s); running recovery chain",
                name,
                len(intercept_records),
            )

        recovery = await recover_inventory(
            RecoveryContext(
                page=page,
                base_url=url,
                dealer_id=dealer_id,
                dealer_name=name,
                dealer_url=url,
                provider=provider,
                intercept_records=intercept_records,
                path_htmls=path_htmls,
                vehicles=all_vehicles,
                parse_fn=_parse_inventory_raw,
                dealer=dealer,
            )
        )
        all_vehicles = recovery.vehicles
        result["inventory_rows"] = len(all_vehicles)
        if recovery.strategies_tried:
            result["inventory_recovery"] = {
                "winning_strategy": recovery.winning_strategy,
                "strategies_tried": recovery.strategies_tried,
                "replaced": recovery.replaced,
            }
        if (
            all_vehicles
            and feed_sufficient
            and not recovery.replaced
            and not recovery.strategies_tried
        ):
            logger.info(
                "Inventory feed sufficient for %s (%d rows, %d intercepts) — skipping platform augmentations",
                name,
                len(all_vehicles),
                len(intercept_records),
            )
        elif not all_vehicles and not recovery.strategies_tried:
            logger.info(
                "Inventory recovery: %s — 0 vehicles after intercept and recovery (SPA shell or unsupported)",
                name,
            )

        if all_vehicles:
            if merged_card_locations:
                try:
                    from backend.scanner.inventory_card_location import apply_card_locations_to_vehicles

                    card_loc_applied = apply_card_locations_to_vehicles(
                        all_vehicles, merged_card_locations
                    )
                    if card_loc_applied:
                        result["inventory_card_locations"] = card_loc_applied
                        logger.info(
                            "Inventory card locations [%s]: applied %d VIN location hint(s)",
                            name,
                            card_loc_applied,
                        )
                except Exception as card_loc_e:
                    logger.debug("Inventory card location merge failed for %s: %s", name, card_loc_e)

            # One row per VIN for downstream VDP enrichment (listing payloads may repeat VINs).
            by_vin: dict[str, dict] = {}
            for v in all_vehicles:
                vin = (v.get("vin") or "").strip()
                if vin:
                    if vin in by_vin:
                        merge_inventory_rows_same_vin(by_vin[vin], v)
                    else:
                        by_vin[vin] = v
            all_vehicles = list(by_vin.values())
            result["deduped_rows"] = len(all_vehicles)
            log_gallery_bins(name, "after_inventory_merge", all_vehicles)

            site_profile = None
            try:
                from backend.scanner.dealer_location import (
                    build_dealer_site_profile,
                    filter_sister_store_vehicles,
                    sister_store_filter_enabled,
                )

                site_profile = build_dealer_site_profile(dealer)
                if sister_store_filter_enabled():
                    all_vehicles, inv_loc_stats = filter_sister_store_vehicles(
                        all_vehicles, site_profile, source="inventory"
                    )
                    result["sister_store_inventory"] = inv_loc_stats
                    result["deduped_rows"] = len(all_vehicles)
            except Exception as loc_e:
                logger.warning(
                    "Sister-store inventory filter failed for %s (continuing): %s",
                    name,
                    loc_e,
                )

            result["vins"] = sorted({(v.get("vin") or "").strip() for v in all_vehicles if (v.get("vin") or "").strip()})

            _ep_raw = (os.environ.get("SCANNER_VDP_EP_MAX") or "").strip()
            _completeness_pass = (
                os.environ.get("SCANNER_VDP_COMPLETENESS_PASS") or ""
            ).strip().lower() in ("1", "true", "yes", "on")
            if not _ep_raw or (_completeness_pass and _ep_raw == "0"):
                os.environ["SCANNER_VDP_EP_MAX"] = str(effective_vdp_ep_max(len(all_vehicles)))
                logger.info(
                    "VDP cap: %s — EP max=%s (completeness_pass=%s)",
                    name,
                    os.environ["SCANNER_VDP_EP_MAX"],
                    _completeness_pass,
                )
            if not (os.environ.get("SCANNER_VDP_PRICE_MAX") or "").strip():
                os.environ["SCANNER_VDP_PRICE_MAX"] = str(effective_vdp_price_max(len(all_vehicles)))

            vdp_stats: dict[str, Any] = {}
            t_vdp0 = time.perf_counter()
            try:
                vdp_stats = await enrich_vehicles_vdp(
                    page, all_vehicles, name, dealer_id=dealer_id, site_profile=site_profile
                )
            except Exception as e:
                logger.warning("VDP enrichment failed for %s (continuing with listing data only): %s", name, e)
            result["phase_secs"]["vdp"] = round(time.perf_counter() - t_vdp0, 2)
            result["vdps_visited"] = int(vdp_stats.get("vdps_visited") or 0)
            result["vehicles_vdp_enriched"] = int(vdp_stats.get("vehicles_enriched") or 0)
            result["gallery_vdp_urls_added"] = int(vdp_stats.get("gallery_vdp_urls_added") or 0)
            log_gallery_bins(name, "after_vdp", all_vehicles)
            result["gallery_bins"] = gallery_https_bin_histogram(all_vehicles)
            if vdp_stats.get("gallery_phase_bins"):
                logger.info("Gallery phase bins [%s]: %s", name, vdp_stats.get("gallery_phase_bins"))

            pre_vdp_n = len(all_vehicles)
            all_vehicles = [v for v in all_vehicles if not v.get("_sister_store_exclude")]
            vdp_excluded = pre_vdp_n - len(all_vehicles)
            if vdp_excluded:
                result["sister_store_vdp_excluded"] = vdp_excluded
                result["deduped_rows"] = len(all_vehicles)
                result["vins"] = sorted(
                    {(v.get("vin") or "").strip() for v in all_vehicles if (v.get("vin") or "").strip()}
                )
                logger.info(
                    "Sister-store filter [%s] VDP: excluded %d vehicle(s) after detail-page location check",
                    name,
                    vdp_excluded,
                )

            # Ensure gallery is always a list for DB (stored as json.dumps(gallery) in database.py)
            for v in all_vehicles:
                g = v.get("gallery")
                if not isinstance(g, list):
                    g = []
                hero = v.get("image_url")
                if (
                    not any(isinstance(x, str) and x.strip().lower().startswith("http") for x in g)
                    and isinstance(hero, str)
                    and hero.strip().lower().startswith("http")
                ):
                    g = [hero.strip()]
                v["gallery"] = g
            inline_gallery = gallery_vision_filter and gallery_vision_inline_enabled()
            if inline_gallery:
                try:
                    gv = await asyncio.to_thread(apply_gallery_vision_filter_to_vehicles, all_vehicles)
                    result["gallery_vision"] = gv
                    logger.info(
                        "Gallery vision filter [%s]: dropped %s of %s unique HTTPS image URLs (Claude)",
                        name,
                        gv.get("gallery_vision_unique_dropped"),
                        gv.get("gallery_vision_unique_before"),
                    )
                except Exception as e:
                    logger.warning(
                        "Gallery vision filter failed for %s (saving unfiltered images): %s",
                        name,
                        e,
                    )
                    result["gallery_vision"] = {"error": str(e)[:200]}
            if monroney_vision:
                try:
                    mv = await asyncio.to_thread(apply_monroney_vision_to_vehicles, all_vehicles)
                    result["monroney_vision"] = mv
                    logger.info(
                        "Monroney vision [%s]: rows_touched=%s sticker_image_calls=%s page_text_calls=%s",
                        name,
                        mv.get("rows_touched"),
                        mv.get("sticker_image_calls"),
                        mv.get("page_text_calls"),
                    )
                except Exception as e:
                    logger.warning("Monroney vision failed for %s: %s", name, e)
                    result["monroney_vision"] = {"error": str(e)[:200]}
            reg_id = dealer.get("dealership_registry_id")
            if reg_id:
                for v in all_vehicles:
                    v.setdefault("dealership_registry_id", reg_id)
            from backend.parsers.vdp_urls import apply_vehicle_source_url

            for v in all_vehicles:
                apply_vehicle_source_url(v)
            t_up0 = time.perf_counter()
            count = await upsert_vehicles_for_dealer(write_coordinator, all_vehicles)
            result["phase_secs"]["upsert"] = round(time.perf_counter() - t_up0, 2)
            result["upserted"] = count
            if reg_id and url:
                try:
                    from backend.db.inventory_db import link_cars_to_dealership_registry

                    linked = await asyncio.to_thread(
                        link_cars_to_dealership_registry,
                        int(reg_id),
                        url,
                        dealer_id_slug=dealer_id,
                    )
                    if linked:
                        result["registry_linked"] = linked
                except Exception as e:
                    logger.debug(
                        "link_cars_to_dealership_registry failed for %s: %s",
                        name,
                        e,
                    )
            try:
                from backend.scanner.inventory_reconcile import (
                    normalized_vin_set_from_vehicles,
                    reconcile_dealer_inventory_after_scan,
                )

                scraped_norm = normalized_vin_set_from_vehicles(all_vehicles)
                result["reconcile"] = await asyncio.to_thread(
                    reconcile_dealer_inventory_after_scan,
                    dealer_id,
                    url,
                    scraped_norm,
                    result,
                )
            except Exception as e:
                logger.warning(
                    "Inventory reconcile failed for %s (inventory already saved): %s",
                    name,
                    e,
                )
                result["reconcile"] = {
                    "ran": False,
                    "scraped_candidates": 0,
                    "marked_inactive": 0,
                    "skipped_reason": "exception",
                    "error": str(e)[:200],
                }
            result["seconds"] = time.perf_counter() - t0
            logger.info(
                "Parsing: %s — extracted %d vehicles (deduped by VIN), upserted %d",
                name,
                len(all_vehicles),
                count,
            )
            logger.info(
                "Dealer complete: %s (%d inventory rows, %d deduped, %d VDP visited, %d VDP-enriched, "
                "%d upserted, %.1fs)",
                name,
                result["inventory_rows"],
                result["deduped_rows"],
                result["vdps_visited"],
                result["vehicles_vdp_enriched"],
                count,
                result["seconds"],
            )
            return result
        # No path returned vehicles
        logger.warning("Parsing: %s — no vehicles from any inventory path", name)
        if provider in KNOWN_HAR_PROVIDERS:
            await capture_scanner_failure_har(browser, url, dealer_id, name)
        DEBUG_DIR.mkdir(parents=True, exist_ok=True)
        screenshot_path = DEBUG_DIR / f"fail_{dealer_id}.png"
        await page.screenshot(path=str(screenshot_path))
        logger.info("Debug: saved screenshot to %s", screenshot_path)
        result["seconds"] = time.perf_counter() - t0
        logger.info(
            "Dealer complete: %s (%d inventory rows, %d deduped, %d VDP visited, %d VDP-enriched, %d upserted, %.1fs)",
            name,
            result["inventory_rows"],
            result["deduped_rows"],
            result["vdps_visited"],
            result["vehicles_vdp_enriched"],
            result["upserted"],
            result["seconds"],
        )
        return result
    except asyncio.CancelledError:
        raise
    except Exception as e:
        result["error"] = str(e)
        result["seconds"] = time.perf_counter() - t0
        if is_playwright_shutdown_error(e):
            return result
        logger.exception("Dealer %s failed: %s", dealer_id, e)
        logger.info(
            "Dealer complete: %s (%d inventory rows, %d deduped, %d VDP visited, %d VDP-enriched, %d upserted, %.1fs) [error]",
            name,
            result["inventory_rows"],
            result["deduped_rows"],
            result["vdps_visited"],
            result["vehicles_vdp_enriched"],
            result["upserted"],
            result["seconds"],
        )
        return result
    finally:
        result["intercept_count"] = len(intercept_records)
        result["filtered_count"] = gate_stats["url_denied"]
        if result.get("gallery_bins") is None:
            result["gallery_bins"] = {}
        result["seconds"] = time.perf_counter() - t0
        emit_dealer_run_summary(result)
        await safe_close_context(context)

__all__ = ['run_dealer', 'apply_monroney_vision_to_vehicles', 'emit_dealer_run_summary']
