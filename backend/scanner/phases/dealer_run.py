"""Per-dealer scan: warmup, inventory, recovery, VDP, upsert."""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from typing import Any

from backend.parsers import parse, resolve_rooftop_attribution
from backend.parsers.vdp_urls import apply_vehicle_source_url
from backend.scanner.constants import DEBUG_DIR, KNOWN_HAR_PROVIDERS
from backend.scanner.dealer_site_url import dealer_inventory_base_url
from backend.scanner.inventory_write import InventoryWriteCoordinator
# One shared rooftop reconcile policy for the browser path and the delta path.
from backend.scanner.rooftop_disown import (
    disown_foreign_rooftop_vins,
    roster_place as rooftop_roster_place,
    split_refusals,
)
from backend.scanner.phases.upsert import upsert_vehicles_for_dealer
from backend.scanner.post_scan.coverage_report import compute_dealer_coverage, format_coverage_log
from backend.scanner.phases.nav import (
    get_rotating_ua,
    is_playwright_shutdown_error,
    safe_close_context,
)
from backend.scanner.post_pipeline import (
    apply_gallery_vision_filter_to_vehicles,
    gallery_vision_filter_env_enabled,
)
from backend.scanner.scan_efficiency import (
    gallery_vision_inline_enabled,
    intercept_feed_is_sufficient,
)
from backend.scanner.scrapers.inventory_vin_merge import merge_inventory_rows_same_vin
from backend.utils.gallery_merge import gallery_https_bin_histogram
from backend.scanner import scan_log

logger = logging.getLogger("scanner")


def _http_only() -> bool:
    """No browser at all (the default): inventory comes from recipe replay, per-car
    data from VDP recipes + HTTP-first. Only ``SCANNER_ALLOW_BROWSER=1`` (discovery
    captures) turns this off — see backend/scanner/browser_gate.py."""
    from backend.scanner.browser_gate import http_only

    return http_only()


_CAPTURE_FIELDS = (
    "price", "msrp", "mileage", "trim", "exterior_color", "interior_color", "engine_description",
    "transmission", "drivetrain", "fuel_type", "body_style", "description", "stock_number",
    "carfax_url", "image_url",
)


def _capture_coverage(vehicles: list[dict[str, Any]]) -> dict[str, Any]:
    """Share of captured rows carrying each field, BEFORE the upsert's COALESCE can
    hide gaps behind values the DB already had. This is the honest measure of what
    the capture path produced."""
    n = len(vehicles)
    out: dict[str, Any] = {"n": n}
    if not n:
        return out
    examples: dict[str, list[str]] = {}

    def _has(v: dict[str, Any], k: str) -> bool:
        val = v.get(k)
        if val is None:
            return False
        if isinstance(val, str):
            return bool(val.strip())
        # Parsers default price / msrp / mileage to 0 when the feed has no
        # value; the gate reported msrp 100% on stores where every row was 0
        # (2026-09-28, F02). Zero is "missing" for these numeric fields.
        if k in ("price", "msrp", "mileage") and isinstance(val, (int, float)) and not isinstance(val, bool):
            return val > 0
        return True

    for k in _CAPTURE_FIELDS:
        hit = 0
        for v in vehicles:
            if _has(v, k):
                hit += 1
            elif len(examples.setdefault(k, [])) < 3:
                examples[k].append(str(v.get("vin") or "")[:17])
        out[k] = round(hit / n, 3)
    g8 = 0
    for v in vehicles:
        g = v.get("gallery")
        cnt = sum(1 for u in g if isinstance(u, str) and u.lower().startswith("https://")) if isinstance(g, list) else 0
        if cnt >= 8:
            g8 += 1
        elif len(examples.setdefault("gallery_8plus", [])) < 3:
            examples["gallery_8plus"].append(str(v.get("vin") or "")[:17])
    out["gallery_8plus"] = round(g8 / n, 3)
    out["missing_examples"] = {k: v for k, v in examples.items() if v}
    return out


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
    scan_log.log_dealer_summary(result, result.get("provider", ""))


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
        "provider": provider,
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
        "vdp_phase_timed_out": False,
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

    page: Any = None
    try:
        ctx_opts: dict[str, Any] = {"viewport": {"width": 1920, "height": 1080}}
        _ua = (os.environ.get("SCANNER_USER_AGENT") or "").strip() or get_rotating_ua()
        ctx_opts["user_agent"] = _ua
        if browser is not None and not _http_only():
            from backend.scanner.browser_gate import require_browser

            require_browser("dealer_run.new_context")
            logger.info("Warmup UA [%s]: %s", name, _ua[:130])
            context = await browser.new_context(**ctx_opts)
            page = await context.new_page()
        else:
            # HTTP-only: no context, no page. Every browser phase below is gated on
            # ``page is None`` / ``_http_only()``; the recipe replay + HTTP-first
            # detail pass are the whole scan.
            page = None
        result["http_only"] = True
        # Warmup, dead-domain URL discovery, the maintenance-page probe and the
        # site profiler were browser phases; they live in discovery now
        # (backend/scanner/discovery_capture.py, docs/HTTP_ONLY_SCANS_PLAN.md).
        _site_profile: Any = None
        inv_paths: list[str] = []

        # Recipe pre-flight: replay endpoints captured on a previous scan over plain
        # HTTP. The browser scrape is skipped only when the yield is near the dealer's
        # last-known lot size — a recipe saved from one listing config can be
        # type-filtered and cover only part of the inventory. Partial yields are still
        # merged (VIN dedup downstream) but the browser scrape runs too.
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
                dealer_id, provider, url, name, union=True, coverage_out=_recipe_cov
            )
            if _recipe_hit:
                recipe_records, _recipe_vins = _recipe_hit
                if _http_only():
                    # No site profile ran; the recipe's own provider hint is the truth.
                    try:
                        from backend.scanner.recipes import load_recipes as _lr

                        _hints = [r.provider_hint for r in _lr(dealer_id) if not r.stale and r.provider_hint]
                        if _hints:
                            result["provider"] = _hints[0]
                    except Exception:
                        pass
                _known = await asyncio.to_thread(last_known_vin_count, dealer_id)
                result["recipe_coverage"] = {
                    k: round(float(v), 3) for k, v in _recipe_cov.items() if k != "n"
                }
                _ok, _why = recipe_yield_replaces_browser(_recipe_vins, _known, _recipe_cov)
                if _ok:
                    result["recipe_fetch"] = "full"
                    inv_paths = []
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
        path_htmls: list[str | None] = []
        merged_card_locations: dict[str, str] = {}
        if recipe_records:
            intercept_records.extend(recipe_records)
        # This store's postal address from the registry, looked up ONCE and
        # handed to every parse() below. A group feed whose rooftops are address
        # blocks carrying no store name can only be told apart by address; omit
        # these and the gate refuses every page of such a feed.
        from backend.scanner.dealer_place import roster_place_with_hints

        # registry town + the page-learned street from scan hints (street-block stamps)
        roster_place = await asyncio.to_thread(roster_place_with_hints, url, dealer_id)

        body_parse_cache: dict[int, list[dict[str, Any]]] = {}
        # Rows this store's own site served that the feed assigns to a DIFFERENT
        # storefront. Keeping them out of ``all_vehicles`` is what keeps them out
        # of ``result["vins"]`` and out of the reconcile pass's "still seen" VIN
        # set — a mis-attributed VIN in that set is what has been holding
        # pre-existing mis-attributions at listing_active = 1 on this path.
        rooftop_refused: list[dict[str, Any]] = []

        def _vehicles_for_body(body: Any) -> list[dict[str, Any]]:
            bid = id(body)
            cached = body_parse_cache.get(bid)
            if cached is not None:
                return cached
            vehicles = list(
                parse(
                    provider, body, base_url=url, dealer_id=dealer_id, dealer_name=name,
                    dealer_url=url, rejected_out=rooftop_refused, **roster_place,
                )
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

        # The row-count heuristics below — feed sufficiency and, through it, the
        # recovery trigger — must be told what the FEED answered with, not what
        # the rooftop gate kept for this store. A group feed that returns 995
        # rows of which 135 are ours is a COMPLETE capture; report 135 and the
        # recovery chain concludes the capture failed and re-fetches the lot
        # through scraper strategies whose rows carry no rooftop evidence at all,
        # so every sibling car it finds would be stored. Measured on
        # audifletcherjones-com: 135 of 1,005 rows are this store's.
        feed_rows = all_vehicles + rooftop_refused
        result["inventory_rows"] = len(feed_rows)

        from backend.scanner.inventory_recovery import unique_vin_count as _unique_vin_count

        merged_unique = _unique_vin_count(feed_rows)
        feed_sufficient = (
            bool(feed_rows)
            and bool(intercept_records)
            and intercept_feed_is_sufficient(
                intercept_records, url, len(feed_rows), unique_vin_count=merged_unique
            )
        )

        def _parse_inventory_raw(raw: Any) -> list[dict[str, Any]]:
            rows = list(
                parse(
                    provider, raw, base_url=url, dealer_id=dealer_id, dealer_name=name,
                    dealer_url=url, rejected_out=rooftop_refused, **roster_place,
                )
            )
            for v in rows:
                v.setdefault("dealer_name", name)
                v.setdefault("dealer_url", url)
            return rows

        from backend.scanner.inventory_recovery import RecoveryContext, recover_inventory

        if not feed_rows:
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
                vehicles=feed_rows,
                parse_fn=_parse_inventory_raw,
                dealer=dealer,
            )
        )
        # Rooftop attribution is settled ONCE, here, over the final row set:
        # recovery may have replaced every intercepted row with output from a
        # scraper strategy that never went through parse(). Per-page marks are
        # cleared first because a page holding nothing but one sibling's cars
        # looks like a single-store payload on its own, and only reads as a
        # sibling next to this store's own rooftop across the whole capture —
        # the same union re-run backend.scanner.delta_scan does.
        all_vehicles = recovery.vehicles
        for _v in all_vehicles:
            _v.pop("_rooftop_reject", None)
        # Rows a store-scoped recipe returned (CarsCommerce facetFilters.source_id,
        # verified at synthesis) are this store's by construction; re-gating them
        # here kept 5 of Tutton CDJR's 346 cars on 2026-09-26 (the gate can only
        # pick ONE of the two stamps its feeds use). Gate only the rest.
        _scoped = [v for v in all_vehicles if v.get("_feed_scoped")]
        _open = [v for v in all_vehicles if not v.get("_feed_scoped")]
        if _open:
            _open, rooftop_refused = resolve_rooftop_attribution(
                _open, dealer_id=dealer_id, dealer_name=name, dealer_url=url, **roster_place,
            )
        else:
            rooftop_refused = []
        if _scoped:
            logger.info("rooftop attribution [%s]: %d row(s) from store-scoped recipe(s) kept without the union gate", dealer_id, len(_scoped))
        all_vehicles = _scoped + _open
        if rooftop_refused:
            result["rooftop_refused_rows"] = len(rooftop_refused)
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

            # Prefetch cheap knowledge BEFORE the browser queue is built: prior-scan
            # immutable fields by VIN + HTTP detail-page structured data. The queue's
            # emptiness gates run after this, so it can only shrink browser work.
            try:
                from backend.scanner.vdp.prefetch import prefetch_before_vdp

                _pf = await prefetch_before_vdp(all_vehicles, dealer_id, name)
                if _pf:
                    result["vdp_prefetch"] = _pf
            except Exception as _pf_e:
                logger.warning("VDP prefetch failed [%s] (continuing): %s", name, _pf_e)

            # The browser VDP pool is gone: the HTTP-first pass above (curl_cffi
            # detail pages + VDP recipes) is the per-car layer.
            vdp_stats: dict[str, Any] = {}
            result["vdps_visited"] = 0
            result["vehicles_vdp_enriched"] = int((result.get("vdp_prefetch") or {}).get("http_first", {}).get("fields_filled", 0) or 0)
            result["gallery_vdp_urls_added"] = int((result.get("vdp_prefetch") or {}).get("http_first", {}).get("galleries_extended", 0) or 0)
            log_gallery_bins(name, "after_vdp", all_vehicles)
            result["gallery_bins"] = gallery_https_bin_histogram(all_vehicles)
            if vdp_stats.get("gallery_phase_bins"):
                logger.info("Gallery phase bins [%s]: %s", name, vdp_stats.get("gallery_phase_bins"))

            pre_vdp_n = len(all_vehicles)
            _vdp_kept = [v for v in all_vehicles if not v.get("_sister_store_exclude")]
            vdp_excluded = pre_vdp_n - len(_vdp_kept)
            if vdp_excluded:
                # Safety: mirror filter_sister_store_vehicles' guard — a misread shared
                # location snippet across VDPs must not silently empty (or near-empty)
                # the whole lot, which would delist real inventory via reconcile.
                _min_keep = 8
                try:
                    _min_keep = max(1, int((os.environ.get("SCANNER_SISTER_STORE_MIN_KEEP") or "8").strip()))
                except ValueError:
                    pass
                _safe_raw = (os.environ.get("SCANNER_SISTER_STORE_SAFE") or "1").strip().lower()
                _safe_enabled = _safe_raw not in ("0", "false", "no", "off")
                if _safe_enabled and pre_vdp_n >= _min_keep and len(_vdp_kept) == 0:
                    logger.warning(
                        "Sister-store filter [%s] VDP: would exclude entire lot (%d rows) — "
                        "keeping all (set SCANNER_SISTER_STORE_SAFE=0 to allow empty result)",
                        name,
                        pre_vdp_n,
                    )
                    result["sister_store_vdp_aborted_empty"] = True
                else:
                    all_vehicles = _vdp_kept
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
            if not reg_id:
                try:
                    from backend.listings.dealer_registry_match import resolve_car_dealership_registry_id

                    reg_id = resolve_car_dealership_registry_id({"dealer_url": url}) or None
                except Exception as e:
                    logger.debug("dealership_registry_id fallback resolution failed for %s: %s", url, e)
                    reg_id = None
            if reg_id:
                for v in all_vehicles:
                    v.setdefault("dealership_registry_id", reg_id)
            from backend.parsers.vdp_urls import apply_vehicle_source_url

            for v in all_vehicles:
                apply_vehicle_source_url(v)
            try:
                from backend.enrichment.vpic_facts import override_vehicles

                _vf = override_vehicles(all_vehicles)
                result["vin_facts"] = _vf
                if _vf.get("drivetrain") or _vf.get("fuel_type"):
                    logger.info(
                        "VIN facts [%s]: %d cached decode(s); drivetrain overridden on %d, fuel on %d",
                        name, _vf["cached"], _vf["drivetrain"], _vf["fuel_type"],
                    )
            except Exception as _vf_e:
                logger.warning("VIN facts failed [%s] (continuing): %s", name, _vf_e)
            result["capture_coverage"] = _capture_coverage(all_vehicles)
            t_up0 = time.perf_counter()
            count = await upsert_vehicles_for_dealer(write_coordinator, all_vehicles)
            result["phase_secs"]["upsert"] = round(time.perf_counter() - t_up0, 2)
            result["upserted"] = count
            scan_log.log_vehicles(dealer_id, name, result.get("provider", provider), all_vehicles)
            if all_vehicles:
                _cov = compute_dealer_coverage(list(all_vehicles), dealer_id=dealer_id)
                result["coverage"] = _cov
                logger.info("%s", format_coverage_log(_cov))
                # Quality loop: when key fields are still thin after every
                # scan-time layer, go back per-car via the gap-fill methods.
                try:
                    from backend.scanner.post_scan.auto_heal import run_auto_heal_for_dealer

                    _heal = await asyncio.to_thread(
                        run_auto_heal_for_dealer, dealer_id, name, _cov, list(all_vehicles)
                    )
                    if _heal:
                        result["auto_heal"] = _heal
                except Exception as _heal_e:
                    logger.warning("Auto-heal failed [%s]: %s", name, _heal_e)
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
            from backend.scanner.inventory_reconcile import (
                condition_buckets_from_vehicles,
                normalized_vin_set_from_vehicles,
                reconcile_dealer_inventory_after_scan,
            )

            scraped_norm = normalized_vin_set_from_vehicles(all_vehicles)
            scraped_conditions = condition_buckets_from_vehicles(all_vehicles)
            # Cars earlier scans stamped onto this store that THIS run's feed
            # hands to a named sibling rooftop. They are gone from
            # ``scraped_norm`` now, but reconcile alone cannot retire them: once
            # the siblings' rows are (correctly) refused this dealer can no
            # longer reach reconcile's VIN-coverage bar. Only refusals that are
            # evidence about a CAR are acted on — see
            # rooftop_disown.EVIDENCE_BACKED_REJECTS; "we could not tell which
            # rooftop is this store" is a statement about our roster and must
            # never un-list anything.
            try:
                evidenced, unidentified = split_refusals(rooftop_refused)
                if unidentified:
                    result["rooftop_unidentified_rows"] = unidentified
                    logger.warning(
                        "Rooftop [%s]: could not identify this store among the rooftops its feed "
                        "names — %d row(s) refused for storage, NOTHING un-listed. Fix by giving "
                        "this dealer a street_address in the `dealerships` roster.",
                        name, unidentified,
                    )
                foreign_vins = normalized_vin_set_from_vehicles(evidenced) - scraped_norm
                if foreign_vins:
                    disowned = await asyncio.to_thread(
                        disown_foreign_rooftop_vins, dealer_id, foreign_vins
                    )
                    result["rooftop_disowned"] = disowned
                    if disowned:
                        logger.info(
                            "Rooftop [%s]: %d car(s) belonged to sibling rooftops in this group "
                            "feed — unlisted from this store",
                            name, disowned,
                        )
            except Exception as e:
                logger.warning("Rooftop disown failed for %s (continuing): %s", name, e)

            try:
                result["reconcile"] = await asyncio.to_thread(
                    reconcile_dealer_inventory_after_scan,
                    dealer_id,
                    url,
                    scraped_norm,
                    result,
                    scraped_conditions=scraped_conditions,
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
