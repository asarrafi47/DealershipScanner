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
from backend.scanner.post_scan.coverage_report import compute_dealer_coverage, format_coverage_log
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
    intercept_feed_is_sufficient,
)
from backend.scanner.scrapers.inventory_vin_merge import merge_inventory_rows_same_vin
from backend.scanner.vdp import enrich_vehicles_vdp
from backend.utils.gallery_merge import gallery_https_bin_histogram
from backend.scanner.phases.site_profile import (
    SiteProfile,
    choose_inventory_paths,
    profile_dealer_site,
)
from backend.scanner.phases.url_discovery import discover_dealer_url
from backend.scanner import scan_log

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
        logger.info("Warmup UA [%s]: %s", name, _ua[:130])
        context = await browser.new_context(**ctx_opts)
        page = await context.new_page()
        warm_pred = playwright_inventory_json_predicate(url)
        _dead_domain_errors = (
            "ERR_NAME_NOT_RESOLVED", "ERR_TOO_MANY_REDIRECTS",
            "net::ERR_NAME_NOT_RESOLVED", "net::ERR_TOO_MANY_REDIRECTS",
            "ERR_CONNECTION_REFUSED", "net::ERR_CONNECTION_REFUSED",
            "ERR_CONNECTION_TIMED_OUT", "net::ERR_CONNECTION_TIMED_OUT",
            "ERR_INTERNET_DISCONNECTED",
        )
        _warmup_403_bypass = False

        async def _warmup_phase() -> None:
            nonlocal page, url, warm_pred, _warmup_403_bypass
            try:
                await goto_with_retries(page, url, log_label=f"Warmup:{name}", timeout_ms=30000)
            except Exception as _warmup_exc:
                _exc_str = str(_warmup_exc)
                if any(e in _exc_str for e in _dead_domain_errors):
                    logger.warning("Warmup: %s — dead domain (%s), attempting URL discovery", name, _exc_str.split("\n")[0])
                    _discovered = await discover_dealer_url(
                        name, url, browser,
                        city=str(dealer.get("city") or "").strip(),
                        state=str(dealer.get("state") or "").strip(),
                    )
                    if _discovered:
                        logger.info("Warmup: %s — discovered URL: %s (was: %s)", name, _discovered, url)
                        url = _discovered
                        warm_pred = playwright_inventory_json_predicate(url)
                        await goto_with_retries(page, url, log_label=f"Warmup:{name}", timeout_ms=30000)
                    else:
                        logger.error("Warmup: %s — URL discovery failed, skipping dealer", name)
                        raise
                elif "403" in _exc_str and provider == "dealer_inspire":
                    logger.warning(
                        "Warmup: %s — HTTP 403 (Cloudflare block) but provider=dealer_inspire; "
                        "bypassing warmup and attempting Algolia recovery directly",
                        name,
                    )
                    _warmup_403_bypass = True
                    try:
                        await page.close()
                    except Exception:
                        pass
                    page = await context.new_page()
                else:
                    raise
            if not _warmup_403_bypass:
                w_post, w_scroll = warmup_delays()
                await warmup_settle_after_base_goto(
                    page,
                    warm_pred,
                    dealer_name=name,
                    max_idle_sec=w_post,
                    scroll_sec=w_scroll,
                )
                from backend.scanner.scrapers.pixel_motion import _dismiss_cookie_banner
                await asyncio.sleep(1.0)
                # Bounded: on some DealerOn sites the stealth scroll trips anti-bot JS
                # that pins the renderer, and the cookie-banner locator query then hangs.
                # Cap it so a wedge here fails the phase fast instead of at the phase cap.
                try:
                    await asyncio.wait_for(_dismiss_cookie_banner(page), timeout=20.0)
                except asyncio.TimeoutError:
                    logger.warning("Warmup: %s — cookie-banner probe wedged (renderer pinned); skipping", name)
                logger.info("Warmup: %s — done (signal race cap=%.1fs + scroll %.1fs)", name, w_post, w_scroll)

        # Hard cap on the whole warmup phase. Some sites (DealerOn: ggkia, tustinkia,
        # robinsford, …) serve pages whose JS pins the renderer at 100% CPU forever;
        # a wedged renderer can stall even Playwright's own navigation timeout, so a
        # per-call timeout is not enough — the phase gets one as a whole.
        _warmup_cap = _warmup_phase_timeout_sec()
        try:
            await asyncio.wait_for(_warmup_phase(), timeout=_warmup_cap)
        except asyncio.TimeoutError:
            raise RuntimeError(
                f"warmup_phase_timeout_{int(_warmup_cap)}s: page wedged "
                "(renderer JS loop — known on DealerOn sites)"
            ) from None

        # Detect permanent maintenance pages that resolve DNS but serve no inventory
        # (e.g. S3/Ceph bucket static 503 — goto succeeds but page is a placeholder).
        # Use specific downtime phrases only — bare "maintenance" fires on every dealer
        # service-menu nav item ("Oil Change & Maintenance", "Maintenance Schedule", etc.).
        if not _warmup_403_bypass:
            try:
                _warmup_html = (await asyncio.wait_for(page.content(), timeout=15.0)).lower()
                _maintenance_markers = (
                    "under maintenance", "down for maintenance", "performing maintenance",
                    "site is currently",
                    "temporarily unavailable", "under construction", "site offline",
                )
                _warmup_title_m = __import__('re').search(r'<title[^>]*>(.*?)</title>', _warmup_html, __import__('re').S)
                _warmup_title = (_warmup_title_m.group(1) if _warmup_title_m else "").strip()
                logger.debug("Warmup: %s — page title after load: %r", name, _warmup_title)
                _title_flags = ("coming soon", "maintenance", "offline", "unavailable", "under construction")
                _title_hit = any(f in _warmup_title for f in _title_flags)
                _body_hit = next((m for m in _maintenance_markers if m in _warmup_html), None)
                _is_maintenance = _title_hit or bool(_body_hit)
                if _body_hit:
                    logger.debug("Warmup: %s — body marker matched: %r", name, _body_hit)
                if provider != "autowall" and _is_maintenance:
                    _match_reason = (f"title={_warmup_title!r}" if _title_hit
                                     else f"body={_body_hit!r}" if _body_hit else "unknown")
                    logger.warning(
                        "Warmup: %s — maintenance page detected after load (matched: %s), attempting URL discovery",
                        name, _match_reason,
                    )
                    _discovered_maint = await discover_dealer_url(
                        name, url, browser,
                        city=str(dealer.get("city") or "").strip(),
                        state=str(dealer.get("state") or "").strip(),
                    )
                    if _discovered_maint:
                        logger.info("Warmup: %s — discovered URL for maintenance site: %s", name, _discovered_maint)
                        url = _discovered_maint
                        warm_pred = playwright_inventory_json_predicate(url)
                        await goto_with_retries(page, url, log_label=f"Warmup:{name}", timeout_ms=30000)
                    else:
                        logger.error("Warmup: %s — maintenance page, URL discovery failed; skipping dealer", name)
                        result["error"] = "maintenance_page_no_discovery"
                        result["seconds"] = time.perf_counter() - t0
                        return result
            except Exception as _maint_exc:
                logger.debug("Warmup: %s — maintenance check error (ignored): %s", name, _maint_exc)

        inv_wait_ms = inventory_wait_ms()
        pag_wait_ms = pagination_response_wait_ms()

        # Scrape all three inventory paths in parallel — each on its own page within the
        # same browser context so session cookies from warmup are shared automatically.
        # ── Site profiler phase ───────────────────────────────────────────
        _site_profile: SiteProfile | None = None
        _profiler_enabled = (os.environ.get("SCANNER_SITE_PROFILER") or "1").strip().lower() not in (
            "0", "false", "no", "off"
        )
        if _warmup_403_bypass:
            # Warmup was blocked (HTTP 403) and skipped for this DealerInspire site.
            # Inject a minimal fallback profile so the recovery chain knows to try Algolia.
            _site_profile = SiteProfile(dealer_url=url)
            _site_profile.detected_provider = "dealer_inspire"
            _site_profile.scrape_risk = "algolia_auth"
            _site_profile.notes.append("warmup_403_bypass:algolia_fallback")
            result["provider"] = "dealer_inspire"
            result["site_profile"] = {
                "provider": "dealer_inspire",
                "pagination": "unknown",
                "confidence": 0.0,
                "scrape_risk": "algolia_auth",
                "paths_found": [],
                "api_eps": 0,
                "notes": ["warmup_403_bypass:algolia_fallback"],
            }
            logger.info(
                "Site profile [%s]: warmup_403_bypass — injected fallback profile "
                "(provider=dealer_inspire, scrape_risk=algolia_auth)",
                name,
            )
        elif _profiler_enabled:
            t_prof0 = time.perf_counter()
            try:
                _site_profile = await profile_dealer_site(context, url, [])
                logger.info(
                    "Site profile [%s]: provider=%s pagination=%s confidence=%.2f "
                    "paths=%s api_eps=%d scrape_risk=%s notes=%s",
                    name,
                    _site_profile.detected_provider,
                    _site_profile.pagination_type,
                    _site_profile.confidence_score,
                    _site_profile.inventory_paths_found[:3],
                    len(_site_profile.api_endpoint_candidates),
                    _site_profile.scrape_risk,
                    _site_profile.notes[:3],
                )
            except Exception as _prof_e:
                logger.warning("Site profiler failed for %s (continuing without profile): %s", name, _prof_e)
            result["phase_secs"]["site_profile"] = round(time.perf_counter() - t_prof0, 2)
            if _site_profile is not None:
                result["site_profile"] = {
                    "provider": _site_profile.detected_provider,
                    "pagination": _site_profile.pagination_type,
                    "confidence": _site_profile.confidence_score,
                    "scrape_risk": _site_profile.scrape_risk,
                    "paths_found": _site_profile.inventory_paths_found[:5],
                    "api_eps": len(_site_profile.api_endpoint_candidates),
                    "notes": _site_profile.notes[:5],
                }
                if _site_profile.detected_provider and _site_profile.detected_provider != "unknown":
                    result["provider"] = _site_profile.detected_provider

        inv_paths = choose_inventory_paths(_site_profile, dealer)

        # Recipe pre-flight: replay endpoints captured on a previous scan over plain
        # HTTP. The browser scrape is skipped only when the yield is near the dealer's
        # last-known lot size — a recipe saved from one listing config can be
        # type-filtered and cover only part of the inventory. Partial yields are still
        # merged (VIN dedup downstream) but the browser scrape runs too.
        recipe_records: list[tuple[str, Any]] | None = None
        try:
            from backend.scanner.recipes import last_known_vin_count, try_fetch_via_recipes

            _recipe_hit = await try_fetch_via_recipes(dealer_id, provider, url, name)
            if _recipe_hit:
                recipe_records, _recipe_vins = _recipe_hit
                _known = await asyncio.to_thread(last_known_vin_count, dealer_id)
                if _known > 0 and _recipe_vins >= int(0.7 * _known):
                    result["recipe_fetch"] = "full"
                    inv_paths = []
                    logger.info(
                        "Recipe fetch [%s]: %d/%d known VIN(s) — skipping browser inventory",
                        name, _recipe_vins, _known,
                    )
                else:
                    result["recipe_fetch"] = "partial"
                    logger.info(
                        "Recipe fetch [%s]: %d VIN(s) vs %d known — merging and still scraping",
                        name, _recipe_vins, _known,
                    )
        except Exception as _rec_e:
            logger.debug("Recipe fetch skipped [%s]: %s", name, str(_rec_e)[:200])

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
                    site_profile=_site_profile,
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
        captured_endpoints: list[Any] = []
        if recipe_records:
            intercept_records.extend(recipe_records)
        for path_records, path_html, path_denied, path_card_locs, path_endpoints in path_results:
            intercept_records.extend(path_records)
            gate_stats["url_denied"] += path_denied
            path_htmls.append(path_html)
            if isinstance(path_card_locs, dict):
                merged_card_locations.update(path_card_locs)
            if path_endpoints:
                captured_endpoints.extend(path_endpoints)

        if captured_endpoints:
            try:
                from backend.scanner.recipes import promote_from_ledger

                promote_from_ledger(dealer_id, provider, captured_endpoints)
            except Exception as _rec_e:
                logger.debug("Recipe promotion skipped [%s]: %s", name, _rec_e)

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
                # When EP VDP is explicitly disabled, also disable price/description VDP by default.
                if _ep_raw == "0":
                    os.environ["SCANNER_VDP_PRICE_MAX"] = "0"
                else:
                    os.environ["SCANNER_VDP_PRICE_MAX"] = str(effective_vdp_price_max(len(all_vehicles)))
            if _ep_raw == "0" and not (os.environ.get("SCANNER_VDP_DESCRIPTION_MAX") or "").strip():
                os.environ["SCANNER_VDP_DESCRIPTION_MAX"] = "0"

            vdp_stats: dict[str, Any] = {}
            t_vdp0 = time.perf_counter()
            _vdp_cap = _vdp_phase_timeout_sec(len(all_vehicles))
            try:
                # Phase-level timeout: a wedged renderer (DealerOn anti-bot tarpit) hangs
                # rather than raises, so per-page timeouts don't fire and VDP would block
                # the dealer's inventory upsert forever. On timeout we keep the listing
                # data already captured and proceed to upsert.
                vdp_stats = await asyncio.wait_for(
                    enrich_vehicles_vdp(
                        page, all_vehicles, name, dealer_id=dealer_id, site_profile=site_profile,
                        provider=provider,
                    ),
                    timeout=_vdp_cap,
                )
            except asyncio.TimeoutError:
                logger.warning(
                    "VDP enrichment timed out for %s after %.0fs (renderer wedge?) — "
                    "upserting %d listing-only rows",
                    name, _vdp_cap, len(all_vehicles),
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
            t_up0 = time.perf_counter()
            count = await upsert_vehicles_for_dealer(write_coordinator, all_vehicles)
            result["phase_secs"]["upsert"] = round(time.perf_counter() - t_up0, 2)
            result["upserted"] = count
            scan_log.log_vehicles(dealer_id, name, result.get("provider", provider), all_vehicles)
            if all_vehicles:
                _cov = compute_dealer_coverage(list(all_vehicles), dealer_id=dealer_id)
                logger.info("%s", format_coverage_log(_cov))
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
