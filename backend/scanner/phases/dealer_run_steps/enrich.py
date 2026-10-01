"""Per-row enrichment before the write: VIN dedupe, sister-store filters,
prefetch (HTTP-first detail pages), gallery normalisation + vision, registry id,
source URLs, VIN facts (vPIC), capture coverage."""
from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

from backend.parsers.vdp_urls import apply_vehicle_source_url
from backend.scanner.phases.dealer_run_steps.state import DealerRun
from backend.scanner.post_pipeline import apply_gallery_vision_filter_to_vehicles
from backend.scanner.scan_efficiency import gallery_vision_inline_enabled
from backend.scanner.scrapers.inventory_vin_merge import merge_inventory_rows_same_vin
from backend.utils.gallery_merge import gallery_https_bin_histogram

logger = logging.getLogger("scanner")

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


def _vin_list(vehicles: list[dict[str, Any]]) -> list[str]:
    return sorted({(v.get("vin") or "").strip() for v in vehicles if (v.get("vin") or "").strip()})


def dedupe_by_vin(run: DealerRun) -> None:
    """One row per VIN for downstream enrichment (listing payloads may repeat VINs)."""
    by_vin: dict[str, dict] = {}
    for v in run.all_vehicles:
        vin = (v.get("vin") or "").strip()
        if vin:
            if vin in by_vin:
                merge_inventory_rows_same_vin(by_vin[vin], v)
            else:
                by_vin[vin] = v
    run.all_vehicles = list(by_vin.values())
    run.result["deduped_rows"] = len(run.all_vehicles)
    log_gallery_bins(run.name, "after_inventory_merge", run.all_vehicles)


def filter_sister_stores(run: DealerRun) -> None:
    """Drop rows whose lot location names a sister store (rows the rooftop gate
    already ruled on are deferred to it), then stamp ``result["vins"]``."""
    try:
        from backend.scanner.dealer_location import (
            build_dealer_site_profile,
            filter_sister_store_vehicles,
            sister_store_filter_enabled,
        )

        site_profile = build_dealer_site_profile(run.dealer)
        if sister_store_filter_enabled():
            run.all_vehicles, inv_loc_stats = filter_sister_store_vehicles(
                run.all_vehicles, site_profile, source="inventory"
            )
            run.result["sister_store_inventory"] = inv_loc_stats
            run.result["deduped_rows"] = len(run.all_vehicles)
    except Exception as loc_e:
        logger.warning(
            "Sister-store inventory filter failed for %s (continuing): %s",
            run.name,
            loc_e,
        )

    run.result["vins"] = _vin_list(run.all_vehicles)


async def prefetch_details(run: DealerRun) -> None:
    """Prior-scan immutable fields by VIN + HTTP detail-page structured data
    (curl_cffi detail pages + VDP recipes) — the whole per-car layer now that the
    browser VDP pool is gone."""
    result, name = run.result, run.name
    try:
        from backend.scanner.vdp.prefetch import prefetch_before_vdp

        _pf = await prefetch_before_vdp(run.all_vehicles, run.dealer_id, name)
        if _pf:
            result["vdp_prefetch"] = _pf
    except Exception as _pf_e:
        logger.warning("VDP prefetch failed [%s] (continuing): %s", name, _pf_e)

    result["vdps_visited"] = 0
    result["vehicles_vdp_enriched"] = int((result.get("vdp_prefetch") or {}).get("http_first", {}).get("fields_filled", 0) or 0)
    result["gallery_vdp_urls_added"] = int((result.get("vdp_prefetch") or {}).get("http_first", {}).get("galleries_extended", 0) or 0)
    log_gallery_bins(name, "after_vdp", run.all_vehicles)
    result["gallery_bins"] = gallery_https_bin_histogram(run.all_vehicles)


def drop_detail_page_sister_rows(run: DealerRun) -> None:
    """Drop rows the detail-page location check marked ``_sister_store_exclude``."""
    result, name = run.result, run.name
    pre_vdp_n = len(run.all_vehicles)
    _vdp_kept = [v for v in run.all_vehicles if not v.get("_sister_store_exclude")]
    vdp_excluded = pre_vdp_n - len(_vdp_kept)
    if not vdp_excluded:
        return
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
        run.all_vehicles = _vdp_kept
        result["sister_store_vdp_excluded"] = vdp_excluded
        result["deduped_rows"] = len(run.all_vehicles)
        result["vins"] = _vin_list(run.all_vehicles)
        logger.info(
            "Sister-store filter [%s] VDP: excluded %d vehicle(s) after detail-page location check",
            name,
            vdp_excluded,
        )


def normalize_galleries(run: DealerRun) -> None:
    """Gallery is always a list for the DB (stored as json.dumps(gallery)); a row
    with no http image falls back to its hero image."""
    for v in run.all_vehicles:
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


async def run_gallery_vision(run: DealerRun, gallery_vision_filter: bool) -> None:
    if not (gallery_vision_filter and gallery_vision_inline_enabled()):
        return
    try:
        gv = await asyncio.to_thread(apply_gallery_vision_filter_to_vehicles, run.all_vehicles)
        run.result["gallery_vision"] = gv
        logger.info(
            "Gallery vision filter [%s]: dropped %s of %s unique HTTPS image URLs (Claude)",
            run.name,
            gv.get("gallery_vision_unique_dropped"),
            gv.get("gallery_vision_unique_before"),
        )
    except Exception as e:
        logger.warning(
            "Gallery vision filter failed for %s (saving unfiltered images): %s",
            run.name,
            e,
        )
        run.result["gallery_vision"] = {"error": str(e)[:200]}


async def run_monroney_vision(run: DealerRun, monroney_vision: bool) -> None:
    if not monroney_vision:
        return
    try:
        mv = await asyncio.to_thread(apply_monroney_vision_to_vehicles, run.all_vehicles)
        run.result["monroney_vision"] = mv
        logger.info(
            "Monroney vision [%s]: rows_touched=%s sticker_image_calls=%s page_text_calls=%s",
            run.name,
            mv.get("rows_touched"),
            mv.get("sticker_image_calls"),
            mv.get("page_text_calls"),
        )
    except Exception as e:
        logger.warning("Monroney vision failed for %s: %s", run.name, e)
        run.result["monroney_vision"] = {"error": str(e)[:200]}


def stamp_registry_and_source_urls(run: DealerRun) -> None:
    """The manifest's dealership_registry_id (else resolved from the URL) onto
    every row that lacks one, then each row's canonical source URL."""
    reg_id = run.dealer.get("dealership_registry_id")
    if not reg_id:
        try:
            from backend.listings.dealer_registry_match import resolve_car_dealership_registry_id

            reg_id = resolve_car_dealership_registry_id({"dealer_url": run.url}) or None
        except Exception as e:
            logger.debug("dealership_registry_id fallback resolution failed for %s: %s", run.url, e)
            reg_id = None
    if reg_id:
        for v in run.all_vehicles:
            v.setdefault("dealership_registry_id", reg_id)
    run.reg_id = reg_id
    for v in run.all_vehicles:
        apply_vehicle_source_url(v)


def apply_vin_facts(run: DealerRun) -> None:
    """NHTSA vPIC drivetrain / fuel outrank the dealer feed (cached decodes only)."""
    try:
        from backend.enrichment.vpic_facts import override_vehicles

        _vf = override_vehicles(run.all_vehicles)
        run.result["vin_facts"] = _vf
        if _vf.get("drivetrain") or _vf.get("fuel_type"):
            logger.info(
                "VIN facts [%s]: %d cached decode(s); drivetrain overridden on %d, fuel on %d",
                run.name, _vf["cached"], _vf["drivetrain"], _vf["fuel_type"],
            )
    except Exception as _vf_e:
        logger.warning("VIN facts failed [%s] (continuing): %s", run.name, _vf_e)
