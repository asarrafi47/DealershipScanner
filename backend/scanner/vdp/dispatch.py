"""
VDP enrichment entry point: builds the per-dealer visit queue and dispatches ``_vdp_visit_one``.

Split out of ``backend/scanner/vdp/core.py`` (2026-09). ``enrich_vehicles_vdp`` is the scanner's
public VDP entry point (imported by ``backend.scanner.phases.dealer_run``); it selects work from
EP/price/spec-gap/description caps, runs the worker-page pool, and folds per-visit results into
scanner stats. Re-imported from ``backend.scanner.vdp.core`` for compatibility with the
historical import surface.
"""
from __future__ import annotations

import asyncio
import logging
from collections import Counter
from typing import Any

from backend.utils.gallery_merge import gallery_https_bin_histogram
from backend.scanner.utils.vdp_price_merge import listing_price_is_empty

from backend.scanner.vdp.config import (
    _max_vdp_concurrency,
    _vdp_description_max_per_dealer,
    _vdp_max_per_dealer,
    _vdp_price_max_per_dealer,
    _vdp_spec_gap_max_per_dealer,
)
from backend.scanner.vdp.extract import _looks_like_vin17
from backend.scanner.vdp.price_hints import _ripple_vdp_price_same_detail_url
from backend.scanner.vdp.queue import (
    _vdp_field_gap_score,
    _vdp_public_incomplete_gap_score,
    _vdp_queue_sort_key,
    _vdp_rotation_enabled,
    _vdp_rotation_seed,
    _vehicle_needs_description_vdp,
    _vehicle_needs_spec_gap_vdp,
)
from backend.scanner.vdp.visit import _vdp_visit_one

log = logging.getLogger("scanner.vdp")


async def enrich_vehicles_vdp(
    page,
    vehicles: list[dict[str, Any]],
    dealer_name: str,
    *,
    dealer_id: str = "",
    site_profile: Any = None,
    provider: str = "",
    ep_max: int | None = None,
    price_max: int | None = None,
    description_max: int | None = None,
) -> dict[str, Any]:
    """
    Mutates vehicles in place: runs VDP extraction and merge_analytics_ep_into_vehicle.
    Expects vehicles deduped by VIN; uses _detail_url when present.

    Returns stats for scanner timing: vdps_visited, vehicles_enriched, rows_inventory (input len).
    """
    stats: dict[str, Any] = {
        "vdps_visited": 0,
        "vehicles_enriched": 0,
        "gallery_vdp_urls_added": 0,
        "inventory_rows": len(vehicles),
        "skipped_no_detail_url": False,
        "gallery_phase_bins": {},
    }
    # Caps come from the caller as arguments (per-dealer, race-free); when omitted
    # they fall back to the process env for standalone callers (scripts/tests).
    ep_cap = _vdp_max_per_dealer(ep_max)
    price_cap = _vdp_price_max_per_dealer(price_max)
    spec_gap_cap = _vdp_spec_gap_max_per_dealer()
    description_cap = _vdp_description_max_per_dealer(description_max)
    if ep_cap == 0 and price_cap == 0 and spec_gap_cap == 0 and description_cap == 0:
        log.info(
            "VDP: %s — enrichment skipped (SCANNER_VDP_EP_MAX=0, SCANNER_VDP_PRICE_MAX=0, SCANNER_VDP_SPEC_GAP_MAX=0)",
            dealer_name,
        )
        return stats

    bins_before = gallery_https_bin_histogram(vehicles)
    seed = _vdp_rotation_seed(dealer_id)
    rot = _vdp_rotation_enabled()
    vehicles.sort(key=lambda v: _vdp_queue_sort_key(v, seed, rotation=rot))

    log.info(
        "VDP: %s — enrichment enabled (EP cap=%d, price-extra cap=%d, spec-gap cap=%d, description cap=%d; rotation=%s)",
        dealer_name,
        ep_cap,
        price_cap,
        spec_gap_cap,
        description_cap,
        rot,
    )

    if not any(str(v.get("_detail_url") or "").strip().startswith("http") for v in vehicles):
        log.info(
            "VDP: %s — no _detail_url on inventory rows; skipping VDP visits (listing JSON may omit VDP links)",
            dealer_name,
        )
        stats["skipped_no_detail_url"] = True
        return stats

    work: list[tuple[dict[str, Any], str, str]] = []
    seen_urls: set[str] = set()
    for v in vehicles:
        if len(work) >= ep_cap:
            break
        u = (v.get("_detail_url") or "").strip()
        if not u.startswith("http"):
            continue
        if u in seen_urls:
            continue
        vin = (v.get("vin") or "").strip().upper()
        if not _looks_like_vin17(vin):
            continue
        seen_urls.add(u)
        work.append((v, u, vin))

    if price_cap > 0:
        added = 0
        for v in vehicles:
            if added >= price_cap:
                break
            if not listing_price_is_empty(v):
                continue
            u = (v.get("_detail_url") or "").strip()
            if not u.startswith("http"):
                continue
            if u in seen_urls:
                continue
            vin = (v.get("vin") or "").strip().upper()
            if not _looks_like_vin17(vin):
                continue
            seen_urls.add(u)
            work.append((v, u, vin))
            added += 1

    if spec_gap_cap > 0:
        vehicles.sort(
            key=lambda v: (
                -_vdp_field_gap_score(v),
                -_vdp_public_incomplete_gap_score(v),
            )
        )
        added = 0
        for v in vehicles:
            if added >= spec_gap_cap:
                break
            if not _vehicle_needs_spec_gap_vdp(v):
                continue
            u = (v.get("_detail_url") or "").strip()
            if not u.startswith("http"):
                continue
            if u in seen_urls:
                continue
            vin = (v.get("vin") or "").strip().upper()
            if not _looks_like_vin17(vin):
                continue
            seen_urls.add(u)
            work.append((v, u, vin))
            added += 1

    if description_cap > 0:
        added = 0
        for v in vehicles:
            if added >= description_cap:
                break
            if not _vehicle_needs_description_vdp(v):
                continue
            u = (v.get("_detail_url") or "").strip()
            if not u.startswith("http"):
                continue
            if u in seen_urls:
                continue
            vin = (v.get("vin") or "").strip().upper()
            if not _looks_like_vin17(vin):
                continue
            seen_urls.add(u)
            work.append((v, u, vin))
            added += 1

    if not work:
        return stats

    conc = min(_max_vdp_concurrency(), len(work))
    preview_lock = asyncio.Lock()
    preview_budget = [2]
    field_fill_counter: Counter[str] = Counter()
    skip_reason_counter: Counter[str] = Counter()
    vehicles_enriched = 0
    visited = 0
    gallery_urls_added_total = 0

    log.info("VDP pool: %s — %d concurrent worker page(s) (%d visit(s) queued)", dealer_name, conc, len(work))

    async def aggregate_one(r: dict[str, Any]) -> None:
        nonlocal visited, vehicles_enriched, gallery_urls_added_total
        visited += int(r.get("visited", 0))
        if r.get("enriched"):
            vehicles_enriched += 1
        gallery_urls_added_total += int(r.get("gallery_added") or 0)
        for fn in r.get("filled") or []:
            field_fill_counter[fn] += 1
        if r.get("price_updated"):
            field_fill_counter["price"] += 1
        for sk in r.get("skipped") or []:
            head = sk.split(":", 1)[0].strip() if ":" in str(sk) else str(sk).strip()
            if head:
                skip_reason_counter[head] += 1

    worker_pages: list[Any] = []

    try:
        if conc <= 1:
            for v, u, vin in work:
                try:
                    r = await _vdp_visit_one(
                        page, dealer_name, v, u, vin, preview_lock, preview_budget,
                        site_profile=site_profile, provider=provider,
                    )
                    await aggregate_one(r)
                except Exception as e:
                    log.warning("VDP: %s — visit error (continuing): %s", dealer_name, e)
        else:
            ctx = page.context
            worker_pages = [await ctx.new_page() for _ in range(conc)]
            pool: asyncio.Queue[Any] = asyncio.Queue()
            for wp in worker_pages:
                await pool.put(wp)

            async def run_item(item: tuple[dict[str, Any], str, str]) -> None:
                v, u, vin = item
                wp = await pool.get()
                try:
                    r = await _vdp_visit_one(
                        wp, dealer_name, v, u, vin, preview_lock, preview_budget,
                        site_profile=site_profile, provider=provider,
                    )
                    await aggregate_one(r)
                except Exception as e:
                    log.warning("VDP: %s — visit error (continuing): %s", dealer_name, e)
                finally:
                    await pool.put(wp)

            results = await asyncio.gather(*[run_item(w) for w in work], return_exceptions=True)
            for res in results:
                if isinstance(res, Exception):
                    log.warning("VDP: %s — worker task failed: %s", dealer_name, res)

        _ripple_vdp_price_same_detail_url(vehicles)

        stats["vdps_visited"] = visited
        stats["vehicles_enriched"] = vehicles_enriched
        stats["gallery_vdp_urls_added"] = gallery_urls_added_total
        stats["gallery_phase_bins"] = {
            "before_vdp": bins_before,
            "after_vdp": gallery_https_bin_histogram(vehicles),
        }
        log.info(
            "VDP: %s — phase summary: visits=%d vehicles_enriched=%d gallery_urls_added=%d "
            "top_fields_filled=%s common_skip_reasons=%s gallery_bins_after=%s",
            dealer_name,
            visited,
            vehicles_enriched,
            gallery_urls_added_total,
            field_fill_counter.most_common(14),
            skip_reason_counter.most_common(10),
            stats["gallery_phase_bins"].get("after_vdp"),
        )
    finally:
        for wp in worker_pages:
            try:
                await wp.close()
            except Exception:
                pass

    return stats
