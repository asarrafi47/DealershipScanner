"""
Recipe-driven delta scan: refresh a dealer's inventory over plain HTTP — no
browser — by replaying the JSON endpoints captured during full scans.

Purpose: cheap, frequent freshness (price changes, new arrivals, sold cars)
between full browser scans. Accuracy contract:
- Rows flow through the SAME parse → prefetch-merge → upsert path as full
  scans, so field semantics are identical and empty values never overwrite
  stored data.
- A dealer is skipped outright when the replay yield fails the quality gate
  (VIN coverage vs. the dealer's known active inventory, and price coverage) —
  a thin or junk feed can neither pollute rows nor mark cars sold.
- Reconcile (mark-inactive) additionally requires near-full VIN coverage on
  top of its own built-in safety gates.

Run: ``python scanner.py --manifest X --delta`` (dealers without usable
recipes are skipped and should be covered by the regular full scan).
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Any

logger = logging.getLogger("scanner")

# Below these, the replay is treated as a partial/junk feed and the dealer skipped.
_MIN_VIN_COVERAGE = 0.5      # of known active inventory — gate for upserting at all
_RECONCILE_VIN_COVERAGE = 0.8  # gate for allowing mark-inactive
_MIN_PRICE_COVERAGE = 0.5


def _delta_concurrency() -> int:
    try:
        return max(1, min(12, int((os.environ.get("SCANNER_DELTA_CONCURRENCY") or "4").strip())))
    except ValueError:
        return 4


def _active_count(dealer_id: str) -> int:
    from backend.db.inventory_db import db_conn

    with db_conn() as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM cars WHERE dealer_id = ? AND COALESCE(listing_active, 1) = 1",
            (dealer_id,),
        ).fetchone()
    return int(row[0]) if row else 0


async def delta_scan_dealer(dealer: dict[str, Any]) -> dict[str, Any]:
    from backend.parsers import parse
    from backend.scanner.inventory_recovery import _dedupe_vin_list, _price_coverage
    from backend.scanner.recipes import try_fetch_via_recipes

    dealer_id = str(dealer.get("dealer_id") or "").strip()
    url = str(dealer.get("url") or "").strip()
    name = str(dealer.get("name") or dealer_id).strip()
    provider = str(dealer.get("provider") or "").strip()
    out: dict[str, Any] = {"dealer_id": dealer_id, "skipped": None, "upserted": 0}
    if not dealer_id or not url:
        out["skipped"] = "bad_manifest_entry"
        return out

    t0 = time.perf_counter()
    fetched = await try_fetch_via_recipes(dealer_id, provider, url, name, union=True)
    if not fetched:
        out["skipped"] = "no_recipe_yield"
        return out
    records, _vin_yield = fetched

    vehicles: list[dict[str, Any]] = []
    for _rec_url, body in records:
        vehicles.extend(parse(
            provider, body, base_url=url, dealer_id=dealer_id,
            dealer_name=name, dealer_url=url,
        ))
    for v in vehicles:
        v.setdefault("dealer_name", name)
        v.setdefault("dealer_url", url)
    vehicles = _dedupe_vin_list(vehicles)
    n = len(vehicles)
    known = _active_count(dealer_id)
    vin_cov = (n / known) if known else 1.0
    price_cov = _price_coverage(vehicles)

    # Quality gate: a partial or price-less replay must not touch the DB.
    if known and vin_cov < _MIN_VIN_COVERAGE:
        out["skipped"] = f"partial_feed ({n}/{known} known VINs)"
        logger.info("Delta [%s]: skipped — %s", name, out["skipped"])
        return out
    if price_cov < _MIN_PRICE_COVERAGE:
        out["skipped"] = f"low_price_coverage ({price_cov:.0%})"
        logger.info("Delta [%s]: skipped — %s", name, out["skipped"])
        return out

    # Same enrichment-carry as full scans (immutable fields only; never price).
    try:
        from backend.scanner.vdp.prefetch import merge_known_fields_from_db, vdp_db_merge_enabled

        if vdp_db_merge_enabled():
            await asyncio.to_thread(merge_known_fields_from_db, vehicles, dealer_id)
    except Exception as e:
        logger.debug("Delta [%s]: db merge skipped: %s", name, e)

    from backend.scanner.inventory_write import InventoryWriteCoordinator

    coordinator = InventoryWriteCoordinator()
    count = await coordinator.upsert_vehicles(vehicles)
    out["upserted"] = count

    # Team Velocity feeds carry imageUrls:null — photos (and the per-car Carfax
    # link) live only on the VDP. Run the platform's own completion step for this
    # dealer so delta-refreshed rows get real images, not just the placeholder.
    try:
        from backend.parsers.team_velocity import detect as _detect_tv
        from backend.parsers.team_velocity import recover_dealer as _tv_recover_dealer

        if any(_detect_tv(body) for _u, body in records):
            tv_stats = await asyncio.to_thread(_tv_recover_dealer, dealer_id)
            out["tv_image_completion"] = tv_stats
            logger.info(
                "Delta [%s]: TV image completion — %d patched (%d imgs, %d carfax) of %d candidates",
                name, tv_stats.get("rows_patched", 0), tv_stats.get("with_images", 0),
                tv_stats.get("with_carfax", 0), tv_stats.get("candidates", 0),
            )
    except Exception as e:
        logger.warning("Delta [%s]: TV image completion failed: %s", name, e)

    if vin_cov >= _RECONCILE_VIN_COVERAGE:
        try:
            from backend.scanner.inventory_reconcile import (
                normalized_vin_set_from_vehicles,
                reconcile_dealer_inventory_after_scan,
            )

            scraped_norm = normalized_vin_set_from_vehicles(vehicles)
            out["reconcile"] = await asyncio.to_thread(
                reconcile_dealer_inventory_after_scan, dealer_id, url, scraped_norm, out
            )
        except Exception as e:
            logger.warning("Delta reconcile failed for %s: %s", name, e)
    else:
        out["reconcile"] = {"ran": False, "skipped_reason": f"vin_coverage {vin_cov:.0%} < 80%"}

    out["seconds"] = round(time.perf_counter() - t0, 1)
    logger.info(
        "Delta [%s]: %d vehicle(s) upserted (%.0f%% of known lot, %.0f%% priced) in %.1fs",
        name, count, vin_cov * 100, price_cov * 100, out["seconds"],
    )
    return out


def _delta_dealer_timeout() -> int:
    """Hard per-dealer wall-clock cap (seconds). A slow/walled dealer whose
    recipe replay drags (huge paced pagination, slow-loris) must not stall the
    whole sweep — bound it and move on. 0 disables. Default 300s."""
    try:
        return max(0, int(os.environ.get("SCANNER_DELTA_DEALER_TIMEOUT", "300")))
    except ValueError:
        return 300


async def run_delta_scan(dealers: list[dict[str, Any]]) -> dict[str, Any]:
    """Delta-refresh every manifest dealer that has a usable recipe."""
    sem = asyncio.Semaphore(_delta_concurrency())
    per_dealer_timeout = _delta_dealer_timeout()

    async def _one(d: dict[str, Any]) -> dict[str, Any]:
        async with sem:
            try:
                coro = delta_scan_dealer(d)
                if per_dealer_timeout:
                    return await asyncio.wait_for(coro, timeout=per_dealer_timeout)
                return await coro
            except asyncio.TimeoutError:
                logger.warning(
                    "Delta scan timed out for %s after %ds — skipping",
                    d.get("dealer_id"), per_dealer_timeout,
                )
                return {"dealer_id": d.get("dealer_id"),
                        "skipped": f"timeout>{per_dealer_timeout}s", "upserted": 0}
            except Exception as e:
                logger.warning("Delta scan failed for %s: %s", d.get("dealer_id"), e)
                return {"dealer_id": d.get("dealer_id"), "skipped": f"error: {e}", "upserted": 0}

    results = await asyncio.gather(*(_one(d) for d in dealers))
    refreshed = [r for r in results if not r.get("skipped")]
    skipped = [r for r in results if r.get("skipped")]
    summary = {
        "dealers": len(dealers),
        "refreshed": len(refreshed),
        "skipped": len(skipped),
        "vehicles_upserted": sum(r.get("upserted", 0) for r in results),
        "results": results,
    }
    logger.info(
        "Delta scan complete: %d/%d dealer(s) refreshed, %d vehicle(s) upserted, %d skipped",
        summary["refreshed"], summary["dealers"], summary["vehicles_upserted"], summary["skipped"],
    )
    return summary
