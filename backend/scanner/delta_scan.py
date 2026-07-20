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
# Priceless feeds up to this size get per-car VDP price completion over HTTP
# (cosmos SRP teaser feeds return 12-24 VINs, so this stays cheap).
_VDP_PRICE_COMPLETE_MAX = 40


def _write_hint_note(dealer_id: str, note: str, extra: dict[str, Any] | None = None) -> None:
    """Record what this run learned about a dealer (best-effort, merge semantics)."""
    try:
        from backend.scanner.recipe_store import set_scan_hints

        hints: dict[str, Any] = {"notes": note[:300], "hint_source": "delta_scan_auto"}
        if extra:
            hints.update(extra)
        set_scan_hints(dealer_id, hints)
    except Exception:
        pass


def _complete_prices_from_vdp(vehicles: list[dict[str, Any]], name: str) -> int:
    """
    Fill missing prices by fetching each car's own VDP page over HTTP.

    Hard-bounded regardless of how it was invoked (the price_source=vdp hint is
    auto-written for ANY priceless feed, including huge ones): at most
    ``_VDP_PRICE_COMPLETE_MAX`` fetch attempts and a wall-clock deadline well
    inside the per-dealer timeout, so a slow dealer can't strand the worker
    thread past the sweep's 300s cap.
    """
    from backend.scanner.post_scan.gap_fill import fetch_listing_html
    from backend.scanner.utils.vdp_spec_parse import parse_price_from_listing_html

    deadline = time.monotonic() + 120.0
    attempts = 0
    filled = 0
    for v in vehicles:
        if attempts >= _VDP_PRICE_COMPLETE_MAX or time.monotonic() > deadline:
            logger.info(
                "Delta [%s]: VDP price completion stopped at cap (%d attempts)",
                name, attempts,
            )
            break
        try:
            price = v.get("price")
            if price and float(price) > 0:
                continue
        except (TypeError, ValueError):
            pass
        vdp_url = (v.get("source_url") or v.get("url") or "").strip()
        if not vdp_url.startswith("http"):
            continue
        attempts += 1
        try:
            html = fetch_listing_html(vdp_url)
            page_price = parse_price_from_listing_html(html) if html else None
            if page_price and page_price > 0:
                v["price"] = int(round(page_price))
                filled += 1
        except Exception:
            continue
    if filled:
        logger.info("Delta [%s]: VDP price completion fetched %d price(s)", name, filled)
    return filled


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
        logger.info("Delta [%s]: skipped — %s", name, out["skipped"])
        return out

    # Per-dealer scan instructions (dealer_recipes.scan_hints) — consulted
    # before any work so hinted skips/conditions are honored and logged.
    hints: dict[str, Any] = {}
    try:
        from backend.scanner.recipe_store import get_scan_hints

        hints = await asyncio.to_thread(get_scan_hints, dealer_id)
    except Exception:
        hints = {}
    if hints.get("skip_reason"):
        out["skipped"] = f"hinted_skip: {hints['skip_reason']}"
        logger.info("Delta [%s]: skipped — %s", name, out["skipped"])
        return out

    t0 = time.perf_counter()
    fetched = await try_fetch_via_recipes(dealer_id, provider, url, name, union=True)
    if not fetched:
        if hints.get("requires_browser"):
            out["skipped"] = "no_recipe (requires_browser hinted)"
        elif hints.get("needs_http_proxy"):
            out["skipped"] = "no_recipe (needs_http_proxy hinted — rerun with SCANNER_HTTP_PROXY)"
        else:
            out["skipped"] = "no_recipe_yield"
            _write_hint_note(dealer_id, "no recipe yield on delta replay; needs synth (full scan) or browser")
        logger.info("Delta [%s]: skipped — %s", name, out["skipped"])
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
        # Small SRP feeds (cosmos teaser: ~12-24 VINs) keep price on the VDP;
        # per-car VDP fetch can plausibly cover them. But per-VDP completion is
        # only viable when the WHOLE feed fits under the fetch cap — a large
        # priceless feed (e.g. a Sonic/Akamai Dealer.com store whose getInventory
        # omits price and whose pricing API + VDP are bot-walled to 40x slow
        # browser renders) can never reach the coverage bar this way and should
        # be deferred to the full browser scan instead of burning the cap.
        completed = 0

        def _priced(_v: dict) -> bool:
            # Same semantics as _price_coverage: a non-numeric price ("Call",
            # "N/A") is not a positive price and must not raise (an unguarded
            # float() here aborted the whole dealer delta scan).
            try:
                return float(_v.get("price") or 0) > 0
            except (TypeError, ValueError):
                return False

        missing_price = sum(1 for _v in vehicles if not _priced(_v))
        if missing_price <= _VDP_PRICE_COMPLETE_MAX:
            completed = await asyncio.to_thread(_complete_prices_from_vdp, vehicles, name)
            price_cov = _price_coverage(vehicles)
            out["vdp_price_completed"] = completed
        if price_cov < _MIN_PRICE_COVERAGE:
            out["skipped"] = f"low_price_coverage ({price_cov:.0%})"
            logger.info("Delta [%s]: skipped — %s", name, out["skipped"])
            if missing_price > _VDP_PRICE_COMPLETE_MAX:
                # Too many priceless rows for VDP completion — needs the full
                # browser scan to price at scale.
                _write_hint_note(
                    dealer_id,
                    f"{missing_price} priceless rows on delta replay (feed carries no price, "
                    f"pricing API/VDP bot-walled); needs full browser scan to price",
                    extra={"price_requires_full_scan": True, "price_source": None},
                )
            elif hints.get("price_source") != "vdp":
                _write_hint_note(
                    dealer_id,
                    f"feed priced {price_cov:.0%} on delta replay; price likely lives on VDP/second endpoint",
                    extra={"price_source": "vdp"},
                )
            return out
        logger.info(
            "Delta [%s]: VDP price completion filled %d price(s) → %.0f%% priced, proceeding",
            name, completed, price_cov * 100,
        )

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

    from backend.scanner.inventory_reconcile import normalized_vin_set_from_vehicles

    scraped_norm = normalized_vin_set_from_vehicles(vehicles)
    # Retirement compares VALID normalized VINs, so its authorization gate must
    # too — raw row count includes placeholder/unknown VINs that would let a
    # junk-heavy feed retire real inventory it never actually covered.
    valid_cov = (len(scraped_norm) / known) if known else 1.0
    if valid_cov >= _RECONCILE_VIN_COVERAGE:
        try:
            from backend.scanner.inventory_reconcile import (
                reconcile_dealer_inventory_after_scan,
            )

            # Reconcile's safety gate reads stats["deduped_rows"]; the delta
            # path never set it, so every dealer failed below_min_rows and no
            # car was EVER marked inactive (found 2026-07-19: listing_removed_at
            # was NULL on all 58k rows). Unique scraped VINs is the deduped
            # row count for a delta run.
            out["deduped_rows"] = len(scraped_norm)
            out["reconcile"] = await asyncio.to_thread(
                reconcile_dealer_inventory_after_scan, dealer_id, url, scraped_norm, out
            )
        except Exception as e:
            logger.warning("Delta reconcile failed for %s: %s", name, e)
    else:
        out["reconcile"] = {"ran": False, "skipped_reason": f"valid_vin_coverage {valid_cov:.0%} < 80%"}

    # Autostart post-scan enrichment for the VINs this dealer just refreshed:
    # EPA/vPIC structured backfill + listing-page (VDP) recovery for still-missing
    # specs/colors. Full scans run this via the post-scan pipeline; the delta
    # path did not, so delta-refreshed cars stayed incomplete. Gated by
    # SCANNER_POST_LISTING_GAP_FILL (default on); skipped when 0.
    if _delta_gap_fill_enabled() and scraped_norm:
        try:
            from backend.scanner.post_scan.gap_fill import run_listing_gap_fill_for_vins

            gf = await asyncio.to_thread(run_listing_gap_fill_for_vins, list(scraped_norm))
            out["gap_fill"] = gf
            if gf.get("rows_patched"):
                logger.info(
                    "Delta [%s]: post-scan enrichment patched %d row(s)",
                    name, gf.get("rows_patched", 0),
                )
        except Exception as e:
            logger.warning("Delta [%s]: post-scan enrichment failed: %s", name, e)

    out["seconds"] = round(time.perf_counter() - t0, 1)
    logger.info(
        "Delta [%s]: %d vehicle(s) upserted (%.0f%% of known lot, %.0f%% priced) in %.1fs",
        name, count, vin_cov * 100, price_cov * 100, out["seconds"],
    )
    return out


def _delta_gap_fill_enabled() -> bool:
    """Post-scan enrichment on delta runs (default on; SCANNER_POST_LISTING_GAP_FILL=0 to disable)."""
    return (os.environ.get("SCANNER_POST_LISTING_GAP_FILL") or "1").strip().lower() not in (
        "0", "false", "no", "off",
    )


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
