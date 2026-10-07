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

# The rooftop reconcile policy is shared with the full (browser) scan path so the
# two cannot drift; see backend/scanner/rooftop_disown.py for the rules.
from backend.scanner.rooftop_disown import (
    disown_foreign_rooftop_vins as _disown_foreign_rooftop_vins,
    split_refusals,
)
# Same store place as the full scan: registry town + the page-learned street
# from scan hints. The lookup and the all-rows pass are backend.attribution's,
# shared with dealer_run.run_dealer and recipes.try_fetch_via_recipes.
from backend.attribution import DealerCtx, decide, parse_page
from backend.scanner.dealer_place import roster_place_with_hints as _roster_place  # noqa: F401 - the one place function (DealerCtx.for_store)

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
    from backend.scanner.inventory_recovery import _dedupe_vin_list, _price_coverage
    from backend.scanner.inventory_reconcile import normalized_vin_set_from_vehicles
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

    # The store's place, looked up ONCE for both the per-page gate and the
    # all-rows pass below. Without it a group feed of unnamed address-block
    # rooftops refuses every page, leaving nothing for the union pass to keep
    # and handing every VIN to _disown_foreign_rooftop_vins. The street learned
    # from the dealer page (scan hints) completes a roster that has only the
    # town, exactly as in the full scan.
    attr_ctx = await asyncio.to_thread(DealerCtx.for_store, dealer_id, name, url)
    page_kept: list[dict[str, Any]] = []
    page_refused: list[dict[str, Any]] = []
    for _rec_url, body in records:
        page_kept.extend(parse_page(provider, body, attr_ctx, page_refused))
    # Settle attribution once over the whole replay, as the full scan does: the
    # per-page pass saw one page at a time, where a page holding only an unnamed
    # sibling rooftop looks like a single-store payload and a page missing this
    # store's own rooftop refuses everything. Every row the replay produced —
    # kept or refused per page — goes into the one all-rows pass; store-scoped
    # recipe rows (_feed_scoped) are kept without the re-gate (Tutton CDJR kept 5
    # of 346 cars on 2026-09-26 when they were re-gated, and the rest were handed
    # to _disown_foreign_rooftop_vins).
    attribution = decide(page_kept + page_refused, attr_ctx)
    vehicles = attribution.kept
    refused = attribution.refused
    for v in vehicles:
        v.setdefault("dealer_name", name)
        v.setdefault("dealer_url", url)
    vehicles = _dedupe_vin_list(vehicles)
    n = len(vehicles)
    # Rows the feed assigns to a SIBLING rooftop. The store's own site served
    # them, so past scans stamped them onto this store; the feed itself is the
    # evidence that it does not sell them, which is enough to hand them back.
    # Which refusals count as that evidence is decided in one place —
    # ``rooftop_disown.EVIDENCE_BACKED_REJECTS`` — shared with the browser path.
    evidenced, unidentified = split_refusals(refused)
    foreign_vins = normalized_vin_set_from_vehicles(evidenced) - normalized_vin_set_from_vehicles(vehicles)
    out["rooftop_refused_rows"] = len(refused)
    if unidentified:
        out["rooftop_unidentified_rows"] = unidentified
        logger.warning(
            "Delta [%s]: could not identify this store among the rooftops its feed names — "
            "%d row(s) refused for storage, NOTHING un-listed. Fix by giving this dealer a "
            "street_address in the `dealerships` roster so the gate can tell it from its siblings.",
            name, unidentified,
        )
        _write_hint_note(
            dealer_id,
            "rooftop gate could not identify this store in its own group feed; "
            "needs roster street_address",
            {"rooftop_unidentified_rows": unidentified},
        )
    if foreign_vins:
        disowned = await asyncio.to_thread(_disown_foreign_rooftop_vins, dealer_id, foreign_vins)
        out["rooftop_disowned"] = disowned
        if disowned:
            logger.info(
                "Delta [%s]: %d car(s) belonged to sibling rooftops in this group feed — unlisted from this store",
                name, disowned,
            )
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
    _upsert_stats: dict = {}
    count = await coordinator.upsert_vehicles(vehicles, _upsert_stats)
    out["upserted"] = count
    if _upsert_stats.get("vin_owner_conflicts"):
        out["vin_owner_conflicts"] = _upsert_stats["vin_owner_conflicts"]
        out["vin_owner_conflict_owners"] = _upsert_stats.get("vin_owner_conflict_owners") or {}

    # Team Velocity feeds carry imageUrls:null — photos (and the per-car Carfax
    # link) live only on the VDP. Run the platform's own completion step for this
    # dealer so delta-refreshed rows get real images, not just the placeholder.
    try:
        from backend.parsers.team_velocity import detect as _detect_tv
        from backend.parsers.team_velocity import is_team_velocity_dealer as _is_tv_dealer
        from backend.parsers.team_velocity import recover_dealer as _tv_recover_dealer

        # Fire for a KNOWN TV dealer even when feed-shape detection misses (its
        # `.json` feed body can arrive as raw text that detect() won't parse) —
        # otherwise these dealers' images never get recovered on the delta path.
        if _is_tv_dealer(dealer_id) or any(_detect_tv(body) for _u, body in records):
            tv_stats = await asyncio.to_thread(_tv_recover_dealer, dealer_id)
            out["tv_image_completion"] = tv_stats
            logger.info(
                "Delta [%s]: TV image completion — %d patched (%d imgs, %d carfax) of %d candidates",
                name, tv_stats.get("rows_patched", 0), tv_stats.get("with_images", 0),
                tv_stats.get("with_carfax", 0), tv_stats.get("candidates", 0),
            )
    except Exception as e:
        logger.warning("Delta [%s]: TV image completion failed: %s", name, e)

    scraped_norm = normalized_vin_set_from_vehicles(vehicles)
    # Retirement compares VALID normalized VINs, so its authorization gate must
    # too — raw row count includes placeholder/unknown VINs that would let a
    # junk-heavy feed retire real inventory it never actually covered.
    valid_cov = (len(scraped_norm) / known) if known else 1.0
    if valid_cov >= _RECONCILE_VIN_COVERAGE:
        try:
            from backend.scanner.inventory_reconcile import (
                condition_buckets_from_vehicles,
                reconcile_dealer_inventory_after_scan,
            )

            # Reconcile's safety gate reads stats["deduped_rows"]; the delta
            # path never set it, so every dealer failed below_min_rows and no
            # car was EVER marked inactive (found 2026-07-19: listing_removed_at
            # was NULL on all 58k rows). Unique scraped VINs is the deduped
            # row count for a delta run.
            out["deduped_rows"] = len(scraped_norm)
            # The same per-condition guards as the full scan (P1A.1): without the
            # run's buckets a new-only replay that clears 80% of the lot retires
            # every used car it never asked for.
            out["reconcile"] = await asyncio.to_thread(
                reconcile_dealer_inventory_after_scan, dealer_id, url, scraped_norm, out,
                scraped_conditions=condition_buckets_from_vehicles(vehicles),
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
