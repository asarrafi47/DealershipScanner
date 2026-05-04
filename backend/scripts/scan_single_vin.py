#!/usr/bin/env python3
"""
Re-scan a single vehicle by VIN (or car id) using the same pipeline as ``scanner.py`` for one row:

- Resolve the dealer (from ``dealers.json`` or the row) and VDP URL candidate(s)
- Open Playwright, probe VDP URL(s) — if every candidate returns 404/410, soft-unlist
  the row (``listing_active=0``) and remove it from ``incomplete_listings``
- Run ``enrich_vehicles_vdp`` (detail page: EP merge, gallery harvest, price hints, etc.)
- Optional LLaVA gallery + Monroney passes (same helpers as the main scanner)
- ``upsert_vehicles`` into ``inventory.db``
- Post pipeline: repair, listing description → packages, interior vision, optional vision-only
  ``InventoryEnricher``
- Spec pipelines (default on): ``run_spec_backfill_for_car`` (EPA trim+aggregate, optional VDP HTML,
  Google→fueleconomy when MPG/cyl still need help) and ``apply_structured_spec_backfill_for_car`` (NHTSA vPIC),
  then re-sync the incomplete index

Run from project root::

    python scan_single_vin.py --vin 1HGBH41JXMN109186
    python scan_single_vin.py --incomplete-random
"""
from __future__ import annotations

import argparse
import asyncio
import copy
import json
import logging
import os
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from backend.utils.project_env import load_project_dotenv

    load_project_dotenv()
except ImportError:
    pass

from backend.db import incomplete_listings_db as incomplete_db
from backend.db.inventory_db import get_car_by_id, get_car_by_vin, refresh_car_data_quality_score, update_car_row_partial
from backend.scanner.database import upsert_vehicles
from backend.parsers.vdp_urls import dealer_style_vdp_url_candidates, looks_like_real_vin
from backend.scanner.post_pipeline import run_post_scan

log = logging.getLogger("scan_single_vin")

GONE_STATUS = frozenset({404, 410})


def _load_manifest() -> list[dict[str, Any]]:
    p = ROOT / "dealers.json"
    if not p.is_file():
        return []
    return json.loads(p.read_text(encoding="utf-8"))


def _dealer_config_for_row(car: dict[str, Any]) -> dict[str, Any] | None:
    did = (car.get("dealer_id") or "").strip()
    for d in _load_manifest():
        if (d.get("dealer_id") or "").strip() == did:
            return d
    return None


def _dealer_for_scan(car: dict[str, Any]) -> dict[str, Any]:
    """Dealer block with url, name, provider, dealer_id (manifest or inferred from the row)."""
    m = _dealer_config_for_row(car)
    if m:
        return dict(m)
    base = (car.get("dealer_url") or "").strip()
    if not base.lower().startswith("http"):
        raise SystemExit("Car row has no manifest match and no usable dealer_url; fix dealers.json or the row.")
    return {
        "dealer_id": (car.get("dealer_id") or "").strip() or "unknown",
        "name": (car.get("dealer_name") or "").strip() or "Dealer",
        "url": base.rstrip("/"),
        "provider": (car.get("provider") or "dealer_dot_com"),
    }


def _stub_for_vdp_paths(car: dict[str, Any]) -> dict[str, Any]:
    return {
        "title": car.get("title") or "",
        "vehicleCondition": car.get("condition") or "",
        "inventoryType": car.get("condition") or "",
        "condition": car.get("condition") or "",
    }


def _ordered_vdp_urls(car: dict[str, Any], dealer_base: str) -> list[str]:
    """Prefer stored ``source_url``, then common Dealer.com-style VDP paths for this VIN."""
    vin = (car.get("vin") or "").strip().upper()
    if not looks_like_real_vin(vin):
        return []
    base = (dealer_base or "").strip().rstrip("/")
    stub = _stub_for_vdp_paths(car)
    seen: set[str] = set()
    out: list[str] = []
    su = (car.get("source_url") or "").strip()
    if su.lower().startswith("http"):
        out.append(su)
        seen.add(su)
    for u in dealer_style_vdp_url_candidates(base, vin, stub):
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def _nav_timeout_ms() -> int:
    raw = (os.environ.get("SCANNER_VDP_NAV_TIMEOUT_MS") or "32000").strip()
    try:
        return max(5000, int(raw))
    except ValueError:
        return 32000


def _row_to_working_vehicle(
    car: dict[str, Any], dealer: dict[str, Any], detail_url: str, alts: list[str]
) -> dict[str, Any]:
    """Copy of DB row for in-place VDP / vision / upsert (no ``id``)."""
    v: dict[str, Any] = copy.deepcopy(car)
    v.pop("id", None)
    v["vin"] = (v.get("vin") or "").strip().upper()
    v["dealer_id"] = dealer.get("dealer_id") or v.get("dealer_id") or ""
    v["dealer_name"] = dealer.get("name") or v.get("dealer_name") or ""
    v["dealer_url"] = (dealer.get("url") or v.get("dealer_url") or "").strip().rstrip("/")
    g = v.get("gallery")
    if isinstance(g, str):
        try:
            v["gallery"] = json.loads(g)
        except (json.JSONDecodeError, TypeError):
            v["gallery"] = []
    elif not isinstance(g, list):
        v["gallery"] = []
    v["_detail_url"] = detail_url
    v["_detail_url_alternates"] = alts[:8]
    return v


def _all_urls_returned_gone(urls: list[str], statuses: list[int | None]) -> bool:
    if not urls or len(urls) != len(statuses):
        return False
    if any(s is None for s in statuses):
        return False
    return all(s in GONE_STATUS for s in statuses)


def _mark_no_longer_listed(car_id: int, *, reason: str) -> None:
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    log.warning("Unlisting car_id=%s: %s", car_id, reason)
    update_car_row_partial(
        car_id,
        {
            "listing_active": 0,
            "listing_removed_at": now,
        },
    )
    try:
        incomplete_db.delete_incomplete_record(car_id)
    except Exception:
        log.exception("delete_incomplete_record failed (car may still show in index until sync)")
    refresh_car_data_quality_score(car_id)


async def _probe_first_live_vdp(page: Any, urls: list[str]) -> tuple[str | None, list[int | None]]:
    """
    Return (url, list of http statuses in order) for the first non-404/410 URL.
    If every attempt is 404/410, (None, statuses).
    """
    tmo = _nav_timeout_ms()
    seen_status: list[int | None] = []
    for u in urls:
        try:
            r = await page.goto(u, wait_until="domcontentloaded", timeout=tmo)
            st = int(r.status) if r is not None else None
            seen_status.append(st)
            if st in GONE_STATUS:
                log.info("VDP candidate gone (HTTP %s): %s", st, u[:200])
                continue
            if st is not None and 200 <= st < 500:
                return u, seen_status
        except Exception as e:
            seen_status.append(None)
            log.info("VDP candidate navigation failed: %s — %s", u[:200], str(e)[:200])
    return None, seen_status


async def _run_async(
    car: dict[str, Any],
    *,
    gallery_vision: bool,
    monroney_vision: bool,
    post_repair: bool,
    post_listing: bool,
    post_interior: bool,
    post_enrich_vision_only: bool,
    post_enrich: bool,
    run_spec_bfill: bool = True,
) -> dict[str, Any]:
    from scanner import _apply_gallery_vision_filter_to_vehicles, _apply_monroney_vision_to_vehicles
    from scanner_vdp import enrich_vehicles_vdp

    out: dict[str, Any] = {
        "vin": car.get("vin"),
        "car_id": car.get("id"),
        "vdp": None,
        "upserted": 0,
        "post_scan": None,
    }
    car_id = int(car["id"])
    dealer = _dealer_for_scan(car)
    urls = _ordered_vdp_urls(car, str(dealer.get("url", "")).strip().rstrip("/"))
    if not urls:
        out["error"] = "no_vdp_urls"
        return out

    dname = (dealer.get("name") or "dealer").strip() or "dealer"
    d_id = (dealer.get("dealer_id") or "")[:200]

    old_ep = os.environ.get("SCANNER_VDP_EP_MAX")
    try:
        os.environ["SCANNER_VDP_EP_MAX"] = "1"
        use_stealth = True
        try:
            from playwright_stealth import Stealth
        except ImportError:
            use_stealth = False
        from playwright.async_api import async_playwright

        if use_stealth:
            p_cm = Stealth().use_async(async_playwright())
        else:
            log.warning("playwright_stealth not found, using plain playwright")
            p_cm = async_playwright()
        async with p_cm as p:
            browser = await p.chromium.launch(headless=True)
            try:
                ctx_opts: dict[str, Any] = {"viewport": {"width": 1920, "height": 1080}}
                _ua = (os.environ.get("SCANNER_USER_AGENT") or "").strip()
                if _ua:
                    ctx_opts["user_agent"] = _ua
                context = await browser.new_context(**ctx_opts)
                page = await context.new_page()
                live, statuses = await _probe_first_live_vdp(page, urls)
                if live is None:
                    if _all_urls_returned_gone(urls, statuses):
                        _mark_no_longer_listed(
                            car_id,
                            reason="all VDP URL candidates returned 404/410; dealer no longer offers this VIN",
                        )
                        out["unlisted"] = True
                    else:
                        out["error"] = f"vdp_unreachable_statuses={statuses!r}"
                    return out

                rest = [u for u in urls if u != live]
                vehicle = _row_to_working_vehicle(car, dealer, live, rest)
                vdp_stats = await enrich_vehicles_vdp(page, [vehicle], dname, dealer_id=d_id)
                out["vdp"] = vdp_stats

                for v in (vehicle,):
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

                if gallery_vision:
                    gv = await asyncio.to_thread(_apply_gallery_vision_filter_to_vehicles, [vehicle])
                    out["gallery_vision"] = gv
                if monroney_vision:
                    mv = await asyncio.to_thread(_apply_monroney_vision_to_vehicles, [vehicle])
                    out["monroney_vision"] = mv
                n = await asyncio.to_thread(upsert_vehicles, [vehicle])
                out["upserted"] = n
            finally:
                await browser.close()
    finally:
        if old_ep is not None:
            os.environ["SCANNER_VDP_EP_MAX"] = old_ep
        else:
            os.environ.pop("SCANNER_VDP_EP_MAX", None)

    vin_s = (car.get("vin") or "").strip().upper()
    out["post_scan"] = run_post_scan(
        [vin_s],
        post_repair=post_repair,
        post_listing_description=post_listing,
        post_interior_vision=post_interior,
        post_enrich=post_enrich and not post_enrich_vision_only,
        post_enrich_vision_only=post_enrich_vision_only,
        post_kbb=False,
    )
    if run_spec_bfill:
        from backend.enrichment.spec_backfill import run_spec_backfill_for_car
        from backend.enrichment.spec_structured_backfill import apply_structured_spec_backfill_for_car

        cid = int(car["id"])

        def _spec_followup() -> dict[str, Any]:
            r1 = run_spec_backfill_for_car(cid, use_vdp=True, use_search=True)
            r2 = apply_structured_spec_backfill_for_car(cid, use_vpic_cache=True)
            try:
                incomplete_db.sync_incomplete_listing_for_car_id(cid)
            except Exception:
                log.exception("incomplete index sync after spec backfill")
            return {
                "spec_backfill": {
                    "ok": r1.ok,
                    "message": r1.message,
                    "updated_fields": r1.updated_fields,
                    "tiers": r1.tiers,
                },
                "structured_spec": {
                    "applied": r2.applied,
                    "skip_reason": r2.skip_reason,
                    "tier1_fields": r2.tier1_fields,
                    "tier2_fields": r2.tier2_fields,
                },
            }

        out["spec_pipelines"] = await asyncio.to_thread(_spec_followup)
    return out


def _vin_from_incomplete_random() -> str:
    path = os.environ.get("INCOMPLETE_LISTINGS_DB_PATH", "incomplete_listings.db")
    conn = sqlite3.connect(path)
    try:
        row = conn.execute("SELECT vin FROM incomplete_listings ORDER BY RANDOM() LIMIT 1").fetchone()
    finally:
        conn.close()
    if not row or not row[0]:
        raise SystemExit("No rows in incomplete_listings (or DB missing).")
    return str(row[0]).strip().upper()


def _resolve_car(vin: str | None, car_id: int | None) -> dict[str, Any]:
    if car_id is not None:
        row = get_car_by_id(int(car_id), include_inactive=True)
        if not row:
            raise SystemExit(f"No car with id {car_id}")
        return row
    if not vin or not str(vin).strip():
        raise SystemExit("Provide --vin, --car-id, or --incomplete-random")
    v0 = str(vin).strip().upper()
    row = get_car_by_vin(v0) or get_car_by_vin(vin.strip())
    if not row:
        raise SystemExit(f"No car with VIN {v0!r} in inventory.db")
    return row


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    ap = argparse.ArgumentParser(
        description="Re-scrape one VIN: live VDP + vision + description/packages pipeline (same as scanner for one row)."
    )
    ap.add_argument("--vin", help="17-character VIN in inventory.db")
    ap.add_argument("--car-id", type=int, help="Numeric cars.id (alternative to --vin)")
    ap.add_argument(
        "--incomplete-random",
        action="store_true",
        help="Pick a random VIN from incomplete_listings.db",
    )
    ap.add_argument(
        "--no-gallery-vision",
        action="store_true",
        help="Skip LLaVA gallery filter before upsert (default: on when SCANNER_GALLERY_VISION_FILTER is on).",
    )
    ap.add_argument(
        "--no-monroney-vision",
        action="store_true",
        help="Skip Monroney/sticker LLaVA pass (default: on).",
    )
    ap.add_argument("--no-post-repair", action="store_true", help="Skip post-scan storage repair for this VIN")
    ap.add_argument(
        "--no-post-listing-description",
        action="store_true",
        help="Skip listing description → packages parse for this VIN",
    )
    ap.add_argument("--no-post-interior-vision", action="store_true", help="Skip post-scan interior LLaVA pass")
    ap.add_argument(
        "--no-post-enrich-vision",
        action="store_true",
        help="Skip InventoryEnricher vision pass after repair (default: run it for a full single-vehicle test).",
    )
    ap.add_argument(
        "--post-enrich-vision-only",
        action="store_true",
        help="Same as default; kept for compatibility with scanner env-based workflows.",
    )
    ap.add_argument("--post-enrich", action="store_true", help="Run full enricher (needs EPA catalog indexed).")
    ap.add_argument(
        "--no-spec-backfill",
        action="store_true",
        help="Skip EPA trim+VDP+search spec backfill and vPIC after post-scan (not recommended).",
    )
    args = ap.parse_args()

    from backend.scanner.post_pipeline import (
        gallery_vision_filter_env_enabled,
        monroney_vision_env_enabled,
        post_interior_vision_env_enabled,
        post_listing_description_env_enabled,
        post_repair_env_enabled,
    )

    vin: str | None = args.vin
    if args.incomplete_random:
        vin = _vin_from_incomplete_random()
        log.info("Picked random incomplete VIN: %s", vin)
    car = _resolve_car(vin, args.car_id)
    gvf = (not args.no_gallery_vision) and gallery_vision_filter_env_enabled()
    mnv = (not args.no_monroney_vision) and monroney_vision_env_enabled()
    p_rep = (not args.no_post_repair) and post_repair_env_enabled()
    p_ld = (not args.no_post_listing_description) and post_listing_description_env_enabled()
    p_iv = (not args.no_post_interior_vision) and post_interior_vision_env_enabled()
    p_e = bool(args.post_enrich)
    p_ev = (not args.no_post_enrich_vision) and (not p_e)

    r = asyncio.run(
        _run_async(
            car,
            gallery_vision=gvf,
            monroney_vision=mnv,
            post_repair=p_rep,
            post_listing=p_ld,
            post_interior=p_iv,
            post_enrich_vision_only=p_ev,
            post_enrich=p_e,
            run_spec_bfill=not bool(args.no_spec_backfill),
        )
    )
    print(json.dumps(r, default=str, indent=2))
    if r.get("error"):
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
