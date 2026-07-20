#!/usr/bin/env python3
"""
Backfill exterior_color and interior_color for incomplete listings.

Layers:
  1. SRP re-fetch  — visit dealer inventory index pages with Playwright,
                     parse ga4ASCDataLayerVehicle / dealer_on JSON to get
                     exterior color by VIN.
  2. VDP re-fetch  — visit individual listing pages (source_url) to extract
                     both exterior and interior color from DOM spec tables
                     and embedded JSON payloads.

Usage:
  python -m backend.scripts.refetch_colors --dry-run
  python -m backend.scripts.refetch_colors
  python -m backend.scripts.refetch_colors --limit 50
  python -m backend.scripts.refetch_colors --dealer https://www.kingwindwardnissan.com
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import re
import sys
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_REPO_ROOT))

from backend.db.inventory_db import db_conn, update_car_row_partial
from backend.utils.listing_completeness import listing_missing_field_codes

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("refetch_colors")

# SRP paths to try for each dealer base URL
_SRP_PATHS = [
    "/new-inventory/index.htm",
    "/used-inventory/index.htm",
    "/certified-inventory/index.htm",
    "/new/",
    "/used/",
    "/inventory/",
]

# CSS selectors for exterior/interior color in VDP DOM
_EXT_COLOR_SELECTORS = [
    "[data-spec='color'] .value",
    "[data-spec='exteriorColor'] .value",
    ".exterior-color .value",
    ".spec-exterior-color",
    "dt:contains('Exterior') + dd",
    "[class*='exterior'][class*='color']",
    ".vehicle-detail-spec-item:has(.label:contains('Exterior')) .value",
]
_INT_COLOR_SELECTORS = [
    "[data-spec='interiorColor'] .value",
    ".interior-color .value",
    ".spec-interior-color",
    "dt:contains('Interior') + dd",
    "[class*='interior'][class*='color']",
    ".vehicle-detail-spec-item:has(.label:contains('Interior')) .value",
]


def _get_color_missing_cars(dealer_filter: str | None = None) -> list[dict]:
    with db_conn() as conn:
        cols = [d[0] for d in conn.execute("SELECT * FROM cars LIMIT 1").description]
        if dealer_filter:
            rows = conn.execute(
                "SELECT * FROM cars WHERE COALESCE(listing_active,1)=1 AND dealer_url=?",
                (dealer_filter,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM cars WHERE COALESCE(listing_active,1)=1"
            ).fetchall()
        cars = [dict(zip(cols, r)) for r in rows]

    result = []
    for car in cars:
        missing = listing_missing_field_codes(car, for_public_filter=True, include_non_actionable=True)
        if "exterior_color" in missing or "interior_color" in missing:
            result.append(car)
    return result


def _extract_colors_from_html(html: str, target_vin: str | None = None) -> dict[str, dict[str, str | None]]:
    """
    Parse ga4ASCDataLayerVehicle or dealer_on JSON from an SRP page.
    Returns {vin: {exterior_color, interior_color}}.
    """
    results: dict[str, dict[str, str | None]] = {}

    # Try ga4ASCDataLayerVehicle (ASC/dealer.com)
    m = re.search(r"ga4ASCDataLayerVehicle\s*=\s*'(\[.*?\])'\s*;", html, re.DOTALL)
    if m:
        try:
            items = json.loads(m.group(1))
            for item in items:
                vin = (item.get("item_id") or item.get("vin") or "").strip().upper()
                if not vin:
                    continue
                if target_vin and vin != target_vin.upper():
                    continue
                ext = (item.get("item_color") or item.get("exteriorColor") or "").strip() or None
                intr = (item.get("interiorColor") or item.get("interior_color") or "").strip() or None
                results[vin] = {"exterior_color": ext, "interior_color": intr}
        except (json.JSONDecodeError, TypeError):
            pass

    if results:
        return results

    # Try __PRELOADED_STATE__ or window.InventoryData
    for pattern in [
        r"__PRELOADED_STATE__\s*=\s*(\{.*?\});?\s*</script>",
        r"window\.InventoryData\s*=\s*(\[.*?\]);?\s*</script>",
        r"window\.__INITIAL_STATE__\s*=\s*(\{.*?\});?\s*</script>",
    ]:
        m2 = re.search(pattern, html, re.DOTALL)
        if not m2:
            continue
        try:
            data = json.loads(m2.group(1))
            _walk_json_for_colors(data, results, target_vin)
            if results:
                break
        except (json.JSONDecodeError, TypeError):
            pass

    return results


def _walk_json_for_colors(
    data: Any,
    results: dict[str, dict[str, str | None]],
    target_vin: str | None,
    depth: int = 0,
) -> None:
    if depth > 6:
        return
    if isinstance(data, list):
        for item in data:
            _walk_json_for_colors(item, results, target_vin, depth + 1)
    elif isinstance(data, dict):
        vin_val = (
            data.get("vin") or data.get("VIN") or data.get("item_id") or ""
        ).strip().upper()
        if vin_val and len(vin_val) == 17:
            if not target_vin or vin_val == target_vin.upper():
                ext = None
                intr = None
                for ek in ("exteriorColor", "exterior_color", "ExteriorColor", "item_color", "color", "paintColor"):
                    v = data.get(ek)
                    if v and isinstance(v, str) and v.strip():
                        ext = v.strip()
                        break
                for ik in ("interiorColor", "interior_color", "InteriorColor", "interiorTrim", "interior_trim"):
                    v = data.get(ik)
                    if v and isinstance(v, str) and v.strip():
                        intr = v.strip()
                        break
                if ext or intr:
                    results[vin_val] = {"exterior_color": ext, "interior_color": intr}
        else:
            for v in data.values():
                _walk_json_for_colors(v, results, target_vin, depth + 1)


def _extract_colors_from_vdp(html: str) -> dict[str, str | None]:
    """
    Extract exterior + interior color from a VDP page.
    Returns {"exterior_color": ..., "interior_color": ...}.
    """
    out: dict[str, str | None] = {"exterior_color": None, "interior_color": None}

    # Try ga4ASCDataLayerVehicle (single-car VDP version)
    m = re.search(r"ga4ASCDataLayerVehicle\s*=\s*'(\[.*?\])'\s*;", html, re.DOTALL)
    if m:
        try:
            items = json.loads(m.group(1))
            if items and isinstance(items[0], dict):
                item = items[0]
                ext = (item.get("item_color") or item.get("exteriorColor") or "").strip() or None
                intr = (item.get("interiorColor") or item.get("interior_color") or "").strip() or None
                if ext:
                    out["exterior_color"] = ext
                if intr:
                    out["interior_color"] = intr
                if ext or intr:
                    return out
        except (json.JSONDecodeError, TypeError):
            pass

    # Try JSON blobs in page
    for json_m in re.finditer(r"\{[^{}]{20,5000}\}", html):
        try:
            obj = json.loads(json_m.group(0))
            if not isinstance(obj, dict):
                continue
            ext = None
            intr = None
            for ek in ("exteriorColor", "exterior_color", "ExteriorColor", "item_color"):
                v = obj.get(ek)
                if v and isinstance(v, str) and 2 < len(v) < 60:
                    ext = v.strip()
                    break
            for ik in ("interiorColor", "interior_color", "InteriorColor", "interiorTrim"):
                v = obj.get(ik)
                if v and isinstance(v, str) and 2 < len(v) < 60:
                    intr = v.strip()
                    break
            if ext and not out["exterior_color"]:
                out["exterior_color"] = ext
            if intr and not out["interior_color"]:
                out["interior_color"] = intr
            if out["exterior_color"] and out["interior_color"]:
                return out
        except (json.JSONDecodeError, ValueError):
            continue

    # Fallback: DOM text patterns (after JS rendering)
    for pat, key in [
        (r"Exterior\s+Color[:\s]+([A-Za-z][A-Za-z0-9 /\-]+?)(?:\n|<|,|\s{2})", "exterior_color"),
        (r"Interior\s+Color[:\s]+([A-Za-z][A-Za-z0-9 /\-]+?)(?:\n|<|,|\s{2})", "interior_color"),
        (r"Paint[:\s]+([A-Za-z][A-Za-z0-9 /\-]+?)(?:\n|<|,|\s{2})", "exterior_color"),
    ]:
        if out[key]:
            continue
        m2 = re.search(pat, html, re.I)
        if m2:
            val = m2.group(1).strip()
            if 2 < len(val) < 60 and val.lower() not in ("color", "n/a", "na", "tbd"):
                out[key] = val

    return out


def _parse_dealer_com_inventory(items: list[dict]) -> dict[str, dict[str, str | None]]:
    """
    Parse Dealer.com inventory items from getInventoryAndFacets or asc_datalayer.items.
    Returns {vin_upper: {exterior_color, interior_color}}.
    """
    results: dict[str, dict[str, str | None]] = {}
    for item in items:
        vin = (item.get("vin") or item.get("VIN") or item.get("item_id") or "").strip().upper()
        if not vin:
            continue
        ext: str | None = None
        intr: str | None = None

        # Dealer.com API: attributes list [{name, value}]
        for attr in item.get("attributes", []):
            n = attr.get("name", "")
            v = (attr.get("value") or "").strip()
            if not v or v.lower() in ("null", "n/a", ""):
                continue
            if n == "exteriorColor" and not ext:
                ext = v
            elif n == "interiorColor" and not intr:
                intr = v
        # Dealer.com API: trackingAttributes list
        for attr in item.get("trackingAttributes", []):
            n = attr.get("name", "")
            v = (attr.get("value") or "").strip()
            if not v or v.lower() in ("null", "n/a", ""):
                continue
            if n == "exteriorColor" and not ext:
                ext = v
            elif n == "interiorColor" and not intr:
                intr = v

        # asc_datalayer.items flat fields
        if not ext:
            ext = (item.get("item_color") or item.get("exteriorColor") or "").strip() or None
        if not intr:
            intr = (item.get("interiorColor") or item.get("interior_color") or "").strip() or None

        # Filter placeholders and junk
        if _is_template_placeholder(ext):
            ext = None
        if _is_template_placeholder(intr):
            intr = None

        if ext or intr:
            results[vin] = {"exterior_color": ext, "interior_color": intr}
    return results


async def _fetch_srp_colors(
    browser,
    dealer_url: str,
    vins_needed: set[str],
) -> dict[str, dict[str, str | None]]:
    """Fetch SRP pages for a dealer and extract colors by VIN."""
    results: dict[str, dict[str, str | None]] = {}
    base = dealer_url.rstrip("/")

    for path in _SRP_PATHS:
        url = base + path
        page = await browser.new_page()
        try:
            # Capture the Dealer.com API call to replay with larger page size
            captured_post: dict | None = None
            api_url_captured: str | None = None

            async def on_request(req):
                nonlocal captured_post, api_url_captured
                if "getInventoryAndFacets" in req.url and req.method == "POST":
                    try:
                        body = req.post_data
                        if body:
                            captured_post = json.loads(body)
                            api_url_captured = req.url
                    except Exception:
                        pass

            page.on("request", on_request)

            await page.goto(url, wait_until="networkidle", timeout=45000)
            await asyncio.sleep(2.5)

            # Strategy 1: Re-issue Dealer.com API with large pageSize
            if captured_post and api_url_captured:
                try:
                    prefs = captured_post.get("preferences", {})
                    prefs["pageSize"] = "500"
                    captured_post["preferences"] = prefs

                    api_resp_text = await page.evaluate(
                        """async ({apiUrl, postBody}) => {
                            try {
                                const r = await fetch(apiUrl, {
                                    method: 'POST',
                                    headers: {'Content-Type': 'application/json'},
                                    credentials: 'include',
                                    body: JSON.stringify(postBody)
                                });
                                if (!r.ok) return null;
                                return await r.text();
                            } catch(e) { return null; }
                        }""",
                        {"apiUrl": api_url_captured, "postBody": captured_post},
                    )
                    if api_resp_text:
                        api_data = json.loads(api_resp_text)
                        inventory = api_data.get("inventory", [])
                        log.info("  DDC API → %d cars", len(inventory))
                        parsed = _parse_dealer_com_inventory(inventory)
                        for vin in list(vins_needed):
                            vu = vin.upper()
                            if vu in parsed and (parsed[vu].get("exterior_color") or parsed[vu].get("interior_color")):
                                results[vu] = parsed[vu]
                except Exception as exc:
                    log.debug("  DDC API replay error: %s", str(exc)[:120])

            # Strategy 2: asc_datalayer.items (already loaded on page)
            if not results or len(results) < len(vins_needed):
                try:
                    asc_items_json = await page.evaluate(
                        "() => { try { return JSON.stringify(window.asc_datalayer && window.asc_datalayer.items || []); } catch(e) { return '[]'; } }"
                    )
                    asc_items = json.loads(asc_items_json) if asc_items_json else []
                    parsed = _parse_dealer_com_inventory(asc_items)
                    for vin in list(vins_needed):
                        vu = vin.upper()
                        if vu in parsed and vu not in results:
                            results[vu] = parsed[vu]
                except Exception:
                    pass

            # Strategy 3: ga4ASCDataLayerVehicle or HTML
            if not results or len(results) < len(vins_needed):
                try:
                    raw_js = await page.evaluate(
                        "() => { try { return JSON.stringify(ga4ASCDataLayerVehicle); } catch(e) { return null; } }"
                    )
                    if raw_js:
                        items = json.loads(raw_js)
                        if isinstance(items, list):
                            parsed = _parse_dealer_com_inventory(items)
                            for vin in list(vins_needed):
                                vu = vin.upper()
                                if vu in parsed and vu not in results:
                                    results[vu] = parsed[vu]
                except Exception:
                    pass

            if not results or len(results) < len(vins_needed):
                html = await page.content()
                found = _extract_colors_from_html(html)
                for vin in list(vins_needed):
                    vu = vin.upper()
                    if vu in found and vu not in results:
                        entry = found[vu]
                        if _is_template_placeholder(entry.get("exterior_color")):
                            entry["exterior_color"] = None
                        if _is_template_placeholder(entry.get("interior_color")):
                            entry["interior_color"] = None
                        results[vu] = entry

            if results:
                log.info("  SRP %s → found %d/%d colors", path, len(results), len(vins_needed))
                if len(results) >= len(vins_needed):
                    break
        except Exception as exc:
            log.debug("  SRP %s error: %s", path, str(exc)[:80])
        finally:
            await page.close()

    return results


def _is_template_placeholder(val: str | None) -> bool:
    """Detect unresolved JS/Angular template strings like [[exteriorColor]], {{color}}."""
    if not val:
        return False
    return bool(re.search(r"\[\[.*?\]\]|\{\{.*?\}\}", val))


async def _fetch_vdp_colors(browser, source_url: str) -> dict[str, str | None]:
    """Fetch a VDP page and extract exterior + interior color."""
    page = await browser.new_page()
    try:
        await page.goto(source_url, wait_until="networkidle", timeout=45000)
        await asyncio.sleep(2.5)
        html = await page.content()
        colors = _extract_colors_from_vdp(html)
        # Discard unresolved template placeholders
        for key in ("exterior_color", "interior_color"):
            if _is_template_placeholder(colors.get(key)):
                colors[key] = None

        # If DOM text didn't help, try JS evaluation for spec rows
        if not colors["exterior_color"] or not colors["interior_color"]:
            js_result = await page.evaluate("""
                () => {
                    const out = {};
                    // Look for labeled spec rows
                    const labels = document.querySelectorAll('dt, .label, .spec-label, th, [class*="label"]');
                    for (const el of labels) {
                        const t = (el.innerText || '').trim().toLowerCase();
                        const sib = el.nextElementSibling;
                        const val = sib ? (sib.innerText || '').trim() : '';
                        if (t.includes('exterior') && t.includes('color') && val) out.ext = val;
                        if (t.includes('interior') && t.includes('color') && val) out.intr = val;
                        if (t === 'color' && val && !out.ext) out.ext = val;
                    }
                    return out;
                }
            """)
            ext_js = js_result.get("ext", "")
            intr_js = js_result.get("intr", "")
            if ext_js and not colors["exterior_color"] and not _is_template_placeholder(ext_js):
                colors["exterior_color"] = ext_js[:60].strip() or None
            if intr_js and not colors["interior_color"] and not _is_template_placeholder(intr_js):
                colors["interior_color"] = intr_js[:60].strip() or None

        return colors
    except Exception as exc:
        log.debug("  VDP error %s: %s", source_url, str(exc)[:80])
        return {"exterior_color": None, "interior_color": None}
    finally:
        await page.close()


async def _run(
    cars: list[dict],
    dry_run: bool,
    workers: int,
) -> dict[str, int]:
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        log.error("Playwright not installed: pip install playwright && playwright install chromium")
        return {}

    stats = {"srp_ext": 0, "srp_intr": 0, "vdp_ext": 0, "vdp_intr": 0, "cars_updated": 0}

    # Group cars by dealer for SRP batch fetching
    by_dealer: dict[str, list[dict]] = {}
    for car in cars:
        du = (car.get("dealer_url") or "").strip()
        if du:
            by_dealer.setdefault(du, []).append(car)

    updates: dict[int, dict[str, str]] = {}  # car_id → patch

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)

        # Layer 1: SRP batch fetch per dealer
        for dealer_url, dealer_cars in by_dealer.items():
            vins_needed = {
                (c.get("vin") or "").strip().upper()
                for c in dealer_cars
                if c.get("vin")
            }
            if not vins_needed:
                continue
            log.info("SRP fetch: %s (%d cars)", dealer_url, len(vins_needed))
            srp_results = await _fetch_srp_colors(browser, dealer_url, vins_needed)

            for car in dealer_cars:
                vin = (car.get("vin") or "").strip().upper()
                if vin not in srp_results:
                    continue
                found = srp_results[vin]
                patch: dict[str, str] = {}
                missing = listing_missing_field_codes(car, for_public_filter=True, include_non_actionable=True)
                if "exterior_color" in missing and found.get("exterior_color"):
                    patch["exterior_color"] = found["exterior_color"]
                    stats["srp_ext"] += 1
                if "interior_color" in missing and found.get("interior_color"):
                    patch["interior_color"] = found["interior_color"]
                    stats["srp_intr"] += 1
                if patch:
                    updates[car["id"]] = patch
                    log.info("  SRP vin=%s: %s", vin, patch)

        # Layer 2: VDP fetch for cars still missing color (have source_url)
        sem = asyncio.Semaphore(workers)
        vdp_tasks = []
        for car in cars:
            su = (car.get("source_url") or "").strip()
            if not su.startswith("http"):
                continue
            missing = listing_missing_field_codes(car, for_public_filter=True, include_non_actionable=True)
            # Skip if already resolved by SRP
            cid = car["id"]
            already = updates.get(cid, {})
            still_missing = [
                f for f in missing
                if f in ("exterior_color", "interior_color") and f not in already
            ]
            if not still_missing:
                continue
            vdp_tasks.append((car, su))

        if vdp_tasks:
            log.info("VDP fetch: %d pages", len(vdp_tasks))

        async def fetch_one_vdp(car: dict, url: str):
            async with sem:
                colors = await _fetch_vdp_colors(browser, url)
                missing = listing_missing_field_codes(car, for_public_filter=True, include_non_actionable=True)
                cid = car["id"]
                patch = dict(updates.get(cid, {}))
                if "exterior_color" in missing and colors.get("exterior_color") and "exterior_color" not in patch:
                    patch["exterior_color"] = colors["exterior_color"]
                    stats["vdp_ext"] += 1
                if "interior_color" in missing and colors.get("interior_color") and "interior_color" not in patch:
                    patch["interior_color"] = colors["interior_color"]
                    stats["vdp_intr"] += 1
                if patch:
                    updates[cid] = patch
                    vin = car.get("vin", "")
                    log.info("  VDP vin=%s: %s", vin, patch)

        await asyncio.gather(*[fetch_one_vdp(car, url) for car, url in vdp_tasks])
        await browser.close()

    # Apply updates
    stats["cars_updated"] = len(updates)
    if not dry_run:
        for car_id, patch in updates.items():
            update_car_row_partial(car_id, patch)
        log.info("Wrote %d car updates", len(updates))
    else:
        log.info("DRY-RUN: would update %d cars", len(updates))

    return stats


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--dealer", help="Process only this dealer URL")
    parser.add_argument("--workers", type=int, default=3, help="VDP concurrent tabs")
    args = parser.parse_args(argv)

    cars = _get_color_missing_cars(dealer_filter=args.dealer)
    if not cars:
        log.info("No color-missing cars found")
        return

    if args.limit > 0:
        cars = cars[: args.limit]

    log.info("Color-missing cars to process: %d", len(cars))
    stats = asyncio.run(_run(cars, dry_run=args.dry_run, workers=args.workers))

    log.info("")
    log.info("=== Summary ===")
    log.info("  Cars updated:     %d", stats.get("cars_updated", 0))
    log.info("  SRP ext color:    %d", stats.get("srp_ext", 0))
    log.info("  SRP intr color:   %d", stats.get("srp_intr", 0))
    log.info("  VDP ext color:    %d", stats.get("vdp_ext", 0))
    log.info("  VDP intr color:   %d", stats.get("vdp_intr", 0))


if __name__ == "__main__":
    main()
