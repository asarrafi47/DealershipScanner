"""
ShopperExpress (WordPress + Serti DMS) inventory scraper.

Sites like Acura of Chattanooga and Kia of Chattanooga use this platform.

Two scraping strategies are available:
1. API-first (preferred): ``fetch_shopperexpress_inventory`` calls /wp-json/v1/vehicles
   to get all VDP URLs, then fetches each VDP page with aiohttp and extracts full
   vehicle details (price, mileage, colors, drivetrain, etc.) from JSON-LD.
2. Playwright fallback: ``scrape_shopperexpress_from_page`` navigates /listings/ and
   /used-listings/ and extracts CollectionPage ItemList JSON-LD via Playwright.
   Does not capture mileage (not present in ItemList schema).

Common inventory paths:
  /listings/        — new vehicles
  /used-listings/   — pre-owned vehicles
  /wp-json/v1/vehicles — REST API listing all vehicles (title, link, search)
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
from typing import Any
from urllib.parse import urljoin

import aiohttp

logger = logging.getLogger("scanner")

# ── API-first scraper (aiohttp, no Playwright needed) ──────────────────────────

_VDP_CONCURRENCY = 20
_VDP_TIMEOUT_SEC = 20
_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
_SCHEMA_SCRIPT_RE = re.compile(
    r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>', re.DOTALL
)


def _offers_condition(item_condition_url: str | None) -> str | None:
    low = (item_condition_url or "").lower()
    if "used" in low:
        return "Used"
    if "new" in low:
        return "New"
    if "refurbish" in low or "certified" in low:
        return "Certified Pre-Owned"
    return None


def _parse_additional_props(props: list[dict]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for p in props or []:
        name = (p.get("name") or "").strip().lower()
        val = p.get("value")
        if val is None:
            continue
        if "city mpg" in name:
            try:
                out["mpg_city"] = int(str(val).replace(",", ""))
            except ValueError:
                pass
        elif "highway mpg" in name:
            try:
                out["mpg_highway"] = int(str(val).replace(",", ""))
            except ValueError:
                pass
        elif name == "certified":
            out["_certified_val"] = str(val)
    return out


def _build_vehicle_from_vdp_schemas(
    schemas: list[dict],
    vdp_url: str,
    dealer_id: str,
    dealer_name: str,
    dealer_url: str,
) -> dict[str, Any] | None:
    # Merge all Vehicle/Car schemas — the richer (typically second) one wins
    merged: dict[str, Any] = {}
    for d in schemas:
        if d.get("@type") not in ("Vehicle", "Car"):
            continue
        merged.update({k: v for k, v in d.items() if v is not None and v != ""})
    if not merged:
        return None

    vin_re = re.compile(r"^[A-HJ-NPR-Z0-9]{17}$", re.I)
    vin = (merged.get("vehicleIdentificationNumber") or "").strip().upper()
    if not vin or not vin_re.match(vin):
        return None

    year_raw = merged.get("vehicleModelDate") or merged.get("modelDate")
    try:
        year = int(year_raw) if year_raw else None
    except (TypeError, ValueError):
        year = None

    make = (merged.get("brand") or {}).get("name") if isinstance(merged.get("brand"), dict) else merged.get("brand")
    model = merged.get("model")
    trim = merged.get("vehicleConfiguration")

    offers = merged.get("offers") or {}
    if isinstance(offers, list):
        offers = offers[0] if offers else {}
    price_raw = offers.get("price") if isinstance(offers, dict) else None
    try:
        price = int(float(str(price_raw).replace(",", ""))) if price_raw else None
    except (TypeError, ValueError):
        price = None

    mileage_obj = merged.get("mileageFromOdometer") or {}
    mileage_val = mileage_obj.get("value") if isinstance(mileage_obj, dict) else mileage_obj
    try:
        mileage = int(float(str(mileage_val).replace(",", ""))) if mileage_val else None
    except (TypeError, ValueError):
        mileage = None

    item_cond = merged.get("itemCondition") or (offers.get("itemCondition") if isinstance(offers, dict) else None)
    condition = _offers_condition(item_cond)

    engine_obj = merged.get("vehicleEngine") or {}
    engine_desc = None
    if isinstance(engine_obj, dict):
        engine_desc = (
            engine_obj.get("description")
            or engine_obj.get("engineDisplacement")
            or engine_obj.get("engineType")
        )

    add_props = _parse_additional_props(merged.get("additionalProperty"))
    if not condition and "_certified_val" in add_props:
        cert_val = add_props["_certified_val"].lower()
        if "used" in cert_val:
            condition = "Used"
        elif "new" in cert_val:
            condition = "New"
        elif "cert" in cert_val:
            condition = "Certified Pre-Owned"

    image_raw = merged.get("image") or ""
    image_url = image_raw if isinstance(image_raw, str) else (image_raw[0] if image_raw else "")

    return {
        "vin": vin,
        "year": year,
        "make": make,
        "model": model,
        "trim": trim,
        "price": price,
        "mileage": mileage,
        "condition": condition,
        "exterior_color": merged.get("color"),
        "interior_color": merged.get("vehicleInteriorColor"),
        "transmission": merged.get("vehicleTransmission"),
        "drivetrain": merged.get("driveWheelConfiguration"),
        "fuel_type": merged.get("fuelType"),
        "body_style": merged.get("bodyType"),
        "engine_description": engine_desc,
        "stock_number": merged.get("sku"),
        "mpg_city": add_props.get("mpg_city"),
        "mpg_highway": add_props.get("mpg_highway"),
        "image_url": image_url or None,
        "gallery": [image_url] if image_url else [],
        "source_url": vdp_url or None,
        "_detail_url": vdp_url or None,
        "dealer_name": dealer_name,
        "dealer_url": dealer_url,
        "dealer_id": dealer_id,
    }


_JS_COMMENT_RE = re.compile(r"(?<!:)//[^\r\n]*")


def _strip_json_control_chars(text: str) -> str:
    """Replace control chars (ord < 32) inside JSON string literals with spaces."""
    result: list[str] = []
    in_string = False
    i = 0
    while i < len(text):
        c = text[i]
        if not in_string:
            if c == '"':
                in_string = True
            result.append(c)
        else:
            if c == '\\':
                result.append(c)
                i += 1
                if i < len(text):
                    result.append(text[i])
            elif c == '"':
                in_string = False
                result.append(c)
            elif ord(c) < 32:
                result.append(' ')
            else:
                result.append(c)
        i += 1
    return ''.join(result)


def _safe_parse_jsonld(raw: str) -> dict | None:
    """Parse JSON-LD, stripping JS comments then control chars on retry.
    Uses negative lookbehind (?<!:) to preserve :// in URLs."""
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, ValueError):
        pass
    cleaned = _JS_COMMENT_RE.sub("", raw)
    try:
        return json.loads(cleaned)
    except (json.JSONDecodeError, ValueError):
        pass
    stripped = _strip_json_control_chars(cleaned)
    try:
        return json.loads(stripped)
    except (json.JSONDecodeError, ValueError):
        return None


async def _fetch_vdp_page(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    vdp_url: str,
    dealer_id: str,
    dealer_name: str,
    dealer_url: str,
) -> dict[str, Any] | None:
    async with sem:
        try:
            async with session.get(vdp_url, timeout=aiohttp.ClientTimeout(total=_VDP_TIMEOUT_SEC)) as resp:
                if resp.status != 200:
                    logger.debug("ShopperExpress VDP %s: HTTP %d", vdp_url, resp.status)
                    return None
                html = await resp.text(encoding="utf-8", errors="replace")
        except Exception as e:
            logger.debug("ShopperExpress VDP fetch failed %s: %s", vdp_url, e)
            return None

    schemas: list[dict] = []
    for raw in _SCHEMA_SCRIPT_RE.findall(html):
        obj = _safe_parse_jsonld(raw)
        if not obj:
            continue
        if isinstance(obj, dict) and obj.get("@type") in ("Vehicle", "Car"):
            schemas.append(obj)
        elif isinstance(obj, dict) and "@graph" in obj:
            for item in obj["@graph"]:
                if isinstance(item, dict) and item.get("@type") in ("Vehicle", "Car"):
                    schemas.append(item)

    if not schemas:
        logger.debug("ShopperExpress VDP %s: no Vehicle JSON-LD", vdp_url)
        return None

    return _build_vehicle_from_vdp_schemas(schemas, vdp_url, dealer_id, dealer_name, dealer_url)


async def fetch_shopperexpress_inventory(
    base_url: str,
    dealer_id: str,
    dealer_name: str,
) -> list[dict[str, Any]]:
    """
    Fetch all ShopperExpress inventory via REST API + VDP JSON-LD (no Playwright needed).

    1. GET /wp-json/v1/vehicles → list of {title, link, search}
    2. Concurrently fetch each VDP link and parse JSON-LD Vehicle schema
    3. Returns vehicles with full details: price, mileage, colors, drivetrain, etc.
    """
    list_url = base_url.rstrip("/") + "/wp-json/v1/vehicles"
    headers = {"User-Agent": _UA, "Accept": "application/json"}
    connector = aiohttp.TCPConnector(ssl=False, limit=_VDP_CONCURRENCY + 4)

    async with aiohttp.ClientSession(headers=headers, connector=connector) as session:
        try:
            async with session.get(list_url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status != 200:
                    logger.warning(
                        "ShopperExpress [%s]: /wp-json/v1/vehicles returned HTTP %d",
                        dealer_name, resp.status,
                    )
                    return []
                payload = await resp.json(content_type=None)
        except Exception as e:
            logger.warning("ShopperExpress [%s]: list fetch failed: %s", dealer_name, e)
            return []

        raw_vehicles = payload.get("vehicles") if isinstance(payload, dict) else None
        if not isinstance(raw_vehicles, list) or not raw_vehicles:
            logger.warning("ShopperExpress [%s]: empty vehicles list from API", dealer_name)
            return []

        logger.info(
            "ShopperExpress [%s]: %d listings from /wp-json/v1/vehicles — fetching VDPs",
            dealer_name, len(raw_vehicles),
        )

        vdp_urls = [(item.get("link") or "").strip() for item in raw_vehicles]
        vdp_urls = [u for u in vdp_urls if u]

        sem = asyncio.Semaphore(_VDP_CONCURRENCY)
        tasks = [
            _fetch_vdp_page(session, sem, u, dealer_id, dealer_name, base_url)
            for u in vdp_urls
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    vehicles: list[dict[str, Any]] = []
    for r in results:
        if isinstance(r, dict) and r.get("vin"):
            vehicles.append(r)
        elif isinstance(r, Exception):
            logger.debug("ShopperExpress VDP task exception: %s", r)

    logger.info(
        "ShopperExpress [%s]: %d vehicles extracted from %d VDPs",
        dealer_name, len(vehicles), len(vdp_urls),
    )
    return vehicles


# ── Playwright-based fallback scraper ─────────────────────────────────────────

_VIN_RE = re.compile(r"^[A-HJ-NPR-Z0-9]{17}$", re.I)
_VIN_RE_GLOBAL = re.compile(r"[A-HJ-NPR-Z0-9]{17}", re.I)

# Two standard ShopperExpress inventory paths
_SHOPPEREXPRESS_PATHS = ("/listings/", "/used-listings/")

# JS snippet injected into Playwright page to extract CollectionPage ItemList
_EXTRACT_JS = """
() => {
    for (const script of document.querySelectorAll('script[type="application/ld+json"]')) {
        let data;
        try { data = JSON.parse(script.textContent); } catch { continue; }
        if (data && data['@type'] === 'CollectionPage') {
            return data.mainEntity?.itemListElement || [];
        }
    }
    return [];
}
"""

_TOTAL_PAGES_JS = """
() => {
    const el = document.querySelector('.pagination-text');
    if (!el) return null;
    const m = el.textContent.match(/Page\\s*\\d+\\s*of\\s*(\\d+)/i);
    return m ? parseInt(m[1]) : null;
}
"""


def _is_shopperexpress_html(html: str) -> bool:
    lc = html.lower()
    return "shopperexpress" in lc or ("serti" in lc and "btn-next" in lc)


def _condition_from_schema(schema_url: str) -> str | None:
    frag = schema_url.rsplit("/", 1)[-1].lower()
    if "new" in frag:
        return "New"
    if "refurbish" in frag or "certified" in frag:
        return "Certified Pre-Owned"
    if "used" in frag:
        return "Used"
    return None


def _parse_shopperexpress_item(
    item: dict[str, Any],
    base_url: str,
    dealer_id: str,
    dealer_name: str,
    dealer_url: str,
) -> dict[str, Any] | None:
    """Convert a schema.org Vehicle dict (from ItemList) to our flat vehicle dict."""
    # item is the "item" sub-dict from a ListItem
    vin = (item.get("vehicleIdentificationNumber") or "").strip().upper()
    if not _VIN_RE.match(vin):
        return None

    year_raw = item.get("vehicleModelDate") or ""
    year: int | None = None
    try:
        year = int(str(year_raw).strip())
    except (ValueError, TypeError):
        pass

    make = (item.get("brand") or {}).get("name") or None
    model_raw = (item.get("model") or "").strip()
    # Model field sometimes contains trailing whitespace/newlines from SSR
    model_raw = re.sub(r"\s+", " ", model_raw).strip()

    # Split model into model + trim (e.g., "Integra w/A-Spec Technology Package")
    # Leave everything as "model_full" for now since there's no clean split point.
    model = model_raw or None
    trim: str | None = None

    # Name format: "New 2026 Acura Integra w/A-Spec Technology Package 19UDE4G73TA017355 AC5381"
    name = (item.get("name") or "").strip()

    # VDP URL
    source_url = item.get("url") or ""

    # Extract stock number from VDP URL slug: ...{VIN}-{stock}/
    stock_number: str | None = None
    slug_m = re.search(r"-([A-HJ-NPR-Z0-9]{17})-([^/]+)/?$", source_url, re.I)
    if slug_m:
        stock_number = slug_m.group(2).upper() or None

    # Image
    image_url = item.get("image") or ""

    # Price and condition from offers
    offers = item.get("offers") or {}
    price: int | None = None
    try:
        price = int(float(str(offers.get("price") or "")))
    except (ValueError, TypeError):
        pass

    condition = _condition_from_schema(offers.get("itemCondition") or "")

    return {
        "vin": vin,
        "year": year,
        "make": make,
        "model": model,
        "trim": trim,
        "price": price,
        "mileage": None,  # not in schema.org ItemList — VDP needed for mileage
        "condition": condition,
        "stock_number": stock_number,
        "image_url": image_url,
        "gallery": [image_url] if image_url else [],
        "source_url": source_url or None,
        "_detail_url": source_url or None,
        "dealer_name": dealer_name,
        "dealer_url": dealer_url,
        "dealer_id": dealer_id,
    }


async def _scrape_shopperexpress_path(
    page: Any,
    path: str,
    inv_base: str,
    dealer_id: str,
    dealer_name: str,
    dealer_url: str,
    by_vin: dict[str, dict[str, Any]],
) -> int:
    """Navigate one ShopperExpress VLP path and click through pagination."""
    url = inv_base.rstrip("/") + path
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=20000)
    except Exception as e:
        logger.debug("ShopperExpress: nav %s failed: %s", url, e)
        return 0

    # Short wait for JS to settle
    await asyncio.sleep(1.0)

    added_total = 0
    max_pages = 30
    for page_num in range(max_pages):
        try:
            items = await page.evaluate(_EXTRACT_JS)
        except Exception as e:
            logger.debug("ShopperExpress: JS extract failed on %s page %d: %s", path, page_num, e)
            break

        if not items:
            break

        new_on_page = 0
        for list_item in items:
            vehicle_data = list_item.get("item") if isinstance(list_item, dict) else None
            if not vehicle_data:
                continue
            v = _parse_shopperexpress_item(vehicle_data, inv_base, dealer_id, dealer_name, dealer_url)
            if v:
                vin = v["vin"]
                if vin not in by_vin:
                    by_vin[vin] = v
                    new_on_page += 1

        added_total += new_on_page
        logger.debug(
            "ShopperExpress: %s%s page %d — +%d new (total %d)",
            dealer_name, path, page_num + 1, new_on_page, len(by_vin),
        )

        if new_on_page == 0 and page_num > 0:
            break

        # Click next page button
        next_btn = page.locator("a.btn-next")
        try:
            if await next_btn.count() == 0:
                break
            first = next_btn.first
            if not await first.is_enabled():
                break
            await first.click(timeout=5000)
            await asyncio.sleep(1.5)
        except Exception:
            break

    return added_total


async def scrape_shopperexpress_from_page(
    page: Any,
    base_url: str,
    dealer_id: str,
    dealer_name: str,
    dealer_url: str,
) -> list[dict[str, Any]]:
    """
    Scrape all ShopperExpress inventory from /listings/ and /used-listings/ paths,
    clicking through JS pagination on each.
    """
    inv_base = base_url.rstrip("/")
    by_vin: dict[str, dict[str, Any]] = {}

    for path in _SHOPPEREXPRESS_PATHS:
        added = await _scrape_shopperexpress_path(
            page, path, inv_base, dealer_id, dealer_name, dealer_url, by_vin
        )
        logger.debug(
            "ShopperExpress: %s%s — +%d new (running total %d)",
            dealer_name, path, added, len(by_vin),
        )

    if by_vin:
        logger.info("ShopperExpress: scraped %d vehicles for %s", len(by_vin), dealer_name)
    return list(by_vin.values())
