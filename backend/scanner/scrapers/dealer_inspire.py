"""
DealerInspire (WordPress + Maven Algolia) inventory scraper.

DealerInspire sites host inventory via Algolia. The Algolia app config
(appId, apiKey, indexName) is embedded in the page's window.mvnAlgoliaConfig
or inline script tags. This scraper extracts that config and queries Algolia
directly, bypassing the need to trigger the SRP UI.

Usage: called from the scanner CLI after warmup navigation when no inventory
JSON is intercepted via standard paths.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
from typing import Any

import requests

logger = logging.getLogger(__name__)

_ALGOLIA_SEARCH_URL = "https://{app_id}-dsn.algolia.net/1/indexes/*/queries"
_ALGOLIA_BATCH_SIZE = 1000

_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


def _extract_algolia_config_from_html(html: str) -> dict[str, str] | None:
    """
    Extract Algolia appId, apiKey, indexName from DealerInspire page HTML.
    Looks for mvnAlgoliaConfig, dealerinspire_inventory_vars, or inline JSON.
    """
    patterns = [
        # mvnAlgoliaConfig JS variable
        r"mvnAlgoliaConfig\s*=\s*(\{[^;]+?\})\s*;",
        # dealerinspire_inventory_vars (WordPress wp_localize_script)
        r"dealerinspire_inventory_vars\s*=\s*(\{[^;]+?\})\s*;",
        # algolia_config or algoliaConfig
        r"algolia[_C]onfig\s*=\s*(\{[^;]+?\})\s*;",
        # window.diInventoryConfig
        r"diInventoryConfig\s*=\s*(\{[^;]+?\})\s*;",
        # WordPress localized script with algolia keys
        r'"algolia_app_id"\s*:\s*"([^"]+)".*?"algolia_search_key"\s*:\s*"([^"]+)".*?"index_name"\s*:\s*"([^"]+)"',
    ]
    for pattern in patterns:
        try:
            m = re.search(pattern, html, re.DOTALL | re.IGNORECASE)
            if not m:
                continue
            if m.lastindex and m.lastindex >= 3:
                return {
                    "appId": m.group(1),
                    "apiKey": m.group(2),
                    "indexName": m.group(3),
                }
            blob = m.group(1)
            try:
                data = json.loads(blob)
            except json.JSONDecodeError:
                # Try to fix common JS issues (single quotes, trailing commas)
                blob = re.sub(r"'", '"', blob)
                blob = re.sub(r",\s*}", "}", blob)
                blob = re.sub(r",\s*]", "]", blob)
                try:
                    data = json.loads(blob)
                except json.JSONDecodeError:
                    continue
            app_id = (
                data.get("appId") or data.get("app_id") or data.get("algolia_app_id")
            )
            api_key = (
                data.get("apiKey") or data.get("apiKeySearch") or data.get("api_key") or
                data.get("search_api_key") or data.get("algolia_search_key") or
                data.get("searchApiKey")
            )
            index = (
                data.get("indexName") or data.get("index_name") or
                data.get("inventory_index") or data.get("vehicles_index")
            )
            if app_id and api_key and index:
                return {"appId": str(app_id), "apiKey": str(api_key), "indexName": str(index)}
        except Exception:
            continue

    # Fallback: search for appId + apiKey near each other in any script block
    script_blocks = re.findall(r"<script[^>]*>(.*?)</script>", html, re.DOTALL | re.IGNORECASE)
    for block in script_blocks:
        app_id_m = re.search(r'"?appId"?\s*[=:]\s*"([A-Z0-9]{8,})"', block, re.IGNORECASE)
        api_key_m = re.search(r'"?(?:search)?(?:Api)?[Kk]ey"?\s*[=:]\s*"([a-f0-9]{20,})"', block, re.IGNORECASE)
        index_m = re.search(r'"?(?:index(?:Name)?)"?\s*[=:]\s*"([a-zA-Z0-9_-]{4,})"', block, re.IGNORECASE)
        if app_id_m and api_key_m and index_m:
            return {
                "appId": app_id_m.group(1),
                "apiKey": api_key_m.group(1),
                "indexName": index_m.group(1),
            }
    return None


def _query_algolia_inventory(
    app_id: str,
    api_key: str,
    index_name: str,
    *,
    filters: str = "",
    page_size: int = _ALGOLIA_BATCH_SIZE,
) -> list[dict[str, Any]]:
    """
    Query Algolia for all vehicles in the index (paginated).
    Returns list of raw Algolia hit dicts.
    """
    url = _ALGOLIA_SEARCH_URL.format(app_id=app_id)
    headers = {
        "X-Algolia-Application-Id": app_id,
        "X-Algolia-API-Key": api_key,
        "Content-Type": "application/json",
    }
    all_hits: list[dict[str, Any]] = []
    page = 0
    while True:
        payload = {
            "requests": [
                {
                    "indexName": index_name,
                    "params": f"hitsPerPage={page_size}&page={page}&filters={filters}",
                }
            ]
        }
        try:
            r = requests.post(url, headers=headers, json=payload, timeout=20)
            if r.status_code in (401, 403):
                logger.warning("DealerInspire Algolia: auth error %d for app=%s", r.status_code, app_id)
                break
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            logger.debug("DealerInspire Algolia query failed: %s", e)
            break
        results = data.get("results", [{}])
        hits = results[0].get("hits", []) if results else []
        if not hits:
            break
        all_hits.extend(hits)
        nb_pages = int(results[0].get("nbPages", 1))
        if page >= nb_pages - 1:
            break
        page += 1
        if len(all_hits) >= 10000:
            break
    return all_hits


def _map_algolia_hit(hit: dict[str, Any], base_url: str, dealer_id: str, dealer_name: str, dealer_url: str) -> dict[str, Any] | None:
    """Map a DealerInspire/Algolia hit dict to the scanner vehicle schema."""
    vin = str(hit.get("vin") or hit.get("VIN") or "").strip().upper()
    if not vin:
        return None

    def _s(key: str) -> str | None:
        v = hit.get(key)
        if v is None:
            return None
        s = str(v).strip()
        return s if s and s.lower() not in ("n/a", "na", "null", "none") else None

    def _i(key: str) -> int:
        try:
            return int(str(hit.get(key) or 0).replace(",", "").strip())
        except (TypeError, ValueError):
            return 0

    year = _i("year") or _i("modelYear")
    make = _s("make") or _s("oem")
    model = _s("model")
    trim = _s("trim")
    price = (
        _i("sellingPrice") or _i("our_price") or _i("internetPrice") or
        _i("price") or _i("msrp")
    )
    mileage = _i("odometer") or _i("mileage") or _i("miles")
    condition = _s("type") or _s("condition") or _s("inventoryType")
    if condition:
        cl = condition.lower()
        if "new" in cl:
            condition = "New"
        elif "certified" in cl or "cpo" in cl or _s("certified") == "1":
            condition = "Certified Pre-Owned"
        elif "used" in cl or "pre" in cl:
            condition = "Used"
    elif _s("certified") == "1":
        condition = "Certified Pre-Owned"

    # Images — thumbnail is the main image on some DealerInspire sites (e.g. MB Charlotte)
    images: list[str] = []
    thumb = _s("thumbnail")
    if thumb and thumb.startswith("http"):
        images.append(thumb)
    for key in ("media", "images", "photos", "gallery"):
        val = hit.get(key)
        if isinstance(val, list):
            for item in val:
                if isinstance(item, str) and item.startswith("http"):
                    images.append(item)
                elif isinstance(item, dict):
                    for k in ("url", "uri", "src", "large", "medium"):
                        u = item.get(k)
                        if isinstance(u, str) and u.startswith("http"):
                            images.append(u)
                            break
        elif isinstance(val, str) and val.startswith("http"):
            images.append(val)

    image_url = images[0] if images else ""

    # VDP URL — some sites use `link` instead of `pageUrl`
    detail_url = _s("pageUrl") or _s("detailPageUrl") or _s("link") or _s("url") or ""
    if detail_url and not detail_url.startswith("http"):
        detail_url = base_url.rstrip("/") + "/" + detail_url.lstrip("/")

    ext_color = _s("exteriorColor") or _s("exterior_color") or _s("extColor") or _s("ext_color")
    int_color = _s("interiorColor") or _s("interior_color") or _s("intColor") or _s("int_color")
    stock = _s("stockNumber") or _s("stock") or _s("stockNum") or _s("api_id")
    body_style = _s("body") or _s("bodyStyle") or _s("body_style")
    drivetrain = _s("drivetrain") or _s("drive_train") or _s("driveTrain")
    fuel_type = _s("fueltype") or _s("fuel_type") or _s("fuelType")
    engine = _s("engine_description") or _s("engineDescription") or _s("engine")
    transmission = _s("transmission_description") or _s("transmission")

    lot_location = ""
    try:
        from backend.scanner.dealer_location import extract_location_from_inventory_object

        lot_location = extract_location_from_inventory_object(hit)
    except ImportError:
        lot_location = _s("dealer_name") or _s("location_name") or _s("dealerName") or ""

    row = {
        "vin": vin,
        "year": year or None,
        "make": make,
        "model": model,
        "trim": trim,
        "price": price or None,
        "mileage": mileage or None,
        "condition": condition,
        "exterior_color": ext_color,
        "interior_color": int_color,
        "body_style": body_style,
        "drivetrain": drivetrain,
        "fuel_type": fuel_type,
        "engine_description": engine,
        "transmission": transmission,
        "stock_number": stock,
        "image_url": image_url,
        "gallery": images[:40],
        "source_url": detail_url or None,
        "_detail_url": detail_url or None,
        "dealer_name": dealer_name,
        "dealer_url": dealer_url,
        "dealer_id": dealer_id,
    }
    if lot_location:
        row["_lot_location"] = lot_location
    return row


async def scrape_dealer_inspire_from_page(
    page: Any,
    base_url: str,
    dealer_id: str,
    dealer_name: str,
    dealer_url: str,
) -> list[dict[str, Any]]:
    """
    Extract Algolia config from already-loaded Playwright page, then query Algolia directly.
    Returns list of mapped vehicle dicts (empty if DealerInspire is not detected).
    """
    try:
        html = await page.content()
    except Exception as e:
        logger.debug("DealerInspire: could not get page content: %s", e)
        return []

    if "dealerinspire" not in html.lower() and "maven-algolia" not in html.lower():
        return []

    config = _extract_algolia_config_from_html(html)
    if not config:
        # Try extracting via JS evaluation
        try:
            config_js = await page.evaluate(
                """() => {
                    const cfg = window.mvnAlgoliaConfig || window.algoliaConfig
                        || window.diInventoryConfig || window.dealerinspire_inventory_vars;
                    if (!cfg) return null;
                    const app = cfg.appId || cfg.app_id || cfg.algolia_app_id;
                    const key = cfg.apiKey || cfg.apiKeySearch || cfg.search_api_key || cfg.searchApiKey || cfg.algolia_search_key;
                    const idx = cfg.indexName || cfg.index_name || cfg.inventory_index;
                    if (!app || !key || !idx) return null;
                    return {appId: app, apiKey: key, indexName: idx};
                }"""
            )
            if config_js and isinstance(config_js, dict):
                config = config_js
        except Exception as e:
            logger.debug("DealerInspire: JS config extraction failed: %s", e)

    if not config:
        # SRP pages often don't embed algoliaConfig — navigate to homepage and intercept
        # the Algolia /queries request to capture app_id, api_key, and index_name directly.
        captured: dict[str, str] = {}
        done_evt = asyncio.Event()

        async def _on_response(response: Any) -> None:
            try:
                rurl = str(getattr(response, "url", "") or "")
                if "algolia" not in rurl or "queries" not in rurl or captured:
                    return
                m = re.search(r"//([^-\.]+)-dsn\.algolia", rurl)
                if not m:
                    return
                captured["appId"] = m.group(1).upper()
                req = getattr(response, "request", None)
                if req:
                    hdrs = await req.all_headers()
                    captured["apiKey"] = hdrs.get("x-algolia-api-key", "")
                body = await response.json()
                results = body.get("results", [{}]) if isinstance(body, dict) else [{}]
                captured["indexName"] = str(results[0].get("index", "") if results else "")
                if all(captured.values()):
                    done_evt.set()
            except Exception:
                pass

        # Algolia requests fire on SRP pages, not on the homepage.
        # Try standard DealerInspire SRP paths until we capture credentials.
        _srp_paths = ["/new-vehicles/", "/used-vehicles/", "/new-inventory/", "/inventory/"]
        page.on("response", _on_response)
        try:
            for srp_path in _srp_paths:
                srp_url = base_url.rstrip("/") + srp_path
                try:
                    await page.goto(srp_url, wait_until="domcontentloaded", timeout=20_000)
                except Exception:
                    continue
                try:
                    await asyncio.wait_for(done_evt.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    pass
                if len(captured) == 3 and all(captured.values()):
                    break
                # Secondary: HTML extraction on this SRP page
                try:
                    srp_html = await page.content()
                    config = _extract_algolia_config_from_html(srp_html)
                    if config:
                        break
                except Exception:
                    pass
        except Exception as e:
            logger.debug("DealerInspire: SRP navigation for config failed: %s", e)
        finally:
            try:
                page.remove_listener("response", _on_response)
            except Exception:
                pass

        if not config and len(captured) == 3 and all(captured.values()):
            config = {"appId": captured["appId"], "apiKey": captured["apiKey"], "indexName": captured["indexName"]}
            logger.debug("DealerInspire: config via request intercept for %s: app=%s idx=%s", dealer_name, config["appId"], config["indexName"])

    if not config:
        logger.debug("DealerInspire: no Algolia config found for %s", dealer_name)
        return []

    app_id = config.get("appId", "")
    api_key = config.get("apiKey", "")
    index_name = config.get("indexName", "")
    if not (app_id and api_key and index_name):
        logger.debug("DealerInspire: incomplete Algolia config for %s: %s", dealer_name, config)
        return []

    logger.info("DealerInspire: querying Algolia app=%s index=%s for %s", app_id, index_name, dealer_name)
    hits = _query_algolia_inventory(app_id, api_key, index_name)
    if not hits:
        logger.info("DealerInspire: 0 hits from Algolia for %s", dealer_name)
        return []

    vehicles = []
    for hit in hits:
        v = _map_algolia_hit(hit, base_url, dealer_id, dealer_name, dealer_url)
        if v:
            vehicles.append(v)
    logger.info("DealerInspire: mapped %d vehicles from %d Algolia hits for %s", len(vehicles), len(hits), dealer_name)
    return vehicles
