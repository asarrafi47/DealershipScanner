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

from backend.scanner.scrapers.algolia_scope import (
    encode_algolia_filter_param,
    infer_algolia_filters,
    post_filter_algolia_hits,
)

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
    filt_param = encode_algolia_filter_param(filters)
    while True:
        payload = {
            "requests": [
                {
                    "indexName": index_name,
                    "params": f"hitsPerPage={page_size}&page={page}&filters={filt_param}",
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


async def _browser_algolia_fetch(page: Any, url: str, app_id: str, api_key: str, payload: dict) -> dict:
    """POST an Algolia batch query through the browser's fetch (sends correct Origin/Referer)."""
    result = await page.evaluate(
        """async (args) => {
            try {
                const r = await fetch(args.url, {
                    method: 'POST',
                    credentials: 'include',
                    headers: {
                        'X-Algolia-Application-Id': args.appId,
                        'X-Algolia-API-Key': args.apiKey,
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify(args.payload)
                });
                if (!r.ok) return {_error: r.status};
                return await r.json();
            } catch(e) { return {_error: String(e)}; }
        }""",
        {"url": url, "appId": app_id, "apiKey": api_key, "payload": payload},
    )
    if isinstance(result, dict) and "_error" in result:
        raise RuntimeError(f"Algolia browser fetch error: {result['_error']}")
    return result


async def _query_algolia_inventory_via_browser(
    page: Any,
    app_id: str,
    api_key: str,
    index_name: str,
    *,
    filters: str = "",
    page_size: int = _ALGOLIA_BATCH_SIZE,
    extra_params: str = "",
) -> list[dict[str, Any]]:
    """
    Query Algolia for all vehicles via the browser's fetch (bypasses referrer/IP restrictions).
    Returns list of raw Algolia hit dicts.

    ``extra_params`` is an optional raw Algolia params string (e.g. ``"type=New"`` or
    ``"facetFilters=type%3ANew"``) that is appended to every request's ``params`` field.
    Use this when a site requires additional query-time parameters that are not expressible
    via the ``filters`` argument (e.g. Land Rover sites that need ``type=New``).
    """
    url = _ALGOLIA_SEARCH_URL.format(app_id=app_id)
    all_hits: list[dict[str, Any]] = []
    page_num = 0
    filt_param = encode_algolia_filter_param(filters)
    extra = ("&" + extra_params.lstrip("&")) if extra_params else ""
    while True:
        payload = {
            "requests": [
                {
                    "indexName": index_name,
                    "params": f"hitsPerPage={page_size}&page={page_num}&filters={filt_param}{extra}",
                }
            ]
        }
        try:
            data = await _browser_algolia_fetch(page, url, app_id, api_key, payload)
        except RuntimeError as e:
            err_str = str(e)
            if "401" in err_str or "403" in err_str:
                logger.warning("DealerInspire Algolia browser fetch: auth error for app=%s: %s", app_id, e)
            else:
                logger.debug("DealerInspire Algolia browser fetch failed: %s", e)
            break
        except Exception as e:
            logger.debug("DealerInspire Algolia browser fetch failed: %s", e)
            break
        results = data.get("results", [{}]) if isinstance(data, dict) else [{}]
        hits = results[0].get("hits", []) if results else []
        if not hits:
            break
        all_hits.extend(hits)
        nb_pages = int(results[0].get("nbPages", 1))
        if page_num >= nb_pages - 1:
            break
        page_num += 1
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

    try:
        from backend.utils.in_transit import apply_in_transit_flags_from_raw

        apply_in_transit_flags_from_raw(hit, source="dealer_inspire_algolia")
    except ImportError:
        pass
    return row


_SRP_PATHS = ["/new-vehicles/", "/used-vehicles/", "/new-inventory/", "/inventory/"]


async def _intercept_algolia_config_from_srp(
    page: Any,
    base_url: str,
    dealer_name: str,
) -> dict[str, str] | None:
    """
    Navigate SRP pages and intercept the live Algolia /queries request to capture
    the real appId, apiKey, and indexName for this specific rooftop.

    This is more reliable than HTML extraction on multi-rooftop group sites where
    the page HTML may embed a config for a different store.
    """
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

    page.on("response", _on_response)
    config: dict[str, str] | None = None
    try:
        for srp_path in _SRP_PATHS:
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
        config = {
            "appId": captured["appId"],
            "apiKey": captured["apiKey"],
            "indexName": captured["indexName"],
        }
        logger.debug(
            "DealerInspire: config via request intercept for %s: app=%s idx=%s",
            dealer_name, config["appId"], config["indexName"],
        )
    return config


async def scrape_dealer_inspire_from_page(
    page: Any,
    base_url: str,
    dealer_id: str,
    dealer_name: str,
    dealer_url: str,
    *,
    dealer: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """
    Extract Algolia config from already-loaded Playwright page, then query Algolia directly.
    Returns list of mapped vehicle dicts (empty if DealerInspire is not detected).

    Manifest overrides supported (set on the dealer dict):
      - ``algolia_index``: override the index name extracted from HTML/intercept.
        Use this when a multi-rooftop group site embeds the wrong store's index in its
        HTML (e.g. hixsonchevrolet.com embeds the DeRidder index instead of Chattanooga).
      - ``algolia_filters``: raw Algolia filter string applied to the query.
      - ``algolia_scope``: shorthand scope key (``full``, ``on_lot``, ``primary_rooftop``).
      - ``algolia_params``: extra raw Algolia ``params`` string appended to every query
        (e.g. ``"type=New"`` to filter by condition when the index mixes new and used).
    """
    try:
        html = await page.content()
    except Exception as e:
        logger.debug("DealerInspire: could not get page content: %s", e)
        return []

    if "dealerinspire" not in html.lower() and "maven-algolia" not in html.lower():
        return []

    config: dict[str, str] | None = None
    config_source = "none"

    config = _extract_algolia_config_from_html(html)
    if config:
        config_source = "html"

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
                config_source = "js_eval"
        except Exception as e:
            logger.debug("DealerInspire: JS config extraction failed: %s", e)

    if not config:
        srp_config = await _intercept_algolia_config_from_srp(page, base_url, dealer_name)
        if srp_config:
            config = srp_config
            config_source = "intercepted"

    if not config:
        logger.debug("DealerInspire: no Algolia config found for %s", dealer_name)
        return []

    app_id = config.get("appId", "")
    api_key = config.get("apiKey", "")
    index_name = config.get("indexName", "")

    # --- Manifest algolia_index override ---
    # If the dealer manifest specifies an explicit index name, use it instead of whatever
    # the page HTML or intercept returned. This is the fix for group sites that embed a
    # sibling store's index (e.g. hixsonchevrolet.com → DeRidder index vs. Chattanooga).
    manifest_index = str((dealer or {}).get("algolia_index") or "").strip()
    if manifest_index:
        if manifest_index != index_name:
            logger.info(
                "DealerInspire: manifest algolia_index override for %s: %r → %r",
                dealer_name, index_name, manifest_index,
            )
        index_name = manifest_index
        config_source = "manifest_override"

    if not (app_id and api_key and index_name):
        logger.debug("DealerInspire: incomplete Algolia config for %s: %s", dealer_name, config)
        return []

    dealer_ctx = dealer if isinstance(dealer, dict) else {
        "dealer_id": dealer_id,
        "name": dealer_name,
        "url": dealer_url,
    }

    # Extra raw params string from manifest (e.g. "type=New" or "facetFilters=type%3ANew")
    extra_params: str = str((dealer or {}).get("algolia_params") or "").strip()

    logger.info("DealerInspire: querying Algolia app=%s index=%s for %s", app_id, index_name, dealer_name)
    probe_hits = await _query_algolia_inventory_via_browser(
        page, app_id, api_key, index_name, page_size=250, extra_params=extra_params
    )

    # If the HTML/JS-extracted config yields 0 hits, it may belong to a different rooftop
    # on a multi-store group site. Try intercepting the live Algolia request from the SRP
    # to get the real index for this specific dealer.
    # Skip this retry when the index was already overridden via the manifest (trust the override).
    if not probe_hits and config_source not in ("intercepted", "manifest_override"):
        logger.debug(
            "DealerInspire: 0 probe hits on %s-extracted index %s for %s — trying SRP intercept",
            config_source, index_name, dealer_name,
        )
        srp_config = await _intercept_algolia_config_from_srp(page, base_url, dealer_name)
        if srp_config and srp_config.get("indexName") != index_name:
            new_index = srp_config["indexName"]
            logger.info(
                "DealerInspire: 0 hits on HTML-extracted index %s for %s — retrying with SRP-intercepted index %s",
                index_name, dealer_name, new_index,
            )
            config = srp_config
            config_source = "intercepted"
            app_id = config["appId"]
            api_key = config["apiKey"]
            index_name = new_index
            probe_hits = await _query_algolia_inventory_via_browser(
                page, app_id, api_key, index_name, page_size=250, extra_params=extra_params
            )

    filters = infer_algolia_filters(dealer_ctx, index_name=index_name, sample_hits=probe_hits)
    if filters:
        logger.info("DealerInspire: Algolia filters for %s: %s", dealer_name, filters)
    hits = await _query_algolia_inventory_via_browser(
        page, app_id, api_key, index_name, filters=filters, extra_params=extra_params
    )
    hits = post_filter_algolia_hits(hits, dealer_ctx)
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
