"""
Dealer Venom (WordPress + Typesense InstantSearch) inventory scraper.

Sites like Toyota of Orange host inventory in Typesense. Config (host, apiKey,
indexName) is embedded in SRP HTML via TypesenseInstantSearchAdapter. This
scraper extracts that config and queries Typesense directly.
"""
from __future__ import annotations

import json
import logging
import re
from typing import Any
from urllib.parse import urljoin

import requests

logger = logging.getLogger(__name__)

_TYPESENSE_PAGE_SIZE = 250
_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


def _is_dealer_venom_html(html: str) -> bool:
    low = html.lower()
    return (
        "dv-framework" in low
        or "dealervenom" in low
        or "typesenseinstantsearchadapter" in low
    )


def _extract_typesense_config_from_html(html: str) -> dict[str, str] | None:
    """Extract Typesense host, apiKey, and collection/index name from page HTML."""
    if not html:
        return None

    api_key = None
    m = re.search(
        r'TypesenseInstantSearchAdapter\s*\(\s*\{[^}]*server:\s*\{[^}]*apiKey:\s*"([^"]+)"',
        html,
        re.DOTALL | re.IGNORECASE,
    )
    if m:
        api_key = m.group(1).strip()

    host = None
    m = re.search(
        r"host:\s*['\"]([^'\"]+typesense[^'\"]*)['\"]",
        html,
        re.IGNORECASE,
    )
    if m:
        host = m.group(1).strip()

    index_name = None
    for pat in (
        r'indexName\s*=\s*["\']([^"\']+)["\']',
        r'var indexName\s*=\s*["\']([^"\']+)["\']',
        r'"indexName"\s*:\s*"([^"]+)"',
        r'collectionName\s*:\s*["\']([^"\']+)["\']',
        r'(vehicles-[A-Z0-9]+)',
    ):
        m = re.search(pat, html, re.IGNORECASE)
        if m:
            index_name = m.group(1).strip()
            break

    if host and api_key and index_name:
        return {"host": host, "apiKey": api_key, "indexName": index_name}
    return None


def _query_typesense_inventory(
    host: str,
    api_key: str,
    index_name: str,
    *,
    page_size: int = _TYPESENSE_PAGE_SIZE,
) -> list[dict[str, Any]]:
    """Paginate Typesense search and return raw document dicts."""
    all_docs: list[dict[str, Any]] = []
    page = 1
    base = f"https://{host.rstrip('/')}"
    headers = {"X-TYPESENSE-API-KEY": api_key}

    while True:
        resp = requests.get(
            f"{base}/collections/{index_name}/documents/search",
            params={
                "q": "*",
                "query_by": "vin",
                "per_page": page_size,
                "page": page,
            },
            headers=headers,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        hits = data.get("hits") or []
        if not hits:
            break
        for hit in hits:
            doc = hit.get("document")
            if isinstance(doc, dict):
                all_docs.append(doc)
        found = int(data.get("found") or 0)
        if len(all_docs) >= found or len(hits) < page_size:
            break
        page += 1
        if page > 50:
            break
    return all_docs


def _parse_price(doc: dict[str, Any]) -> int | None:
    for key in ("finalPriceInt", "sortPrice", "internetPrice"):
        val = doc.get(key)
        if val is None:
            continue
        try:
            n = int(str(val).replace(",", "").strip())
            if n > 0:
                return n
        except (TypeError, ValueError):
            continue
    for key in ("finalPrice", "sellingPrice", "advertisedPrice", "price", "msrp"):
        val = doc.get(key)
        if not val:
            continue
        m = re.search(r"[\d,]+", str(val))
        if m:
            try:
                n = int(m.group().replace(",", ""))
                if n > 0:
                    return n
            except ValueError:
                continue
    return None


def _parse_mileage(doc: dict[str, Any]) -> int | None:
    val = doc.get("mileage")
    if val is None:
        return None
    m = re.search(r"[\d,]+", str(val).replace(",", ""))
    if not m:
        return None
    try:
        return int(m.group().replace(",", ""))
    except ValueError:
        return None


def _collect_images(doc: dict[str, Any]) -> list[str]:
    images: list[str] = []
    raw = doc.get("imageUrls")
    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, str) and item.startswith("http"):
                images.append(item)
    elif isinstance(raw, str) and raw.startswith("http"):
        images.append(raw)
    # Dedupe while preserving order
    seen: set[str] = set()
    out: list[str] = []
    for u in images:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def _map_typesense_document(
    doc: dict[str, Any],
    base_url: str,
    dealer_id: str,
    dealer_name: str,
    dealer_url: str,
) -> dict[str, Any] | None:
    vin = str(doc.get("vin") or "").strip().upper()
    if not vin:
        return None

    def _s(key: str) -> str | None:
        v = doc.get(key)
        if v is None:
            return None
        s = str(v).strip()
        return s if s and s.lower() not in ("n/a", "na", "null", "none") else None

    year_raw = doc.get("year") or doc.get("yr")
    try:
        year = int(str(year_raw).strip()) if year_raw is not None else None
    except (TypeError, ValueError):
        year = None

    condition = _s("condition")
    if doc.get("certified") is True:
        condition = "Certified Pre-Owned"

    images = _collect_images(doc)
    detail_path = _s("vdpUrl") or ""
    detail_url = urljoin(base_url.rstrip("/") + "/", detail_path.lstrip("/")) if detail_path else ""

    carfax_url = None
    carfax = doc.get("carfax")
    if isinstance(carfax, dict):
        raw_url = carfax.get("url")
        if raw_url:
            carfax_url = str(raw_url).strip()

    row: dict[str, Any] = {
        "vin": vin,
        "year": year,
        "make": _s("make"),
        "model": _s("model"),
        "trim": _s("trim") or _s("altStyle"),
        "price": _parse_price(doc),
        "mileage": _parse_mileage(doc),
        "condition": condition,
        "exterior_color": _s("exteriorColor") or _s("genericColor"),
        "interior_color": _s("interiorColor") or _s("genericInteriorColor"),
        "body_style": _s("body"),
        "drivetrain": _s("drivetrain"),
        "fuel_type": _s("fuel"),
        "engine_description": _s("engine"),
        "transmission": _s("transmission") or _s("transmissionType"),
        "stock_number": _s("stockNumber"),
        "image_url": images[0] if images else "",
        "gallery": images[:40],
        "source_url": detail_url or None,
        "_detail_url": detail_url or None,
        "dealer_name": dealer_name,
        "dealer_url": dealer_url,
        "dealer_id": dealer_id,
    }
    if carfax_url:
        row["carfax_url"] = carfax_url
    mpg_city = doc.get("mpgCity")
    mpg_hwy = doc.get("mpgHway")
    if mpg_city is not None:
        row["mpg_city"] = mpg_city
    if mpg_hwy is not None:
        row["mpg_highway"] = mpg_hwy
    return row


async def scrape_dealer_venom_from_page(
    page: Any,
    base_url: str,
    dealer_id: str,
    dealer_name: str,
    dealer_url: str,
) -> list[dict[str, Any]]:
    """
    Extract Typesense config from a loaded Playwright page and query inventory.
    Returns mapped vehicle dicts (empty if Dealer Venom / Typesense is not detected).
    """
    try:
        html = await page.content()
    except Exception as e:
        logger.debug("DealerVenom: could not get page content: %s", e)
        return []

    if not _is_dealer_venom_html(html):
        return []

    config = _extract_typesense_config_from_html(html)
    if not config:
        try:
            config_js = await page.evaluate(
                """() => {
                    const idx = window.indexName || window.collectionName;
                    const adapter = window.typesenseAdapter;
                    const server = adapter && adapter.configuration && adapter.configuration.server;
                    if (!server || !server.apiKey || !server.nodes || !server.nodes.length) return null;
                    const host = server.nodes[0].host;
                    if (!host || !idx) return null;
                    return { host, apiKey: server.apiKey, indexName: idx };
                }"""
            )
            if config_js and isinstance(config_js, dict):
                config = config_js
        except Exception as e:
            logger.debug("DealerVenom: JS config extraction failed: %s", e)

    if not config:
        for srp_path in ("/inventory/", "/new-vehicles/", "/used-vehicles/", "/shop/new/", "/shop/used/"):
            srp_url = base_url.rstrip("/") + srp_path
            try:
                await page.goto(srp_url, wait_until="domcontentloaded", timeout=20_000)
                html = await page.content()
                config = _extract_typesense_config_from_html(html)
                if config:
                    break
            except Exception:
                continue

    if not config:
        logger.debug("DealerVenom: no Typesense config found for %s", dealer_name)
        return []

    host = str(config.get("host") or "").strip()
    api_key = str(config.get("apiKey") or "").strip()
    index_name = str(config.get("indexName") or "").strip()
    if not (host and api_key and index_name):
        logger.debug("DealerVenom: incomplete Typesense config for %s: %s", dealer_name, config)
        return []

    logger.info(
        "DealerVenom: querying Typesense host=%s index=%s for %s",
        host,
        index_name,
        dealer_name,
    )
    try:
        docs = _query_typesense_inventory(host, api_key, index_name)
    except Exception as e:
        logger.warning("DealerVenom: Typesense query failed for %s: %s", dealer_name, e)
        return []

    if not docs:
        logger.info("DealerVenom: 0 documents from Typesense for %s", dealer_name)
        return []

    vehicles: list[dict[str, Any]] = []
    for doc in docs:
        v = _map_typesense_document(doc, base_url, dealer_id, dealer_name, dealer_url)
        if v:
            vehicles.append(v)
    logger.info(
        "DealerVenom: mapped %d vehicles from %d Typesense docs for %s",
        len(vehicles),
        len(docs),
        dealer_name,
    )
    return vehicles
