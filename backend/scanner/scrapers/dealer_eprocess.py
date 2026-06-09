"""
DealerEProcess inventory scraper.

DealerEProcess sites host inventory as static JSON datasets under:
  {base_url}/assets/{site_id}/datasets/vehicle-facts.json
  {base_url}/assets/{site_id}/datasets/canonicallexicon.json

vehicle-facts.json: dict of vehicle_id → vehicle_data where numeric field values
  are integer IDs that decode via canonicallexicon.json.
canonicallexicon.json: list of {canonical, type, id} mapping integer IDs to strings.

Detection: page HTML or assets URLs contain "dealereprocess".
Site ID: extracted from /assets/{site_id}/ URL pattern in page HTML or response URLs.
"""
from __future__ import annotations

import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


def _extract_site_id(html: str) -> str | None:
    """Extract DealerEProcess site ID from /assets/{site_id}/ URL pattern."""
    m = re.search(r"/assets/([a-zA-Z0-9_-]+)/", html)
    return m.group(1) if m else None


def _build_lexicon(lexicon_data: list[dict]) -> dict[str, dict[str, str]]:
    """Build lookup: {field_type: {str_id: canonical_name}}."""
    lookup: dict[str, dict[str, str]] = {}
    for entry in lexicon_data:
        if not isinstance(entry, dict):
            continue
        t = str(entry.get("type") or "").strip()
        eid = str(entry.get("id") or "").strip()
        canonical = str(entry.get("canonical") or "").strip()
        if t and eid and canonical:
            lookup.setdefault(t, {})[eid] = canonical
    return lookup


def _decode(lexicon: dict[str, dict[str, str]], field_type: str, raw_id: Any) -> str | None:
    """Decode integer ID to canonical string using the lexicon."""
    if raw_id is None:
        return None
    sub = lexicon.get(field_type)
    if not sub:
        return None
    return sub.get(str(raw_id))


def _parse_condition(vehicle: dict) -> str:
    if vehicle.get("flag_new") == 1:
        return "New"
    if vehicle.get("flag_certified") == 1:
        return "Certified Pre-Owned"
    return "Used"


def _map_vehicle(
    vehicle: dict,
    lexicon: dict[str, dict[str, str]],
    base_url: str,
    dealer_id: str,
    dealer_name: str,
    dealer_url: str,
) -> dict[str, Any] | None:
    vin = str(vehicle.get("vin") or "").strip().upper()
    if not vin:
        return None

    year_raw = vehicle.get("year")
    year = int(year_raw) if year_raw and str(year_raw).isdigit() else None

    make_raw = _decode(lexicon, "make", vehicle.get("make"))
    model_raw = _decode(lexicon, "model", vehicle.get("model"))
    trim_raw = _decode(lexicon, "trim", vehicle.get("trim"))

    price_raw = vehicle.get("price")
    price: int | None = None
    try:
        p = float(price_raw or 0)
        if p > 0:
            price = int(p)
    except (TypeError, ValueError):
        pass

    drivetrain = _decode(lexicon, "drivetrain", vehicle.get("drivetrain"))
    transmission = _decode(lexicon, "transmission", vehicle.get("transmission"))
    fuel_type = _decode(lexicon, "fuel_type", vehicle.get("fuel_type"))
    engine = _decode(lexicon, "engine", vehicle.get("engine"))
    ext_color_generic = _decode(lexicon, "exterior_generic_color", vehicle.get("exterior_generic_color"))

    condition = _parse_condition(vehicle)
    stock = str(vehicle.get("stock") or "").strip() or None

    out = {
        "vin": vin,
        "year": year,
        "make": make_raw,
        "model": model_raw,
        "trim": trim_raw,
        "price": price,
        "mileage": None,
        "condition": condition,
        "exterior_color": ext_color_generic,
        "interior_color": None,
        "body_style": None,
        "transmission": transmission,
        "fuel_type": fuel_type,
        "drivetrain": drivetrain,
        "engine_description": engine,
        "stock_number": stock,
        "image_url": "",
        "gallery": [],
        "dealer_name": dealer_name,
        "dealer_url": dealer_url,
        "dealer_id": dealer_id,
    }
    from backend.parsers.vdp_urls import apply_vehicle_source_url

    apply_vehicle_source_url(out)
    return out


async def _browser_fetch_json(page: Any, url: str) -> Any:
    """Fetch a URL using the browser's JS fetch (inherits session cookies and sec-* headers)."""
    try:
        result = await page.evaluate(
            """async (url) => {
                try {
                    const r = await fetch(url, {credentials: 'include'});
                    if (!r.ok) return {_error: r.status};
                    return await r.json();
                } catch(e) { return {_error: String(e)}; }
            }""",
            url,
        )
        if isinstance(result, dict) and "_error" in result:
            raise RuntimeError(f"browser fetch error: {result['_error']}")
        return result
    except Exception as e:
        raise RuntimeError(f"browser fetch failed for {url}: {e}") from e


async def scrape_dealer_eprocess_from_page(
    page: Any,
    base_url: str,
    dealer_id: str,
    dealer_name: str,
    dealer_url: str,
) -> list[dict[str, Any]]:
    """
    Extract DealerEProcess inventory by fetching static JSON datasets via browser JS fetch
    (shares browser session, cookies, and sec-* headers to bypass 403).
    Returns list of vehicle dicts (empty when DealerEProcess not detected).
    """
    try:
        html = await page.content()
    except Exception as e:
        logger.debug("DealerEProcess: could not get page content: %s", e)
        return []

    html_lower = html.lower()
    if "dealereprocess" not in html_lower:
        return []

    site_id = _extract_site_id(html)
    if not site_id:
        logger.debug("DealerEProcess: could not extract site_id for %s", dealer_name)
        return []

    logger.info("DealerEProcess: detected for %s — site_id=%s", dealer_name, site_id)

    datasets_base = f"{base_url.rstrip('/')}/assets/{site_id}/datasets"
    facts_url = f"{datasets_base}/vehicle-facts.json"
    lexicon_url = f"{datasets_base}/canonicallexicon.json"

    try:
        facts_data = await _browser_fetch_json(page, facts_url)
    except Exception as e:
        logger.warning("DealerEProcess: failed to fetch vehicle-facts.json for %s: %s", dealer_name, e)
        return []

    try:
        lexicon_data = await _browser_fetch_json(page, lexicon_url)
    except Exception as e:
        logger.warning("DealerEProcess: failed to fetch canonicallexicon.json for %s: %s", dealer_name, e)
        lexicon_data = []

    if not isinstance(facts_data, dict):
        logger.warning("DealerEProcess: vehicle-facts.json is not a dict for %s", dealer_name)
        return []

    lexicon = _build_lexicon(lexicon_data if isinstance(lexicon_data, list) else [])

    vehicles: list[dict[str, Any]] = []
    for _vid, vehicle in facts_data.items():
        if not isinstance(vehicle, dict):
            continue
        v = _map_vehicle(vehicle, lexicon, base_url, dealer_id, dealer_name, dealer_url)
        if v:
            vehicles.append(v)

    logger.info("DealerEProcess: scraped %d vehicles for %s", len(vehicles), dealer_name)
    return vehicles
