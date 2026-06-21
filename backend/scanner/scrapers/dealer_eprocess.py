"""
DealerEProcess inventory scraper.

DealerEProcess sites host inventory via three mechanisms:

**Legacy (v3) — static JSON datasets:**
  {base_url}/assets/{site_id}/datasets/vehicle-facts.json
  {base_url}/assets/{site_id}/datasets/canonicallexicon.json

  vehicle-facts.json: dict of vehicle_id → vehicle_data where numeric field values
    are integer IDs that decode via canonicallexicon.json.
  canonicallexicon.json: list of {canonical, type, id} mapping integer IDs to strings.

**Newer (v4) — REST API at /resrc/inventory/:**
  {base_url}/resrc/inventory/?flag_new=1    → new inventory
  {base_url}/resrc/inventory/?flag_used=1   → used inventory

  Returns a JSON object with a "vehicles" list (or similar) where all fields are
  already decoded human-readable strings. No lexicon required.

**Newer (v4+) — JSON-LD in SRP HTML:**
  SRP pages embed schema.org Vehicle JSON-LD blocks directly in the HTML.
  These contain VIN, price, color, transmission, etc. and full image galleries.
  SRP URLs: /search/{make}-{city}-{state}/?cy={zip}&tp=new|used|cpo

**Universal — /resrc/inventory/results/ REST API (Strategy 0):**
  {base_url}/resrc/inventory/results/?flag_new=0   → used/CPO inventory
  {base_url}/resrc/inventory/results/?flag_new=1   → new inventory

  Pure HTTP request with universal CDN credential (hardcoded in filter_search.min.js).
  Returns {"total": N, "details": {vehicle_id: {"title": ..., "url": ..., "detail": {...}}}}.
  Works even when SRP pages are Cloudflare-protected because /resrc/* paths are unguarded.
  This is tried first — if it returns vehicles, all browser-based strategies are skipped.

Detection: page HTML or assets URLs contain "dealereprocess".
Site ID: extracted from /assets/{site_id}/ URL pattern in page HTML or response URLs.
"""
from __future__ import annotations

import asyncio
import base64 as _base64
import json as _json
import logging
import re
import urllib.error as _urllib_error
import urllib.request as _urllib_request
from typing import Any

logger = logging.getLogger(__name__)

_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

# Universal credential embedded in cdn.dealereprocess.org/cdn/js/search/filter_search.min.js
_RESULTS_AUTH = "Basic " + _base64.b64encode(b"DEP:20tD3QEscbHFY").decode()


_GENERIC_ASSET_NAMES = frozenset({
    "libs", "lib", "js", "css", "static", "dist", "build", "bundle",
    "vendor", "assets", "shared", "common", "scripts", "styles", "fonts",
    "images", "img", "icons", "media", "public",
})


def _extract_site_id(html: str) -> str | None:
    """Extract DealerEProcess site ID from /assets/{site_id}/ URL pattern.

    Skips generic folder names that appear in standard asset paths but are
    not dealer-specific site IDs (e.g. /assets/libs/, /assets/js/).
    """
    for m in re.finditer(r"/assets/([a-zA-Z0-9_-]+)/", html):
        candidate = m.group(1)
        if candidate.lower() not in _GENERIC_ASSET_NAMES and len(candidate) >= 3:
            return candidate
    return None


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


_SRP_PATHS = ["/inventory/", "/new-inventory/", "/used-inventory/", "/inventory/new/"]

# ---------------------------------------------------------------------------
# DealerEProcess v4 /resrc/inventory/ REST API
# ---------------------------------------------------------------------------
# On newer eProcess sites the static vehicle-facts.json is not published.
# Instead the SRP makes XHR calls to /resrc/inventory/ which returns vehicle
# records as plain JSON with human-readable string fields (no lexicon needed).
#
# Known response shapes (may vary by site):
#   • {"vehicles": [...], "total": N, ...}
#   • {"data": [...], ...}
#   • [...] (bare list)
#
# Each vehicle record typically has: vin, year, make, model, trim, price,
# mileage, stock, condition (or flag_new/flag_used), exterior_color, etc.
# ---------------------------------------------------------------------------


def _map_vehicle_resrc(
    vehicle: dict,
    base_url: str,
    dealer_id: str,
    dealer_name: str,
    dealer_url: str,
) -> dict[str, Any] | None:
    """Map a /resrc/inventory/ vehicle record to the canonical vehicle dict.

    Fields are already human-readable strings — no lexicon decoding needed.
    """
    vin = str(vehicle.get("vin") or "").strip().upper()
    if not vin:
        return None

    year_raw = vehicle.get("year")
    year: int | None = None
    try:
        if year_raw is not None:
            year = int(year_raw)
    except (TypeError, ValueError):
        pass

    price_raw = vehicle.get("price") or vehicle.get("msrp") or vehicle.get("internet_price")
    price: int | None = None
    try:
        p = float(price_raw or 0)
        if p > 0:
            price = int(p)
    except (TypeError, ValueError):
        pass

    mileage_raw = vehicle.get("mileage") or vehicle.get("miles")
    mileage: int | None = None
    try:
        m = float(mileage_raw or 0)
        if m > 0:
            mileage = int(m)
    except (TypeError, ValueError):
        pass

    # Condition: check flag fields first, then condition string
    if vehicle.get("flag_new") in (1, "1", True):
        condition = "New"
    elif vehicle.get("flag_certified") in (1, "1", True):
        condition = "Certified Pre-Owned"
    elif vehicle.get("flag_used") in (1, "1", True):
        condition = "Used"
    else:
        cond_str = str(vehicle.get("condition") or vehicle.get("type") or "").lower()
        if "new" in cond_str:
            condition = "New"
        elif "certif" in cond_str:
            condition = "Certified Pre-Owned"
        else:
            condition = "Used"

    ext_color = (
        str(vehicle.get("exterior_color") or vehicle.get("color") or
            vehicle.get("ext_color") or vehicle.get("exterior_generic_color") or "").strip()
        or None
    )
    int_color = (
        str(vehicle.get("interior_color") or vehicle.get("int_color") or "").strip()
        or None
    )

    out = {
        "vin": vin,
        "year": year,
        "make": str(vehicle.get("make") or "").strip() or None,
        "model": str(vehicle.get("model") or "").strip() or None,
        "trim": str(vehicle.get("trim") or "").strip() or None,
        "price": price,
        "mileage": mileage,
        "condition": condition,
        "exterior_color": ext_color,
        "interior_color": int_color,
        "body_style": str(vehicle.get("body_style") or vehicle.get("body") or "").strip() or None,
        "transmission": str(vehicle.get("transmission") or "").strip() or None,
        "fuel_type": str(vehicle.get("fuel_type") or vehicle.get("fuel") or "").strip() or None,
        "drivetrain": str(vehicle.get("drivetrain") or vehicle.get("drive") or "").strip() or None,
        "engine_description": str(vehicle.get("engine") or vehicle.get("engine_description") or "").strip() or None,
        "stock_number": str(vehicle.get("stock") or vehicle.get("stock_number") or "").strip() or None,
        "image_url": "",
        "gallery": [],
        "dealer_name": dealer_name,
        "dealer_url": dealer_url,
        "dealer_id": dealer_id,
    }
    from backend.parsers.vdp_urls import apply_vehicle_source_url

    apply_vehicle_source_url(out)
    return out


def _extract_vehicles_from_resrc(data: Any) -> list[dict]:
    """Extract vehicle list from various /resrc/inventory/ response shapes."""
    if isinstance(data, list):
        return [v for v in data if isinstance(v, dict)]
    if isinstance(data, dict):
        for key in ("vehicles", "data", "inventory", "results", "items"):
            val = data.get(key)
            if isinstance(val, list):
                return [v for v in val if isinstance(v, dict)]
        # Numeric-keyed dict (same shape as vehicle-facts.json)
        vehicles = []
        for v in data.values():
            if isinstance(v, dict) and v.get("vin"):
                vehicles.append(v)
        return vehicles
    return []


async def _fetch_resrc_inventory(
    page: Any,
    base_url: str,
    dealer_name: str,
) -> list[dict]:
    """
    Fetch inventory from the DealerEProcess v4 /resrc/inventory/ REST API.

    Tries new + used + all inventory endpoints in turn.  Returns a flat list
    of raw vehicle dicts (pre-mapping); empty list on failure.
    """
    base = base_url.rstrip("/")

    # Param sets to try: (new, used, all-without-filter)
    param_sets = [
        "?flag_new=1&tp=new",
        "?flag_used=1&tp=used",
        "",  # all inventory
    ]

    all_vehicles: list[dict] = []
    seen_vins: set[str] = set()

    for params in param_sets:
        url = f"{base}/resrc/inventory/{params}"
        try:
            data = await _browser_fetch_json(page, url)
        except Exception as e:
            logger.warning("DealerEProcess resrc: %s → %s", url, e)
            continue

        vehicles = _extract_vehicles_from_resrc(data)
        if not vehicles:
            logger.warning(
                "DealerEProcess resrc: no vehicles in response from %s — raw type=%s preview=%s",
                url, type(data).__name__, str(data)[:300],
            )
            continue

        new_count = 0
        for v in vehicles:
            vin = str(v.get("vin") or "").strip().upper()
            if vin and vin not in seen_vins:
                seen_vins.add(vin)
                all_vehicles.append(v)
                new_count += 1

        logger.info(
            "DealerEProcess resrc: fetched %d vehicles from %s for %s",
            new_count,
            url,
            dealer_name,
        )
        # If we got data from the first endpoint, don't also pull the "all" endpoint
        # to avoid duplicates; stop after getting vehicles from both new + used.
        if all_vehicles and params == "":
            break  # all-inventory endpoint would duplicate new+used

    return all_vehicles


async def _fetch_facts_with_fallback(
    page: Any,
    primary_facts_url: str,
    primary_lexicon_url: str,
    base_url: str,
    site_id: str,
    dealer_name: str,
) -> tuple[Any, str]:
    """
    Try to fetch vehicle-facts.json using three strategies in order:
    1. Primary constructed path (/assets/{site_id}/datasets/vehicle-facts.json)
    2. Alternate path without datasets/ subfolder (/assets/{site_id}/vehicle-facts.json)
    3. SRP navigation + response interception to discover the real vehicle-facts.json URL

    Returns (facts_data, lexicon_url) where facts_data is None on total failure.
    For v4 /resrc/inventory/ sites use _fetch_resrc_inventory() separately.
    """
    # Strategy 1: primary path
    try:
        data = await _browser_fetch_json(page, primary_facts_url)
        return data, primary_lexicon_url
    except Exception as e:
        logger.warning("DealerEProcess: primary vehicle-facts.json 404/error for %s: %s", dealer_name, e)

    # Strategy 2: alternate path without datasets/ subfolder
    alt_facts_url = f"{base_url.rstrip('/')}/assets/{site_id}/vehicle-facts.json"
    alt_lexicon_url = f"{base_url.rstrip('/')}/assets/{site_id}/canonicallexicon.json"
    try:
        data = await _browser_fetch_json(page, alt_facts_url)
        logger.info("DealerEProcess: alternate path succeeded for %s: %s", dealer_name, alt_facts_url)
        return data, alt_lexicon_url
    except Exception:
        pass

    # Strategy 3: navigate SRP pages and intercept the real vehicle-facts.json URL
    captured_url: list[str] = []
    done_evt = asyncio.Event()

    def _on_response(resp: Any) -> None:
        try:
            url = str(getattr(resp, "url", "") or "")
            if "vehicle-facts.json" in url and not captured_url:
                captured_url.append(url)
                done_evt.set()
        except Exception:
            pass

    page.on("response", _on_response)
    try:
        for srp_path in _SRP_PATHS:
            srp_url = base_url.rstrip("/") + srp_path
            try:
                await page.goto(srp_url, wait_until="domcontentloaded", timeout=15_000)
            except Exception:
                continue
            try:
                await asyncio.wait_for(done_evt.wait(), timeout=12.0)
            except asyncio.TimeoutError:
                pass
            if captured_url:
                break
    finally:
        try:
            page.remove_listener("response", _on_response)
        except Exception:
            pass

    if not captured_url:
        logger.warning("DealerEProcess: could not discover vehicle-facts.json URL for %s", dealer_name)
        return None, primary_lexicon_url

    intercepted_facts_url = captured_url[0]
    intercepted_lexicon_url = intercepted_facts_url.replace("vehicle-facts.json", "canonicallexicon.json")
    logger.info(
        "DealerEProcess: intercepted vehicle-facts.json URL for %s: %s",
        dealer_name,
        intercepted_facts_url,
    )
    try:
        data = await _browser_fetch_json(page, intercepted_facts_url)
        return data, intercepted_lexicon_url
    except Exception as e:
        logger.warning("DealerEProcess: intercepted URL fetch failed for %s: %s", dealer_name, e)
        return None, primary_lexicon_url


# ---------------------------------------------------------------------------
# DealerEProcess v4+ JSON-LD in SRP HTML
# ---------------------------------------------------------------------------
# Newer eProcess sites embed schema.org Vehicle JSON-LD blocks directly in
# the SRP HTML. These contain VIN, price, colors, transmission, gallery, etc.
# The SRP URL typically has the form:
#   /search/{make}-{city}-{state}/?cy={zip}&tp=new|used|cpo
# and is reached via redirect from /new-inventory/index.htm.
# ---------------------------------------------------------------------------

_JSONLD_VEH_TYPES = frozenset({"Vehicle", "Car", "BusOrCoach", "MotorizedBicycle"})

_SRP_TYPE_PATHS = [
    ("/new-inventory/index.htm", "new"),
    ("/used-inventory/index.htm", "used"),
    ("/certified-inventory/index.htm", "cpo"),
    ("/new-inventory/", "new"),
    ("/used-inventory/", "used"),
    ("/certified-inventory/", "cpo"),
]


def _extract_jsonld_vehicles_from_html(html: str) -> list[dict]:
    """Extract schema.org Vehicle JSON-LD objects from raw HTML."""
    vehicles = []
    for raw in re.findall(
        r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html, re.S,
    ):
        try:
            data = _json.loads(raw)
        except Exception:
            continue
        items = data if isinstance(data, list) else [data]
        for item in items:
            if isinstance(item, dict) and item.get("@type") in _JSONLD_VEH_TYPES:
                vehicles.append(item)
    return vehicles


def _map_vehicle_jsonld(
    item: dict,
    base_url: str,
    dealer_id: str,
    dealer_name: str,
    dealer_url: str,
    condition_hint: str = "",
) -> dict[str, Any] | None:
    """Map a schema.org Vehicle JSON-LD block to the canonical vehicle dict."""
    vin = str(item.get("vehicleIdentificationNumber") or "").strip().upper()
    if not vin or len(vin) != 17:
        return None

    # Year from vehicleModelDate (ISO date "2026") or name ("New 2026 Kia K4 LX")
    year: int | None = None
    year_raw = str(item.get("vehicleModelDate") or item.get("releaseDate") or "")
    m = re.search(r"\b(19[89]\d|20[0-3]\d)\b", year_raw)
    if not m:
        m = re.search(r"\b(19[89]\d|20[0-3]\d)\b", str(item.get("name") or ""))
    if m:
        year = int(m.group(1))

    # Make/model from name ("New 2026 Kia K4 LX" or "2026 Kia K4 LX")
    name_str = str(item.get("name") or "")
    make_str: str | None = None
    model_str: str | None = None
    trim_str: str | None = None
    name_clean = re.sub(r"^(?:New|Used|Certified|Pre-Owned|CPO)\s+", "", name_str, flags=re.I)
    name_clean = re.sub(r"^\d{4}\s+", "", name_clean).strip()
    # manufacturer / brand fields
    brand = item.get("brand") or item.get("manufacturer")
    if isinstance(brand, dict):
        make_str = str(brand.get("name") or "").strip() or None
    elif isinstance(brand, str):
        make_str = brand.strip() or None
    if not make_str and item.get("model"):
        model_raw = item["model"]
        if isinstance(model_raw, dict):
            model_str = str(model_raw.get("name") or "").strip() or None
        elif isinstance(model_raw, str):
            model_str = model_raw.strip() or None
    # Fall back: split name_clean into make / rest
    if not make_str and name_clean:
        parts = name_clean.split(None, 1)
        make_str = parts[0] if parts else None
        if len(parts) > 1:
            rest = parts[1].split(None, 1)
            if not model_str:
                model_str = rest[0] if rest else None
            if len(rest) > 1:
                trim_str = rest[1] or None

    # Price from offers
    price: int | None = None
    offers = item.get("offers")
    if isinstance(offers, dict):
        try:
            p = float(offers.get("price") or 0)
            if p > 0:
                price = int(p)
        except (TypeError, ValueError):
            pass

    # Mileage
    mileage: int | None = None
    odometer = item.get("mileageFromOdometer")
    if isinstance(odometer, dict):
        try:
            mileage = int(float(odometer.get("value") or 0)) or None
        except (TypeError, ValueError):
            pass
    elif isinstance(odometer, (int, float)):
        mileage = int(odometer) or None

    # Condition
    condition = "Used"
    if condition_hint:
        condition = condition_hint
    elif "New" in name_str:
        condition = "New"
    elif "Certified" in name_str or "CPO" in name_str:
        condition = "Certified Pre-Owned"

    # Gallery: first image is main, rest are gallery
    images = item.get("image") or []
    if isinstance(images, str):
        images = [images]
    image_url = images[0] if images else ""
    gallery = list(images[1:]) if len(images) > 1 else []

    # VDP URL
    source_url = str(item.get("url") or "").strip()

    out: dict[str, Any] = {
        "vin": vin,
        "year": year,
        "make": make_str,
        "model": model_str,
        "trim": trim_str,
        "price": price,
        "mileage": mileage,
        "condition": condition,
        "exterior_color": str(item.get("color") or "").strip() or None,
        "interior_color": str(item.get("vehicleInteriorColor") or "").strip() or None,
        "body_style": None,
        "transmission": str(item.get("vehicleTransmission") or "").strip() or None,
        "fuel_type": str(item.get("fuelType") or "").strip() or None,
        "drivetrain": None,
        "engine_description": None,
        "stock_number": str(item.get("sku") or item.get("mpn") or "").strip() or None,
        "image_url": image_url,
        "gallery": gallery,
        "dealer_name": dealer_name,
        "dealer_url": dealer_url,
        "dealer_id": dealer_id,
    }
    engine = item.get("vehicleEngine")
    if isinstance(engine, dict):
        out["engine_description"] = str(engine.get("engineType") or engine.get("name") or "").strip() or None
    elif isinstance(engine, str):
        out["engine_description"] = engine.strip() or None

    if source_url:
        out["_source_url"] = source_url

    from backend.parsers.vdp_urls import apply_vehicle_source_url
    apply_vehicle_source_url(out)
    return out


async def _fetch_jsonld_from_srp(
    page: Any,
    base_url: str,
    dealer_id: str,
    dealer_name: str,
    dealer_url: str,
) -> list[dict[str, Any]]:
    """
    Navigate the SRP pages and extract schema.org Vehicle JSON-LD.

    Tries /new-inventory/index.htm, /used-inventory/index.htm, and
    /certified-inventory/index.htm in sequence.  Each typically redirects
    to a search URL that embeds 12–20 vehicles per load in JSON-LD.
    """
    seen_vins: set[str] = set()
    vehicles: list[dict[str, Any]] = []

    for srp_path, condition_hint in _SRP_TYPE_PATHS:
        srp_url = base_url.rstrip("/") + srp_path
        try:
            await page.goto(srp_url, wait_until="domcontentloaded", timeout=20_000)
        except Exception as e:
            logger.debug("DealerEProcess jsonld: SRP nav failed %s: %s", srp_url, e)
            continue

        try:
            html = await page.content()
        except Exception:
            continue

        items = _extract_jsonld_vehicles_from_html(html)
        if not items:
            continue

        count = 0
        cond_map = {"new": "New", "used": "Used", "cpo": "Certified Pre-Owned"}
        cond = cond_map.get(condition_hint, "")
        for item in items:
            v = _map_vehicle_jsonld(item, base_url, dealer_id, dealer_name, dealer_url, cond)
            if v and v["vin"] not in seen_vins:
                seen_vins.add(v["vin"])
                vehicles.append(v)
                count += 1

        logger.info(
            "DealerEProcess jsonld: %d vehicles from %s for %s",
            count, srp_url, dealer_name,
        )
        # Only try paired new+used paths (not duplicate generic paths)
        if srp_path.endswith("index.htm") and len(seen_vins) > 0:
            continue  # keep going for new/used/cpo
        elif not srp_path.endswith("index.htm"):
            break  # fallback generic paths - stop after first hit

    return vehicles


def _map_vehicle_results(
    vehicle_id: str,
    obj: dict,
    base_url: str,
    dealer_id: str,
    dealer_name: str,
    dealer_url: str,
) -> dict[str, Any] | None:
    """Map a /resrc/inventory/results/ vehicle entry to the canonical vehicle dict."""
    detail = obj.get("detail") or {}
    vin = str(detail.get("vin") or "").strip().upper()
    if not vin or len(vin) != 17:
        return None

    certified = str(detail.get("certified") or "").lower()
    cond_raw = str(detail.get("condition") or "").lower()
    if certified == "yes":
        condition = "Certified Pre-Owned"
    elif "new" in cond_raw:
        condition = "New"
    else:
        condition = "Used"

    price: int | None = None
    try:
        p = float(detail.get("price") or 0)
        if p > 0:
            price = int(p)
    except (TypeError, ValueError):
        pass

    mileage: int | None = None
    try:
        m = float(detail.get("odometer") or 0)
        if m > 0:
            mileage = int(m)
    except (TypeError, ValueError):
        pass

    year: int | None = None
    try:
        if detail.get("year"):
            year = int(detail["year"])
    except (TypeError, ValueError):
        pass

    ext_color = (
        str(detail.get("exterior_color_manufacturer") or detail.get("exterior_color") or "").strip()
        or None
    )
    int_color = (
        str(detail.get("interior_color_manufacturer") or detail.get("interior_color") or "").strip()
        or None
    )

    vdp_path = str(obj.get("url") or "").strip()

    out: dict[str, Any] = {
        "vin": vin,
        "year": year,
        "make": str(detail.get("make") or "").strip() or None,
        "model": str(detail.get("model") or "").strip() or None,
        "trim": str(detail.get("trim") or "").strip() or None,
        "price": price,
        "mileage": mileage,
        "condition": condition,
        "exterior_color": ext_color,
        "interior_color": int_color,
        "body_style": str(detail.get("body") or "").strip() or None,
        "transmission": str(detail.get("transmission") or "").strip() or None,
        "fuel_type": None,
        "drivetrain": str(detail.get("drivetrain") or "").strip() or None,
        "engine_description": None,
        "stock_number": str(detail.get("stock") or "").strip() or None,
        "image_url": "",
        "gallery": [],
        "dealer_name": dealer_name,
        "dealer_url": dealer_url,
        "dealer_id": dealer_id,
    }

    if vdp_path:
        out["_source_url"] = base_url.rstrip("/") + "/" + vdp_path.lstrip("/")

    from backend.parsers.vdp_urls import apply_vehicle_source_url
    apply_vehicle_source_url(out)
    return out


async def _fetch_results_api(
    base_url: str,
    dealer_id: str,
    dealer_name: str,
    dealer_url: str,
) -> list[dict[str, Any]]:
    """
    Fetch full inventory from /resrc/inventory/results/ using the universal DEP credential.

    Pure HTTP — no Playwright required. Works even when SRP pages are Cloudflare-protected
    because /resrc/* paths are not covered by the CF Turnstile gate.

    Fetches used/CPO (flag_new=0) and new (flag_new=1) in two separate calls to ensure
    complete coverage and correct condition labelling.
    """
    base = base_url.rstrip("/")
    headers = {
        "Authorization": _RESULTS_AUTH,
        "Referer": base_url.rstrip("/") + "/",
        "User-Agent": _UA,
        "Accept": "application/json, */*",
    }

    condition_params = [
        ("?flag_new=0", "used/CPO"),
        ("?flag_new=1", "new"),
    ]

    vehicles: list[dict[str, Any]] = []
    seen_vins: set[str] = set()

    def _do_get(url: str) -> dict:
        req = _urllib_request.Request(url, headers=headers)
        with _urllib_request.urlopen(req, timeout=20) as resp:
            return _json.loads(resp.read())

    for params, label in condition_params:
        url = f"{base}/resrc/inventory/results/{params}"
        try:
            data = await asyncio.to_thread(_do_get, url)
        except _urllib_error.HTTPError as e:
            logger.warning("DealerEProcess results_api: %s → HTTP %s", url, e.code)
            continue
        except Exception as e:
            logger.warning("DealerEProcess results_api: %s → %s", url, e)
            continue

        details = data.get("details") or {}
        total = data.get("total", 0)
        if not details:
            logger.debug(
                "DealerEProcess results_api: empty details from %s (total=%d) for %s",
                url, total, dealer_name,
            )
            continue

        count = 0
        for vehicle_id, obj in details.items():
            v = _map_vehicle_results(vehicle_id, obj, base_url, dealer_id, dealer_name, dealer_url)
            if v and v["vin"] not in seen_vins:
                seen_vins.add(v["vin"])
                vehicles.append(v)
                count += 1

        logger.info(
            "DealerEProcess results_api: fetched %d/%d %s vehicles from %s for %s",
            count, total, label, url, dealer_name,
        )

    return vehicles


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

    Strategy order:
      0.   Universal /resrc/inventory/results/ REST API (pure HTTP, no CF bypass needed)
      1–3. Legacy vehicle-facts.json via _fetch_facts_with_fallback()
      4.   v4 /resrc/inventory/ REST API via _fetch_resrc_inventory()
      5.   JSON-LD in SRP HTML via _fetch_jsonld_from_srp()
    """
    try:
        html = await page.content()
    except Exception as e:
        logger.debug("DealerEProcess: could not get page content: %s", e)
        return []

    html_lower = html.lower()
    if "dealereprocess" not in html_lower:
        return []

    # Strategy 0: universal /resrc/inventory/results/ — pure HTTP, bypasses CF-gated SRP pages
    results_vehicles = await _fetch_results_api(base_url, dealer_id, dealer_name, dealer_url)
    if results_vehicles:
        logger.info(
            "DealerEProcess: scraped %d vehicles via results_api for %s",
            len(results_vehicles), dealer_name,
        )
        return results_vehicles

    logger.info(
        "DealerEProcess: results_api returned nothing for %s — falling back to browser strategies",
        dealer_name,
    )

    site_id = _extract_site_id(html)
    if not site_id:
        logger.debug("DealerEProcess: could not extract site_id for %s", dealer_name)
        return []

    logger.info("DealerEProcess: detected for %s — site_id=%s", dealer_name, site_id)

    datasets_base = f"{base_url.rstrip('/')}/assets/{site_id}/datasets"
    facts_url = f"{datasets_base}/vehicle-facts.json"
    lexicon_url = f"{datasets_base}/canonicallexicon.json"

    facts_data, lexicon_url = await _fetch_facts_with_fallback(
        page, facts_url, lexicon_url, base_url, site_id, dealer_name
    )

    # Strategy 4: v4 /resrc/inventory/ REST API (newer eProcess sites without vehicle-facts.json)
    if facts_data is None:
        logger.info(
            "DealerEProcess: vehicle-facts.json unavailable for %s — trying /resrc/inventory/ API",
            dealer_name,
        )
        resrc_raws = await _fetch_resrc_inventory(page, base_url, dealer_name)
        if resrc_raws:
            vehicles: list[dict[str, Any]] = []
            for raw in resrc_raws:
                v = _map_vehicle_resrc(raw, base_url, dealer_id, dealer_name, dealer_url)
                if v:
                    vehicles.append(v)
            logger.info(
                "DealerEProcess: scraped %d vehicles via /resrc/inventory/ for %s",
                len(vehicles),
                dealer_name,
            )
            return vehicles

        # Strategy 5: JSON-LD in SRP HTML (v4+ sites where API is unavailable)
        logger.info(
            "DealerEProcess: /resrc/inventory/ unavailable for %s — trying JSON-LD SRP extraction",
            dealer_name,
        )
        jsonld_vehicles = await _fetch_jsonld_from_srp(
            page, base_url, dealer_id, dealer_name, dealer_url
        )
        if not jsonld_vehicles:
            logger.warning(
                "DealerEProcess: all strategies failed for %s — no inventory returned",
                dealer_name,
            )
            return []
        logger.info(
            "DealerEProcess: scraped %d vehicles via JSON-LD SRP for %s",
            len(jsonld_vehicles),
            dealer_name,
        )
        return jsonld_vehicles

    # Legacy path: decode via lexicon
    try:
        lexicon_data = await _browser_fetch_json(page, lexicon_url)
    except Exception as e:
        logger.warning("DealerEProcess: failed to fetch canonicallexicon.json for %s: %s", dealer_name, e)
        lexicon_data = []

    if not isinstance(facts_data, dict):
        logger.warning("DealerEProcess: vehicle-facts.json is not a dict for %s", dealer_name)
        return []

    lexicon = _build_lexicon(lexicon_data if isinstance(lexicon_data, list) else [])

    vehicles = []
    for _vid, vehicle in facts_data.items():
        if not isinstance(vehicle, dict):
            continue
        v = _map_vehicle(vehicle, lexicon, base_url, dealer_id, dealer_name, dealer_url)
        if v:
            vehicles.append(v)

    logger.info("DealerEProcess: scraped %d vehicles for %s", len(vehicles), dealer_name)
    return vehicles
