"""
Parser for dealer.com-powered sites. Uses recursive search for vehicle list and
key-variant mapping. Accepts raw JSON (e.g. /getInventory, Algolia) or HTML.
"""
import json
import logging
import re
from backend.parsers.base import (
    extract_image_url,
    extract_mileage,
    extract_price,
    find_vehicle_list,
    norm_int,
    norm_str,
)

logger = logging.getLogger(__name__)


def _safe_str(v, default: str = "N/A") -> str:
    """Trimmed string; use default when missing or empty."""
    s = norm_str(v) if v is not None else ""
    return s if s else default


def _build_title(obj: dict, year: int, make: str, model: str) -> str:
    """Title: if vehicle.title is array -> join(' '); if string use it; else fallback to year make model."""
    fallback = f"{year or ''} {make or ''} {model or ''}".strip() or "N/A"
    raw = obj.get("title") or obj.get("name")
    if raw is None:
        return fallback
    if isinstance(raw, list):
        s = " ".join(str(x).strip() for x in raw if x is not None).strip()
        return s if s else fallback
    s = norm_str(raw)
    return s if s else fallback


def _map_vehicle(obj: dict, base_url: str, dealer_id: str, dealer_name: str, dealer_url: str) -> dict | None:
    """Map one vehicle object. Defensive to nested pricing/attributes/media and missing keys."""
    if not isinstance(obj, dict):
        return None
    vin = norm_str(obj.get("vin") or obj.get("VIN") or obj.get("stockNumber") or "")
    if not vin:
        vin = f"unknown-{hash(str(obj)) % 10**8}"
    year = norm_int(obj.get("year") or obj.get("modelYear") or obj.get("model_year"))
    make = norm_str(obj.get("make") or obj.get("Make") or "")
    model = norm_str(obj.get("model") or obj.get("Model") or obj.get("modelName") or "")
    title = _build_title(obj, year, make, model)
    return {
        "vin": vin,
        "year": year,
        "make": make,
        "model": model,
        "trim": _safe_str(obj.get("trim") or obj.get("Trim") or obj.get("trimName")),
        "price": extract_price(obj),
        "mileage": extract_mileage(obj),
        "image_url": extract_image_url(obj, base_url),
        "dealer_id": dealer_id,
        "dealer_name": dealer_name,
        "dealer_url": dealer_url,
        "title": title,
        "zip_code": _safe_str(obj.get("zipCode") or obj.get("zip_code")),
        "fuel_type": _safe_str(obj.get("fuelType") or obj.get("fuel_type")),
        "transmission": _safe_str(obj.get("transmission") or obj.get("transmissionType")),
        "drivetrain": _safe_str(obj.get("drivetrain") or obj.get("driveType")),
        "exterior_color": _safe_str(obj.get("exteriorColor") or obj.get("exterior_color")),
        "interior_color": _safe_str(obj.get("interiorColor") or obj.get("interior_color")),
    }


def _has_vehicle_ident(obj: dict) -> bool:
    """True if object looks like a vehicle (has vin/VIN/stockNumber or nested pricing)."""
    if obj.get("vin") or obj.get("VIN") or obj.get("stockNumber"):
        return True
    pricing = obj.get("pricing") if isinstance(obj.get("pricing"), dict) else None
    if pricing and (pricing.get("internetPrice") or pricing.get("msrp") or pricing.get("salePrice")):
        return True
    return False


def _parse_json_list(data, base_url: str, dealer_id: str, dealer_name: str, dealer_url: str) -> list[dict]:
    items = find_vehicle_list(data)
    if not items:
        return []
    out = []
    for obj in items:
        if not isinstance(obj, dict):
            continue
        if not _has_vehicle_ident(obj):
            continue
        mapped = _map_vehicle(obj, base_url, dealer_id, dealer_name, dealer_url)
        if mapped:
            out.append(mapped)
    return out


def _extract_from_html(html: str) -> list | dict | None:
    if not html or "__PRELOADED_STATE__" not in html and "InventoryData" not in html:
        return None
    m = re.search(r"__PRELOADED_STATE__\s*=\s*(\{.*?\});?\s*(?:</script>|$)", html, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(1))
        except json.JSONDecodeError:
            pass
    m = re.search(r"window\.InventoryData\s*=\s*(\[.*?\]);?\s*(?:</script>|$)", html, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(1))
        except json.JSONDecodeError:
            pass
    return None


def parse(raw_data, base_url: str, dealer_id: str, dealer_name: str = "", dealer_url: str = ""):
    """
    raw_data: either a list/dict (from network JSON) or HTML string.
    Uses recursive search for first list with >= 3 dicts with vin/VIN, then maps key variants.
    """
    if isinstance(raw_data, (list, dict)):
        return _parse_json_list(raw_data, base_url, dealer_id, dealer_name, dealer_url)
    if isinstance(raw_data, str):
        extracted = _extract_from_html(raw_data)
        if extracted is not None:
            return _parse_json_list(extracted, base_url, dealer_id, dealer_name, dealer_url)
    return []
