"""
Parser for dealer.com-powered sites. Uses recursive search for vehicle list and
key-variant mapping. Accepts raw JSON (e.g. /getInventory, Algolia) or HTML.
"""
import json
import logging
import re
from backend.parsers.base import (
    clean_image_url,
    extract_image_url,
    extract_mileage,
    extract_price,
    find_vehicle_list,
    norm_int,
    norm_str,
)

logger = logging.getLogger(__name__)


def _map_vehicle(obj: dict, base_url: str, dealer_id: str, dealer_name: str, dealer_url: str) -> dict | None:
    vin = norm_str(obj.get("vin") or obj.get("VIN") or obj.get("stockNumber") or "")
    if not vin:
        vin = f"unknown-{hash(str(obj)) % 10**8}"
    return {
        "vin": vin,
        "year": norm_int(obj.get("year") or obj.get("modelYear") or obj.get("model_year")),
        "make": norm_str(obj.get("make") or obj.get("Make")),
        "model": norm_str(obj.get("model") or obj.get("Model") or obj.get("modelName")),
        "trim": norm_str(obj.get("trim") or obj.get("Trim") or obj.get("trimName")),
        "price": extract_price(obj),
        "mileage": extract_mileage(obj),
        "image_url": extract_image_url(obj, base_url),
        "dealer_id": dealer_id,
        "dealer_name": dealer_name,
        "dealer_url": dealer_url,
        "title": norm_str(obj.get("title") or obj.get("name") or ""),
        "zip_code": norm_str(obj.get("zipCode") or obj.get("zip_code")),
        "fuel_type": norm_str(obj.get("fuelType") or obj.get("fuel_type")),
        "transmission": norm_str(obj.get("transmission") or obj.get("transmissionType")),
        "drivetrain": norm_str(obj.get("drivetrain") or obj.get("driveType")),
        "exterior_color": norm_str(obj.get("exteriorColor") or obj.get("exterior_color")),
        "interior_color": norm_str(obj.get("interiorColor") or obj.get("interior_color")),
    }


def _parse_json_list(data, base_url: str, dealer_id: str, dealer_name: str, dealer_url: str) -> list[dict]:
    items = find_vehicle_list(data)
    if not items:
        return []
    out = []
    for obj in items:
        if not isinstance(obj, dict):
            continue
        if not (obj.get("vin") or obj.get("VIN") or obj.get("price") or obj.get("salePrice") or obj.get("internetPrice")):
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
