"""
Parser for dealer.com getInventory API. Maps exact schema:
  title (array -> join), trackingPricing.internetPrice, trackingAttributes (odometer, exteriorColor),
  images[].uri -> gallery, vin, stockNumber, fuelType.
"""
import json
import logging
import re
from backend.parsers.base import (
    clean_image_url,
    find_tracking_attr,
    find_vehicle_list,
    norm_float,
    norm_int,
    norm_str,
)

logger = logging.getLogger(__name__)

DEFAULT_STR = "N/A"


def _safe_str(v, default: str = DEFAULT_STR) -> str:
    """Trimmed string; use default when missing or empty."""
    if v is None:
        return default
    s = norm_str(v)
    return s if s else default


def _extract_title(obj: dict, year: int, make: str, model: str) -> str:
    """Dealer.com returns title as array e.g. ["Used 2026 BMW", "i5 eDrive40"]. Join with ' '. Fallback: year make model."""
    raw = obj.get("title") or obj.get("name")
    if raw is None:
        return f"{year or ''} {make or ''} {model or ''}".strip() or DEFAULT_STR
    if isinstance(raw, list):
        s = " ".join(str(x).strip() for x in raw if x is not None).strip()
        return s if s else f"{year or ''} {make or ''} {model or ''}".strip() or DEFAULT_STR
    s = norm_str(raw)
    return s if s else f"{year or ''} {make or ''} {model or ''}".strip() or DEFAULT_STR


def _extract_price_dealer_com(obj: dict) -> float:
    """Exact path: vehicle.trackingPricing.internetPrice. Fallback: vehicle.pricing.retailPrice. Strip $ and ','."""
    tracking = obj.get("trackingPricing") if isinstance(obj.get("trackingPricing"), dict) else None
    v = None
    if tracking is not None:
        v = tracking.get("internetPrice")
    if v is None or (isinstance(v, str) and "contact" in (v or "").lower()):
        pricing = obj.get("pricing") if isinstance(obj.get("pricing"), dict) else None
        v = pricing.get("retailPrice") if pricing else None
    if v is None:
        pricing = obj.get("pricing") if isinstance(obj.get("pricing"), dict) else None
        v = obj.get("price") or obj.get("internetPrice") or (pricing.get("salePrice") if pricing else None)
    return norm_float(v)


def _extract_mileage_dealer_com(obj: dict) -> int:
    """Find in vehicle.trackingAttributes where name === 'odometer', extract value. Default 0."""
    arr = obj.get("trackingAttributes")
    v = find_tracking_attr(arr, "odometer", "value")
    if v is not None and v != "":
        return norm_int(v)
    v = obj.get("odometer") or obj.get("mileage") or (find_tracking_attr(arr, "mileage", "value") if arr else None)
    return norm_int(v)


def _extract_gallery(obj: dict, base_url: str) -> list[str]:
    """Map vehicle.images to list of URLs using uri. Defensive: missing or empty array -> []."""
    images = obj.get("images") or obj.get("Images")
    if not isinstance(images, list) or len(images) == 0:
        return []
    out = []
    for item in images:
        if not isinstance(item, dict):
            continue
        u = item.get("uri") or item.get("url") or item.get("URL")
        if u and isinstance(u, str) and u.strip():
            out.append(clean_image_url(u.strip(), base_url))
    return out


def _extract_exterior_color(obj: dict) -> str:
    """Find in vehicle.trackingAttributes where name === 'exteriorColor'. Default N/A."""
    arr = obj.get("trackingAttributes")
    v = find_tracking_attr(arr, "exteriorColor", "value")
    if v is not None and str(v).strip():
        return norm_str(v) or DEFAULT_STR
    return _safe_str(obj.get("exteriorColor") or obj.get("exterior_color"))


def _map_vehicle(obj: dict, base_url: str, dealer_id: str, dealer_name: str, dealer_url: str) -> dict | None:
    """Map one vehicle from Dealer.com getInventory schema. Defensive: missing arrays/keys -> N/A or 0."""
    if not isinstance(obj, dict):
        return None

    vin = norm_str(obj.get("vin") or obj.get("VIN") or "")
    if not vin:
        vin = norm_str(obj.get("stockNumber") or "")
    if not vin:
        vin = f"unknown-{hash(str(obj)) % 10**8}"

    stock_number = _safe_str(obj.get("stockNumber"), default="")
    if stock_number == DEFAULT_STR:
        stock_number = ""

    year = norm_int(obj.get("year") or obj.get("modelYear") or obj.get("model_year"))
    make = norm_str(obj.get("make") or obj.get("Make") or "")
    model = norm_str(obj.get("model") or obj.get("Model") or obj.get("modelName") or "")

    title = _extract_title(obj, year, make, model)
    price = _extract_price_dealer_com(obj)
    mileage = _extract_mileage_dealer_com(obj)
    gallery = _extract_gallery(obj, base_url)
    image_url = gallery[0] if gallery else ""
    exterior_color = _extract_exterior_color(obj)
    fuel_type = _safe_str(obj.get("fuelType") or obj.get("fuel_type"))

    return {
        "vin": vin,
        "stock_number": stock_number,
        "year": year,
        "make": make,
        "model": model,
        "trim": _safe_str(obj.get("trim") or obj.get("Trim") or obj.get("trimName")),
        "title": title,
        "price": price,
        "mileage": mileage,
        "image_url": image_url,
        "gallery": gallery,
        "dealer_id": dealer_id,
        "dealer_name": dealer_name,
        "dealer_url": dealer_url,
        "zip_code": _safe_str(obj.get("zipCode") or obj.get("zip_code")),
        "fuel_type": fuel_type,
        "transmission": _safe_str(obj.get("transmission") or obj.get("transmissionType")),
        "drivetrain": _safe_str(obj.get("drivetrain") or obj.get("driveType")),
        "exterior_color": exterior_color,
        "interior_color": _safe_str(obj.get("interiorColor") or obj.get("interior_color")),
    }


def _has_vehicle_ident(obj: dict) -> bool:
    """True if object looks like a vehicle (vin, VIN, stockNumber, or trackingPricing)."""
    if obj.get("vin") or obj.get("VIN") or obj.get("stockNumber"):
        return True
    tp = obj.get("trackingPricing") if isinstance(obj.get("trackingPricing"), dict) else None
    if tp and (tp.get("internetPrice") or tp.get("retailPrice")):
        return True
    p = obj.get("pricing") if isinstance(obj.get("pricing"), dict) else None
    if p and (p.get("retailPrice") or p.get("internetPrice") or p.get("salePrice")):
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
    raw_data: list/dict (getInventory JSON) or HTML string.
    Maps Dealer.com schema: trackingPricing, trackingAttributes, images->gallery, etc.
    """
    if isinstance(raw_data, (list, dict)):
        return _parse_json_list(raw_data, base_url, dealer_id, dealer_name, dealer_url)
    if isinstance(raw_data, str):
        extracted = _extract_from_html(raw_data)
        if extracted is not None:
            return _parse_json_list(extracted, base_url, dealer_id, dealer_name, dealer_url)
    return []
