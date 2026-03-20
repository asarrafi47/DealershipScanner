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
# Fallback when no images; frontend resolves /static/ relative to app
FALLBACK_IMAGE_URL = "/static/placeholder.svg"


def _safe_str(v, default: str = DEFAULT_STR) -> str:
    """Trimmed string; use default when missing or empty."""
    if v is None:
        return default
    s = norm_str(v)
    return s if s else default


def _extract_title(obj: dict, year: int, make: str, model: str) -> str:
    """Dealer.com: title is an array. Join with ' ' (e.g. vehicle['title'].join(' ')). Fallback: year make model."""
    raw = obj.get("title") or obj.get("name")
    if raw is None:
        return f"{year or ''} {make or ''} {model or ''}".strip() or DEFAULT_STR
    if isinstance(raw, list):
        s = " ".join(str(x).strip() for x in raw if x is not None).strip()
        return s if s else f"{year or ''} {make or ''} {model or ''}".strip() or DEFAULT_STR
    s = norm_str(raw)
    return s if s else f"{year or ''} {make or ''} {model or ''}".strip() or DEFAULT_STR


def _first_price(*vals) -> float:
    for v in vals:
        if v is None or v is False:
            continue
        if isinstance(v, str) and "contact" in v.lower():
            continue
        n = norm_float(v)
        if n > 0:
            return n
    return 0.0


def _extract_price_dealer_com(obj: dict) -> int:
    """
    Priority: trackingPricing.internetPrice → pricing.internetPrice → pricing.finalPrice
    → pricing.salePrice → pricing.msrp → price → trackingAttributes price/msrp.
    """
    tracking = obj.get("trackingPricing") or obj.get("tracking_pricing")
    pricing = obj.get("pricing") if isinstance(obj.get("pricing"), dict) else None

    raw = _first_price(
        isinstance(tracking, dict) and tracking.get("internetPrice"),
        isinstance(tracking, dict) and tracking.get("internet_price"),
        pricing and pricing.get("internetPrice"),
        pricing and pricing.get("internet_price"),
        pricing and pricing.get("finalPrice"),
        pricing and pricing.get("final_price"),
        pricing and pricing.get("salePrice"),
        pricing and pricing.get("sale_price"),
        pricing and pricing.get("msrp"),
        pricing and pricing.get("MSRP"),
        obj.get("price"),
        obj.get("internetPrice"),
        pricing and pricing.get("retailPrice"),
        pricing and pricing.get("retail_price"),
    )
    if raw == 0:
        arr = obj.get("trackingAttributes") or obj.get("tracking_attributes") or obj.get("attributes")
        if isinstance(arr, list):
            v2 = find_tracking_attr(arr, "price", "value") or find_tracking_attr(arr, "msrp", "value")
            if v2 is not None and str(v2).strip():
                raw = norm_float(v2)
    return int(round(raw))


def _extract_msrp_dealer_com(obj: dict) -> int:
    """MSRP for display when sale price is hidden (Unlock Price)."""
    pricing = obj.get("pricing") if isinstance(obj.get("pricing"), dict) else None
    raw = _first_price(
        pricing and pricing.get("msrp"),
        pricing and pricing.get("MSRP"),
        pricing and pricing.get("retailMsrp"),
        obj.get("msrp"),
    )
    if raw == 0:
        arr = obj.get("trackingAttributes") or obj.get("tracking_attributes") or obj.get("attributes")
        if isinstance(arr, list):
            v2 = find_tracking_attr(arr, "msrp", "value")
            if v2 is not None and str(v2).strip():
                raw = norm_float(v2)
    return int(round(raw))


def _extract_mileage_dealer_com(obj: dict) -> int:
    """Find object in trackingAttributes where name == 'odometer' and map its value to mileage. Default 0."""
    arr = obj.get("trackingAttributes") or obj.get("tracking_attributes")
    v = find_tracking_attr(arr, "odometer", "value")
    if v is not None and v != "":
        return norm_int(v)
    v = obj.get("odometer") or obj.get("mileage")
    if v is None and isinstance(arr, list):
        v = find_tracking_attr(arr, "mileage", "value")
    return norm_int(v)


def _extract_featured_or_thumbnail(obj: dict, base_url: str) -> str:
    """If vehicle.images is empty, use featuredImage or thumbnail as backup. Returns URL or ''."""
    for key in ("featuredImage", "featured_image", "thumbnail", "Thumbnail", "primaryImage", "primary_image"):
        val = obj.get(key)
        if val is None:
            continue
        if isinstance(val, str) and val.strip():
            return clean_image_url(val.strip(), base_url)
        if isinstance(val, dict):
            u = val.get("uri") or val.get("url") or val.get("URL")
            if u and isinstance(u, str) and u.strip():
                return clean_image_url(u.strip(), base_url)
    return ""


def _best_image_url(item: dict, base_url: str) -> str:
    """Prefer largest / full-res Dealer.com image fields, then fall back to thumbnail."""
    if not isinstance(item, dict):
        return ""
    for key in (
        "xxlargeUri",
        "xlargeUri",
        "largeUri",
        "fullUri",
        "hiResUri",
        "uri",
        "url",
        "URL",
        "imageUrl",
        "thumbnailUri",
        "thumbUrl",
    ):
        u = item.get(key)
        if u and isinstance(u, str) and u.strip():
            return clean_image_url(u.strip(), base_url)
    return ""


def _extract_gallery(obj: dict, base_url: str) -> list[str]:
    """Map vehicle.images to list of URLs (prefer full-res keys over thumbnail)."""
    images = obj.get("images") or obj.get("Images")
    if isinstance(images, list) and len(images) > 0:
        out = []
        seen: set[str] = set()
        for item in images:
            u = _best_image_url(item, base_url) if isinstance(item, dict) else ""
            if u and u not in seen:
                seen.add(u)
                out.append(u)
        if out:
            return out
    # Backup: single image from featuredImage or thumbnail
    one = _extract_featured_or_thumbnail(obj, base_url)
    return [one] if one else []


def _extract_history_highlights(obj: dict) -> list[str]:
    """Extract history badges from callout, badges, highlightedAttributes (e.g. 'No Accidents Reported', '1-Owner')."""
    out: list[str] = []
    seen: set[str] = set()

    def add(s: str) -> None:
        s = norm_str(s)
        if s and s.lower() not in seen:
            seen.add(s.lower())
            out.append(s)

    # callout: often array of badge strings or objects with text/label
    for key in ("callout", "callouts", "badges", "Badges", "historyBadges", "history_badges"):
        val = obj.get(key)
        if isinstance(val, list):
            for item in val:
                if isinstance(item, str):
                    add(item)
                elif isinstance(item, dict):
                    add(item.get("text") or item.get("label") or item.get("name") or item.get("value") or "")
        elif isinstance(val, str):
            add(val)

    # highlightedAttributes: sometimes Condition/History summary
    ha = obj.get("highlightedAttributes") or obj.get("highlighted_attributes")
    if isinstance(ha, list):
        for item in ha:
            if isinstance(item, dict):
                name = item.get("name") or item.get("key") or item.get("label")
                value = item.get("value") or item.get("text")
                if name and value:
                    add(f"{name}: {value}")
                elif value:
                    add(str(value))
                elif name:
                    add(str(name))
            elif isinstance(item, str):
                add(item)
    return out


def _extract_exterior_color(obj: dict) -> str:
    """Find in vehicle.trackingAttributes where name === 'exteriorColor'. Default N/A."""
    arr = obj.get("trackingAttributes") or obj.get("tracking_attributes")
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
    msrp = _extract_msrp_dealer_com(obj)
    mileage = _extract_mileage_dealer_com(obj)
    gallery = _extract_gallery(obj, base_url)
    image_url = gallery[0] if gallery else ""
    # Fallback image when gallery and image_url are empty
    if not image_url or not gallery:
        image_url = image_url or FALLBACK_IMAGE_URL
        gallery = gallery if gallery else [FALLBACK_IMAGE_URL]
    exterior_color = _extract_exterior_color(obj)
    fuel_type = _safe_str(obj.get("fuelType") or obj.get("fuel_type"))
    # Prefer dealer VHR/partner link (vhr.carfax.com) — less likely to trigger iframe verification than consumer link
    vhr_url = obj.get("vhr_url") or obj.get("carfax_token")
    if vhr_url and isinstance(vhr_url, str) and vhr_url.strip().startswith("http"):
        carfax_url = norm_str(vhr_url)
    elif vhr_url and vin and not vin.startswith("unknown"):
        carfax_url = f"https://vhr.carfax.com/main?vin={vin}"
    else:
        carfax_url = norm_str(
            obj.get("carfax_url")
            or obj.get("carfaxUrl")
            or obj.get("carfaxLink")
            or obj.get("history_report_url")
            or obj.get("vehicleHistoryUrl")
            or obj.get("vehicle_history_url")
            or ""
        )

    cyl = norm_int(obj.get("cylinders") or 0)
    if not cyl:
        arr = obj.get("trackingAttributes") or obj.get("tracking_attributes")
        c2 = find_tracking_attr(arr, "cylinders", "value") if isinstance(arr, list) else None
        if c2 is not None:
            cyl = norm_int(c2)

    return {
        "vin": vin,
        "stock_number": stock_number,
        "year": year,
        "make": make,
        "model": model,
        "trim": _safe_str(obj.get("trim") or obj.get("Trim") or obj.get("trimName")),
        "title": title,
        "price": price,
        "msrp": msrp,
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
        "carfax_url": carfax_url if carfax_url else None,
        "history_highlights": _extract_history_highlights(obj),
        "cylinders": cyl or None,
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
