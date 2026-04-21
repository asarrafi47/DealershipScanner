"""
Parser for dealer.com getInventory API. Maps exact schema:
  title (array -> join), trackingPricing.internetPrice, trackingAttributes (odometer, exteriorColor),
  images[].uri -> gallery, vin, stockNumber, fuelType.
"""
import json
import logging
import re
from urllib.parse import urljoin

from backend.parsers.vdp_urls import dealer_style_vdp_url_candidates, suggest_dealer_style_vdp_url
from backend.parsers.base import (
    clean_image_url,
    dedupe_urls_order_prefer_large,
    find_tracking_attr,
    find_vehicle_list,
    harvest_image_urls_from_json,
    inventory_gallery_max,
    norm_float,
    norm_int,
    norm_str,
    normalize_image_url_https,
)
from backend.utils.field_clean import normalize_optional_str

logger = logging.getLogger(__name__)

# Fallback when no images; frontend resolves /static/ relative to app
FALLBACK_IMAGE_URL = "/static/placeholder.svg"


def _opt_str(v) -> str | None:
    """Missing / placeholder → None (never persist 'N/A' to SQLite)."""
    if v is None:
        return None
    s = norm_str(v)
    return normalize_optional_str(s)


def _extract_title(obj: dict, year: int, make: str, model: str) -> str | None:
    """Dealer.com: title is an array. Join with ' '. Fallback: year make model."""
    raw = obj.get("title") or obj.get("name")
    if raw is None:
        return normalize_optional_str(f"{year or ''} {make or ''} {model or ''}".strip())
    if isinstance(raw, list):
        s = " ".join(str(x).strip() for x in raw if x is not None).strip()
        return normalize_optional_str(s) or normalize_optional_str(
            f"{year or ''} {make or ''} {model or ''}".strip()
        )
    s = norm_str(raw)
    return normalize_optional_str(s) or normalize_optional_str(
        f"{year or ''} {make or ''} {model or ''}".strip()
    )


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
    """Map vehicle.images to list of URLs (prefer full-res keys), then deep-harvest nested JSON."""
    mx = inventory_gallery_max()
    images = obj.get("images") or obj.get("Images")
    out: list[str] = []
    if isinstance(images, list) and len(images) > 0:
        seen: set[str] = set()
        for item in images:
            u = _best_image_url(item, base_url) if isinstance(item, dict) else ""
            nu = normalize_image_url_https(u) if u else ""
            if nu.startswith("https://") and nu not in seen:
                seen.add(nu)
                out.append(nu)
        if out:
            base = dedupe_urls_order_prefer_large(out, max_len=mx)
            extra = harvest_image_urls_from_json(obj, base_url, max_urls=mx)
            return dedupe_urls_order_prefer_large(base + extra, max_len=mx)
    one = _extract_featured_or_thumbnail(obj, base_url)
    base = []
    if one:
        no = normalize_image_url_https(one)
        if no.startswith("https://"):
            base = [no]
    extra = harvest_image_urls_from_json(obj, base_url, max_urls=mx)
    return dedupe_urls_order_prefer_large(base + extra, max_len=mx)


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


def _pick_vehicle_detail_url(obj: dict, base_url: str) -> str | None:
    """Absolute VDP URL when present in listing payload (used by scanner VDP enrichment)."""
    candidates = [
        obj.get("vdpUrl"),
        obj.get("vdp_url"),
        obj.get("vehicleUrl"),
        obj.get("vehicle_url"),
        obj.get("vehicleLink"),
        obj.get("vehicle_link"),
        obj.get("detailUrl"),
        obj.get("detail_url"),
        obj.get("detailPageUrl"),
        obj.get("detail_page_url"),
        obj.get("vehicleDetailsUrl"),
        obj.get("vehicle_details_url"),
        obj.get("inventoryUrl"),
        obj.get("inventory_url"),
        obj.get("webUrl"),
        obj.get("web_url"),
        obj.get("url"),
        obj.get("href"),
        obj.get("link"),
        obj.get("seoUri"),
        obj.get("seo_uri"),
    ]
    for raw in candidates:
        if not raw or not isinstance(raw, str):
            continue
        u = raw.strip()
        if u.startswith("//"):
            u = "https:" + u
        elif not u.lower().startswith("http"):
            try:
                u = urljoin(base_url.rstrip("/") + "/", u.lstrip("/"))
            except Exception:
                continue
        low = u.lower()
        if (
            "/vdp/" in low
            or "vehicle-inventory" in low
            or "/inventory/" in low
            or "/used/" in low
            or "/new/" in low
            or "certified" in low
        ):
            return u
    return None


def _extract_exterior_color(obj: dict) -> str | None:
    """Find in vehicle.trackingAttributes where name === 'exteriorColor'."""
    arr = obj.get("trackingAttributes") or obj.get("tracking_attributes")
    v = find_tracking_attr(arr, "exteriorColor", "value")
    if v is not None and str(v).strip():
        return _opt_str(norm_str(v))
    return _opt_str(obj.get("exteriorColor") or obj.get("exterior_color"))


def _norm_tracking_key(name: object) -> str:
    if name is None:
        return ""
    return re.sub(r"[^a-z0-9]+", "", str(name).strip().lower())


def _tracking_attr_value_by_norm_substr(arr: object, needles: frozenset[str]) -> str | None:
    """First trackingAttributes entry whose normalized name matches a *needles* substring."""
    if not isinstance(arr, list):
        return None
    for item in arr:
        if not isinstance(item, dict):
            continue
        nk = _norm_tracking_key(item.get("name") or item.get("key") or item.get("id"))
        if not nk:
            continue
        for nd in needles:
            if len(nd) < 5:
                continue
            if nd in nk or nk in nd:
                v = item.get("value") or item.get("text")
                if v is not None and str(v).strip():
                    got = _opt_str(norm_str(v))
                    if got:
                        return got
    return None


_INTERIOR_ATTR_SUBSTR = frozenset(
    {
        "interiorcolor",
        "interiortrim",
        "upholstery",
        "seatcolor",
        "seattrim",
        "cabincolor",
        "leathercolor",
        "intcolor",
        "interiorpackage",
        "cabintrim",
    }
)

_BODY_ATTR_SUBSTR = frozenset(
    {
        "bodystyle",
        "bodytype",
        "vehiclebody",
        "vehicletype",
        "vehbodystyle",
        "bodyshape",
    }
)


def _extract_interior_color(obj: dict) -> str | None:
    """Prefer trackingAttributes (Dealer.com varies label casing) then top-level fields."""
    v = _tracking_attr_value_by_norm_substr(
        obj.get("trackingAttributes") or obj.get("tracking_attributes"),
        _INTERIOR_ATTR_SUBSTR,
    )
    if v:
        return v
    for nm in ("interiorColor", "interior_color", "interiorTrim", "interior_trim"):
        got = _opt_str(obj.get(nm))
        if got:
            return got
    return None


def _extract_body_style(obj: dict) -> str | None:
    """bodyStyle / bodyType on object or in trackingAttributes."""
    direct = _opt_str(
        obj.get("bodyStyle")
        or obj.get("body_style")
        or obj.get("bodyType")
        or obj.get("body_type")
    )
    if direct:
        return direct
    return _tracking_attr_value_by_norm_substr(
        obj.get("trackingAttributes") or obj.get("tracking_attributes"),
        _BODY_ATTR_SUBSTR,
    )


def _map_vehicle(obj: dict, base_url: str, dealer_id: str, dealer_name: str, dealer_url: str) -> dict | None:
    """Map one vehicle from Dealer.com getInventory schema. Missing text → None (not 'N/A')."""
    if not isinstance(obj, dict):
        return None

    vin = norm_str(obj.get("vin") or obj.get("VIN") or "")
    if not vin:
        vin = norm_str(obj.get("stockNumber") or "")
    if not vin:
        vin = f"unknown-{hash(str(obj)) % 10**8}"

    stock_raw = _opt_str(obj.get("stockNumber"))
    stock_number = stock_raw or ""

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
    fuel_type = _opt_str(obj.get("fuelType") or obj.get("fuel_type"))
    # Prefer an explicit dealer/feed URL (matches the link customers click). Synthesized
    # vhr.carfax.com/main?vin=… is a last resort when the feed only provides a token.
    vhr_url = obj.get("vhr_url") or obj.get("carfax_token")
    explicit = norm_str(
        obj.get("carfax_url")
        or obj.get("carfaxUrl")
        or obj.get("carfaxLink")
        or obj.get("history_report_url")
        or obj.get("vehicleHistoryUrl")
        or obj.get("vehicle_history_url")
        or ""
    )
    if explicit.startswith("http"):
        carfax_url = explicit
    elif vhr_url and isinstance(vhr_url, str) and vhr_url.strip().startswith("http"):
        carfax_url = norm_str(vhr_url)
    elif vhr_url and vin and not vin.startswith("unknown"):
        carfax_url = f"https://vhr.carfax.com/main?vin={vin}"
    else:
        carfax_url = explicit if explicit else None

    cyl = norm_int(obj.get("cylinders") or 0)
    if not cyl:
        arr = obj.get("trackingAttributes") or obj.get("tracking_attributes")
        c2 = find_tracking_attr(arr, "cylinders", "value") if isinstance(arr, list) else None
        if c2 is not None:
            cyl = norm_int(c2)

    detail_url = _pick_vehicle_detail_url(obj, base_url)
    if not detail_url and vin and not str(vin).lower().startswith("unknown"):
        detail_url = suggest_dealer_style_vdp_url(base_url, vin, obj)
    detail_alternates: list[str] = []
    if vin and not str(vin).lower().startswith("unknown"):
        detail_alternates = [
            x for x in dealer_style_vdp_url_candidates(base_url, vin, obj) if x != detail_url
        ]

    out = {
        "vin": vin,
        "stock_number": stock_number,
        "year": year,
        "make": make,
        "model": model,
        "trim": _opt_str(obj.get("trim") or obj.get("Trim") or obj.get("trimName")),
        "title": title,
        "price": price,
        "msrp": msrp,
        "mileage": mileage,
        "image_url": image_url,
        "gallery": gallery,
        "dealer_id": dealer_id,
        "dealer_name": dealer_name,
        "dealer_url": dealer_url,
        "zip_code": _opt_str(obj.get("zipCode") or obj.get("zip_code")),
        "fuel_type": fuel_type,
        "transmission": _opt_str(obj.get("transmission") or obj.get("transmissionType")),
        "drivetrain": _opt_str(obj.get("drivetrain") or obj.get("driveType")),
        "exterior_color": exterior_color,
        "interior_color": _extract_interior_color(obj),
        "body_style": _extract_body_style(obj),
        "carfax_url": carfax_url if (carfax_url and str(carfax_url).strip().lower().startswith("http")) else None,
        "history_highlights": _extract_history_highlights(obj),
        "cylinders": cyl or None,
    }
    if detail_url:
        out["_detail_url"] = detail_url
    if detail_alternates:
        out["_detail_url_alternates"] = detail_alternates[:12]
    return out


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
