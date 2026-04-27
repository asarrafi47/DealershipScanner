"""
Parser for dealer.on / CDK-style sites. Uses recursive search for vehicle list and
key-variant mapping. Accepts raw JSON or HTML with __PRELOADED_STATE__ etc.
"""
import json
import logging
import re
from urllib.parse import urljoin

from backend.parsers.vdp_urls import suggest_dealer_style_vdp_url
from backend.parsers.base import (
    extract_gallery_urls,
    extract_image_url,
    extract_mileage,
    extract_price,
    find_tracking_attr,
    find_vehicle_list,
    norm_int,
    norm_str,
)
from backend.utils.field_clean import clean_car_row_dict, normalize_optional_str

logger = logging.getLogger(__name__)


def _opt_str(v) -> str | None:
    if v is None:
        return None
    s = norm_str(v)
    return normalize_optional_str(s)


def _norm_tracking_key(name: object) -> str:
    if name is None:
        return ""
    return re.sub(r"[^a-z0-9]+", "", str(name).strip().lower())


def _tracking_value_by_needles(arr: object, needles: frozenset[str], *, min_len: int = 4) -> str | None:
    """First tracking/attributes entry whose normalized name contains a needle substring."""
    if not isinstance(arr, list):
        return None
    for item in arr:
        if not isinstance(item, dict):
            continue
        nk = _norm_tracking_key(item.get("name") or item.get("key") or item.get("id"))
        if not nk or len(nk) < min_len:
            continue
        for nd in needles:
            if len(nd) < min_len:
                continue
            if nd in nk or nk in nd:
                raw = item.get("value") or item.get("text") or item.get("displayValue")
                got = _opt_str(raw)
                if got:
                    return got
    return None


_ATTR_KEYS = ("trackingAttributes", "tracking_attributes", "attributes", "vehicleAttributes")


def _attr_arrays(obj: dict) -> list[list]:
    out: list[list] = []
    for k in _ATTR_KEYS:
        arr = obj.get(k)
        if isinstance(arr, list):
            out.append(arr)
    return out


def _first_tracking_value(obj: dict, needles: frozenset[str]) -> str | None:
    for arr in _attr_arrays(obj):
        got = _tracking_value_by_needles(arr, needles)
        if got:
            return got
    return None


_EXTERIOR_NEEDLES = frozenset(
    {
        "exteriorcolor",
        "exteriorpaint",
        "paintcolor",
        "bodycolor",
        "extcolor",
        "outersurface",
        "primaryexterior",
    }
)
_INTERIOR_NEEDLES = frozenset(
    {
        "interiorcolor",
        "interiortrim",
        "upholstery",
        "seatcolor",
        "seattrim",
        "cabincolor",
        "intcolor",
    }
)
_ENGINE_NEEDLES = frozenset(
    {
        "enginedescription",
        "enginedesc",
        "enginetype",
        "engine",
        "powertrain",
        "motor",
        "displacement",
    }
)
_CONDITION_NEEDLES = frozenset(
    {
        "vehiclecondition",
        "inventorytype",
        "newused",
        "stocktype",
        "saletype",
        "vehicletypecode",
    }
)
_BODY_NEEDLES = frozenset({"bodystyle", "bodytype", "vehiclebody", "vehbodystyle"})
_MPG_CITY_NEEDLES = frozenset({"mpgcity", "citympg", "epacity", "fuelcity"})
_MPG_HWY_NEEDLES = frozenset(
    {"mpghighway", "highwaympg", "epahighway", "fuelhighway", "hwympg", "mpghwy"}
)


def _pick_exterior_color(obj: dict) -> str | None:
    for key in (
        "exteriorColor",
        "exterior_color",
        "primaryExteriorColor",
        "primary_exterior_color",
        "extColor",
        "ext_color",
        "exterior",
        "color",
        "paint",
    ):
        got = _opt_str(obj.get(key))
        if got:
            return got
    return _first_tracking_value(obj, _EXTERIOR_NEEDLES)


def _pick_interior_color(obj: dict) -> str | None:
    for key in ("interiorColor", "interior_color", "interiorTrim", "interior_trim", "intColor", "int_color"):
        got = _opt_str(obj.get(key))
        if got:
            return got
    return _first_tracking_value(obj, _INTERIOR_NEEDLES)


def _pick_engine_description(obj: dict) -> str | None:
    for key in (
        "engineDescription",
        "engine_description",
        "engineDesc",
        "engine",
        "motor",
        "powertrain",
        "engineName",
        "engine_name",
    ):
        got = _opt_str(obj.get(key))
        if got:
            return got
    return _first_tracking_value(obj, _ENGINE_NEEDLES)


def _pick_condition(obj: dict) -> str | None:
    for key in (
        "condition",
        "vehicleCondition",
        "vehicle_condition",
        "inventoryType",
        "inventory_type",
        "newUsed",
        "new_used",
        "stockType",
        "stock_type",
        "certified",
        "isCertified",
    ):
        got = _opt_str(obj.get(key))
        if got:
            return got
    got = _first_tracking_value(obj, _CONDITION_NEEDLES)
    if got:
        return got
    if obj.get("certified") is True or obj.get("isCertified") is True:
        return "Certified"
    return None


def _pick_body_style(obj: dict) -> str | None:
    for key in ("bodyStyle", "body_style", "bodyType", "body_type", "vehicleType", "vehicle_type"):
        got = _opt_str(obj.get(key))
        if got:
            return got
    return _first_tracking_value(obj, _BODY_NEEDLES)


def _pick_cylinders(obj: dict) -> int | None:
    v = obj.get("cylinders") or obj.get("cylinderCount") or obj.get("engCylinders") or obj.get("engineCylinders")
    n = norm_int(v)
    if n > 0:
        return n
    for arr in _attr_arrays(obj):
        if not isinstance(arr, list):
            continue
        for name_key in ("cylinders", "engineCylinders", "cylinder"):
            hit = find_tracking_attr(arr, name_key, "value")
            if hit is not None and str(hit).strip():
                n2 = norm_int(hit)
                if n2 > 0:
                    return n2
    return None


def _pick_mpg_city(obj: dict) -> int | None:
    for key in ("mpgCity", "mpg_city", "cityMpg", "city_mpg", "epaCityMpg", "epa_city_mpg"):
        n = norm_int(obj.get(key))
        if n > 0:
            return n
    fe = obj.get("fuelEconomy") or obj.get("fuel_economy")
    if isinstance(fe, dict):
        for key in ("city", "cityMpg", "mpgCity", "combined"):
            n = norm_int(fe.get(key))
            if n > 0:
                return n
    got = _first_tracking_value(obj, _MPG_CITY_NEEDLES)
    if got:
        n = norm_int(got)
        if n > 0:
            return n
    return None


def _pick_mpg_highway(obj: dict) -> int | None:
    for key in ("mpgHighway", "mpg_highway", "highwayMpg", "highway_mpg", "epaHighwayMpg", "epa_highway_mpg"):
        n = norm_int(obj.get(key))
        if n > 0:
            return n
    fe = obj.get("fuelEconomy") or obj.get("fuel_economy")
    if isinstance(fe, dict):
        for key in ("highway", "highwayMpg", "mpgHighway", "hwy"):
            n = norm_int(fe.get(key))
            if n > 0:
                return n
    got = _first_tracking_value(obj, _MPG_HWY_NEEDLES)
    if got:
        n = norm_int(got)
        if n > 0:
            return n
    return None


def _pick_engine_l(obj: dict) -> float | None:
    for key in ("engineDisplacement", "engine_displacement", "displacement", "liters", "engineL", "engine_l"):
        v = obj.get(key)
        if v is None:
            continue
        s = str(v).strip().lower().replace("l", "")
        try:
            f = float(s.replace(",", ""))
            if f > 0:
                return round(f, 2)
        except (TypeError, ValueError):
            continue
    return None


def _pick_detail_url_dealer_on(obj: dict, base_url: str) -> str | None:
    candidates = [
        obj.get("vdpUrl"),
        obj.get("vdp_url"),
        obj.get("vehicleUrl"),
        obj.get("vehicle_url"),
        obj.get("vehicleLink"),
        obj.get("detailUrl"),
        obj.get("detail_url"),
        obj.get("inventoryUrl"),
        obj.get("url"),
        obj.get("href"),
        obj.get("link"),
        obj.get("webUrl"),
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
            or "/viewdetails" in low
            or "/vehicle" in low
        ):
            return u
    return None


def _map_vehicle(obj: dict, base_url: str, dealer_id: str, dealer_name: str, dealer_url: str) -> dict | None:
    vin = norm_str(obj.get("vin") or obj.get("VIN") or obj.get("stockNumber") or obj.get("stock_number") or "")
    if not vin:
        vin = f"unknown-{hash(str(obj)) % 10**8}"
    hero = extract_image_url(obj, base_url)
    gallery = extract_gallery_urls(obj, base_url)
    if not gallery and hero.startswith("http"):
        gallery = [hero]
    ext = _pick_exterior_color(obj)
    intr = _pick_interior_color(obj)
    eng_desc = _pick_engine_description(obj)
    fuel = _opt_str(obj.get("fuelType") or obj.get("fuel_type") or obj.get("fuel"))
    if not fuel:
        fuel = _first_tracking_value(obj, frozenset({"fueltype", "fuel", "powerfuel"}))
    cond = _pick_condition(obj)
    cyl = _pick_cylinders(obj)
    mpg_c = _pick_mpg_city(obj)
    mpg_h = _pick_mpg_highway(obj)
    eng_l = _pick_engine_l(obj)
    body = _pick_body_style(obj)
    stock = _opt_str(obj.get("stockNumber") or obj.get("stock_number") or obj.get("stock") or obj.get("stockNo"))

    row = {
        "vin": vin,
        "year": norm_int(obj.get("year") or obj.get("modelYear") or obj.get("model_year") or obj.get("yr")),
        "make": norm_str(obj.get("make") or obj.get("Make") or obj.get("manufacturer")),
        "model": norm_str(obj.get("model") or obj.get("Model") or obj.get("modelName") or obj.get("model_name")),
        "trim": norm_str(obj.get("trim") or obj.get("Trim") or obj.get("trimName") or obj.get("trim_name")),
        "price": extract_price(obj),
        "mileage": extract_mileage(obj),
        "image_url": hero,
        "gallery": gallery,
        "dealer_id": dealer_id,
        "dealer_name": dealer_name,
        "dealer_url": dealer_url,
        "title": norm_str(obj.get("title") or obj.get("name") or obj.get("vehicleTitle") or ""),
        "zip_code": norm_str(obj.get("zipCode") or obj.get("zip_code") or obj.get("zip")),
        "fuel_type": fuel or "",
        "transmission": norm_str(obj.get("transmission") or obj.get("transmissionType")),
        "drivetrain": norm_str(obj.get("drivetrain") or obj.get("driveType")),
        "exterior_color": ext or "",
        "interior_color": intr or "",
        "body_style": body or "",
        "stock_number": stock or "",
        "engine_description": eng_desc or "",
        "condition": cond or "",
        "cylinders": cyl,
        "mpg_city": mpg_c,
        "mpg_highway": mpg_h,
        "engine_l": eng_l,
    }
    du = _pick_detail_url_dealer_on(obj, base_url)
    if not du and vin and not str(vin).lower().startswith("unknown"):
        du = suggest_dealer_style_vdp_url(dealer_url or base_url, vin, obj)
    if du:
        row["_detail_url"] = du
    return row


def _parse_json_list(data, base_url: str, dealer_id: str, dealer_name: str, dealer_url: str) -> list[dict]:
    items = find_vehicle_list(data)
    if not items:
        return []
    out = []
    for obj in items:
        if not isinstance(obj, dict):
            continue
        if not (
            obj.get("vin")
            or obj.get("VIN")
            or obj.get("stockNumber")
            or obj.get("stock_number")
            or obj.get("stock")
            or obj.get("price")
            or obj.get("salePrice")
            or obj.get("internetPrice")
            or obj.get("sellingPrice")
            or obj.get("internet_Price")
            or obj.get("internet_price")
        ):
            continue
        mapped = _map_vehicle(obj, base_url, dealer_id, dealer_name, dealer_url)
        if mapped:
            out.append(clean_car_row_dict(mapped))
    return out


def _extract_from_html(html: str) -> list | dict | None:
    if not html:
        return None
    for pattern, group in [
        (r"__PRELOADED_STATE__\s*=\s*(\{.*?\});?\s*(?:</script>|$)", 1),
        (r"window\.InventoryData\s*=\s*(\[.*?\]);?\s*(?:</script>|$)", 1),
        (r"window\.__INITIAL_STATE__\s*=\s*(\{.*?\});?\s*(?:</script>|$)", 1),
        (r"vehicleList\s*=\s*(\[.*?\]);?\s*(?:</script>|$)", 1),
    ]:
        m = re.search(pattern, html, re.DOTALL)
        if m:
            try:
                return json.loads(m.group(group))
            except json.JSONDecodeError:
                continue
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
