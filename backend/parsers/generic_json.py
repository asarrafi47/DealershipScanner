"""Generic parser for flat vehicle lists with snake_case (or camelCase) fields.

One-off dealer sites (honestcardeal-com: Vite SPA + Supabase edge function
``public-inventory``, 2026-09-26) return ``{"vehicles": [{"vin", "year",
"make", "model", "trim", "asking_price", "exterior_color", "photo_urls", …}]}``.
No platform parser owns that shape; the dealer.com fallback claimed it and
mapped price 0 / placeholder image. This parser takes any list of dicts that
carry a VIN and at least three of the common vehicle keys.
"""
from __future__ import annotations

import re
from typing import Any

from backend.parsers.base import find_vehicle_list, norm_float, norm_int, norm_str

_VIN_RE = re.compile(r"^[A-HJ-NPR-Z0-9]{17}$")

_PRICE_KEYS = ("asking_price", "askingPrice", "sale_price", "salePrice", "internet_price", "internetPrice", "price", "selling_price", "sellingPrice", "list_price", "listPrice")
_MSRP_KEYS = ("msrp", "compare_price", "comparePrice", "retail_price", "retailPrice")
_FIELDS = {
    "trim": ("trim", "trim_level", "trimLevel"),
    "exterior_color": ("exterior_color", "exteriorColor", "color", "ext_color"),
    "interior_color": ("interior_color", "interiorColor", "int_color"),
    "mileage": ("mileage", "odometer", "miles"),
    "stock_number": ("stock_number", "stockNumber", "stock", "stock_no"),
    "engine_description": ("engine", "engine_description", "engineDescription", "engine_type"),
    "transmission": ("transmission", "transmission_description"),
    "drivetrain": ("drivetrain", "drive_train", "driveTrain", "drive_type"),
    "fuel_type": ("fuel_type", "fuelType", "fuel"),
    "body_style": ("body_style", "bodyStyle", "body_type", "bodyType", "body"),
    "description": ("description", "dealer_notes", "comments"),
}
_IMAGE_KEYS = ("primary_photo_url", "primaryPhotoUrl", "image_url", "imageUrl", "image", "thumbnail")
_GALLERY_KEYS = ("photo_urls", "photoUrls", "images", "photos", "gallery")
_CONDITION_KEYS = ("condition", "type", "inventory_type", "inventoryType", "new_used", "is_new", "isNew")
_DETAIL_KEYS = ("vdp_url", "vdpUrl", "url", "link", "detail_url")
_SIGNAL_KEYS = ("year", "make", "model") + _PRICE_KEYS + _FIELDS["trim"] + _FIELDS["mileage"] + _FIELDS["exterior_color"]


def _first(d: dict, keys: tuple[str, ...]) -> Any:
    for k in keys:
        v = d.get(k)
        if v not in (None, "", [], {}):
            return v
    return None


def _condition(d: dict) -> str:
    v = _first(d, _CONDITION_KEYS)
    if isinstance(v, bool):
        return "New" if v else "Used"
    s = norm_str(v).lower()
    if s.startswith("new"):
        return "New"
    if "certified" in s or s in ("cpo",):
        return "Certified"
    if s:
        return "Used"
    return "Used"  # a flat used-car feed (independents) never says so


def _urls(v: Any) -> list[str]:
    if isinstance(v, str):
        return [v] if v.startswith("http") else []
    out: list[str] = []
    for x in v or []:
        u = x if isinstance(x, str) else (x.get("url") or x.get("large") or x.get("src") if isinstance(x, dict) else "")
        if isinstance(u, str) and u.startswith("http"):
            out.append(u)
    return out


def detect(raw_data: Any) -> list[dict] | None:
    items = raw_data if isinstance(raw_data, list) else find_vehicle_list(raw_data, min_vin_count=1)
    if not items or not isinstance(items, list):
        return None
    dicts = [x for x in items if isinstance(x, dict)]
    if len(dicts) < max(1, len(items) // 2):
        return None
    good = 0
    for d in dicts[:20]:
        vin = norm_str(d.get("vin") or d.get("VIN") or d.get("vehicleIdentificationNumber")).upper()
        if _VIN_RE.match(vin) and sum(1 for k in _SIGNAL_KEYS if d.get(k) not in (None, "")) >= 3:
            good += 1
    return dicts if good >= max(1, min(3, len(dicts))) else None


def parse(raw_data: Any, base_url: str = "", dealer_id: str = "", dealer_name: str = "", dealer_url: str = "", **_kw: Any) -> list[dict]:
    items = detect(raw_data)
    if not items:
        return []
    out: list[dict] = []
    for d in items:
        vin = norm_str(d.get("vin") or d.get("VIN") or d.get("vehicleIdentificationNumber")).upper()
        if not _VIN_RE.match(vin):
            continue
        row: dict[str, Any] = {
            "vin": vin, "dealer_id": dealer_id, "dealer_name": dealer_name or dealer_id, "dealer_url": dealer_url or base_url,
            "year": norm_int(d.get("year")), "make": norm_str(d.get("make")), "model": norm_str(d.get("model")),
            "price": norm_float(_first(d, _PRICE_KEYS)), "msrp": norm_float(_first(d, _MSRP_KEYS)) or None,
            "condition": _condition(d), "gallery": _urls(_first(d, _GALLERY_KEYS)), "image_url": "",
        }
        for field, keys in _FIELDS.items():
            v = _first(d, keys)
            if field == "mileage":
                row[field] = norm_int(v)
            elif v is not None and not isinstance(v, (dict, list)):
                row[field] = norm_str(v)
        img = _first(d, _IMAGE_KEYS)
        row["image_url"] = img if isinstance(img, str) and img.startswith("http") else (row["gallery"][0] if row["gallery"] else "")
        link = _first(d, _DETAIL_KEYS)
        if isinstance(link, str) and link:
            row["_detail_url"] = link if link.startswith("http") else (base_url.rstrip("/") + "/" + link.lstrip("/"))
        feats = d.get("features")
        if isinstance(feats, list) and feats and all(isinstance(x, str) for x in feats):
            row["features"] = feats
        out.append(row)
    return out
