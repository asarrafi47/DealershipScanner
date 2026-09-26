"""OneAudi dealer sites (the Audi "falcon" renderer: audihuntsville.com,
2026-09-24) serve inventory from ``https://omnigraph.audi.com/graphql`` —
``stockCarSearch(searchParameter, stockIdentifier)`` paged by
``paging.offset`` in steps of 48. The SSR page carries only the first 48 cars, so
the recipe replays the GraphQL query itself (see recipe_synth ``oneaudi``).

One response::

    {"data": {"stockCarSearch": {"resultNumber": 133, "results": {"cars": [
        {"stockCar": {"vin": ..., "titleText": "2025 Audi Q8", "subtitleText": "Premium Plus 55 TFSI® quattro®",
                      "modelInfo": {"genericModel": {"text": "Q8"}, "modelyear": 2025},
                      "preUse": {"code": "R", "text": "Used car"}, "cartypeText": "U",
                      "carPrices": [{"type": "final", "price": {"value": 62411}}, {"type": "list", ...}],
                      "mileage": {"unitText": "miles", "value": {"number": 5918}},
                      "colorInfo": {"exteriorColor": {"colorInfo": {"text": ...}}, "interiorColor": {...}},
                      "driveText": "All-wheel drive", "engineInfo": {"fuel": {"text": "Gas"}},
                      "images": [{"url": ...}], "weblink": ..., "dealer": {"name": ..., "city": ...},
                      "dynamicAttributes": [{"id": "VEHICLE_ID", "value": "P1592"}, {"id": "URL_CARFAX", "value": ...}]}}]}}}}
"""
from __future__ import annotations

import re
from typing import Any

from backend.utils.field_clean import clean_car_row_dict

_TITLE_RE = re.compile(r"^\s*((?:19|20)\d{2})\s+(\S+)\s*(.*)$")


def _cars(raw_data: Any) -> list[dict]:
    if not isinstance(raw_data, dict):
        return []
    data = raw_data.get("data") if isinstance(raw_data.get("data"), dict) else raw_data
    search = data.get("stockCarSearch") if isinstance(data, dict) else None
    results = (search or {}).get("results") if isinstance(search, dict) else None
    cars = (results or {}).get("cars") if isinstance(results, dict) else None
    return [c for c in (cars or []) if isinstance(c, dict)]


def detect(raw_data: Any) -> bool:
    return bool(_cars(raw_data)) or (isinstance(raw_data, dict) and isinstance((raw_data.get("data") or {}).get("stockCarSearch"), dict))


def total_count(raw_data: Any) -> int | None:
    if not isinstance(raw_data, dict):
        return None
    search = (raw_data.get("data") or {}).get("stockCarSearch") if isinstance(raw_data.get("data"), dict) else raw_data.get("stockCarSearch")
    n = (search or {}).get("resultNumber") if isinstance(search, dict) else None
    return int(n) if isinstance(n, (int, float)) and n >= 0 else None


def _text(node: Any, *path: str) -> str:
    cur = node
    for key in path:
        if not isinstance(cur, dict):
            return ""
        cur = cur.get(key)
    return str(cur).strip() if isinstance(cur, (str, int, float)) and cur not in ("", None) else ""


def _price(car: dict, *types: str) -> float | None:
    prices = car.get("carPrices") if isinstance(car.get("carPrices"), list) else []
    by_type = {str(p.get("type")): p for p in prices if isinstance(p, dict)}
    for t in types:
        p = by_type.get(t)
        v = ((p or {}).get("price") or {}).get("value") if isinstance(p, dict) else None
        try:
            f = float(v)
        except (TypeError, ValueError):
            continue
        if 500 <= f <= 2_000_000:
            return f
    return None


def _condition(car: dict) -> str:
    """``cartypeText`` is the feed's own flag ("N" / "U"); ``preUse`` is present on
    NEW cars too ({"code": "N", "text": "New car"}) — on 2026-09-25 treating any
    preUse as used filed all 133 new Audi Huntsville cars as Used."""
    ct = str(car.get("cartypeText") or "").strip().upper()
    pre = car.get("preUse") if isinstance(car.get("preUse"), dict) else {}
    pre_text = str(pre.get("text") or "").lower()
    used = ct == "U" or (ct != "N" and ("used" in pre_text or "pre-owned" in pre_text or "demo" in pre_text))
    if not used:
        return "New"
    labels = car.get("qualityLabel") if isinstance(car.get("qualityLabel"), list) else []
    if any("certified" in str(x).lower() for x in labels):
        return "Certified Pre-Owned"
    return "Used"


def parse(raw_data: Any, base_url: str = "", dealer_id: str = "", dealer_name: str = "", dealer_url: str = "", **_kw: Any) -> list[dict]:
    out: list[dict] = []
    seen: set[str] = set()
    for wrapper in _cars(raw_data):
        car = wrapper.get("stockCar") if isinstance(wrapper.get("stockCar"), dict) else wrapper
        vin = str(car.get("vin") or "").strip().upper()
        if len(vin) != 17 or vin in seen:
            continue
        seen.add(vin)
        title = str(car.get("titleText") or "").strip()
        m = _TITLE_RE.match(title)
        year = int(m.group(1)) if m else None
        make = m.group(2) if m else ""
        model = _text(car, "modelInfo", "genericModel", "text") or (m.group(3).strip() if m else "")
        try:
            year = int(_text(car, "modelInfo", "modelyear")) or year
        except ValueError:
            pass
        attrs = {str(a.get("id")): str(a.get("value") or "") for a in (car.get("dynamicAttributes") or []) if isinstance(a, dict)}
        images = [str(i.get("url")) for i in (car.get("images") or []) if isinstance(i, dict) and i.get("url")]
        mileage = None
        try:
            mileage = int(float(_text(car, "mileage", "value", "number") or "nan"))
        except ValueError:
            pass
        row = {
            "vin": vin,
            "year": year,
            "make": make,
            "model": model,
            "trim": str(car.get("subtitleText") or "").replace("®", "").strip(),
            "condition": _condition(car),
            "price": _price(car, "final", "sale", "dealerPrice", "list"),
            "msrp": _price(car, "list"),
            "mileage": mileage,
            "exterior_color": _text(car, "colorInfo", "exteriorColor", "colorInfo", "text") or _text(car, "colorInfo", "exteriorColor", "baseColorInfo", "text"),
            "interior_color": _text(car, "colorInfo", "interiorColor", "colorInfo", "text") or _text(car, "colorInfo", "interiorColor", "baseColorInfo", "text"),
            "drivetrain": str(car.get("driveText") or "").strip(),
            "fuel_type": _text(car, "engineInfo", "fuel", "text"),
            "transmission": str(car.get("gearText") or "").strip(),
            "stock_number": attrs.get("VEHICLE_ID", ""),
            "carfax_url": attrs.get("URL_CARFAX", ""),
            "image_url": images[0] if images else "",
            "gallery": images,
            "title": title,
            "dealer_id": dealer_id,
            "dealer_name": dealer_name or dealer_id,
            "dealer_url": dealer_url or base_url,
        }
        link = str(car.get("weblink") or attrs.get("URLAOA_AUDI") or "").strip()
        if link.startswith("http"):
            row["source_url"] = link
            row["_detail_url"] = link
        out.append(clean_car_row_dict(row))
    return out
