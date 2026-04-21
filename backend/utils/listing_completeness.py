"""
Detect gaps in the car detail "regular listing" spec sheet (car.html col 2).

* ``for_public_filter=True`` — subset used with public ``/listings`` (``is_car_incomplete``).
* ``for_public_filter=False`` — full queue used for ``incomplete_listings.db`` + dev page.

Efficiency (MPG) is intentionally excluded: many rows lack EPA data and would flood the queue.
"""
from __future__ import annotations

import json
from collections import Counter
from typing import Any

from backend.knowledge_engine import prepare_car_detail_context
from backend.utils.car_serialize import DISPLAY_DASH, serialize_car_for_api

# Human labels for ``listing_missing_field_codes`` / ``incomplete_missing_fields`` (dev UI + summaries).
INCOMPLETE_FIELD_LABELS: dict[str, str] = {
    "title": "Title",
    "images": "Images / photos",
    "price": "Price",
    "year": "Year",
    "make": "Make",
    "model": "Model",
    "trim": "Trim",
    "engine": "Engine",
    "transmission": "Transmission",
    "drivetrain": "Drivetrain",
    "body_style": "Body style",
    "fuel_type": "Fuel type",
    "condition": "Condition",
    "cylinders": "Cylinders",
    "exterior_color": "Exterior color",
    "interior_color": "Interior color",
    "vin": "VIN",
}

# Public listings: same core as historical ``is_car_incomplete`` plus real gaps
# on drivetrain/transmission/colors/fuel, but do not require trim/engine/body/condition/cylinders/VIN.
_PUBLIC_INCOMPLETE_KEYS = frozenset(
    {
        "title",
        "images",
        "price",
        "year",
        "make",
        "model",
        "transmission",
        "drivetrain",
        "fuel_type",
        "exterior_color",
        "interior_color",
    }
)


def _has_http_image(car: dict[str, Any]) -> bool:
    img = car.get("image_url")
    if img and str(img).strip().startswith("http"):
        return True
    g = car.get("gallery")
    if isinstance(g, list):
        for u in g:
            if isinstance(u, str) and u.strip().startswith("http"):
                return True
    if isinstance(g, str) and g.strip().startswith("["):
        try:
            parsed = json.loads(g)
            if isinstance(parsed, list):
                for u in parsed:
                    if isinstance(u, str) and u.strip().startswith("http"):
                        return True
        except (TypeError, ValueError):
            pass
    return False


def _dash(val: Any) -> bool:
    if val is None:
        return True
    s = str(val).strip()
    if not s:
        return True
    return s == DISPLAY_DASH


def _cylinders_display_missing(car: dict[str, Any], verified_specs: dict[str, Any]) -> bool:
    vd = verified_specs.get("cylinders_display")
    if vd is not None:
        return False
    cyl = car.get("cylinders")
    if cyl is not None and str(cyl).strip() != "":
        try:
            int(cyl)
            return False
        except (TypeError, ValueError):
            return True
    return True


def listing_missing_field_codes(car_raw: dict[str, Any], *, for_public_filter: bool) -> list[str]:
    """
    Ordered-ish stable labels for missing spec-sheet fields.
    When *for_public_filter* is True, only keys in ``_PUBLIC_INCOMPLETE_KEYS`` are returned.
    """
    if not car_raw:
        return []
    ctx = prepare_car_detail_context(dict(car_raw))
    vs = ctx.get("verified_specs") or {}
    car = serialize_car_for_api(dict(car_raw), include_verified=False, verified_specs=vs)
    missing: list[str] = []

    if _dash(car.get("title")):
        missing.append("title")
    if not _has_http_image(dict(car_raw)):
        missing.append("images")

    p = car.get("price")
    msrp = car.get("msrp")
    try:
        pi = int(p) if p is not None and str(p).strip() != "" else 0
    except (TypeError, ValueError):
        pi = 0
    try:
        mi = int(msrp) if msrp is not None and str(msrp).strip() != "" else 0
    except (TypeError, ValueError):
        mi = 0
    if (pi is None or pi <= 0) and (mi is None or mi <= 0):
        missing.append("price")

    if car.get("year") is None:
        missing.append("year")
    if _dash(car.get("make")):
        missing.append("make")
    if _dash(car.get("model")):
        missing.append("model")
    if _dash(car.get("trim")):
        missing.append("trim")
    if _dash(car.get("engine_display")):
        missing.append("engine")
    if _dash(car.get("transmission_display")):
        missing.append("transmission")
    if _dash(car.get("drivetrain_display")):
        missing.append("drivetrain")
    if _dash(car.get("body_style")):
        missing.append("body_style")
    if _dash(car.get("fuel_type")):
        missing.append("fuel_type")
    if _dash(car.get("condition")):
        missing.append("condition")
    if _cylinders_display_missing(car, vs):
        missing.append("cylinders")
    if _dash(car.get("exterior_color")):
        missing.append("exterior_color")
    if _dash(car.get("interior_color")):
        missing.append("interior_color")

    vin_raw = (car_raw.get("vin") or "").strip()
    if not vin_raw or vin_raw.lower().startswith("unknown"):
        missing.append("vin")
    elif _dash(car.get("vin")):
        missing.append("vin")

    if for_public_filter:
        missing = [m for m in missing if m in _PUBLIC_INCOMPLETE_KEYS]
    return missing


def is_car_incomplete_for_public_listings(car: dict[str, Any]) -> bool:
    return bool(listing_missing_field_codes(car, for_public_filter=True))


def summarize_incomplete_missing_fields(cars: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Aggregate missing-field codes across incomplete rows (e.g. dev dashboard).

    Each item: ``{"code", "label", "count", "pct"}`` where *pct* is percent of incomplete
    cars that list that code. Sorted by *count* descending.
    """
    if not cars:
        return []
    ctr: Counter[str] = Counter()
    for c in cars:
        raw = c.get("incomplete_missing_fields")
        if not isinstance(raw, list):
            continue
        for code in raw:
            if isinstance(code, str) and code.strip():
                ctr[code.strip()] += 1
    n = len(cars)
    out: list[dict[str, Any]] = []
    for code, count in ctr.most_common():
        label = INCOMPLETE_FIELD_LABELS.get(code, code.replace("_", " ").title())
        out.append(
            {
                "code": code,
                "label": label,
                "count": int(count),
                "pct": round(100.0 * float(count) / float(n), 1) if n else 0.0,
            }
        )
    return out
