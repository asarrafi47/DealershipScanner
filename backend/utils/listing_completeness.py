"""
Detect gaps in the car detail "regular listing" spec sheet (car.html col 2).

* ``for_public_filter=True`` — keys aligned with the ``car.html`` spec sheet for ``/listings``
  (``is_car_incomplete`` / ``public_incomplete`` pill).
* ``for_public_filter=False`` — same field codes as public (used for ``incomplete_listings.db`` + dev tools).

Efficiency (MPG / ``fuel_economy_display``) remains excluded: many rows lack EPA data.
"""
from __future__ import annotations

import json
from collections import Counter
from typing import Any

from backend.enrichment.knowledge_engine import prepare_car_detail_context
from backend.utils.car_serialize import DISPLAY_DASH, serialize_car_for_api
from backend.utils.in_transit import vehicle_is_in_transit

# Human labels for ``listing_missing_field_codes`` / ``incomplete_missing_fields`` (dev UI + summaries).
INCOMPLETE_FIELD_LABELS: dict[str, str] = {
    "title": "Title",
    "images": "Images / photos",
    "price": "Price",
    "year": "Year",
    "make": "Make",
    "model": "Model",
    "trim": "Trim",
    "mileage": "Mileage",
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
    "on_lot": "On lot (vehicle in transit)",
}

# Public listings / incomplete index: spec-sheet fields from ``car.html`` (col 2), minus MPG.
_PUBLIC_INCOMPLETE_KEYS = frozenset(
    {
        "title",
        "images",
        "price",
        "year",
        "make",
        "model",
        "trim",
        "mileage",
        "engine",
        "transmission",
        "drivetrain",
        "body_style",
        "fuel_type",
        "condition",
        "cylinders",
        "exterior_color",
        "interior_color",
        "vin",
        "on_lot",
    }
)


def _is_valid_image_url(u: str) -> bool:
    """True for HTTP(S) URLs and local /car-images/ paths (served by Flask)."""
    s = u.strip()
    return s.startswith("http") or s.startswith("/car-images/")


def _has_http_image(car: dict[str, Any]) -> bool:
    img = car.get("image_url")
    if img and _is_valid_image_url(str(img)):
        return True
    g = car.get("gallery")
    if isinstance(g, list):
        for u in g:
            if isinstance(u, str) and _is_valid_image_url(u):
                return True
    if isinstance(g, str) and g.strip().startswith("["):
        try:
            parsed = json.loads(g)
            if isinstance(parsed, list):
                for u in parsed:
                    if isinstance(u, str) and _is_valid_image_url(u):
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


def _mileage_blank(car_raw: dict[str, Any]) -> bool:
    """Match ``car.html`` mileage row: missing only when NULL or empty string (0 mi is valid)."""
    m = car_raw.get("mileage")
    if m is None:
        return True
    if isinstance(m, str) and not str(m).strip():
        return True
    return False


def listing_missing_field_codes(
    car_raw: dict[str, Any],
    *,
    for_public_filter: bool,
    detail_ctx: dict[str, Any] | None = None,
    include_non_actionable: bool = False,
) -> list[str]:
    """
    Ordered-ish stable labels for missing spec-sheet fields.
    When *for_public_filter* is True, only keys in ``_PUBLIC_INCOMPLETE_KEYS`` are returned
    (same codes as the full queue today).

    Pass *detail_ctx* from ``prepare_car_detail_context`` when already computed (avoids duplicate enrichment).
    """
    if not car_raw:
        return []
    if detail_ctx is not None:
        ctx = detail_ctx
    else:
        ctx = prepare_car_detail_context(dict(car_raw))
    vs = ctx.get("verified_specs") or {}
    car = serialize_car_for_api(dict(car_raw), include_verified=False, verified_specs=vs)
    missing: list[str] = []

    if _dash(car.get("title")):
        missing.append("title")
    if not _has_http_image(dict(car_raw)):
        missing.append("images")

    def _row_field_blank(val: Any) -> bool:
        """True for NULL/empty; False for zero (0 is a stored value, e.g. TBD/free list price)."""
        if val is None:
            return True
        if isinstance(val, str) and not str(val).strip():
            return True
        return False

    # Use raw row so list/MSRP ``0`` is not confused with "missing" after serialization.
    p_blank = _row_field_blank(car_raw.get("price"))
    m_blank = _row_field_blank(car_raw.get("msrp"))
    if p_blank and m_blank:
        missing.append("price")

    if car.get("year") is None:
        missing.append("year")
    if _dash(car.get("make")):
        missing.append("make")
    if _dash(car.get("model")):
        missing.append("model")
    if _dash(car.get("trim")):
        # A blank trim is only "missing" when the model actually has trim
        # variants. Standalone models (BMW XM/M6/i8, base F-250) have no trim —
        # the catalog shows a single trim equal to the model name — so a blank
        # is correct, not a gap.
        try:
            from backend.enrichment.knowledge_engine import catalog_model_is_trimless

            trimless = catalog_model_is_trimless(
                car_raw.get("year"), car_raw.get("make") or "", car_raw.get("model") or ""
            )
        except Exception:
            trimless = False
        if not trimless:
            missing.append("trim")
    if _mileage_blank(car_raw):
        missing.append("mileage")
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

    in_transit = vehicle_is_in_transit(car_raw)
    if in_transit:
        missing.append("on_lot")

    # Distinguish "we failed to capture it" from "the data doesn't exist at the
    # source." These codes are not capture failures and must not flag a car
    # incomplete (they inflated the queue with un-actionable rows, 2026-07-20):
    #   - interior_color: dealer-discretionary; many dealers never publish it
    #     (e.g. High Country Toyota lists it for 0% of inventory), so a blank is
    #     the dealer's, not ours.
    #   - on_lot: an in-transit delivery status, not missing spec data.
    #   - images: a photo-less car isn't a missing-DATA problem — the dealer
    #     often hasn't shot it yet (esp. new inventory). It's handled by sorting
    #     such cars to the bottom of listings, not by flagging them incomplete.
    #     (Whether OUR scraper is dropping photos it should get is a separate
    #     scanner-health signal, tracked per-dealer, not per-listing here.)
    # ``include_non_actionable=True`` restores the full list (admin detail view).
    _NON_ACTIONABLE = {"interior_color", "on_lot", "images"}
    if not include_non_actionable:
        missing = [m for m in missing if m not in _NON_ACTIONABLE]

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
