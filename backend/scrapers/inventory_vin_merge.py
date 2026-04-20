"""
When the same VIN appears in multiple inventory JSON payloads, merge galleries plus
fill **empty** scalar fields from the later row (conservative — never overwrites real data).
"""
from __future__ import annotations

from typing import Any

from backend.utils.field_clean import is_effectively_empty, normalize_optional_url
from backend.utils.gallery_merge import merge_inventory_row_galleries

# Text / scalar columns safe to fill only when the destination is effectively empty.
_MERGE_TEXT_KEYS: tuple[str, ...] = (
    "exterior_color",
    "interior_color",
    "fuel_type",
    "transmission",
    "drivetrain",
    "body_style",
    "engine_description",
    "condition",
    "stock_number",
    "zip_code",
    "trim",
    "title",
    "model_full_raw",
    "description",
)

_MERGE_URL_KEYS: tuple[str, ...] = ("carfax_url", "image_url", "source_url")

_MERGE_INT_KEYS: tuple[str, ...] = (
    "cylinders",
    "mpg_city",
    "mpg_highway",
    "msrp",
    "mileage",
    "year",
)

# For these, 0 is never a useful listing value — allow fill from src when dst is null or <= 0.
_MERGE_INT_NONPOS_IS_MISSING: frozenset[str] = frozenset(
    {"cylinders", "mpg_city", "mpg_highway", "msrp", "year"}
)


def _text_missing(v: Any) -> bool:
    if v is None:
        return True
    if isinstance(v, str) and not v.strip():
        return True
    return is_effectively_empty(v)


def _int_src_usable(k: str, sv: Any) -> bool:
    try:
        n = int(float(sv))
    except (TypeError, ValueError):
        return False
    if k in _MERGE_INT_NONPOS_IS_MISSING:
        return n > 0
    return True


def merge_inventory_rows_same_vin(dst: dict[str, Any], src: dict[str, Any]) -> None:
    """
    Mutate *dst* in place: gallery union (existing rules), then copy from *src* only
    where *dst* has no useful value. Later intercepts win for empty slots only.
    """
    merge_inventory_row_galleries(dst, src)

    for k in _MERGE_TEXT_KEYS:
        if not _text_missing(dst.get(k)):
            continue
        sv = src.get(k)
        if _text_missing(sv):
            continue
        dst[k] = sv

    for k in _MERGE_URL_KEYS:
        if normalize_optional_url(dst.get(k)):
            continue
        nu = normalize_optional_url(src.get(k))
        if nu:
            dst[k] = nu

    for k in _MERGE_INT_KEYS:
        dv = dst.get(k)
        if k == "mileage":
            if dv is not None:
                continue
        else:
            try:
                dnum = int(float(dv)) if dv is not None else None
            except (TypeError, ValueError):
                dnum = None
            if dnum is not None and dnum > 0:
                continue
        sv = src.get(k)
        if sv is None or not _int_src_usable(k, sv):
            continue
        dst[k] = sv

    # Price: fill only when dst has no positive price.
    try:
        dp = float(dst.get("price") or 0)
    except (TypeError, ValueError):
        dp = 0.0
    try:
        sp = float(src.get("price") or 0)
    except (TypeError, ValueError):
        sp = 0.0
    if dp <= 0 and sp > 0:
        dst["price"] = src.get("price")
