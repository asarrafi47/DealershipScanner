"""
Populate dealer inventory fields from a VIN using NHTSA vPIC + EPA merge (no dealer DMS).
"""
from __future__ import annotations

import logging
from typing import Any, Callable

from backend.knowledge_engine import merge_verified_specs
from backend.nhtsa_vpic import (
    decode_vpic_http_response,
    fetch_decode_vin_values_extended,
    flat_vpic_result_to_car_patch,
    looks_like_decode_vin,
)
from backend.utils.field_clean import clean_car_row_dict

_log = logging.getLogger(__name__)


def build_vehicle_prefill_from_vin(
    vin: str,
    *,
    get_json: Callable[[str], dict[str, Any]] | None = None,
) -> tuple[dict[str, Any], str | None]:
    """
    Return ``(row_dict, error)``. *row_dict* always includes normalized ``vin`` when decodable.
    On partial decode failure, still returns whatever vPIC provided plus EPA merge hints.
    """
    raw_v = (vin or "").strip().upper()
    if not looks_like_decode_vin(raw_v):
        return {}, "invalid_vin"

    api_body, flat, err = fetch_decode_vin_values_extended(raw_v, get_json=get_json)
    if not flat and api_body is not None:
        flat = decode_vpic_http_response(api_body)
    patch: dict[str, Any] = flat_vpic_result_to_car_patch(flat or {}) if flat else {}

    row: dict[str, Any] = {"vin": raw_v}
    for k in (
        "year",
        "make",
        "model",
        "trim",
        "fuel_type",
        "cylinders",
        "transmission",
        "drivetrain",
        "body_style",
        "engine_description",
    ):
        v = patch.get(k)
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        row[k] = v

    title_bits = [str(row.get("year") or "").strip(), str(row.get("make") or "").strip(), str(row.get("model") or "").strip()]
    trim = str(row.get("trim") or "").strip()
    if trim:
        title_bits.append(trim)
    title = " ".join(x for x in title_bits if x).strip()
    if title:
        row["title"] = title

    car_for_merge = clean_car_row_dict(dict(row))
    car_for_merge.setdefault("title", title or "")
    vs = merge_verified_specs(car_for_merge)
    td = vs.get("transmission_display")
    if td and str(td).strip() and str(td).strip() != "—":
        if not (row.get("transmission") and str(row.get("transmission")).strip()):
            row["transmission"] = str(td).strip()
    dd = vs.get("drivetrain_display")
    if dd and str(dd).strip() and str(dd).strip() != "—":
        if not (row.get("drivetrain") and str(row.get("drivetrain")).strip()):
            row["drivetrain"] = str(dd).strip()

    if not patch and err:
        return row, err
    return clean_car_row_dict(row), None
