"""Row cleanup rules for :func:`backend.scanner.database.upsert_vehicles`.

Pure functions: one scanner dict in, the cleaned values the INSERT binds out.
No database access here.
"""
from __future__ import annotations

from typing import Any

from backend.utils.analytics_ep import apply_ep_from_scanner_dict
from backend.utils.car_serialize import infer_engine_l_for_db
from backend.utils.field_clean import clean_car_row_dict, is_effectively_empty
from backend.utils.fuel_label_plausibility import cylinders_override_for_electric_claim
from backend.utils.fuel_type_normalize import normalize_fuel_type_for_storage

PLACEHOLDER_IMAGE_URL = "/static/placeholder.svg"


def dedupe_sorted_by_vin(vehicles: list[dict]) -> tuple[dict[str, dict], list[dict]]:
    """One row per stripped VIN (the last occurrence wins), in sorted-VIN order.

    Rows with a blank VIN are dropped. Sorted by VIN so every concurrent writer
    (fleet shards) takes row and index locks in the same order; Chapman Ford's
    upsert died with DeadlockDetected on 2026-09-28 when two shards inserted
    overlapping rows in feed order.
    """
    by_vin: dict[str, dict] = {}
    for v in vehicles:
        vin = (v.get("vin") or "").strip()
        if vin:
            by_vin[vin] = v
    return by_vin, [by_vin[k] for k in sorted(by_vin)]


def normalize_vehicle_row(raw: dict) -> dict:
    """EP merge, source URL, field cleanup and the storage-time label fixes."""
    merged = apply_ep_from_scanner_dict(dict(raw))
    from backend.parsers.vdp_urls import apply_vehicle_source_url

    apply_vehicle_source_url(merged)
    v = clean_car_row_dict(merged)
    # 48V mild hybrids (Ram 1500 eTorque) arrive labelled "Hybrid" from
    # the feed. Correct the label BEFORE it is stored: the fuel FILTERS
    # (search_cars, facet cascade, nearby counts) read cars.fuel_type
    # directly, so a read-time-only correction leaves the card and the
    # filter disagreeing, and any one-off backfill is overwritten by the
    # next scan of the same dealer.
    _ft_fixed = normalize_fuel_type_for_storage(v)
    if _ft_fixed:
        v["fuel_type"] = _ft_fixed
    # Battery-electric rows arrive with the gas sibling's cylinder count
    # (Toyota C-HR BEV "4") or a feed sentinel (GM "99", nulled by
    # clean_car_row_dict above). Zero the count ONLY when the electric
    # label is plausible; when combustion evidence contradicts it (a gas
    # GX 550 fed as "Electric"), the cylinders ARE the evidence — they
    # are kept, and the label correction above / the read-time display
    # handles the fuel type. Runs AFTER the label normalization so a row
    # it just relabelled to gas/hybrid is no longer an electric claim.
    _cyl_fixed = cylinders_override_for_electric_claim(v)
    if _cyl_fixed is not None:
        v["cylinders"] = _cyl_fixed
    if not v.get("transmission_type") and v.get("transmission"):
        from backend.utils.transmission_normalize import normalize_transmission_standard
        _y = v.get("year")
        _tt, _ = normalize_transmission_standard(
            v["transmission"],
            make=v.get("make"),
            model=v.get("model"),
            trim=v.get("trim"),
            title=v.get("title"),
            year=_y if isinstance(_y, int) else None,
            vin=v.get("vin"),
            log_weak=False,
        )
        if _tt:
            v["transmission_type"] = _tt
    if is_effectively_empty(v.get("engine_l")):
        _eng = infer_engine_l_for_db(v)
        if _eng is not None:
            v["engine_l"] = _eng
    return v


def row_title(v: dict) -> str:
    return (
        v.get("title")
        or f"{v.get('year') or ''} {v.get('make') or ''} {v.get('model') or ''} {v.get('trim') or ''}".strip()
        or "Unknown vehicle"
    )


def coerce_price(value: Any) -> int | None:
    """Price as a positive int; NULL for missing, unparseable or <= 0."""
    try:
        price = int(round(float(value))) if value is not None and str(value).strip() != "" else None
    except (TypeError, ValueError):
        price = None
    if price is not None and price <= 0:
        price = None
    return price


def coerce_mileage(value: Any) -> int | None:
    """Mileage as an int; NULL when the feed had no odometer (a stored 0 on a
    used row hides the gap from the completeness tally and the VDP gap fill,
    F12 2026-09-28)."""
    try:
        return int(float(str(value).replace(",", ""))) if value is not None and str(value).strip() != "" else None
    except (TypeError, ValueError):
        return None


def coerce_msrp(value: Any) -> int | None:
    try:
        msrp = int(round(float(value))) if value is not None and str(value).strip() != "" else None
        if msrp is not None and msrp <= 0:
            msrp = None
    except (TypeError, ValueError):
        msrp = None
    return msrp


def interior_pano_url(value: Any) -> str | None:
    """Interior panorama: a single http(s) URL string or None."""
    interior_pano = value.strip() if isinstance(value, str) else None
    if not interior_pano or not interior_pano.startswith("http"):
        interior_pano = None
    return interior_pano


def image_url_or_placeholder(value: Any) -> Any:
    if not value or not str(value).strip().startswith("http"):
        return PLACEHOLDER_IMAGE_URL
    return value
