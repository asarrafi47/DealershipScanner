"""
``serialize_car_for_api`` steps: which verified specs apply, the base copy of the
row, first-seen fields, the display names, and the condition/mileage flag.

Split out of ``serialize.serialize_car_for_api`` (2026-10-01) with no behaviour
change; ``backend/tests/test_serialize_car_for_api_golden.py`` pins the output.
"""
from __future__ import annotations

from typing import Any

from backend.utils.field_clean import is_effectively_empty

from ._common import SENSITIVE_CAR_ROW_KEYS, format_display_value
from .api_media import gallery_for_api

# Numeric columns passed through verbatim (no display dash for None).
_PASSTHROUGH_KEYS = (
    "price",
    "mileage",
    "year",
    "msrp",
    "cylinders",
    "mpg_city",
    "mpg_highway",
    "id",
    "distance_miles",
    "dealership_registry_id",
)


def resolve_verified_specs(
    c: dict[str, Any],
    *,
    include_verified: bool,
    verified_specs: dict[str, Any] | None,
) -> dict[str, Any]:
    """The caller's merged specs, else a fresh merge (when allowed), else ``{}``."""
    vs: dict[str, Any] = {}
    if verified_specs is not None:
        vs = verified_specs
    elif include_verified:
        try:
            from backend.enrichment.knowledge_engine import merge_verified_specs

            vs = merge_verified_specs(c)
        except Exception:
            vs = {}
    return vs


def copy_row_fields(c: dict[str, Any]) -> dict[str, Any]:
    """Shallow copy of the cleaned row: sensitive keys dropped, strings display-dashed."""
    out: dict[str, Any] = {}
    for k, v in c.items():
        if k in SENSITIVE_CAR_ROW_KEYS:
            continue
        if k == "gallery":
            out[k] = gallery_for_api(v)
            continue
        if k in ("spin_frames", "interior_pano"):
            # Handled explicitly after the loop (contract defaults: [] / null);
            # must not fall through to format_display_value (None -> em-dash).
            continue
        if k == "history_highlights":
            out[k] = v
            continue
        if k == "packages":
            if is_effectively_empty(v) or str(v).strip() in ("{}", "[]"):
                out[k] = None
            else:
                out[k] = v
            continue
        if k in _PASSTHROUGH_KEYS:
            out[k] = v
            continue
        if k == "engine_l":
            out[k] = v
            continue
        if k == "data_quality_score" and isinstance(v, (int, float)):
            out[k] = v
            continue
        if isinstance(v, (dict, list)) and k not in ("gallery", "history_highlights"):
            out[k] = v
            continue
        if isinstance(v, (int, float)):
            out[k] = v
            continue
        out[k] = format_display_value(v)
    return out


def apply_first_seen(out: dict[str, Any], c: dict[str, Any]) -> None:
    from backend.utils.first_seen import first_seen_fields

    out.update(first_seen_fields(c))


def apply_display_names(
    out: dict[str, Any], model_d: Any, trim_d: Any, engine_disp: Any
) -> None:
    """BMW-normalized model/trim (display only) and the engine line."""
    out["model"] = model_d
    out["trim"] = trim_d
    out["engine_display"] = engine_disp


def apply_mileage_not_listed(out: dict[str, Any], c: dict[str, Any]) -> None:
    from backend.utils.mileage_display import mileage_not_listed

    out["mileage_not_listed"] = mileage_not_listed(
        c.get("mileage"), condition=out.get("condition"), is_cpo=c.get("is_cpo"), year=c.get("year")
    )
