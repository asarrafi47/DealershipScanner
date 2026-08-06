"""
Condition (New / Used / CPO / Pre-Owned) derivation for display and storage.

Split out of the former monolith ``backend/utils/car_serialize.py`` and
re-exported from the package facade so the public import surface is unchanged.
"""
from __future__ import annotations

import re
from typing import Any

from backend.utils.field_clean import clean_car_row_dict, is_effectively_empty
from backend.utils.fuel_type_normalize import fill_normalized_fuel_type_for_display

from ._common import DISPLAY_DASH, format_display_value
from .bmw import _bmw_resolve_condition_for_display


def listings_inventory_is_new(condition: Any) -> bool:
    """True when derived display condition is new retail inventory."""
    return str(condition or "").strip().lower() == "new"


def listings_inventory_is_pre_owned(condition: Any) -> bool:
    """True for used / CPO / pre-owned inventory (anything explicitly not new)."""
    c = str(condition or "").strip().lower()
    if not c or c in ("—", "-", "n/a"):
        return False
    return c != "new"


def fill_derived_condition_for_display(c: dict[str, Any], out: dict[str, Any]) -> None:
    """
    Mutates *out* ``condition`` from title / CPO / mileage / model year heuristics.
    *out* must already contain ``condition`` from ``format_display_value``.

    Also runs the mild-hybrid fuel-type correction: this is the one derivation
    hook both serialize paths (``serialize_car_for_api`` and
    ``serialize_car_for_listings_grid``) call after ``engine_display`` and
    ``fuel_type`` are settled, so a Ram 1500 eTorque reads as gas on the VDP and
    in the grid without either caller growing its own copy of the rule.
    """
    fill_normalized_fuel_type_for_display(c, out)
    _fill_condition_from_signals(c, out)


def _fill_condition_from_signals(c: dict[str, Any], out: dict[str, Any]) -> None:
    """
    Condition-only half of :func:`fill_derived_condition_for_display`.

    Kept separate so the STORAGE caller (:func:`infer_condition_for_storage`,
    which runs once per row during enrichment persist) does not also run the
    display-only fuel-type correction — that correction can hit the ``epa_master``
    catalog and its result is thrown away on the storage path.
    """
    tit = (c.get("title") or "").strip()
    low = tit.lower()
    if (c.get("make") or "").strip().upper() == "BMW":
        _bmw_resolve_condition_for_display(c, out, title_lower=low)
    elif tit and (is_effectively_empty(c.get("condition")) or out.get("condition") == DISPLAY_DASH):
        if "certified pre-owned" in low or "certified preowned" in low:
            out["condition"] = "Certified Pre-Owned"
        elif "mazda certified" in low:
            out["condition"] = "Certified Pre-Owned"
        elif re.search(r"\bcpo\b", low):
            out["condition"] = "Certified Pre-Owned"
        elif " certified " in f" {low} " or low.startswith("certified "):
            out["condition"] = "Certified"
        elif low.startswith("used "):
            out["condition"] = "Used"
        elif low.startswith("new "):
            out["condition"] = "New"

    _oc = out.get("condition")
    if _oc is None or str(_oc).strip() in ("", DISPLAY_DASH):
        if c.get("is_cpo") in (1, True, "1"):
            out["condition"] = "Certified Pre-Owned"
        else:
            _low = (c.get("title") or "").lower()
            _su = (c.get("source_url") or "").lower()
            # CPO / certified inventory before mileage→Used so listings are not mislabeled.
            if (
                re.search(r"\bcpo\b", _low)
                or "certified pre-owned" in _low
                or "certified preowned" in _low
                or "mazda certified" in _low
                or (" certified " in f" {_low} " and "pre-owned" in _low)
                or _low.startswith("certified ")
                or "/certified" in _su
                or "cpo-inventory" in _su
                or "-cpo-" in _su
                or "certified_inventory" in _su.replace("-", "_")
            ):
                out["condition"] = "Certified Pre-Owned"
            else:
                _mi: int | None
                try:
                    raw_m = c.get("mileage")
                    if raw_m is None or str(raw_m).strip() == "":
                        _mi = None
                    else:
                        _mi = int(float(str(raw_m).replace(",", "")))
                except (TypeError, ValueError):
                    _mi = None
                if _mi is not None and _mi > 0:
                    out["condition"] = "Used"
                elif _mi == 0:
                    _ttl = (c.get("title") or "").lower()
                    if (
                        "/new-inventory" in _su
                        or "/new/" in _su
                        or "newinventory" in _su.replace("-", "").replace("_", "")
                        or _ttl.startswith("new ")
                    ):
                        out["condition"] = "New"

    # Model year <= 2023: almost never new retail; default Pre-Owned if still unknown.
    # Prefer Certified Pre-Owned when listing text/URL still suggests a CPO program.
    _oc3 = out.get("condition")
    if _oc3 is None or str(_oc3).strip() in ("", DISPLAY_DASH):
        try:
            yy = int(c.get("year")) if c.get("year") is not None else None
        except (TypeError, ValueError):
            yy = None
        if yy is not None and yy < 2024:
            low3 = (c.get("title") or "").lower()
            su3 = (c.get("source_url") or "").lower()
            cpo_hint = (
                c.get("is_cpo") in (1, True, "1")
                or re.search(r"\bcpo\b", low3)
                or "certified pre-owned" in low3
                or "certified preowned" in low3
                or "mazda certified" in low3
                or (" certified " in f" {low3} " and "pre-owned" in low3)
                or "/certified" in su3
                or "cpo-inventory" in su3
                or "-cpo-" in su3
            )
            if cpo_hint:
                out["condition"] = "Certified Pre-Owned"
            else:
                out["condition"] = "Pre-Owned"

    # 2024+ with no title/mileage signal: inventory SRP / VDP URL often encodes new vs used.
    _oc4 = out.get("condition")
    if _oc4 is None or str(_oc4).strip() in ("", DISPLAY_DASH):
        try:
            yy4 = int(c.get("year")) if c.get("year") is not None else None
        except (TypeError, ValueError):
            yy4 = None
        if yy4 is not None and yy4 >= 2024:
            su4 = (c.get("source_url") or "").lower()
            if (
                "used-inventory" in su4
                or "used_inventory" in su4
                or "pre-owned" in su4
                or "preowned" in su4
                or "/used/" in su4
            ):
                out["condition"] = "Used"
            elif (
                "new-inventory" in su4
                or "newinventory" in su4.replace("-", "").replace("_", "")
                or "/new/" in su4
            ):
                out["condition"] = "New"

    # Final fallback: mileage=0 on a current/upcoming model year → New.
    # These are new inventory rows scraped without a URL or "New" title prefix.
    _oc5 = out.get("condition")
    if _oc5 is None or str(_oc5).strip() in ("", DISPLAY_DASH):
        try:
            yy5 = int(c.get("year")) if c.get("year") is not None else None
        except (TypeError, ValueError):
            yy5 = None
        if yy5 is not None and yy5 >= 2024:
            try:
                raw_m5 = c.get("mileage")
                mi5 = (
                    int(float(str(raw_m5).replace(",", "")))
                    if raw_m5 is not None and str(raw_m5).strip() != ""
                    else None
                )
            except (TypeError, ValueError):
                mi5 = None
            if mi5 == 0:
                out["condition"] = "New"


def normalize_condition_for_storage(car: dict[str, Any]) -> str | None:
    """
    Normalize non-blank but incorrect stored condition values.

    - "Certified" → "Certified Pre-Owned" when CPO signals present in title/URL
    - "Pre-Owned"  → "Used"

    Returns the corrected value, or None if no change needed.
    """
    cond = (car.get("condition") or "").strip()
    if not cond:
        return None

    low_t = (car.get("title") or "").lower()
    low_u = (car.get("source_url") or "").lower()
    is_cpo_flag = car.get("is_cpo") in (1, True, "1")

    if cond == "Certified":
        if (
            "certified pre-owned" in low_t
            or "certified preowned" in low_t
            or "/certified" in low_u
            or "-cpo-" in low_u
            or "cpo-inventory" in low_u
            or is_cpo_flag
        ):
            return "Certified Pre-Owned"

    if cond == "Pre-Owned":
        return "Used"

    return None


def infer_condition_for_storage(car: dict[str, Any]) -> str | None:
    """
    Return a ``cars.condition`` value to persist when the row has no real condition yet.
    None means leave the column unchanged (caller may still NULL junk via ``clean_car_row_dict``).
    """
    c = clean_car_row_dict(dict(car))
    if not is_effectively_empty(c.get("condition")):
        return None
    out: dict[str, Any] = {"condition": format_display_value(c.get("condition"))}
    # Condition-only: the fuel-type correction is display logic and would be
    # discarded here (this function returns ``condition`` alone).
    _fill_condition_from_signals(c, out)
    fin = out.get("condition")
    if not fin or str(fin).strip() in ("", DISPLAY_DASH):
        return None
    return str(fin).strip()
