"""
Merge LLaVA Monroney / window-sticker JSON into a scanner vehicle dict (conservative fills).
"""
from __future__ import annotations

import json
from typing import Any

from backend.utils.field_clean import is_effectively_empty


def _parse_existing_packages(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, str) and raw.strip():
        try:
            p = json.loads(raw)
            return dict(p) if isinstance(p, dict) else {}
        except (json.JSONDecodeError, TypeError):
            return {}
    return {}


def merge_monroney_parsed_into_vehicle(vehicle: dict[str, Any], parsed: dict[str, Any]) -> list[str]:
    """
    Mutate *vehicle* with sticker-derived fields only where the destination is empty.
    Merges option lines into ``packages`` JSON (``monroney_options``, ``sticker_vision`` metadata).
    Returns list of field keys touched.
    """
    if not isinstance(parsed, dict):
        return []
    touched: list[str] = []

    def fill_text(key: str, val: Any) -> None:
        if key not in vehicle or not is_effectively_empty(vehicle.get(key)):
            return
        s = str(val).strip() if val is not None else ""
        if not s or is_effectively_empty(s):
            return
        vehicle[key] = s[:500]
        touched.append(key)

    fill_text("engine_description", parsed.get("engine_description") or parsed.get("engine"))
    fill_text("transmission", parsed.get("transmission"))
    fill_text("drivetrain", parsed.get("drivetrain") or parsed.get("drive_type"))
    fill_text("fuel_type", parsed.get("fuel_type"))

    cyl = parsed.get("cylinders")
    if (
        cyl is not None
        and (vehicle.get("cylinders") is None or int(vehicle.get("cylinders") or 0) <= 0)
    ):
        try:
            n = int(float(cyl))
            if n > 0:
                vehicle["cylinders"] = n
                touched.append("cylinders")
        except (TypeError, ValueError):
            pass

    for mpg_key, src in (("mpg_city", "mpg_city"), ("mpg_highway", "mpg_highway")):
        if vehicle.get(mpg_key) is not None and int(vehicle.get(mpg_key) or 0) > 0:
            continue
        raw_m = parsed.get(src) or parsed.get(src.replace("_", ""))
        if raw_m is None:
            continue
        try:
            n = int(float(raw_m))
            if n > 0:
                vehicle[mpg_key] = n
                touched.append(mpg_key)
        except (TypeError, ValueError):
            pass

    msrp = parsed.get("msrp") or parsed.get("total_vehicle_price") or parsed.get("base_msrp")
    if msrp is not None and (vehicle.get("msrp") is None or int(vehicle.get("msrp") or 0) <= 0):
        try:
            n = int(round(float(msrp)))
            if n > 0:
                vehicle["msrp"] = n
                touched.append("msrp")
        except (TypeError, ValueError):
            pass

    opts = parsed.get("optional_packages") or parsed.get("installed_options") or parsed.get("options")
    std = parsed.get("standard_equipment_summary") or parsed.get("standard_equipment")
    pkg = _parse_existing_packages(vehicle.get("packages"))
    changed = False
    if isinstance(opts, list):
        bucket = pkg.setdefault("monroney_options", [])
        if not isinstance(bucket, list):
            bucket = []
            pkg["monroney_options"] = bucket
        seen = {str(x).strip().lower() for x in bucket if isinstance(x, str)}
        for item in opts:
            s = str(item).strip()[:400]
            if s and s.lower() not in seen:
                bucket.append(s)
                seen.add(s.lower())
                changed = True
    if isinstance(std, list):
        bucket2 = pkg.setdefault("monroney_standard_highlights", [])
        if not isinstance(bucket2, list):
            bucket2 = []
            pkg["monroney_standard_highlights"] = bucket2
        seen2 = {str(x).strip().lower() for x in bucket2 if isinstance(x, str)}
        for item in std:
            s = str(item).strip()[:300]
            if s and s.lower() not in seen2:
                bucket2.append(s)
                seen2.add(s.lower())
                changed = True
    try:
        conf = float(parsed.get("confidence"))
    except (TypeError, ValueError):
        conf = 0.0
    conf = max(0.0, min(1.0, conf))
    sv = pkg.setdefault("sticker_vision", {})
    if isinstance(sv, dict):
        vm = str(parsed.get("vision_model") or "").strip()[:80]
        if vm:
            sv["ollama_model"] = vm
        sv["confidence"] = conf
        vv = str(parsed.get("vin_visible") or "").strip()[:20]
        sv["vin_visible"] = vv or None
        changed = True
    if changed or pkg:
        vehicle["packages"] = pkg
        if "packages" not in touched:
            touched.append("packages")

    return touched
