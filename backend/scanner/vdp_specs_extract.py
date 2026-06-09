"""Structured VDP spec-sheet extraction (separate from packages tab)."""
from __future__ import annotations

import json
from typing import Any


def spec_sheet_from_bundle(bundle: dict[str, Any] | None) -> dict[str, Any]:
    """Build ``spec_sheet_normalized`` from PAGE_EXTRACT domSpecs + spec sections."""
    if not isinstance(bundle, dict):
        return {}

    specs_raw = bundle.get("domSpecs") or {}
    rows: list[dict[str, str]] = []
    if isinstance(specs_raw, dict):
        for label, val in list(specs_raw.items())[:80]:
            lk = str(label or "").strip()
            vk = str(val or "").strip()
            if lk and vk and len(lk) < 80 and len(vk) < 400:
                rows.append({"label": lk[:80], "value": vk[:400]})

    sections = bundle.get("domPackagesSections") or []
    section_list = [str(s) for s in sections if s][:20] if isinstance(sections, list) else []

    if not rows and not section_list:
        return {}

    out: dict[str, Any] = {"source": "vdp_dom", "rows": rows[:60]}
    if section_list:
        out["sections_seen"] = section_list
    return out


def merge_spec_sheet_into_vehicle(vehicle: dict[str, Any], bundle: dict[str, Any] | None) -> bool:
    """Persist spec sheet under ``spec_source_json.spec_sheet_normalized``."""
    sheet = spec_sheet_from_bundle(bundle)
    if not sheet:
        return False

    try:
        from backend.utils.spec_provenance import merge_spec_source_json
    except ImportError:
        return False

    raw = vehicle.get("spec_source_json")
    merged = merge_spec_source_json(
        raw if isinstance(raw, str) else (json.dumps(raw) if isinstance(raw, dict) else None),
        {"spec_sheet_normalized": sheet},
    )
    if merged:
        vehicle["spec_source_json"] = merged
        return True
    return False


__all__ = ["merge_spec_sheet_into_vehicle", "spec_sheet_from_bundle"]
