"""Backfill missing ``adds_by_trim`` entries (especially base trims like SE)."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from backend.enrichment.dictionary_catalog import catalog_key
from backend.enrichment.dictionary_paths import TRIM_ADDS_BY_YEAR_DIR
from backend.enrichment.trim_ladder import _lookup_brochure_adds_key
from backend.enrichment.trim_ladder_knowledge import sanitize_brochure_trim_adds

# Curated base-trim standards when brochure matrices are not machine-parseable.
_CURATED_BASE_TRIM_ADDS: dict[str, dict[str, list[str]]] = {
    "2023|hyundai|elantra": {
        "SE": [
            "2.0L I-4 Atkinson Cycle engine (147 hp / 132 lb-ft) with Smartstream IVT",
            "Forward Collision-Avoidance Assist with Pedestrian Detection",
            "Lane Keeping Assist and Lane Following Assist",
            "Blind-Spot Collision Warning and Rear Cross-Traffic Collision-Avoidance Assist",
            "Dual LED projector-type headlights with High Beam Assist",
            "8.0-inch Display Audio with wireless Android Auto and Apple CarPlay",
            "Cloth seating surfaces with 6-way adjustable driver seat",
            "Rearview camera, Drive Mode Select, and 4-wheel disc brakes",
        ],
    },
    "2024|hyundai|elantra": {
        "SE": [
            "2.0L I-4 Atkinson Cycle engine (147 hp / 132 lb-ft) with Smartstream IVT",
            "Forward Collision-Avoidance Assist with Pedestrian Detection",
            "Lane Keeping Assist and Lane Following Assist",
            "Blind-Spot Collision Warning and Rear Cross-Traffic Collision-Avoidance Assist",
            "Dual LED projector-type headlights with High Beam Assist",
            "8.0-inch Display Audio with wireless Android Auto and Apple CarPlay",
            "Cloth seating surfaces with 6-way adjustable driver seat",
            "Rearview camera, Drive Mode Select, and 4-wheel disc brakes",
        ],
    },
    "2025|hyundai|elantra": {
        "SE": [
            "2.0L I-4 Atkinson Cycle engine (147 hp / 132 lb-ft) with Smartstream IVT",
            "Forward Collision-Avoidance Assist with Pedestrian Detection",
            "Lane Keeping Assist and Lane Following Assist",
            "Blind-Spot Collision Warning and Rear Cross-Traffic Collision-Avoidance Assist",
            "Dual LED projector-type headlights with High Beam Assist",
            "8.0-inch Display Audio with wireless Android Auto and Apple CarPlay",
            "Cloth seating surfaces with 6-way adjustable driver seat",
            "Rearview camera, Drive Mode Select, and 4-wheel disc brakes",
        ],
    },
}


def missing_trim_names(overlay: dict[str, Any]) -> list[str]:
    adds = overlay.get("adds_by_trim") or {}
    trims = list(overlay.get("trims_available") or [])
    if not trims:
        trims = list(adds.keys())
    return [str(t).strip() for t in trims if str(t).strip() and not adds.get(str(t).strip())]


def _read_overlay_file(year: int, make: str, model: str) -> dict[str, Any] | None:
    if year < 2010:
        return None
    path = TRIM_ADDS_BY_YEAR_DIR / f"{catalog_key(year, make, model).replace('|', '__')}.json"
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return data if isinstance(data, dict) else None


def _adds_for_trim(
    overlay: dict[str, Any],
    trim_name: str,
    *,
    make: str,
    model: str,
) -> list[str]:
    adds = overlay.get("adds_by_trim") or {}
    key = _lookup_brochure_adds_key(trim_name, [], adds, make=make, model=model)
    if key and adds.get(key):
        return [str(x).strip() for x in adds[key] if str(x).strip()]
    standard = overlay.get("standard_by_trim") or {}
    if isinstance(standard.get(trim_name), list):
        return [str(x).strip() for x in standard[trim_name] if str(x).strip()]
    return []


def _neighbor_adds(
    year: int,
    make: str,
    model: str,
    trim_name: str,
) -> list[str]:
    for delta in (-1, 1, -2, 2, -3, 3, -4, 4):
        neighbor = _read_overlay_file(year + delta, make, model)
        if not neighbor:
            continue
        bullets = _adds_for_trim(neighbor, trim_name, make=make, model=model)
        if bullets:
            return bullets
    return []


def _curated_adds(catalog_ck: str, trim_name: str) -> list[str]:
    bucket = _CURATED_BASE_TRIM_ADDS.get(catalog_ck) or {}
    return list(bucket.get(trim_name) or [])


def backfill_overlay_missing_adds(
    overlay: dict[str, Any],
    *,
    overwrite: bool = False,
) -> tuple[dict[str, Any], list[str]]:
    """
    Fill ``adds_by_trim`` holes using standard_by_trim, neighbor years, or curated data.
    Returns (overlay copy, list of trim names filled).
    """
    if not isinstance(overlay, dict):
        return overlay, []

    missing = missing_trim_names(overlay)
    if not missing:
        return overlay, []

    make = str(overlay.get("make") or "")
    model = str(overlay.get("model") or "")
    year = int(overlay.get("year") or 0)
    ck = str(overlay.get("catalog_key") or catalog_key(year, make, model))
    adds = dict(overlay.get("adds_by_trim") or {})
    standard = dict(overlay.get("standard_by_trim") or {})
    filled: list[str] = []

    for trim_name in missing:
        if adds.get(trim_name) and not overwrite:
            continue

        candidates: list[str] = []
        if isinstance(standard.get(trim_name), list):
            candidates.extend(str(x).strip() for x in standard[trim_name] if str(x).strip())
        if not candidates:
            candidates = _curated_adds(ck, trim_name)
        if not candidates:
            candidates = _neighbor_adds(year, make, model, trim_name)

        cleaned = sanitize_brochure_trim_adds(candidates, trim_name)
        if cleaned:
            adds[trim_name] = cleaned
            filled.append(trim_name)

    out = dict(overlay)
    out["adds_by_trim"] = adds
    return out, filled


def enrich_brochure_overlay(overlay: dict[str, Any] | None) -> dict[str, Any] | None:
    """Apply in-memory backfill before trim ladder merge."""
    if not overlay:
        return overlay
    enriched, _ = backfill_overlay_missing_adds(overlay)
    return enriched
