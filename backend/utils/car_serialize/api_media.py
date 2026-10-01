"""
``serialize_car_for_api`` steps: gallery filtering, URL normalization and the 360
spin / interior pano assets.

Split out of ``serialize.serialize_car_for_api`` (2026-10-01) with no behaviour
change; ``backend/tests/test_serialize_car_for_api_golden.py`` pins the output.
"""
from __future__ import annotations

import json
from typing import Any

from backend.parsers.base import filter_spyne_gallery_variants
from backend.utils.field_clean import normalize_optional_url

_URL_KEYS = ("image_url", "source_url", "carfax_url", "dealer_url")


def gallery_for_api(v: Any) -> Any:
    """Spyne size variants dropped from a list (or JSON-list string) gallery; else as-is."""
    if isinstance(v, list):
        return filter_spyne_gallery_variants(v)
    if isinstance(v, str):
        try:
            parsed = json.loads(v)
            return filter_spyne_gallery_variants(parsed) if isinstance(parsed, list) else v
        except Exception:
            return v
    return v


def apply_url_fields(out: dict[str, Any], c: dict[str, Any]) -> None:
    for url_key in _URL_KEYS:
        if url_key in c:
            out[url_key] = normalize_optional_url(c.get(url_key))


def apply_spin_assets(out: dict[str, Any], c: dict[str, Any]) -> None:
    """360 spin assets: always emitted with contract defaults ([] / null)."""
    _spin = c.get("spin_frames")
    if isinstance(_spin, str):
        try:
            _spin = json.loads(_spin)
        except (TypeError, ValueError):
            _spin = None
    out["spin_frames"] = (
        [u for u in _spin if isinstance(u, str) and u.strip()] if isinstance(_spin, list) else []
    )
    out["interior_pano"] = normalize_optional_url(c.get("interior_pano"))
