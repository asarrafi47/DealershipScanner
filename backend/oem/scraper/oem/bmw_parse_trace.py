"""Explain BMW locator JSON parse outcomes (for debug artifacts)."""
from __future__ import annotations

from typing import Any


def top_level_keys(data: Any, *, max_keys: int = 80) -> list[str] | str:
    if isinstance(data, dict):
        return list(data.keys())[:max_keys]
    if isinstance(data, list):
        return f"array[{len(data)}]"
    return type(data).__name__


def explain_parse_outcome(data: Any, records: list[dict], *, source_hint: str) -> str:
    """Human-readable reason when zero dealer rows were extracted."""
    if records:
        return ""
    if data is None:
        return "null_payload"
    if isinstance(data, dict) and data.get("type") == "FeatureCollection":
        feats = data.get("features") or []
        if not isinstance(feats, list) or len(feats) == 0:
            return "feature_collection_empty_features"
        # had features but no names
        return "feature_collection_no_named_dealers"
    if isinstance(data, list):
        if len(data) == 0:
            return "empty_json_array"
        return "array_items_not_recognized_as_dealers"
    if isinstance(data, dict):
        for key in ("dealers", "items", "locations", "results", "data", "outlets"):
            v = data.get(key)
            if isinstance(v, list) and len(v) > 0:
                return "dealer_array_present_but_no_parseable_names"
        return "object_has_no_recognized_dealer_arrays_or_geojson"
    return "unsupported_json_shape"
