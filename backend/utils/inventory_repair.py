"""
Batch repairs for SQLite ``cars``: normalize placeholders and backfill from EPA/trim merge.

Used by ``scripts/repair_inventory_fields.py`` (not imported on normal app requests).
"""
from __future__ import annotations

from typing import Any

from backend.enrichment.knowledge_engine import merge_verified_specs
from backend.utils.car_serialize import DISPLAY_DASH, _dealer_spec_wins, infer_condition_for_storage, infer_is_cpo_for_storage, normalize_condition_for_storage
from backend.utils.field_clean import clean_car_row_dict, is_effectively_empty
from backend.utils.spec_field_normalize import collect_raw_spec_heuristic_updates
from backend.utils.history_highlights import coalesce_history_highlights_for_storage, history_highlights_json
from backend.utils.interior_color_buckets import interior_color_buckets_json

_CLEANABLE_FOR_SQL = frozenset(
    {
        "trim",
        "zip_code",
        "transmission",
        "drivetrain",
        "interior_color",
        "exterior_color",
        "fuel_type",
        "body_style",
        "engine_description",
        "condition",
        "dealer_url",
        "carfax_url",
        "stock_number",
        "title",
        "description",
    }
)


def _displayish_junk(s: Any) -> bool:
    if s is None:
        return True
    t = str(s).strip()
    if not t:
        return True
    if t in ("—", "-", DISPLAY_DASH):
        return True
    return is_effectively_empty(t)


def collect_cleaned_field_updates(raw: dict[str, Any]) -> dict[str, Any]:
    """Column → value from ``clean_car_row_dict`` when it differs from *raw* (e.g. ``--`` → NULL)."""
    cleaned = clean_car_row_dict(dict(raw))
    out: dict[str, Any] = {}
    for k in _CLEANABLE_FOR_SQL:
        if k not in cleaned:
            continue
        if cleaned.get(k) != raw.get(k):
            out[k] = cleaned.get(k)
    return out


def collect_merge_spec_storage_updates(raw: dict[str, Any]) -> dict[str, Any]:
    """
    Fill missing / placeholder drivetrain, transmission, cylinders, fuel_type, body_style
    from ``merge_verified_specs`` (EPA + trim decoder). Does not overwrite real dealer fields.
    """
    vs = merge_verified_specs(raw)
    c = clean_car_row_dict(dict(raw))
    out: dict[str, Any] = {}

    if not _dealer_spec_wins(c.get("transmission")):
        t = vs.get("transmission_display")
        if t and not _displayish_junk(t):
            out["transmission"] = str(t).strip()

    if not _dealer_spec_wins(c.get("drivetrain")):
        d = vs.get("drivetrain_display")
        if d and not _displayish_junk(d):
            out["drivetrain"] = str(d).strip()

    try:
        dc = c.get("cylinders")
        dc_i = int(dc) if dc is not None and str(dc).strip() != "" else None
    except (TypeError, ValueError):
        dc_i = None
    if dc_i is None or dc_i == 0:
        cyl = vs.get("cylinders")
        if cyl is None:
            cyl = vs.get("cylinders_display")
        if cyl is not None:
            try:
                ci = int(cyl)
                if ci >= 0:
                    out["cylinders"] = ci
            except (TypeError, ValueError):
                pass

    if is_effectively_empty(c.get("fuel_type")):
        ft = vs.get("fuel_type_hint")
        if ft and not is_effectively_empty(ft):
            out["fuel_type"] = str(ft).strip()

    if is_effectively_empty(c.get("body_style")):
        bs = vs.get("body_style_display")
        if bs and not is_effectively_empty(bs):
            out["body_style"] = str(bs).strip()

    return out


def collect_row_storage_repairs(raw: dict[str, Any]) -> dict[str, Any]:
    """Single-row patch dict for ``update_car_row_partial`` (may be empty)."""
    updates = collect_cleaned_field_updates(raw)
    updates.update(collect_merge_spec_storage_updates(raw))
    merged = {**raw, **updates}
    updates.update(collect_raw_spec_heuristic_updates(merged))
    cond = infer_condition_for_storage(raw)
    if cond:
        updates["condition"] = cond
    # Normalize non-blank but wrong stored values ("Certified" → CPO, "Pre-Owned" → Used)
    norm_source = {**raw, **updates}
    norm = normalize_condition_for_storage(norm_source)
    if norm:
        updates["condition"] = norm
    if raw.get("is_cpo") is None and "is_cpo" not in updates:
        cpo = infer_is_cpo_for_storage({**raw, **updates})
        if cpo is not None:
            updates["is_cpo"] = cpo
    if "interior_color" in updates:
        updates["interior_color_buckets"] = interior_color_buckets_json(
            updates.get("interior_color"), raw.get("make")
        )
    merged_row = {**raw, **updates}
    if not _row_has_history_highlights(merged_row.get("history_highlights")):
        highlights = coalesce_history_highlights_for_storage(merged_row)
        payload = history_highlights_json(highlights)
        if payload != merged_row.get("history_highlights"):
            updates["history_highlights"] = payload
    return updates


def _row_has_history_highlights(raw: Any) -> bool:
    if raw is None or raw == "" or str(raw).strip() == "[]":
        return False
    if isinstance(raw, list):
        return len(raw) > 0
    try:
        import json

        parsed = json.loads(str(raw))
        return isinstance(parsed, list) and len(parsed) > 0
    except (json.JSONDecodeError, TypeError):
        return bool(str(raw).strip())
