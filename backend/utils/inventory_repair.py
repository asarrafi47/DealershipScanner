"""
Batch repairs for SQLite ``cars``: normalize placeholders and backfill from EPA/trim merge.

Used by ``scripts/repair_inventory_fields.py`` (not imported on normal app requests).
"""
from __future__ import annotations

from typing import Any, NamedTuple

from backend.enrichment.knowledge_engine import merge_verified_specs
from backend.utils.car_serialize import DISPLAY_DASH, _dealer_spec_wins, infer_condition_for_storage, normalize_condition_for_storage
from backend.utils.field_clean import (
    canonical_body_style_or_none,
    clean_car_row_dict,
    is_canonical_body_style,
    is_effectively_empty,
)
from backend.utils.spec_field_normalize import collect_raw_spec_heuristic_updates
from backend.utils.interior_color_buckets import interior_color_buckets_json
from backend.vehicle_facts import precedence as prec

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
        "model_full_raw",
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
            except (TypeError, ValueError):
                ci = None
            if ci is not None and ci >= 0:
                # Trim-decoded cylinder counts are year-collision-prone
                # ("E 350" → modern turbo-four); never contradict the engine
                # text the listing already carries.
                from backend.utils.engine_consistency import cylinders_conflicts_with_engine_text

                if not cylinders_conflicts_with_engine_text(
                    ci, c.get("engine_description"), c.get("fuel_type")
                ):
                    out["cylinders"] = ci

    if is_effectively_empty(c.get("fuel_type")):
        ft = vs.get("fuel_type_hint")
        if ft and not is_effectively_empty(ft):
            out["fuel_type"] = str(ft).strip()

    cur_bs = c.get("body_style")
    bs = vs.get("body_style_display")
    if is_effectively_empty(cur_bs):
        if bs and not is_effectively_empty(bs):
            out["body_style"] = str(bs).strip()
    elif not is_canonical_body_style(cur_bs):
        # D-N32 normalize only: the catalog re-spells a noncanonical value, and only as a preset.
        canon = canonical_body_style_or_none(bs)
        if canon:
            out["body_style"] = canon

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
    if "interior_color" in updates:
        updates["interior_color_buckets"] = interior_color_buckets_json(
            updates.get("interior_color"), raw.get("make")
        )
    return updates


class StorageProposals(NamedTuple):
    """Tier-1 repairs split by how they may be written (P8B.6a).

    ``proposals``: ``(field, value, source)`` for columns under fact precedence
    (``precedence.FIELDS``); the caller decides them with ``apply_fact_proposals``.
    ``plain``: columns with no precedence rank (condition, trim, body_style, ...)
    plus housekeeping on a fact column that is not a new fact (a junk value cleared
    to NULL, a re-spelling of the stored value such as "All-Wheel Drive" -> "AWD").
    ``sources``: provenance label for each ``plain`` column that has one."""

    proposals: list[tuple[str, Any, str]]
    plain: dict[str, Any]
    sources: dict[str, str]


def _split_proposals(raw: dict[str, Any], tagged: list[tuple[str, dict[str, Any]]]) -> StorageProposals:
    """Route each ``(source, updates)`` group: fact columns become proposals unless the
    change is a clear or a re-spelling of the stored value, which stays plain."""
    proposals: list[tuple[str, Any, str]] = []
    plain: dict[str, Any] = {}
    sources: dict[str, str] = {}
    for src, updates in tagged:
        for k, v in updates.items():
            if k in prec.FIELDS and v is not None:
                if prec.is_blank(k, raw.get(k)) or not prec._same(k, raw.get(k), v):
                    proposals.append((k, v, src))
                    continue
            plain[k] = v
            sources[k] = prec.DEALER_NORMALIZED if k in prec.FIELDS else src
    return StorageProposals(proposals, plain, sources)


def collect_merge_spec_storage_proposals(raw: dict[str, Any]) -> StorageProposals:
    """``collect_merge_spec_storage_updates`` as proposals: EPA catalog / trim decoder
    values, labelled ``epa_catalog`` (they rank below the VIN decode and dealer text)."""
    return _split_proposals(dict(raw), [(prec.EPA_CATALOG, collect_merge_spec_storage_updates(raw))])


def collect_row_storage_proposals(raw: dict[str, Any]) -> StorageProposals:
    """``collect_row_storage_repairs`` as proposals with honest tier-1 labels:
    ``dealer_normalized`` (clean_car_row_dict), ``epa_catalog`` (merge_verified_specs),
    ``dealer_text_heuristic`` (spec_field_normalize heuristics). Same inputs and
    order as the patch builder; the ranks decide instead of the last writer."""
    cleaned = collect_cleaned_field_updates(raw)
    merged = collect_merge_spec_storage_updates(raw)
    heur = collect_raw_spec_heuristic_updates({**raw, **cleaned, **merged})
    out = _split_proposals(dict(raw), [
        (prec.DEALER_NORMALIZED, cleaned), (prec.EPA_CATALOG, merged), (prec.DEALER_TEXT, heur),
    ])
    plain, sources = out.plain, out.sources
    cond = infer_condition_for_storage(raw)
    if cond:
        plain["condition"] = cond
    norm = normalize_condition_for_storage({**raw, **cleaned, **merged, **heur, **plain})
    if norm:
        plain["condition"] = norm
    plain.update(storage_derived_columns(raw, plain))
    return StorageProposals(out.proposals, plain, sources)


def storage_derived_columns(raw: dict[str, Any], columns: dict[str, Any]) -> dict[str, Any]:
    """Columns derived from the final *columns* patch (interior colour buckets), computed
    after precedence has decided which values land."""
    if "interior_color" not in columns:
        return {}
    return {"interior_color_buckets": interior_color_buckets_json(columns.get("interior_color"), raw.get("make"))}
