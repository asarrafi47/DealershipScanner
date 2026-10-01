"""Provenance list, engine string, and the dict :func:`merge_verified_specs` returns."""

from __future__ import annotations

from typing import Any

from backend.enrichment.verified_specs.sources import SpecSources


def master_engine_string(src: SpecSources) -> Any:
    from backend.enrichment import knowledge_engine as ke

    return ke.build_master_engine_string(src.make, src.model, src.trim, src.title, src.regex, src.epa)


def provenance_sources(src: SpecSources) -> list[str]:
    """Which reference sources contributed anything (the ``sources`` key)."""
    regex, epa = src.regex, src.epa
    sources = []
    if (
        regex.get("cylinders") is not None
        or regex.get("drivetrain")
        or regex.get("fuel_type_hint")
        or regex.get("body_style_hint")
        or regex.get("transmission_hint")
    ):
        sources.append("Trim decoder")
    if epa.get("cylinders") is not None or epa.get("drivetrain") or epa.get("gears") or epa.get("transmission"):
        sources.append("EPA dataset")
    if src.dict_specs:
        sources.append("Model specs dictionary")
    return sources


def build_result(
    src: SpecSources,
    *,
    cylinders: Any,
    cylinders_display: Any,
    cylinders_verified: bool,
    drivetrain: Any,
    drivetrain_display: Any,
    drivetrain_verified: bool,
    gears: Any,
    transmission_display: Any,
    sources: list[str],
    master_engine_string: Any,
    fuel_economy_display: Any,
    body_style_display: Any,
) -> dict[str, Any]:
    """The 35-key dict, in its historical key order."""
    epa, vpic, epa_trim, sourced, gen = src.epa, src.vpic, src.epa_trim, src.sourced, src.generation
    return {
        "cylinders": cylinders,
        "cylinders_display": cylinders_display,
        "cylinders_verified": cylinders_verified,
        "catalog_link_rejected": src.catalog_link_rejected,
        "epa_fuzzy_rejected": src.epa_fuzzy_rejected,
        "drivetrain": drivetrain,
        "vpic_electrification": vpic.get("electrification"),
        "drivetrain_display": drivetrain_display,
        "drivetrain_verified": drivetrain_verified,
        "gears": gears,
        "transmission_display": transmission_display,
        "fuel_type_hint": src.regex.get("fuel_type_hint"),
        "sources": sources,
        "dealer_cylinders": src.dealer_cyl,
        "master_engine_string": master_engine_string,
        "fuel_economy_display": fuel_economy_display,
        "epa_displacement": epa.get("displacement") or vpic.get("engine_l"),
        # VIN-decoded displacement, kept separate so the engine line can rank it
        # above catalog / known-trim text (see car_serialize.engine).
        "vpic_engine_l": vpic.get("engine_l"),
        "body_style_display": body_style_display,
        # Per-trim EPA fields (populated from DICTIONARY via build_epa_master.py)
        "epa_fuel_type": epa.get("fuel_type"),
        "epa_engine_description": epa_trim.get("engine_description"),
        "epa_city08": epa.get("city08"),
        "epa_highway08": epa.get("highway08"),
        # Extended specs. Every key below is either quoted from a page this row
        # names, or None. See the block comment above SOURCED_EXTENDED_SPEC_FIELDS.
        "horsepower": sourced.get("horsepower"),
        "torque_lb_ft": sourced.get("torque_lb_ft"),
        # Curated cohort figure wins outright: it is only present where the
        # stored value is known to belong to a different trim.
        "zero_to_60_sec": (
            src.curated_060 if src.curated_060 is not None else sourced.get("zero_to_60_sec")
        ),
        "fuel_tank_gal": src.tank_gal,
        "ev_range_miles": src.factory_range,
        # BLANK_EXTENDED_SPEC_FIELDS — no per-trim source exists for any of
        # these, so they are blank rather than approximate.
        "curb_weight_lb": None,
        "battery_kwh": None,
        "tow_capacity_lb": None,
        # Catalog link + generation (backend.catalog; see docs/data_architecture_plan.md)
        "epa_master_id": src.linked_id,
        "generation_code": gen.get("generation") if gen else None,
        "generation_years": (
            f"{gen['year_start']}–{gen['year_end'] or 'present'}" if gen else None
        ),
        "generation_notes": gen.get("notes") if gen else None,
    }
