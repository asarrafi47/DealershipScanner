"""Gather every input :func:`merge_verified_specs` ranks, in the original call order."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from backend.enrichment.verified_specs import plausibility
from backend.enrichment.verified_specs._coerce import int_or_none


@dataclass
class SpecSources:
    """Everything the per-field chains read. Built once per car by :func:`gather_sources`."""

    car: dict[str, Any]
    make: str
    model: str
    trim: str
    title: str
    year: int | None
    dealer_cyl: Any
    dealer_drive: str
    dealer_trans: str
    dealer_ft: str
    title_for_decode: str
    regex: dict[str, Any]
    linked_id: Any
    generation: dict[str, Any] | None
    epa_trim: dict[str, Any]
    catalog_link_rejected: str | None
    linked_exact: bool
    vpic: dict[str, Any]
    vpic_cyl: int | None
    vpic_cyl_ok: bool
    epa: dict[str, Any]
    epa_fuzzy_rejected: str | None
    sourced: dict[str, float] = field(default_factory=dict)
    curated_060: float | None = None
    tank_gal: float | None = None
    factory_range: int | None = None
    dict_specs: dict[str, Any] | None = None


def _decode_title(title: str, make: str, dealer_ft: str) -> str:
    """BMW fuel_type rides along into the trim decoder's title."""
    title_for_decode = (title or "").strip()
    if (make or "").strip().upper() == "BMW" and dealer_ft:
        title_for_decode = f"{title_for_decode} {dealer_ft}".strip()
    return title_for_decode


def _generation(make: str, model: str, y: int | None) -> dict[str, Any] | None:
    try:
        from backend.catalog.generations import generation_for

        return generation_for(make, model, y)
    except Exception:
        return None


def _catalog_rows(car: dict[str, Any], y: int | None, make: str, model: str, trim: str):
    """Resolved catalog link first (cars.epa_master_id), per-trim fuzzy lookup as fallback.

    Returns ``(epa_trim, catalog_link_rejected, linked_exact)``. The by-id row is
    exact (written by the one resolver in backend.catalog) — no fuzzy re-matching —
    unless it contradicts the dealer's own engine text.
    """
    from backend.enrichment import knowledge_engine as ke

    linked_id = car.get("epa_master_id")
    epa_trim = ke.lookup_epa_master_by_id(linked_id) if linked_id else {}
    catalog_link_rejected: str | None = None
    if epa_trim:
        catalog_link_rejected = plausibility.linked_catalog_row_rejection(car, epa_trim)
        if catalog_link_rejected:
            epa_trim = {}
    linked_exact = bool(epa_trim)  # True only when the by-id row actually resolved
    if not epa_trim:
        # Per-trim lookup (exact match from build_epa_master.py data), then aggregate fallback
        epa_trim = ke.lookup_epa_by_trim(y, make, model, trim) if trim else {}
    return epa_trim, catalog_link_rejected, linked_exact


def _extended_specs(car: dict[str, Any], include_extended_specs: bool):
    """hp / torque / 0-60 / tank / factory range — only document-backed figures.

    ``lookup_epa_extended_specs`` is deliberately NOT called: its rows are a
    model-level scrape (see the block above SOURCED_EXTENDED_SPEC_FIELDS).
    """
    if not include_extended_specs:
        return {}, None, None, None
    from backend.enrichment import knowledge_engine_specs as kes

    sourced = kes.sourced_extended_specs(car)
    curated_060 = kes.curated_zero_to_60_sec(car)
    tank_gal = kes._document_backed_fuel_tank_gal(car)
    factory_range = kes._document_backed_factory_range(car)
    return sourced, curated_060, tank_gal, factory_range


def gather_sources(car: dict[str, Any], *, include_extended_specs: bool) -> SpecSources:
    from backend.enrichment import knowledge_engine as ke
    from backend.utils.field_clean import clean_car_row_dict

    car = clean_car_row_dict(dict(car))
    make = car.get("make") or ""
    model = car.get("model") or ""
    trim = car.get("trim") or ""
    title = car.get("title") or ""
    year = car.get("year")
    try:
        y = int(year) if year is not None else None
    except (TypeError, ValueError):
        y = None

    dealer_cyl = plausibility.text_corrected_dealer_cylinders(car)
    dealer_drive = (car.get("drivetrain") or "").strip()
    dealer_trans = (car.get("transmission") or "").strip()
    dealer_ft = (car.get("fuel_type") or "").strip()
    title_for_decode = _decode_title(title, make, dealer_ft)
    regex = ke.decode_trim_logic(make, model, trim, title_for_decode)

    linked_id = car.get("epa_master_id")
    generation = _generation(make, model, y)
    epa_trim, catalog_link_rejected, linked_exact = _catalog_rows(car, y, make, model, trim)

    vpic = ke.lookup_vpic_from_cache(car.get("vin"))
    vpic_cyl = int_or_none(vpic.get("cylinders"))
    vpic_cyl_ok = plausibility.vpic_cylinders_ok(car, vpic_cyl)

    epa = ke.lookup_epa_aggregate(
        y, make, model, title=title_for_decode, trim=trim,
        # The VIN-decoded count steers the aggregate toward the right engine
        # family (an X5 M is the 8-cylinder rows, not the X5's 3.0L V6).
        prefer_cylinders=vpic_cyl if vpic_cyl_ok and vpic_cyl else int_or_none(dealer_cyl),
    )
    # Merge: per-trim values win over aggregate for any key they provide
    epa = {**epa, **{k: v for k, v in epa_trim.items() if v is not None}}
    epa, epa_fuzzy_rejected = plausibility.drop_fuzzy_engine_family_mismatch(car, epa, linked_exact)

    sourced, curated_060, tank_gal, factory_range = _extended_specs(car, include_extended_specs)

    from backend.enrichment.model_specs_dictionary import lookup_model_specs_dictionary

    dict_specs = lookup_model_specs_dictionary(make, model)

    return SpecSources(
        car=car,
        make=make,
        model=model,
        trim=trim,
        title=title,
        year=y,
        dealer_cyl=dealer_cyl,
        dealer_drive=dealer_drive,
        dealer_trans=dealer_trans,
        dealer_ft=dealer_ft,
        title_for_decode=title_for_decode,
        regex=regex,
        linked_id=linked_id,
        generation=generation,
        epa_trim=epa_trim,
        catalog_link_rejected=catalog_link_rejected,
        linked_exact=linked_exact,
        vpic=vpic,
        vpic_cyl=vpic_cyl,
        vpic_cyl_ok=vpic_cyl_ok,
        epa=epa,
        epa_fuzzy_rejected=epa_fuzzy_rejected,
        sourced=sourced,
        curated_060=curated_060,
        tank_gal=tank_gal,
        factory_range=factory_range,
        dict_specs=dict_specs,
    )
