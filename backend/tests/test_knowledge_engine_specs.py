"""Verified spec merge: placeholders must not block EPA/regex inference."""

from __future__ import annotations

import json
import sqlite3

import pytest

from backend.enrichment.knowledge_engine import (
    _bmw_has_40i_suffix,
    _is_na_spec,
    _model_epa_fallbacks,
    clear_epa_aggregate_lookup_cache,
    clear_epa_trim_lookup_cache,
    decode_trim_logic,
    lookup_epa_aggregate,
    lookup_epa_by_trim,
    merge_verified_specs,
    prepare_car_detail_context,
)

# ---------------------------------------------------------------------------
# Seeded epa_master rows for the lookups under test.
#
# Under pytest the inventory is an (empty) SQLite database, so the F-150 / BMW
# i4 / Audi Q5 tests below used to hit their mid-test "epa_master has no rows"
# skips on EVERY machine — the narrowing logic they assert was never executed
# anywhere. The fixture seeds the exact row shapes those lookups resolve
# (`lookup_epa_aggregate` / `lookup_epa_by_trim` read epa_master through `?`
# placeholders, so SQLite serves them fine) and the skips are gone.
#
# ``scratch_dictionary_root`` is part of the fixture on purpose:
# ``_lookup_epa_by_trim_uncached`` falls back to the REAL dictionary EPA CSVs
# when the database row is missing, and on a dev machine that fallback would
# answer with different values than the seeded rows — the test must read what
# it seeded, on every machine the same.
# ---------------------------------------------------------------------------

_EPA_MASTER_SEED_ROWS: tuple[tuple, ...] = (
    # Dealer "F-150" resolves via the "F150 Pickup%" LIKE pattern, never exact.
    (2018, "Ford", "F150 Pickup 2WD", "XLT", 6, 3.5, "Automatic",
     "Rear-Wheel Drive", "Regular Gasoline", 18, 25, None, None, None),
    (2018, "Ford", "F150 Pickup 4WD", "XLT", 6, 3.5, "Automatic",
     "4-Wheel Drive", "Regular Gasoline", 17, 23, None, None, None),
    # BEV: city08/highway08 hold MPGe; city_e/highway_e hold kWh/100mi. The i4
    # test asserts the display never shows the 31/33 kWh figures as MPGe.
    (2023, "BMW", "i4 eDrive40 Gran Coupe", "eDrive40", None, None,
     "Automatic (A1)", "Rear-Wheel Drive", "Electricity", 109, 108, 31.0, 33.0, "EV"),
    (2022, "Audi", "Q5", "Premium Plus 45 TFSI quattro", 4, 2.0,
     "Automatic (S7)", "All-Wheel Drive", "Premium Gasoline", 23, 28, None, None, None),
)


@pytest.fixture
def seeded_epa_master(sqlite_inventory, scratch_dictionary_root):
    conn = sqlite3.connect(str(sqlite_inventory.path))
    try:
        existing = {row[1] for row in conn.execute("PRAGMA table_info(epa_master)")}
        for col in ("trim", "body_style", "engine_description"):
            if col not in existing:
                conn.execute(f"ALTER TABLE epa_master ADD COLUMN {col} TEXT")
        conn.executemany(
            "INSERT INTO epa_master (year, make, model, trim, cylinders,"
            " displacement, trany, drive, fuel_type, city08, highway08,"
            " city_e, highway_e, atv_type) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            _EPA_MASTER_SEED_ROWS,
        )
        conn.commit()
    finally:
        conn.close()
    clear_epa_aggregate_lookup_cache()
    clear_epa_trim_lookup_cache()
    try:
        yield sqlite_inventory
    finally:
        # Results memoised against the seeded rows must not outlive them.
        clear_epa_aggregate_lookup_cache()
        clear_epa_trim_lookup_cache()


@pytest.mark.parametrize(
    "raw",
    ["--", "—", "-", "N/A", "na", "  ", "unknown", "tbd"],
)
def test_is_na_spec_placeholders(raw: str) -> None:
    assert _is_na_spec(raw) is True


def test_is_na_spec_real_values() -> None:
    assert _is_na_spec("AWD") is False
    assert _is_na_spec("8-Speed Automatic") is False
    assert _is_na_spec(4) is False


def test_is_na_spec_treats_false_as_placeholder() -> None:
    assert _is_na_spec(False) is True
    assert _is_na_spec(True) is False


def test_bmw_40i_suffix_is_case_insensitive() -> None:
    assert _bmw_has_40i_suffix("xDrive40i Sedan")
    assert _bmw_has_40i_suffix("m340i xdrive")


def test_lookup_epa_ford_f150_hyphen_model_matches_pickup_rows(seeded_epa_master) -> None:
    """Dealer ``F-150`` must resolve to EPA ``F150 Pickup *`` rows (transmission + trany formatting)."""
    epa = lookup_epa_aggregate(2018, "Ford", "F-150")
    assert epa.get("transmission"), "seeded F150 Pickup rows must resolve via the LIKE pattern"
    vs = merge_verified_specs(
        {
            "make": "Ford",
            "model": "F-150",
            "year": 2018,
            "trim": "XLT",
            "title": "Used 2018 Ford F-150 XLT",
            "transmission": None,
            "drivetrain": "4WD",
            "cylinders": 6,
            "fuel_type": "Gasoline",
            "body_style": None,
        }
    )
    assert vs.get("transmission_display")
    assert vs.get("transmission_display") == "Automatic"


def test_decode_trim_ford_ten_speed_from_title() -> None:
    hints = decode_trim_logic("Ford", "F-150", "Lariat", "2020 Ford F-150 Lariat Electronic Ten-Speed Automatic")
    assert hints.get("transmission_hint") == "10-Speed Automatic"
    assert hints.get("gears") == 10


def test_merge_verified_specs_does_not_use_double_dash_as_drivetrain() -> None:
    car = {
        "make": "Toyota",
        "model": "Camry",
        "year": 2020,
        "trim": "LE",
        "title": "Used 2020 Toyota Camry LE",
        "drivetrain": "--",
        "transmission": "--",
        "cylinders": None,
        "fuel_type": None,
        "body_style": None,
    }
    vs = merge_verified_specs(car)
    assert vs.get("drivetrain") != "--"
    assert vs.get("transmission_display") != "--"
    td = vs.get("transmission_display")
    if isinstance(td, str):
        assert td.strip().lower() not in ("--", "-", "n/a", "na")


def test_prepare_car_detail_package_evidence_when_no_features() -> None:
    car = {
        "gallery": [],
        "packages": json.dumps(
            {
                "packages_normalized": [
                    {
                        "name": "Luxury Group",
                        "features": [],
                        "evidence_spans": ["Leather seats", "Premium audio"],
                    }
                ],
            }
        ),
    }
    ctx = prepare_car_detail_context(car)
    sections = ctx.get("listing_packages_sections") or []
    assert len(sections) == 1
    assert sections[0]["evidence"] == ["Leather seats", "Premium audio"]


def test_prepare_car_detail_packages_uses_name_fallbacks_and_extras() -> None:
    """Car detail Packages panel: canonical_name / vision lists / standalone features."""
    car = {
        "gallery": [],
        "packages": json.dumps(
            {
                "packages_normalized": [
                    {
                        "name": "",
                        "canonical_name": "Cold Weather Package",
                        "name_verbatim": "Winter Pkg",
                        "features": ["Heated seats"],
                    }
                ],
                "standalone_features_from_description": ["Panoramic roof"],
                "possible_packages": ["Tech Package"],
                "observed_features": ["Roof rails"],
            }
        ),
    }
    ctx = prepare_car_detail_context(car)
    sections = ctx.get("listing_packages_sections") or []
    titles = [s["name"] for s in sections]
    assert "Cold Weather Package" in titles
    assert "Tech Package" not in titles
    assert ctx.get("listing_possible_packages") == []
    assert ctx.get("listing_standalone_features") == ["Panoramic roof"]
    assert ctx.get("listing_observed_features") == []
    assert ctx.get("packages_panel_has_content") is True


def test_prepare_car_detail_hides_photo_analysis_for_jeep() -> None:
    car = {
        "vin": "1C4JJXSJ5MW755034",
        "make": "Jeep",
        "gallery": [],
        "packages": json.dumps(
            {
                "possible_packages": ["Rubicon Package"],
                "observed_features": ["Hood scoop"],
                "llava_interior_cabin": {
                    "interior_guess_text": "Black leather",
                    "interior_buckets": ["black"],
                },
            }
        ),
    }
    ctx = prepare_car_detail_context(car)
    assert ctx.get("hide_photo_analysis") is True
    assert ctx.get("listing_possible_packages") == []
    assert ctx.get("listing_observed_features") == []
    assert ctx.get("listing_photo_detected_equipment") == []
    assert ctx.get("llava_interior_section") is None


def test_prepare_car_detail_llava_interior_section() -> None:
    car = {
        "gallery": [],
        "packages": json.dumps(
            {
                "llava_interior_cabin": {
                    "interior_guess_text": "Black cabin",
                    "interior_buckets": ["black"],
                    "evidence": "dash photo",
                    "confidence": 0.77,
                }
            }
        ),
        "spec_source_json": json.dumps({"interior_cabin_vision": {"source": "llava_vision"}}),
    }
    ctx = prepare_car_detail_context(car)
    sec = ctx.get("llava_interior_section")
    assert isinstance(sec, dict)
    assert sec.get("guess") == "Black cabin"
    assert sec.get("buckets") == ["black"]
    assert ctx.get("interior_from_llava_vision") is True
    assert ctx.get("packages_panel_has_content") is True


def test_prepare_car_detail_packages_empty_state() -> None:
    car = {"gallery": [], "packages": None, "spec_source_json": None}
    ctx = prepare_car_detail_context(car)
    assert ctx.get("packages_panel_has_content") is False


def test_lookup_epa_bmw_i4_short_model_and_edrive40_narrowing(seeded_epa_master) -> None:
    """Dealer model ``i4`` + eDrive40 trim must resolve to EPA city08/08 (MPGe), not kWh/100mi."""
    epa = lookup_epa_aggregate(
        2023,
        "BMW",
        "i4",
        title="2023 BMW i4 eDrive40",
        trim="eDrive40",
    )
    c8, h8 = epa.get("city08"), epa.get("highway08")
    assert c8 is not None and h8 is not None, "seeded i4 eDrive40 rows must narrow"
    assert c8 > 50 and h8 > 50
    assert c8 < 200 and h8 < 200
    vs = merge_verified_specs(
        {
            "make": "BMW",
            "model": "i4",
            "year": 2023,
            "trim": "eDrive40",
            "title": "2023 BMW i4 eDrive40",
            "fuel_type": "Electric",
            "cylinders": 0,
            "drivetrain": "RWD",
            "transmission": "Single-speed automatic",
            "mpg_city": None,
            "mpg_highway": None,
        }
    )
    fe = vs.get("fuel_economy_display") or ""
    assert "MPGe" in fe
    assert "33" not in fe  # not kWh/100mi masquerading as MPGe


def test_bmw_x2_xdrive28i_fuel_economy_from_epa_dictionary() -> None:
    epa = lookup_epa_by_trim(2020, "BMW", "X2", "xDrive28i")
    assert epa.get("city08") == 24
    assert epa.get("highway08") == 31
    vs = merge_verified_specs(
        {
            "make": "BMW",
            "model": "X2",
            "year": 2020,
            "trim": "xDrive28i",
            "title": "2020 BMW X2 xDrive28i",
            "fuel_type": "Gasoline",
            "cylinders": 4,
            "drivetrain": "AWD",
            "transmission": "8-Speed Automatic",
            "mpg_city": None,
            "mpg_highway": None,
        }
    )
    assert vs.get("fuel_economy_display") == "24 City / 31 Hwy"


def test_prepare_car_detail_monroney_option_lists() -> None:
    car = {
        "gallery": [],
        "packages": json.dumps(
            {
                "monroney_options": ["M Sport Package", "Panoramic roof"],
                "monroney_standard_highlights": ["xDrive AWD", "SensaTec"],
            }
        ),
    }
    ctx = prepare_car_detail_context(car)
    assert ctx.get("listing_monroney_options") == ["M Sport Package", "Panoramic roof"]
    assert ctx.get("listing_monroney_standard") == ["xDrive AWD", "SensaTec"]
    assert ctx.get("packages_panel_has_content") is True


def test_model_epa_fallbacks_audi_q6_etron() -> None:
    fallbacks = _model_epa_fallbacks("Audi", "Q6 e-tron quattro")
    assert "Q6 e-tron" in fallbacks
    assert "Q6" in fallbacks


def test_model_epa_fallbacks_audi_a8_l() -> None:
    fallbacks = _model_epa_fallbacks("Audi", "A8 L")
    assert "A8 L" in fallbacks
    assert "A8" in fallbacks


def test_lookup_epa_audi_q5_premium_plus_trim(seeded_epa_master) -> None:
    epa = lookup_epa_by_trim(2022, "Audi", "Q5", "Premium Plus 45 TFSI quattro")
    assert epa.get("city08"), "seeded Q5 Premium Plus row must match by exact trim"
    assert epa.get("city08") > 0
    assert epa.get("highway08") > 0


# --- VIN decode outranks the stored cylinders column -------------------------


def _silverado(cyl_column: int) -> dict:
    return {
        "vin": "1GCPKWEK3TZ441126",
        "make": "Chevrolet",
        "model": "Silverado 1500",
        "year": 2026,
        "trim": "RST",
        "title": "2026 Chevrolet Silverado 1500 RST",
        "cylinders": cyl_column,
        "engine_l": None,
        "engine_description": "2.7L I4 L3B Turbo",
        "drivetrain": "4WD",
        "transmission": "Automatic",
        "fuel_type": "Gasoline",
        "body_style": "Truck",
    }


def test_vpic_cylinders_outrank_stored_column(monkeypatch) -> None:
    import backend.enrichment.knowledge_engine as ke

    monkeypatch.setattr(ke, "lookup_vpic_from_cache", lambda vin: {"cylinders": 4, "engine_l": 2.7})
    vs = merge_verified_specs(_silverado(cyl_column=8))
    assert vs["cylinders_display"] == 4
    assert vs["cylinders_verified"] is True


def test_dealer_engine_text_outranks_stored_column_without_vpic(monkeypatch) -> None:
    import backend.enrichment.knowledge_engine as ke

    monkeypatch.setattr(ke, "lookup_vpic_from_cache", lambda vin: {})
    vs = merge_verified_specs(_silverado(cyl_column=8))
    # "2.7L I4" on the listing beats an enrichment-written 8 in the column.
    assert vs["cylinders_display"] == 4


def test_vpic_that_contradicts_engine_text_is_ignored(monkeypatch) -> None:
    import backend.enrichment.knowledge_engine as ke

    # vPIC warns its decoded model year can be wrong; a decode that names a
    # layout the listing's own text rules out must not win.
    monkeypatch.setattr(ke, "lookup_vpic_from_cache", lambda vin: {"cylinders": 8})
    vs = merge_verified_specs(_silverado(cyl_column=4))
    assert vs["cylinders_display"] == 4


def test_linked_catalog_row_that_contradicts_engine_text_is_rejected(monkeypatch) -> None:
    import backend.enrichment.knowledge_engine as ke

    monkeypatch.setattr(ke, "lookup_vpic_from_cache", lambda vin: {})
    monkeypatch.setattr(
        ke, "lookup_epa_master_by_id",
        lambda mid: {"cylinders": 8, "displacement": 5.3, "fuel_type": "Regular Gasoline"} if mid == 77 else {},
    )
    car = _silverado(cyl_column=None)
    car["epa_master_id"] = 77
    vs = merge_verified_specs(car)
    assert vs["catalog_link_rejected"].startswith("displacement 2.7L vs catalog 5.3L")
    assert vs["cylinders_display"] == 4


def test_vpic_normalization_reports_electrification():
    from backend.enrichment.knowledge_engine import _normalize_vpic_response

    assert _normalize_vpic_response({"FuelTypePrimary": "Gasoline", "ElectrificationLevel": "Strong HEV (Hybrid Electric Vehicle)"})["electrification"] == "hybrid"
    assert _normalize_vpic_response({"FuelTypePrimary": "Gasoline", "ElectrificationLevel": "PHEV (Plug-in Hybrid Electric Vehicle)"})["electrification"] == "phev"
    assert _normalize_vpic_response({"FuelTypePrimary": "Electric", "ElectrificationLevel": "BEV (Battery Electric Vehicle)"})["electrification"] == "ev"
    assert _normalize_vpic_response({"FuelTypePrimary": "Gasoline", "ElectrificationLevel": "Mild HEV (Hybrid Electric Vehicle) - Level 2"})["electrification"] is None
    assert _normalize_vpic_response({"FuelTypePrimary": "Gasoline", "ElectrificationLevel": ""})["electrification"] is None


def test_vpic_4x2_is_not_rear_wheel_drive():
    """vPIC 'DriveType: 4x2' means two-wheel drive of either end; on FWD Camrys it
    read as RWD and the resolver then linked 316 of 1,611 lab rows to AWD rows."""
    from backend.enrichment.knowledge_engine import _normalize_vpic_response

    assert _normalize_vpic_response({"DriveType": "4x2"})["drivetrain"] is None
    assert _normalize_vpic_response({"DriveType": "4x2/2-Wheel Drive"})["drivetrain"] is None
    assert _normalize_vpic_response({"DriveType": "RWD/Rear-Wheel Drive"})["drivetrain"] == "RWD"
    assert _normalize_vpic_response({"DriveType": "4WD/4-Wheel Drive/4x4"})["drivetrain"] == "4WD"
