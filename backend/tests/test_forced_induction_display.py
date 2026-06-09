"""Forced induction suffixes on buyer-facing engine display."""

from __future__ import annotations

from backend.dictionary.epa_engine import catalog_engine_fields, format_epa_engine_display
from backend.dictionary.enrich_from_dictionary import _load_epa_csv
from backend.utils.car_serialize import build_engine_display
from backend.utils.forced_induction import (
    apply_forced_induction_to_engine_display,
    classify_forced_induction_from_car_row,
    forced_induction_short_suffix,
)


def test_bmw_330i_engine_display_includes_turbo() -> None:
    car = {
        "year": 2017,
        "make": "BMW",
        "model": "3 Series",
        "trim": "330i",
        "engine_l": 2.0,
        "cylinders": 4,
        "fuel_type": "Gasoline",
    }
    assert classify_forced_induction_from_car_row(car) == "Turbocharged"
    assert build_engine_display(car, {}) == "2.0L I4 Turbo"


def test_epa_dictionary_row_bmw_330i_includes_turbo() -> None:
    rows = _load_epa_csv(2017, "BMW", "3 Series")
    row = next(r for r in rows if (r.get("Trim") or "").strip() == "330i")
    assert format_epa_engine_display(row) == "2.0L I4 Turbo"


def test_dodge_hellcat_supercharged_suffix() -> None:
    car = {
        "make": "Dodge",
        "model": "Challenger",
        "trim": "SRT Hellcat",
        "engine_l": 6.2,
        "cylinders": 8,
    }
    assert classify_forced_induction_from_car_row(car) == "Supercharged"
    assert (
        apply_forced_induction_to_engine_display("6.2L V8", car) == "6.2L V8 Supercharged"
    )


def test_forced_induction_short_suffix_mapping() -> None:
    assert forced_induction_short_suffix("Twin Turbocharged") == "Twin Turbo"
    assert forced_induction_short_suffix("Turbocharged") == "Turbo"
    assert forced_induction_short_suffix("Supercharged") == "Supercharged"


def test_bmw_x5_xdrive40i_is_3_0_i6_turbo() -> None:
    rows = _load_epa_csv(2024, "BMW", "X5")
    row = next(r for r in rows if (r.get("Trim") or "").strip() == "xDrive40i")
    assert format_epa_engine_display(row) == "3.0L I6 Turbo"
    assert catalog_engine_fields(row)["forcedInduction"] == "Turbocharged"


def test_enrich_car_engine_from_dictionary_overwrites() -> None:
    from backend.dictionary.epa_engine import enrich_car_engine_from_dictionary

    car = {
        "year": 2017,
        "make": "BMW",
        "model": "3 Series",
        "trim": "330i",
        "engine_l": None,
        "cylinders": None,
        "engine_description": None,
        "forced_induction": None,
    }
    updates = enrich_car_engine_from_dictionary(car, overwrite=True)
    assert updates.get("engine_l") == 2.0
    assert updates.get("cylinders") == 4
    assert updates.get("forced_induction") == "Turbocharged"


def test_tiguan_2_0t_sel_trim() -> None:
    car = {
        "year": 2018,
        "make": "Volkswagen",
        "model": "Tiguan",
        "trim": "2.0T SEL",
        "title": "2018 Volkswagen Tiguan 2.0T SEL",
        "engine_l": 2.0,
        "cylinders": 4,
    }
    assert build_engine_display(car, {}) == "2.0L I4 Turbo"


def test_hyundai_santa_fe_sport_2_0t_trim() -> None:
    car = {
        "make": "Hyundai",
        "model": "Santa Fe Sport",
        "trim": "2.0T",
        "title": "2014 Hyundai Santa Fe Sport 2.0T",
        "engine_l": 2.0,
        "cylinders": 4,
    }
    assert build_engine_display(car, {}) == "2.0L I4 Turbo"


def test_infiniti_q60_3_0t_twin_turbo() -> None:
    car = {
        "make": "INFINITI",
        "model": "Q60",
        "trim": "3.0t LUXE",
        "title": "2018 INFINITI Q60 3.0t LUXE",
        "engine_l": 3.0,
        "cylinders": 6,
    }
    assert build_engine_display(car, {}) == "3.0L V6 Twin Turbo"

