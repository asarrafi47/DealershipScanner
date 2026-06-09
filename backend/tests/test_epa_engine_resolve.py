"""EPA dictionary engine resolution for conflicting displacement / cylinder data."""

from __future__ import annotations

from backend.dictionary.epa_engine import (
    format_epa_engine_display,
    pick_epa_engine_row,
    resolve_engine_display_from_epa,
)
from backend.dictionary.enrich_from_dictionary import _load_epa_csv, enrich_car
from backend.utils.car_serialize import build_engine_display


def test_resolve_audi_a6_four_cylinder_not_3_0_i4() -> None:
    car = {
        "year": 2017,
        "make": "Audi",
        "model": "A6",
        "trim": "Premium Plus",
        "title": "2017 Audi A6 Premium Plus Sedan TFSI four-cylinder engine",
        "drivetrain": "AWD",
        "engine_l": 3.0,
        "cylinders": 4,
        "engine_description": "3.0L",
        "fuel_type": "Gasoline",
    }
    assert build_engine_display(car, {}) == "2.0L I4 Turbo"


def test_resolve_audi_a6_v6_when_title_signals_v6() -> None:
    car = {
        "year": 2017,
        "make": "Audi",
        "model": "A6",
        "trim": "Premium Plus",
        "title": "2017 Audi A6 Premium Plus 3.0L V6 quattro",
        "drivetrain": "AWD",
        "engine_l": 3.0,
        "cylinders": 6,
        "fuel_type": "Gasoline",
    }
    assert build_engine_display(car, {}) == "3.0L V6 Turbo"


def test_pick_epa_engine_row_prefers_four_cylinder_title() -> None:
    rows = _load_epa_csv(2017, "Audi", "A6")
    assert len(rows) >= 2
    car = {
        "title": "2017 Audi A6 Premium Plus Sedan TFSI four-cylinder engine",
        "trim": "Premium Plus",
        "drivetrain": "AWD",
        "engine_l": 3.0,
        "cylinders": 4,
    }
    picked = pick_epa_engine_row(rows, car, car_lit=3.0, car_cyl=4)
    assert picked is not None
    assert format_epa_engine_display(picked) == "2.0L I4 Turbo"


def test_enrich_car_corrects_conflicting_audi_engine_fields() -> None:
    car = {
        "year": 2017,
        "make": "Audi",
        "model": "A6",
        "trim": "Premium Plus",
        "title": "2017 Audi A6 Premium Plus Sedan TFSI four-cylinder engine",
        "drivetrain": "AWD",
        "engine_l": 3.0,
        "cylinders": 4,
        "engine_description": "3.0L",
        "transmission": "8-Speed Automatic",
    }
    updates = enrich_car(car, use_vpic=False)
    assert updates.get("engine_l") == 2.0
    assert updates.get("cylinders") == 4
    assert "2.0L I4" in (updates.get("engine_description") or "")


def test_format_epa_ram_2500_cummins_row_is_i6() -> None:
    from backend.dictionary.epa_engine import format_epa_engine_display
    from backend.dictionary.enrich_from_dictionary import _load_epa_csv

    rows = _load_epa_csv(2018, "Ram", "2500")
    row = next(r for r in rows if "Cummins" in (r.get("engineOptions") or ""))
    assert format_epa_engine_display(row) == "6.7L I6 Turbo"


def test_resolve_engine_display_skips_when_stored_values_are_consistent() -> None:
    car = {
        "year": 2017,
        "make": "Audi",
        "model": "A6",
        "trim": "Premium Plus",
        "title": "2017 Audi A6 Premium Plus 3.0L V6 quattro",
        "engine_l": 3.0,
        "cylinders": 6,
    }
    assert resolve_engine_display_from_epa(car, {}) is None
