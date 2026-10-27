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
    # P8B.5: the correction applies because the dealer's own title states "four-cylinder"
    # (D-PR3 d); an uncorroborated copy is covered in test_dictionary_enrich_precedence.py.
    updates = enrich_car(car, use_vpic=False)
    assert updates.get("engine_l") == 2.0
    assert updates.get("cylinders", 4) == 4  # already 4: nothing to change
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


def test_resolve_ram_1500_keeps_dealer_3_6_over_family_3_0() -> None:
    # 2026 Ram 1500 offers 3.0L I6, 3.6L V6, 5.7L and 6.2L V8. Dealer text "3.6L V6"
    # names a real option; trim/drivetrain words must not move it to the 3.0L row
    # (25 of a 1,500-car sample rendered the wrong displacement, 2026-09-21).
    car = {
        "year": 2026, "make": "Ram", "model": "1500", "trim": "Express",
        "title": "2026 Ram 1500 Express Pickup", "drivetrain": "2WD",
        "engine_l": 3.6, "cylinders": 6, "engine_description": "3.6L V6", "fuel_type": "Gasoline",
    }
    assert build_engine_display(car, {}).startswith("3.6L V6")


def test_resolve_bronco_keeps_dealer_2_7_over_raptor_3_0() -> None:
    car = {
        "year": 2026, "make": "Ford", "model": "Bronco", "trim": "Badlands",
        "title": "2026 Ford Bronco Badlands", "drivetrain": "4WD",
        "engine_l": 2.7, "cylinders": 6, "engine_description": "2.7L V6 EcoBoost", "fuel_type": "Gasoline",
    }
    assert build_engine_display(car, {}).startswith("2.7L V6")


# --- P10A.4: the catalog row's label is honoured ---------------------------------------------

def _row(**kw):
    base = {"Make": "Acura", "Model": "Integra", "Trim": "Integra", "Year": "2000",
            "engineOptions": "1.8L I4", "cylinders": "4", "displacement": "1.8", "fuelType": "Regular Gasoline"}
    base.update(kw)
    return base


def test_explicit_na_label_never_reaches_the_classifier(monkeypatch) -> None:
    from backend.dictionary.epa_engine import catalog_engine_fields
    from backend.utils import forced_induction as fi

    def boom(_car):
        raise AssertionError("classifier must not run for an explicit label")

    monkeypatch.setattr(fi, "classify_forced_induction_from_car_row", boom)
    out = catalog_engine_fields(_row(forcedInduction="Naturally Aspirated"))
    assert out == {"engineDisplay": "1.8L I4", "forcedInduction": "Naturally Aspirated"}


def test_row_label_drives_display_and_blank_row_still_classifies() -> None:
    from backend.dictionary.epa_engine import catalog_engine_fields

    out = catalog_engine_fields(_row(Make="BMW", Model="3 Series", Trim="330i", Year="2017",
                                     engineOptions="2.0L I4", displacement="2.0", forcedInduction="Turbocharged"))
    assert out["engineDisplay"] == "2.0L I4 Turbo"
    blank = catalog_engine_fields(_row(Make="BMW", Model="3 Series", Trim="330i", Year="2017",
                                       engineOptions="2.0L I4", displacement="2.0"))
    assert blank["forcedInduction"] == "Turbocharged"


def test_row_label_written_only_to_an_empty_car_without_a_marker() -> None:
    from backend.dictionary.epa_engine import engine_updates_from_dictionary_row

    row = _row(forcedInduction="Naturally Aspirated")
    assert engine_updates_from_dictionary_row({"forced_induction": None}, row)["forced_induction"] == "Naturally Aspirated"
    # overwrite does not let the catalog replace a car's own value
    assert "forced_induction" not in engine_updates_from_dictionary_row(
        {"forced_induction": "Turbocharged"}, row, overwrite=True)
    # nor contradict a marker in the car's own strong text
    assert "forced_induction" not in engine_updates_from_dictionary_row(
        {"forced_induction": None, "trim": "Type S Turbo"}, row)
    assert "forced_induction" not in engine_updates_from_dictionary_row(
        {"forced_induction": None, "engine_description": "1.8L I4 Turbocharged"}, row)
    # the free-text description is not strong text
    assert engine_updates_from_dictionary_row(
        {"forced_induction": None, "description": "turbo charged fun"}, row)["forced_induction"] == "Naturally Aspirated"
