"""F10 (docs/DATA_COMPLETENESS_FINDINGS_2026_09_28.md): dealer.com and Dealer eProcess
feeds send drivetrain / transmission "Other"; stored verbatim it survived the
upsert's COALESCE(excluded.drivetrain, cars.drivetrain) and overwrote the
vPIC-healed value on every rescan (1,464 rows). Placeholders must become None
before the upsert."""
from __future__ import annotations

import pytest

from backend.parsers import dealer_dot_com
from backend.utils.field_clean import clean_car_row_dict, coerce_drivetrain_stored, is_spec_placeholder


@pytest.mark.parametrize("val", ["Other", "other", "OTHER", "Others", "Unspecified", "N/A", "-", "", "  ", None, "Unknown"])
def test_placeholders_recognised(val):
    assert is_spec_placeholder(val)
    assert coerce_drivetrain_stored(val) is None


@pytest.mark.parametrize("val,want", [("AWD", "AWD"), ("Front-Wheel Drive", "FWD"), ("4WD", "4WD"), ("xDrive", "AWD")])
def test_real_drivetrains_untouched(val, want):
    assert not is_spec_placeholder(val)
    assert coerce_drivetrain_stored(val) == want


def test_clean_car_row_dict_nulls_placeholders_for_all_parsers():
    row = clean_car_row_dict({"vin": "1C4RJHBG5S8712345", "drivetrain": "Other", "transmission": "Unspecified",
                              "fuel_type": "Gasoline", "exterior_color": "Other"})
    assert row["drivetrain"] is None and row["transmission"] is None
    assert row["fuel_type"] == "Gasoline"
    assert row["exterior_color"] == "Other"  # only spec fields are affected


def test_dealer_dot_com_other_is_absent_not_a_label():
    assert dealer_dot_com._opt_label_str("Other") is None
    assert dealer_dot_com._opt_label_str({"label": "Other"}) is None
    assert dealer_dot_com._opt_label_str("8-Speed Automatic") == "8-Speed Automatic"
    row = dealer_dot_com._map_vehicle(
        {"vin": "5TFLA5DB2TX254221", "year": 2026, "make": "Toyota", "model": "Tacoma", "internetPrice": 45000,
         "drivetrain": "Other", "transmission": "other"},
        "https://d.example", "d1", "D", "https://d.example")
    assert row["drivetrain"] is None and row["transmission"] is None
