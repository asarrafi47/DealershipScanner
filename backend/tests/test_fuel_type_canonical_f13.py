"""F13 (docs/DATA_COMPLETENESS_FINDINGS_2026_09_28.md): non-canonical fuel strings
passed through coerce_fuel_type_stored unchanged ('G' 1,054 rows, 'Direct
Injection' 477, 'Flexible' 145, 'Others' 133, 'UNL' 94, 'H' 73, 'HYB' 64...)."""
from __future__ import annotations

import pytest

from backend.utils.field_clean import clean_car_row_dict, coerce_fuel_type_stored


@pytest.mark.parametrize("raw,want", [
    ("G", "Gasoline"), ("g", "Gasoline"), ("UNL", "Gasoline"), ("Regular Gasoline", "Gasoline"),
    ("Premium Gasoline", "Gasoline"), ("Flexible", "Gasoline"), ("FlexFuel", "Gasoline"), ("Flex-Fuel", "Gasoline"),
    ("FFV", "Gasoline"), ("Regular Unleaded", "Gasoline"),
    ("H", "Hybrid"), ("HYB", "Hybrid"), ("Hybrid", "Hybrid"), ("Gas/Electric Hybrid", "Hybrid"),
    ("D", "Diesel"), ("Diesel Fuel", "Diesel"),
    ("E", "Electric"), ("Electricity", "Electric"), ("Electric", "Electric"),
    ("Plug-In Hybrid", "Plug-In Hybrid"), ("PHEV", "Plug-In Hybrid"),
    ("Hydrogen", "Hydrogen"), ("Hydrogen Fuel Cell", "Hydrogen"),
])
def test_canonical_labels(raw, want):
    assert coerce_fuel_type_stored(raw) == want


@pytest.mark.parametrize("raw", ["Direct Injection", "Port/Direct Injection", "Others", "Other", "", "  ", "N/A", None, "Unknown"])
def test_not_a_fuel_is_none(raw):
    assert coerce_fuel_type_stored(raw) is None


def test_clean_car_row_dict_applies_it():
    assert clean_car_row_dict({"fuel_type": "G"})["fuel_type"] == "Gasoline"
    assert clean_car_row_dict({"fuel_type": "Direct Injection"})["fuel_type"] is None
    assert clean_car_row_dict({"fuel_type": "Hydrogen"})["fuel_type"] == "Hydrogen"
