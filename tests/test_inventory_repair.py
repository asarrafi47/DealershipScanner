"""inventory_repair helpers."""

from __future__ import annotations

from backend.utils.field_clean import clean_car_row_dict
from backend.utils.inventory_repair import collect_cleaned_field_updates, collect_row_storage_repairs


def test_collect_cleaned_field_updates_nulls_double_dash() -> None:
    raw = {
        "id": 1,
        "vin": "1HGBH41JXMN109185",
        "drivetrain": "--",
        "transmission": "—",
        "year": 2020,
        "make": "Honda",
        "model": "Accord",
    }
    u = collect_cleaned_field_updates(raw)
    assert u.get("drivetrain") is None
    assert u.get("transmission") is None


def test_clean_car_row_dict_strips_booleanish_condition() -> None:
    raw = {"vin": "X", "condition": "0"}
    c = clean_car_row_dict(raw)
    assert c.get("condition") is None


def test_collect_row_adds_condition_when_missing() -> None:
    raw = {
        "id": 2,
        "vin": "2HGCM82633A004352",
        "year": 2021,
        "make": "Honda",
        "model": "Civic",
        "title": "2021 Honda Civic Sport",
        "condition": None,
        "mileage": None,
        "is_cpo": 0,
        "source_url": "https://dealer.example.com/used-inventory/",
        "drivetrain": None,
        "transmission": None,
    }
    u = collect_row_storage_repairs(raw)
    assert u.get("condition") == "Pre-Owned"
