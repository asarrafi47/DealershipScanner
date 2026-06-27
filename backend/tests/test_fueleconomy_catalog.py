"""Tests for FuelEconomy.gov matching helpers."""

from __future__ import annotations

from backend.enrichment.fueleconomy_catalog import (
    _parse_fueleconomy_row,
    best_fueleconomy_match,
    build_fueleconomy_index,
    patch_from_fueleconomy,
    score_fueleconomy_match,
)


def test_parse_fueleconomy_row_ev() -> None:
    rec = _parse_fueleconomy_row(
        {
            "id": "47913",
            "year": "2024",
            "make": "Tesla",
            "model": "Model Y Long Range AWD",
            "baseModel": "Model Y",
            "atvType": "EV",
            "cityE": "27.6222",
            "highwayE": "30.0131",
            "city08": "122",
            "highway08": "132",
            "trany": "Automatic (A1)",
            "drive": "All-Wheel Drive",
            "fuelType1": "Electricity",
            "VClass": "Small Sport Utility Vehicle 4WD",
        }
    )
    assert rec is not None
    assert rec.epa_vehicle_id == 47913
    assert rec.atv_type == "EV"
    assert rec.city_e == 27.6222


def test_match_volvo_xc90_trim() -> None:
    fe = _parse_fueleconomy_row(
        {
            "id": "999",
            "year": "2026",
            "make": "Volvo",
            "model": "XC90 B6 AWD",
            "baseModel": "XC90",
            "city08": "20",
            "highway08": "26",
            "trany": "Automatic (S8)",
            "drive": "All-Wheel Drive",
            "fuelType1": "Premium Gasoline",
            "cylinders": "4",
            "displ": "2.0",
        }
    )
    assert fe is not None
    index = build_fueleconomy_index([fe])
    db_row = {
        "year": 2026,
        "make": "Volvo",
        "model": "XC90",
        "trim": "B6 AWD",
        "trany": "Automatic (S8)",
        "drive": "All-Wheel Drive",
        "cylinders": 4,
        "displacement": 2.0,
        "fuel_type": "Premium Gasoline",
    }
    assert score_fueleconomy_match(db_row, fe) >= 18
    hit = best_fueleconomy_match(db_row, index)
    assert hit is not None
    assert hit.epa_vehicle_id == 999


def test_patch_only_fills_nulls() -> None:
    fe = _parse_fueleconomy_row(
        {
            "id": "1",
            "year": "2024",
            "make": "Tesla",
            "model": "Model Y",
            "baseModel": "Model Y",
            "atvType": "EV",
            "cityE": "30",
            "highwayE": "32",
            "fuelType1": "Electricity",
        }
    )
    assert fe is not None
    patch = patch_from_fueleconomy({"city08": 120, "epa_vehicle_id": None}, fe)
    assert patch["epa_vehicle_id"] == 1
    assert patch["atv_type"] == "EV"
    assert "city08" not in patch
