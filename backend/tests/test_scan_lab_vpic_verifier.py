"""N6.6 verifier fixes: PHEV bucket, MINI door-only models, vPIC decode errors."""
import pytest

from backend.scripts.scan_lab_report import (
    _catalog_fuel_bucket,
    _vpic_fuel_bucket,
    evaluate_car,
)


@pytest.mark.parametrize(
    "vpic, expected",
    [
        ({"ElectrificationLevel": "PHEV (Plug-in Hybrid Electric Vehicle)", "FuelTypePrimary": "Electric"}, "phev"),
        ({"ElectrificationLevel": "HEV (Hybrid Electric Vehicle)", "FuelTypePrimary": "Gasoline"}, "hybrid"),
        ({"ElectrificationLevel": "Mild HEV", "FuelTypePrimary": "Gasoline"}, ""),
        ({"ElectrificationLevel": "BEV (Battery Electric Vehicle)"}, "ev"),
        ({"FuelTypePrimary": "Diesel"}, "diesel"),
        ({"FuelTypePrimary": "Gasoline"}, ""),
    ],
)
def test_vpic_fuel_bucket(vpic, expected):
    assert _vpic_fuel_bucket(vpic) == expected


@pytest.mark.parametrize(
    "row, expected",
    [
        ({"atv_type": "Plug-in Hybrid"}, "phev"),
        ({"fuel_type": "Regular Gasoline / Electricity"}, "phev"),
        ({"atv_type": "Hybrid"}, "hybrid"),
        ({"atv_type": "EV"}, "ev"),
        ({"fuel_type": "Diesel"}, "diesel"),
    ],
)
def test_catalog_fuel_bucket(row, expected):
    assert _catalog_fuel_bucket(row) == expected


def _car(**kw):
    base = {"vin": "X" * 17, "year": 2025, "make": "Jeep", "model": "Wrangler", "fuel_type": "Plug-in Hybrid",
            "epa_master_id": 1}
    base.update(kw)
    return base


def _disc(car, vpic):
    return evaluate_car(car, vpic, False, catalog_row={})[1]


def test_phev_dealer_label_matches_vpic_phev():
    v = {"Make": "JEEP", "Model": "Wrangler", "ModelYear": "2025", "ErrorCode": "0",
         "ElectrificationLevel": "PHEV", "FuelTypePrimary": "Electric"}
    assert "vpic_fuel" not in _disc(_car(), v)


def test_plain_hybrid_label_vs_vpic_phev_still_flagged():
    v = {"Make": "JEEP", "Model": "Wrangler", "ModelYear": "2025", "ErrorCode": "0",
         "ElectrificationLevel": "PHEV", "FuelTypePrimary": "Electric"}
    assert "vpic_fuel" in _disc(_car(fuel_type="Hybrid"), v)


def test_mini_door_only_model_not_a_discrepancy():
    car = _car(make="MINI", model="Cooper S", fuel_type="Gasoline")
    for m in ("2 Door", "4 Door", "4-Door"):
        v = {"Make": "MINI", "Model": m, "ModelYear": "2025", "ErrorCode": "0"}
        assert "vpic_model" not in _disc(car, v)


def test_non_mini_wrong_model_still_flagged():
    v = {"Make": "JEEP", "Model": "Gladiator", "ModelYear": "2025", "ErrorCode": "0"}
    assert "vpic_model" in _disc(_car(), v)


@pytest.mark.parametrize("code, flagged", [("0", True), ("", True), ("8", False), ("1,8", False), ("0,6", False)])
def test_vpic_error_code_suppresses_discrepancies(code, flagged):
    v = {"Make": "FORD", "Model": "F-150", "ModelYear": "2020", "ErrorCode": code}
    d = _disc(_car(fuel_type="Gasoline"), v)
    assert ("vpic_make" in d) is flagged
