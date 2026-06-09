"""TCO MPG average and fuel-tank resolution."""

from __future__ import annotations

import pytest

from backend.intelligence.tco_fuel_estimates import (
    average_mpg_city_highway,
    compute_fill_up_cost_usd,
    resolve_fuel_tank_gallons,
    resolve_tco_avg_mpg,
    resolve_tco_ev_efficiency,
)
from backend.utils.car_serialize import serialize_car_for_api


def test_average_mpg_city_highway_mean() -> None:
    assert average_mpg_city_highway(20, 30) == 25.0


def test_average_mpg_city_highway_single_value() -> None:
    assert average_mpg_city_highway(22, None) == 22.0
    assert average_mpg_city_highway(None, 28) == 28.0


def test_resolve_tco_avg_mpg_defaults() -> None:
    assert resolve_tco_avg_mpg({"mpg_city": 24, "mpg_highway": 32}) == 28.0
    assert resolve_tco_avg_mpg({}) == 25.0


def test_resolve_fuel_tank_model_override() -> None:
    gal = resolve_fuel_tank_gallons(
        {"year": 2020, "make": "Toyota", "model": "Camry", "body_style": "Sedan"}
    )
    assert gal == 15.8


def test_resolve_fuel_tank_body_style_pickup() -> None:
    gal = resolve_fuel_tank_gallons(
        {"year": 2015, "make": "Unknown", "model": "Truck", "body_style": "Pickup"}
    )
    assert gal == 26.0


def test_compute_fill_up_cost() -> None:
    assert compute_fill_up_cost_usd(3.5, 15.8) == 55.3


def test_resolve_tco_ev_efficiency_from_mpge() -> None:
    assert resolve_tco_ev_efficiency({"mpg_city": 120, "mpg_highway": 100}) == 30.6


def test_serialize_car_includes_tco_fuel_fields() -> None:
    row = {
        "vin": "1HGBH41JXMN109186",
        "year": 2019,
        "make": "Toyota",
        "model": "Camry",
        "mpg_city": 28,
        "mpg_highway": 39,
        "zip_code": "27513",
    }
    out = serialize_car_for_api(row, include_verified=False)
    assert out["tco_avg_mpg"] == 33.5
    assert out["fuel_tank_gallons"] == 15.8


def test_serialize_ev_includes_tco_ev_efficiency() -> None:
    row = {
        "vin": "5YJ3E1EA1KF123456",
        "year": 2023,
        "make": "Tesla",
        "model": "Model 3",
        "fuel_type": "Electric",
        "mpg_city": 134,
        "mpg_highway": 126,
    }
    out = serialize_car_for_api(row, include_verified=False)
    assert out["tco_ev_efficiency"] == 25.9


def test_serialize_ev_includes_factory_range(monkeypatch: pytest.MonkeyPatch) -> None:
    import backend.intelligence.ev_range_estimates as ev_range

    monkeypatch.setattr(ev_range, "lookup_epa_range_miles", lambda *a, **k: 330)
    row = {
        "vin": "5YJ3E1EA1KF123456",
        "year": 2023,
        "make": "Tesla",
        "model": "Model Y",
        "trim": "Long Range",
        "fuel_type": "Electric",
    }
    out = serialize_car_for_api(row, include_verified=False)
    assert out["factory_range"] == 330
