"""Inventory JSON MPG extraction."""

from __future__ import annotations

from backend.parsers.dealer_dot_com import _map_vehicle
from backend.parsers.inventory_mpg import pick_mpg_city, pick_mpg_highway


def test_pick_mpg_from_dealer_com_keys():
    obj = {
        "vin": "5UX53GP08T9426805",
        "cityFuelEconomy": 27,
        "highwayFuelEconomy": 33,
    }
    assert pick_mpg_city(obj) == 27
    assert pick_mpg_highway(obj) == 33


def test_map_vehicle_sets_mpg():
    obj = {
        "vin": "5UX53GP08T9426805",
        "year": 2026,
        "make": "BMW",
        "model": "X3",
        "cityFuelEconomy": 27,
        "highwayFuelEconomy": 33,
        "internetPrice": 55000,
    }
    row = _map_vehicle(obj, "https://www.irvinebmw.com", "irvinebmw-com", "Irvine BMW", "https://www.irvinebmw.com")
    assert row is not None
    assert row.get("mpg_city") == 27
    assert row.get("mpg_highway") == 33
