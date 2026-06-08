"""Inventory listing card location helpers."""

from __future__ import annotations

from backend.scanner.inventory_card_location import (
    INVENTORY_CARD_LOCATION_JS,
    apply_card_locations_to_vehicles,
)


def test_inventory_card_location_js_has_vin_and_location_patterns():
    assert "data-vin" in INVENTORY_CARD_LOCATION_JS
    assert "location" in INVENTORY_CARD_LOCATION_JS.lower()
    assert "watermark" in INVENTORY_CARD_LOCATION_JS.lower()


def test_apply_card_locations_to_vehicles():
    vehicles = [
        {"vin": "1" * 17, "_lot_location": ""},
        {"vin": "2" * 17},
    ]
    locs = {"1" * 17: "Parks Luxury of Roanoke"}
    n = apply_card_locations_to_vehicles(vehicles, locs)
    assert n == 1
    assert vehicles[0]["_lot_location"] == "Parks Luxury of Roanoke"
