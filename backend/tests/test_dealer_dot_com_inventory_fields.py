"""Dealer.com inventory JSON: description and Carfax without VDP."""

from __future__ import annotations

from backend.parsers import dealer_dot_com


def test_inventory_extended_description():
    obj = {
        "vin": "WBA43DA00SCU34030",
        "year": 2025,
        "make": "BMW",
        "model": "X5",
        "internetPrice": 75000,
        "extendedDescription": "Irvine BMW proudly presents this exclusive service loaner with premium features.",
    }
    row = dealer_dot_com._map_vehicle(obj, "https://www.irvinebmw.com", "irvinebmw-com", "Irvine BMW", "https://www.irvinebmw.com")
    assert row is not None
    assert "Irvine BMW proudly presents" in (row.get("description") or "")


def test_inventory_carfax_from_token():
    obj = {
        "vin": "1HGBH41JXMN109186",
        "year": 2021,
        "make": "Honda",
        "model": "Accord",
        "carfax_token": "abc123",
    }
    row = dealer_dot_com._map_vehicle(obj, "https://dealer.example.com", "d1", "Test", "https://dealer.example.com")
    assert row is not None
    assert row.get("carfax_url") == "https://vhr.carfax.com/main?vin=1HGBH41JXMN109186"


def test_inventory_carfax_from_badge_signal():
    obj = {
        "vin": "1HGBH41JXMN109186",
        "year": 2021,
        "make": "Honda",
        "model": "Accord",
        "callout": ["Carfax 1-Owner"],
    }
    row = dealer_dot_com._map_vehicle(obj, "https://dealer.example.com", "d1", "Test", "https://dealer.example.com")
    assert row is not None
    assert row.get("carfax_url") == "https://vhr.carfax.com/main?vin=1HGBH41JXMN109186"
