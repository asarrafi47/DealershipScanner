"""dealer_on (CDK / dealer.on) field extraction."""

from __future__ import annotations

from backend.parsers import dealer_on


def _ford_like_inventory(*vehicles: dict) -> dict:
    return {"inventory": list(vehicles)}


def test_dealer_on_ford_like_colors_engine_mpg() -> None:
    v = {
        "vin": "1FTEW1EP5LFA12345",
        "year": 2020,
        "make": "Ford",
        "model": "F-150",
        "trim": "Lariat",
        "internetPrice": 42900,
        "mileage": 12000,
        "exteriorColor": "Oxford White",
        "interiorColor": "Medium Earth Gray",
        "engineDescription": "2.7L V6 EcoBoost",
        "fuelType": "Gasoline",
        "condition": "Used",
        "cylinders": 6,
        "mpgCity": 20,
        "mpgHighway": 26,
    }
    payload = _ford_like_inventory(v, dict(v, vin="1FTEW1EP5LFB23456"), dict(v, vin="1FTEW1EP5LFC34567"))
    rows = dealer_on.parse(payload, "https://dealer.example.com", "d1", "Test", "https://dealer.example.com")
    assert len(rows) >= 1
    r = next(x for x in rows if x.get("vin") == "1FTEW1EP5LFA12345")
    assert r.get("exterior_color") == "Oxford White"
    assert r.get("interior_color") == "Medium Earth Gray"
    assert r.get("engine_description") == "2.7L V6 EcoBoost"
    assert r.get("fuel_type") == "Gasoline"
    assert r.get("condition") == "Used"
    assert r.get("cylinders") == 6
    assert r.get("mpg_city") == 20
    assert r.get("mpg_highway") == 26


def test_dealer_on_tracking_attributes_exterior_mpg() -> None:
    v = {
        "vin": "3FMTK3SU6MMA90123",
        "year": 2021,
        "make": "Ford",
        "model": "Mustang Mach-E",
        "internetPrice": 55000,
        "mileage": 5000,
        "trackingAttributes": [
            {"name": "Exterior Paint", "value": "Rapid Red Metallic"},
            {"name": "EPA Est MPG City", "value": "105"},
            {"name": "EPA Est MPG Hwy", "value": "93"},
            {"name": "Engine", "value": "Electric Motor"},
        ],
    }
    payload = _ford_like_inventory(v, dict(v, vin="3FMTK3SU6MMB01234"), dict(v, vin="3FMTK3SU6MMC12345"))
    rows = dealer_on.parse(payload, "https://dealer.example.com", "d1", "Test", "https://dealer.example.com")
    r = rows[0]
    assert r.get("exterior_color") == "Rapid Red Metallic"
    assert r.get("mpg_city") == 105
    assert r.get("mpg_highway") == 93
    assert "Electric" in (r.get("engine_description") or "") or r.get("engine_description") == "Electric Motor"


def test_dealer_on_fuel_economy_nested() -> None:
    v = {
        "vin": "1FM5K8GC5LGA11111",
        "year": 2020,
        "make": "Ford",
        "model": "Explorer",
        "internetPrice": 38000,
        "mileage": 30000,
        "fuelEconomy": {"city": 21, "highway": "28"},
    }
    payload = _ford_like_inventory(v, dict(v, vin="1FM5K8GC5LGB22222"), dict(v, vin="1FM5K8GC5LGC33333"))
    rows = dealer_on.parse(payload, "https://dealer.example.com", "d1", "Test", "https://dealer.example.com")
    r = rows[0]
    assert r.get("mpg_city") == 21
    assert r.get("mpg_highway") == 28
