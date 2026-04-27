"""dealer_on (CDK / dealer.on) field extraction."""

from __future__ import annotations

from backend.parsers import dealer_dot_com, dealer_on


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


def test_dealer_on_homenet_style_selling_internet_price() -> None:
    """Keffer-style payloads: stock + sellingPrice / internet_Price, not Dealer.com nesting."""
    base = {
        "year": 2016,
        "make": "Jeep",
        "model": "Cherokee",
        "stock": "J251107A",
        "sellingPrice": 12000,
        "internet_Price": 12000.0,
        "miles": 111117,
    }
    v1 = dict(base, vin="1C4PJMCS4GW131990")
    v2 = dict(base, vin="1C4PJMCS4GW131991", stock="J251107B")
    v3 = dict(base, vin="1C4PJMCS4GW131992", stock="J251107C")
    payload = _ford_like_inventory(v1, v2, v3)
    rows = dealer_on.parse(payload, "https://www.kefferjeep.com", "k", "Keffer", "https://www.kefferjeep.com")
    by_vin = {r["vin"]: r for r in rows}
    assert by_vin["1C4PJMCS4GW131990"]["price"] == 12000.0
    assert by_vin["1C4PJMCS4GW131990"].get("stock_number") == "J251107A"


def test_dealer_dot_com_parse_homenet_top_level_price() -> None:
    """Mis-tagged CDK inventory parsed as dealer_dot_com should still recover price."""
    vehicles = [
        {"vin": "1C4PJMCS4GW131990", "make": "Jeep", "model": "Cherokee", "year": 2016, "sellingPrice": 12000},
        {"vin": "1C4PJMCS4GW131991", "make": "Jeep", "model": "Cherokee", "year": 2016, "internet_Price": 12100},
        {"vin": "1C4PJMCS4GW131992", "make": "Jeep", "model": "Cherokee", "year": 2016, "sellingPrice": 12200},
    ]
    rows = dealer_dot_com.parse(
        {"inventory": vehicles},
        "https://www.kefferjeep.com",
        "k",
        "Keffer",
        "https://www.kefferjeep.com",
    )
    assert {r["vin"]: r["price"] for r in rows} == {
        "1C4PJMCS4GW131990": 12000,
        "1C4PJMCS4GW131991": 12100,
        "1C4PJMCS4GW131992": 12200,
    }
