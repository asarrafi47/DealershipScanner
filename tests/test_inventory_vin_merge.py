"""Same-VIN merge: gallery + fill empty specs from a richer second payload."""

from __future__ import annotations

from backend.scrapers.inventory_vin_merge import merge_inventory_rows_same_vin


def test_merge_fills_empty_specs_keeps_existing() -> None:
    a = {
        "vin": "1HGBH41JXMN109185",
        "make": "Honda",
        "model": "Accord",
        "exterior_color": "",
        "interior_color": "Black",
        "price": 22000,
        "gallery": ["https://cdn.example.com/a.jpg"],
        "image_url": "https://cdn.example.com/a.jpg",
    }
    b = {
        "vin": "1HGBH41JXMN109185",
        "make": "Honda",
        "model": "Accord",
        "exterior_color": "Platinum White",
        "interior_color": "Gray",
        "engine_description": "1.5L Turbo",
        "carfax_url": "https://vhr.carfax.com/main?vin=1HGBH41JXMN109185",
        "gallery": ["https://cdn.example.com/b.jpg"],
        "image_url": "https://cdn.example.com/b.jpg",
    }
    merge_inventory_rows_same_vin(a, b)
    assert a["exterior_color"] == "Platinum White"
    assert a["interior_color"] == "Black"
    assert a["engine_description"] == "1.5L Turbo"
    assert "carfax_url" in a and a["carfax_url"].startswith("https://")
    assert "a.jpg" in a["gallery"][0] or "b.jpg" in str(a["gallery"])


def test_merge_fills_price_when_missing() -> None:
    a = {"vin": "X", "price": 0, "gallery": []}
    b = {"vin": "X", "price": 19995, "gallery": ["https://x/y.jpg"]}
    merge_inventory_rows_same_vin(a, b)
    assert a["price"] == 19995


def test_merge_fills_year_when_zero_or_negative() -> None:
    a = {"vin": "X", "year": 0, "gallery": []}
    b = {"vin": "X", "year": 2024, "gallery": []}
    merge_inventory_rows_same_vin(a, b)
    assert a["year"] == 2024


def test_merge_keeps_mileage_zero_without_src_none() -> None:
    """0 mileage can mean new — do not overwrite with another row's miles."""
    a = {"vin": "X", "mileage": 0, "gallery": []}
    b = {"vin": "X", "mileage": 12000, "gallery": []}
    merge_inventory_rows_same_vin(a, b)
    assert a["mileage"] == 0
