"""Flat snake_case vehicle lists (honestcardeal-com Supabase public-inventory, 2026-09-26)."""
from __future__ import annotations

from backend.parsers import parse

_ITEM = {"id": "b5a7", "vin": "YV4902DZ6E2553580", "year": 2014, "make": "Volvo", "model": "XC60", "trim": "R-Design Premier Plus", "body_style": "SUV",
         "engine": "6 B6304T4", "transmission": "Automatic", "drivetrain": "AWD", "exterior_color": "Ruby Red Metallic", "interior_color": "Tan",
         "fuel_type": "Gasoline", "mileage": 96806, "asking_price": 9995, "compare_price": None, "description": None, "features": ["Sunroof", "Nav"],
         "stock_number": "553580", "primary_photo_url": None, "photo_urls": ["https://cdn.x/a.jpg", "https://cdn.x/b.jpg"]}


def test_generic_json_maps_snake_case_fields_before_platform_fallbacks():
    payload = {"vehicles": [_ITEM, dict(_ITEM, vin="1HGCV1F30PA000002", asking_price=None, price=21000, condition="new")], "pagination": {"total": 2}}
    rows = parse("unknown", payload, base_url="https://honestcardeal.com", dealer_id="honestcardeal-com", dealer_name="Trinity", dealer_url="https://honestcardeal.com", rejected_out=[])
    assert len(rows) == 2
    r = rows[0]
    assert (r["price"], r["trim"], r["exterior_color"], r["interior_color"], r["mileage"], r["stock_number"]) == (9995.0, "R-Design Premier Plus", "Ruby Red Metallic", "Tan", 96806, "553580")
    assert r["image_url"] == "https://cdn.x/a.jpg" and r["gallery"] == ["https://cdn.x/a.jpg", "https://cdn.x/b.jpg"] and r["condition"] == "Used"
    assert r["engine_description"] == "6 B6304T4" and r["drivetrain"] == "AWD" and r["features"] == ["Sunroof", "Nav"]
    assert rows[1]["price"] == 21000.0 and rows[1]["condition"] == "New"


def test_generic_json_ignores_non_vehicle_lists():
    assert parse("unknown", {"items": [{"id": 1, "name": "x"}, {"id": 2, "name": "y"}]}, base_url="https://x.com", dealer_id="x-com", rejected_out=[]) == []
