"""URL fields in serialize_car_for_api must never become display dashes."""

from __future__ import annotations

from backend.utils.car_serialize import serialize_car_for_api


def test_image_url_em_dash_becomes_null_not_display_dash() -> None:
    row = {
        "vin": "1HGBH41JXMN109185",
        "year": 2019,
        "make": "Honda",
        "model": "Accord",
        "image_url": "—",
        "source_url": "—",
        "dealer_url": "https://dealer.example.com/",
    }
    out = serialize_car_for_api(row, include_verified=False)
    assert out.get("image_url") is None
    assert out.get("source_url") is None
    assert out.get("dealer_url") == "https://dealer.example.com/"


def test_model_year_before_2024_pre_owned_when_condition_unknown() -> None:
    row = {
        "vin": "1HGBH41JXMN109185",
        "year": 2021,
        "make": "Honda",
        "model": "Accord",
        "title": "2021 Honda Accord LX",
        "condition": None,
        "mileage": None,
        "is_cpo": 0,
        "source_url": "https://dealer.example.com/used-inventory/index.htm",
    }
    out = serialize_car_for_api(row, include_verified=False)
    assert out.get("condition") == "Pre-Owned"


def test_model_year_before_2024_cpo_title_certified_pre_owned() -> None:
    row = {
        "vin": "1HGBH41JXMN109185",
        "year": 2019,
        "make": "Lexus",
        "model": "ES",
        "title": "2019 Lexus ES 350 CPO",
        "condition": None,
        "mileage": 22000,
        "is_cpo": 0,
        "source_url": "https://x.com/",
    }
    out = serialize_car_for_api(row, include_verified=False)
    assert out.get("condition") == "Certified Pre-Owned"


def test_model_year_2024_not_pre_owned_heuristic() -> None:
    row = {
        "vin": "1HGBH41JXMN109185",
        "year": 2024,
        "make": "Honda",
        "model": "Accord",
        "title": "2024 Honda Accord",
        "condition": None,
        "mileage": None,
        "is_cpo": 0,
        "source_url": "https://x.com/",
    }
    out = serialize_car_for_api(row, include_verified=False)
    assert out.get("condition") in ("—", None) or str(out.get("condition")).strip() in ("", "—")


def test_model_year_2024_used_inferred_from_used_inventory_url() -> None:
    row = {
        "vin": "1HGBH41JXMN109185",
        "year": 2024,
        "make": "Honda",
        "model": "Accord",
        "title": "2024 Honda Accord",
        "condition": None,
        "mileage": None,
        "is_cpo": 0,
        "source_url": "https://dealer.example.com/used-inventory/index.htm",
    }
    out = serialize_car_for_api(row, include_verified=False)
    assert out.get("condition") == "Used"
