"""Listings-grid price-drop signal, derived from cars.price_provenance_json."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from backend.utils.car_serialize import serialize_car_for_listings_grid


def _iso(days_ago: float) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=days_ago)).isoformat().replace("+00:00", "Z")


def _base_car(price_provenance_json=None, price=35000) -> dict:
    return {
        "id": 1,
        "make": "Honda",
        "model": "Accord",
        "trim": "Sport",
        "title": "2023 Honda Accord Sport",
        "year": 2023,
        "price": price,
        "mileage": 12000,
        "fuel_type": "Gas",
        "drivetrain": "FWD",
        "price_provenance_json": price_provenance_json,
    }


def test_no_history_means_no_drop() -> None:
    out = serialize_car_for_listings_grid(_base_car())
    assert out["price_drop_amount"] is None
    assert out["price_drop_days_ago"] is None


def test_single_entry_means_no_drop() -> None:
    hist = json.dumps([{"date": _iso(1), "price": 35000}])
    out = serialize_car_for_listings_grid(_base_car(hist))
    assert out["price_drop_amount"] is None


def test_recent_price_drop_detected() -> None:
    hist = json.dumps(
        [
            {"date": _iso(20), "price": 37000},
            {"date": _iso(3), "price": 35000},
        ]
    )
    out = serialize_car_for_listings_grid(_base_car(hist))
    assert out["price_drop_amount"] == 2000
    assert out["price_drop_days_ago"] == 3


def test_price_increase_is_not_a_drop() -> None:
    hist = json.dumps(
        [
            {"date": _iso(20), "price": 33000},
            {"date": _iso(3), "price": 35000},
        ]
    )
    out = serialize_car_for_listings_grid(_base_car(hist))
    assert out["price_drop_amount"] is None
    assert out["price_drop_days_ago"] is None


def test_stale_drop_outside_recent_window_is_hidden() -> None:
    hist = json.dumps(
        [
            {"date": _iso(90), "price": 37000},
            {"date": _iso(60), "price": 35000},
        ]
    )
    out = serialize_car_for_listings_grid(_base_car(hist))
    assert out["price_drop_amount"] is None
    assert out["price_drop_days_ago"] is None


def test_malformed_json_does_not_raise() -> None:
    out = serialize_car_for_listings_grid(_base_car("not-json"))
    assert out["price_drop_amount"] is None
