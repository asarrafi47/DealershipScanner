"""Dealership map payload for car detail."""

from __future__ import annotations

from backend.listings.dealer_map import build_dealer_map_for_car


def test_build_dealer_map_from_registry_coords() -> None:
    car = {"dealer_name": "Lot A", "dealer_url": "https://example.com", "zip_code": "90210"}
    dealer_info = {
        "name": "Tony Chevrolet",
        "street_address": "1 Main St",
        "city": "Hilo",
        "state": "HI",
        "zip_code": "96720",
        "latitude": 19.72,
        "longitude": -155.09,
    }
    m = build_dealer_map_for_car(car, dealer_info)
    assert m is not None
    assert m["has_pin"] is True
    assert m["lat"] == 19.72
    assert m["lon"] == -155.09
    assert "google.com/maps" in m["google_maps_url"]
    assert "query=" in m["google_maps_url"]
    assert "1 Main St" in m["address_line"]


def test_build_dealer_map_zip_fallback(monkeypatch) -> None:
    monkeypatch.setattr(
        "backend.listings.dealer_map._coords_from_dealer_url",
        lambda _u: None,
    )
    monkeypatch.setattr(
        "backend.listings.dealer_map._coords_from_registry",
        lambda _r: None,
    )
    monkeypatch.setattr(
        "backend.listings.dealer_map._coords_from_zip",
        lambda z: (34.05, -118.25) if z == "90210" else None,
    )
    m = build_dealer_map_for_car(
        {"dealer_name": "Remote Motors", "zip_code": "90210"},
        None,
    )
    assert m is not None
    assert m["has_pin"] is True
    assert m["lat"] == 34.05


def test_build_dealer_map_address_only() -> None:
    m = build_dealer_map_for_car(
        {"dealer_name": "City Cars"},
        {
            "name": "City Cars",
            "city": "Austin",
            "state": "TX",
            "latitude": None,
            "longitude": None,
        },
    )
    assert m is not None
    assert m["has_pin"] is False
    assert "Austin" in m["address_line"]
    assert "google.com/maps" in m["google_maps_url"]
