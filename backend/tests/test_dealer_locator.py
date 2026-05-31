"""Dealer locator: center resolution and DB/Google merge."""

from __future__ import annotations

from backend.listings.dealer_locator import (
    find_nearby_dealers,
    resolve_search_center,
    _matches_registry,
)


def test_resolve_center_from_zip(monkeypatch) -> None:
    monkeypatch.setattr("backend.db.geo.zip_to_coords", lambda _z: (35.2, -80.8))
    assert resolve_search_center(zip_code="28173") == (35.2, -80.8)


def test_resolve_center_from_city_state(monkeypatch) -> None:
    monkeypatch.setattr(
        "backend.db.dealerships_db.geocode_city_state",
        lambda c, s: (35.0, -80.0) if c == "Charlotte" and s == "NC" else None,
    )
    assert resolve_search_center(city="Charlotte", state="NC") == (35.0, -80.0)


def test_matches_registry_by_host() -> None:
    g = {"name": "Foo Motors", "latitude": 35.0, "longitude": -80.0, "website_url": "https://www.foomotors.com"}
    r = {
        "name": "Foo Auto",
        "latitude": 35.5,
        "longitude": -79.0,
        "website_url": "https://foomotors.com/inventory",
        "dealer_website_url": "",
    }
    assert _matches_registry(g, r) is True


def test_find_nearby_merges_db_and_google(monkeypatch) -> None:
    db_rows = [
        {
            "id": 10,
            "name": "Registry Dealer",
            "city": "Charlotte",
            "state": "NC",
            "latitude": 35.1,
            "longitude": -80.7,
            "distance_miles": 1.0,
            "street_address": "1 Main St",
            "zip_code": "28217",
            "website_url": "https://registry.example",
            "dealer_website_url": "https://registry.example",
        }
    ]

    class FakeCandidate:
        name = "Google Only"
        city = "Charlotte"
        state = "NC"
        street_address = "2 Other St"
        zip_code = "28217"
        latitude = 35.2
        longitude = -80.6
        dealer_website_url = "https://google-only.example"
        website_url = "https://google-only.example"

    monkeypatch.setattr("backend.db.geo.zip_to_coords", lambda _z: (35.15, -80.65))
    monkeypatch.setattr(
        "backend.db.dealerships_db.search_dealerships_by_radius",
        lambda *_a, **_k: db_rows,
    )
    monkeypatch.setattr(
        "backend.listings.nearby_dealers._active_listing_counts",
        lambda ids: {10: 3},
    )
    monkeypatch.setenv("GOOGLE_MAPS_API_KEY", "test-key")
    monkeypatch.setattr(
        "backend.discovery.google_places.fetch_google_places_dealerships",
        lambda *_a, **_k: [FakeCandidate()],
    )

    out = find_nearby_dealers(zip_code="28173", radius_miles=25)
    assert out["ok"] is True
    assert out["total"] == 2
    assert out["in_database_count"] == 1
    names = {d["name"] for d in out["dealers"]}
    assert "Registry Dealer" in names
    assert "Google Only" in names
    reg = next(d for d in out["dealers"] if d["name"] == "Registry Dealer")
    assert reg["in_database"] is True
    assert reg["listing_count"] == 3
    goog = next(d for d in out["dealers"] if d["name"] == "Google Only")
    assert goog["in_database"] is False


def test_location_not_found(monkeypatch) -> None:
    monkeypatch.setattr(
        "backend.db.dealerships_db.geocode_city_state",
        lambda *_a, **_k: None,
    )
    monkeypatch.setattr("backend.db.geo.zip_to_coords", lambda _z: None)
    out = find_nearby_dealers(city="Nowhereville", state="ZZ", radius_miles=10)
    assert out["ok"] is False
    assert out["error"] == "location_not_found"
