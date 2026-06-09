"""Nearby dealer picker: inventory filter, cap, and search mode."""

from __future__ import annotations

from backend.listings.nearby_dealers import resolve_nearby_dealers_for_listings


def test_broad_mode_caps_at_ten(monkeypatch) -> None:
    rows = [
        {
            "id": i,
            "name": f"D{i}",
            "city": "C",
            "state": "NC",
            "distance_miles": float(i),
            "listing_count": 100 - i,
        }
        for i in range(1, 20)
    ]

    monkeypatch.setattr("backend.db.geo.zip_to_coords", lambda _z: (35.0, -80.0))
    monkeypatch.setattr(
        "backend.db.dealerships_db.search_dealerships_by_radius",
        lambda *_a, **_k: [{"id": i} for i in range(1, 30)],
    )
    monkeypatch.setattr(
        "backend.listings.nearby_dealers._dealers_with_inventory_near",
        lambda *_a, **_k: rows,
    )

    out = resolve_nearby_dealers_for_listings(
        zip_code="28173", radius_miles=25, search_query=None, cap=10
    )
    assert out["mode"] == "broad"
    assert out["capped"] is True
    assert len(out["dealers"]) == 10
    assert out["dealers"][0]["listing_count"] == 99
    assert out["total_with_inventory"] == 19


def test_search_mode_matches_cars_by_dealer_host(monkeypatch) -> None:
    rows = [
        {"id": 1, "name": "A", "city": "X", "state": "NC", "distance_miles": 1.0, "listing_count": 5},
        {"id": 2, "name": "B", "city": "Y", "state": "NC", "distance_miles": 2.0, "listing_count": 5},
    ]

    monkeypatch.setattr("backend.db.geo.zip_to_coords", lambda _z: (35.0, -80.0))
    monkeypatch.setattr(
        "backend.db.dealerships_db.search_dealerships_by_radius",
        lambda *_a, **_k: rows,
    )
    monkeypatch.setattr(
        "backend.listings.nearby_dealers._dealers_with_inventory_near",
        lambda *_a, **_k: rows,
    )
    monkeypatch.setattr(
        "backend.listings.nearby_dealers._cars_matching_search_in_radius",
        lambda *_a, **_k: [
            {"dealership_registry_id": None, "dealer_url": "https://www.dealer-a.com/vdp"},
        ],
    )
    monkeypatch.setattr(
        "backend.listings.dealer_registry_match.registry_id_by_dealer_host",
        lambda *_a, **_k: {"dealer-a.com": 1},
    )

    out = resolve_nearby_dealers_for_listings(
        zip_code="28173",
        radius_miles=25,
        search_query="jeep wrangler",
        cap=10,
    )
    assert out["mode"] == "search"
    assert len(out["dealers"]) == 1
    assert out["dealers"][0]["id"] == 1


def test_search_mode_returns_matching_dealers_only(monkeypatch) -> None:
    rows = [
        {"id": 1, "name": "A", "city": "X", "state": "NC", "distance_miles": 1.0, "listing_count": 5},
        {"id": 2, "name": "B", "city": "Y", "state": "NC", "distance_miles": 2.0, "listing_count": 5},
        {"id": 3, "name": "C", "city": "Z", "state": "NC", "distance_miles": 3.0, "listing_count": 5},
    ]

    monkeypatch.setattr("backend.db.geo.zip_to_coords", lambda _z: (35.0, -80.0))
    monkeypatch.setattr(
        "backend.db.dealerships_db.search_dealerships_by_radius",
        lambda *_a, **_k: rows,
    )
    monkeypatch.setattr(
        "backend.listings.nearby_dealers._dealers_with_inventory_near",
        lambda *_a, **_k: rows,
    )
    monkeypatch.setattr(
        "backend.listings.nearby_dealers._cars_matching_search_in_radius",
        lambda *_a, **_k: [
            {"dealership_registry_id": 1},
            {"dealership_registry_id": 1},
        ],
    )

    out = resolve_nearby_dealers_for_listings(
        zip_code="28173",
        radius_miles=25,
        search_query="jeep wrangler",
        cap=10,
    )
    assert out["mode"] == "search"
    assert out["capped"] is False
    assert len(out["dealers"]) == 1
    assert out["dealers"][0]["id"] == 1


def test_excludes_dealers_without_inventory(monkeypatch) -> None:
    rows = [
        {"id": 1, "name": "Has stock", "city": "", "state": "NC", "distance_miles": 1.0, "listing_count": 3},
    ]
    monkeypatch.setattr("backend.db.geo.zip_to_coords", lambda _z: (35.0, -80.0))
    monkeypatch.setattr(
        "backend.db.dealerships_db.search_dealerships_by_radius",
        lambda *_a, **_k: [
            {"id": 1, "name": "Has stock"},
            {"id": 2, "name": "Empty"},
        ],
    )
    monkeypatch.setattr(
        "backend.listings.nearby_dealers._dealers_with_inventory_near",
        lambda *_a, **_k: rows,
    )

    out = resolve_nearby_dealers_for_listings(
        zip_code="28173", radius_miles=25, search_query=None, cap=10
    )
    assert len(out["dealers"]) == 1
    assert out["dealers"][0]["id"] == 1


def test_uses_inventory_geo_not_registry_only(monkeypatch) -> None:
    """Dealers with stock near ZIP appear even when registry radius search is empty."""
    monkeypatch.setattr("backend.db.geo.zip_to_coords", lambda _z: (34.04, -118.45))
    monkeypatch.setattr(
        "backend.db.dealerships_db.search_dealerships_by_radius",
        lambda *_a, **_k: [],
    )
    monkeypatch.setattr(
        "backend.listings.nearby_dealers._dealers_with_inventory_near",
        lambda *_a, **_k: [
            {
                "id": 99,
                "name": "Local Toyota",
                "city": "LA",
                "state": "CA",
                "distance_miles": 4.2,
                "listing_count": 12,
            }
        ],
    )

    out = resolve_nearby_dealers_for_listings(zip_code="90025", radius_miles=50)
    assert out["ok"] is True
    assert len(out["dealers"]) == 1
    assert out["dealers"][0]["id"] == 99
