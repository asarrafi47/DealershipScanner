"""Nearby dealer picker: inventory filter, cap, and search mode."""

from __future__ import annotations

from backend.listings.nearby_dealers import resolve_nearby_dealers_for_listings


def test_broad_mode_caps_at_ten(monkeypatch) -> None:
    rows = [
        {"id": i, "name": f"D{i}", "city": "C", "state": "NC", "distance_miles": float(i)}
        for i in range(1, 20)
    ]
    counts = {i: 100 - i for i in range(1, 20)}

    monkeypatch.setattr("backend.db.geo.zip_to_coords", lambda _z: (35.0, -80.0))
    monkeypatch.setattr(
        "backend.db.dealerships_db.search_dealerships_by_radius",
        lambda *_a, **_k: rows,
    )
    monkeypatch.setattr(
        "backend.listings.nearby_dealers._active_listing_counts",
        lambda ids: {k: counts[k] for k in ids if k in counts},
    )

    out = resolve_nearby_dealers_for_listings(
        zip_code="28173", radius_miles=25, search_query=None, cap=10
    )
    assert out["mode"] == "broad"
    assert out["capped"] is True
    assert len(out["dealers"]) == 10
    assert out["dealers"][0]["listing_count"] == 99
    assert out["total_with_inventory"] == 19


def test_search_mode_returns_matching_dealers_only(monkeypatch) -> None:
    rows = [
        {"id": 1, "name": "A", "city": "X", "state": "NC", "distance_miles": 1.0},
        {"id": 2, "name": "B", "city": "Y", "state": "NC", "distance_miles": 2.0},
        {"id": 3, "name": "C", "city": "Z", "state": "NC", "distance_miles": 3.0},
    ]

    monkeypatch.setattr("backend.db.geo.zip_to_coords", lambda _z: (35.0, -80.0))
    monkeypatch.setattr(
        "backend.db.dealerships_db.search_dealerships_by_radius",
        lambda *_a, **_k: rows,
    )
    monkeypatch.setattr(
        "backend.listings.nearby_dealers._active_listing_counts",
        lambda ids: {i: 5 for i in ids},
    )
    monkeypatch.setattr(
        "backend.utils.hybrid_search.hybrid_search_with_kwargs",
        lambda _q, _kw, **kwargs: (
            [{"dealership_registry_id": 1}, {"dealership_registry_id": 1}],
            {},
        ),
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
        {"id": 1, "name": "Has stock", "city": "", "state": "NC", "distance_miles": 1.0},
        {"id": 2, "name": "Empty", "city": "", "state": "NC", "distance_miles": 2.0},
    ]
    monkeypatch.setattr("backend.db.geo.zip_to_coords", lambda _z: (35.0, -80.0))
    monkeypatch.setattr(
        "backend.db.dealerships_db.search_dealerships_by_radius",
        lambda *_a, **_k: rows,
    )
    monkeypatch.setattr(
        "backend.listings.nearby_dealers._active_listing_counts",
        lambda ids: {1: 3} if 1 in ids else {},
    )

    out = resolve_nearby_dealers_for_listings(
        zip_code="28173", radius_miles=25, search_query=None, cap=10
    )
    assert len(out["dealers"]) == 1
    assert out["dealers"][0]["id"] == 1
