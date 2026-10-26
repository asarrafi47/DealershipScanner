"""SEC-A-008: Places results cached by rounded centre; anonymous callers get one tile."""

from __future__ import annotations

import pytest

from backend.listings import dealer_locator as dl


@pytest.fixture
def places(monkeypatch):
    calls = []

    def fake(lat, lon, radius):
        calls.append((lat, lon, radius))
        return []

    monkeypatch.setenv("GOOGLE_MAPS_API_KEY", "k")
    monkeypatch.setattr("backend.discovery.google_places.fetch_google_places_dealerships", fake)
    monkeypatch.setattr(dl, "resolve_search_center", lambda **kw: (kw["lat"], kw["lon"]))
    monkeypatch.setattr("backend.db.dealerships_db.search_dealerships_by_radius", lambda *a: [])
    monkeypatch.setattr("backend.listings.nearby_dealers.attach_listing_counts", lambda d: None)
    monkeypatch.setattr("backend.listings.nearby_dealers.attach_last_synced_at", lambda d: None)
    dl.clear_google_cache()
    return calls


def test_nearby_centres_share_one_places_fetch(places):
    dl.find_nearby_dealers(lat=33.6801, lon=-117.7701, radius_miles=25)
    dl.find_nearby_dealers(lat=33.6804, lon=-117.7698, radius_miles=25)
    assert len(places) == 1
    dl.find_nearby_dealers(lat=34.5, lon=-117.7701, radius_miles=25)
    assert len(places) == 2


def test_anonymous_radius_cap_limits_tiling(places):
    dl.find_nearby_dealers(lat=33.7, lon=-117.8, radius_miles=50, google_radius_cap_miles=25)
    assert places[-1][2] == 25
    dl.find_nearby_dealers(lat=35.7, lon=-117.8, radius_miles=50)
    assert places[-1][2] == 50
