"""SEC-A-005: cache-miss builds of /api/listings/cars and coords-to-zip are per-IP limited."""

from __future__ import annotations

import gzip

import pytest

from backend.config import Config
from backend.routes import listings_api


@pytest.fixture
def client():
    from backend.main import app

    app.config["TESTING"] = True
    with app.test_client() as c:
        yield c


def test_cache_miss_builds_are_limited_but_hits_are_not(client, monkeypatch):
    monkeypatch.setattr(Config, "RATE_LIMIT_LISTINGS_BUILD_PER_MIN", 3)
    monkeypatch.setattr("backend.db.geo.zip_to_coords", lambda z: (33.0, -117.0))
    builds = []

    def fake_build(zip_code, origin, radius, hidden, token):
        builds.append(zip_code)
        return listings_api._CarsScopeEntry(
            token=token, store_gen=None, etag=f'W/"{zip_code}"', gz_body=gzip.compress(b"{}"), count=0
        )

    monkeypatch.setattr(listings_api, "_build_cars_scope_entry", fake_build)
    monkeypatch.setattr(
        "backend.db.repositories.grid_cards_repo.grid_scope_token", lambda: "t"
    )
    listings_api.clear_cars_scope_cache()
    codes = ["92694", "92695", "92696", "92697"]
    statuses = [client.get(f"/api/listings/cars?zip={z}&radius=50").status_code for z in codes]
    assert statuses[:3] == [200, 200, 200]
    assert statuses[3] == 429
    assert builds == codes[:3]
    # A cached scope is still served once the build budget is spent.
    assert client.get("/api/listings/cars?zip=92694&radius=50").status_code == 200
    listings_api.clear_cars_scope_cache()


def test_coords_to_zip_is_limited(client, monkeypatch):
    monkeypatch.setattr(Config, "RATE_LIMIT_COORDS_TO_ZIP_PER_MIN", 2)
    monkeypatch.setattr(
        "backend.db.geo.nearest_us_postal_meta", lambda lat, lon: {"postal_code": "92694"}
    )
    codes = [client.get(f"/api/coords-to-zip?lat=33.{i}&lon=-117.1").status_code for i in range(3)]
    assert codes == [200, 200, 429]
