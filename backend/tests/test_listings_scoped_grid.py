"""Radius-scoped listings grid (owner decision 2026-09-28).

The listings page no longer downloads the whole fleet. ``GET /api/listings/cars``
answers only ``?zip=&radius=`` from the persisted card store, and nothing on a
request path may build the whole-fleet grid any more.
"""

from __future__ import annotations

import gzip
import json
import sqlite3

import pytest

# pgeocode centroid for 92694 (Ladera Ranch, CA) is ~(33.55, -117.64).
NEAR = (33.56, -117.66)      # ~1 mi
MID = (33.75, -117.87)       # ~18 mi
FAR = (36.17, -115.14)       # Las Vegas, ~230 mi


def _seed(sqlite_inventory, *, extra_cars=()):
    conn = sqlite3.connect(str(sqlite_inventory.path))
    try:
        for rid, (lat, lon), host in (
            (1, NEAR, "near-motors"),
            (2, MID, "mid-motors"),
            (3, FAR, "far-motors"),
        ):
            conn.execute(
                "INSERT INTO dealerships (id, name, website_url, city, state, latitude, longitude, is_active) "
                "VALUES (?, ?, ?, 'X', 'CA', ?, ?, 1)",
                (rid, host, f"https://{host}.test", lat, lon),
            )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS dealer_geopoints (dealer_url TEXT PRIMARY KEY, dealer_name TEXT, "
            "lat REAL, lon REAL, zip_code TEXT, city TEXT, state TEXT, geocode_source TEXT, geocoded_at TEXT)"
        )
        # A dealer with no registry row, located only through dealer_geopoints.
        conn.execute(
            "INSERT INTO dealer_geopoints (dealer_url, lat, lon) VALUES (?, ?, ?)",
            ("https://geo-only.test", NEAR[0] + 0.02, NEAR[1]),
        )
        conn.commit()
    finally:
        conn.close()

    def car(i, dealer, rid, **over):
        row = {
            "title": f"2022 Toyota Camry #{i}",
            "year": 2022,
            "make": "Toyota",
            "model": "Camry",
            "trim": "LE",
            "price": 20000 + i,
            "mileage": 1000 + i,
            "image_url": f"https://cdn.example.com/{i}.jpg",
            "gallery": json.dumps([f"https://cdn.example.com/{i}-{k}.jpg" for k in range(3)]),
            "dealer_name": dealer,
            "dealer_url": f"https://{dealer}.test",
            "dealer_id": f"{dealer}-test",
            "dealership_registry_id": rid,
            "body_style": "Sedan",
            "fuel_type": "Gasoline",
        }
        row.update(over)
        return row

    rows = [
        car(1, "near-motors", 1),
        car(2, "near-motors", 1, price=18000),
        car(3, "mid-motors", 2),
        car(4, "far-motors", 3),
        car(5, "geo-only", None),        # dealer_url coordinate fallback
        car(6, "nowhere-motors", None),  # no coordinates at all -> missing_coords
        *extra_cars,
    ]
    sqlite_inventory.add_cars(rows)


@pytest.fixture
def scoped(sqlite_inventory, monkeypatch):
    from backend.db.repositories import grid_cards_repo as gc
    from backend.db.repositories import listings_repo as lr
    from backend.routes import listings_api

    lr.clear_inventory_listings_cache()
    gc.reset_grid_cards_state()
    listings_api.clear_cars_scope_cache()
    _seed(sqlite_inventory)
    yield sqlite_inventory
    lr.clear_inventory_listings_cache()


@pytest.fixture
def client():
    from backend.main import app

    app.config["TESTING"] = True
    with app.test_client() as c:
        yield c


def _cars(resp):
    body = resp.get_data()
    if resp.headers.get("Content-Encoding") == "gzip":
        body = gzip.decompress(body)
    return json.loads(body)


def _forbid_whole_fleet(monkeypatch):
    import backend.db.inventory_db as inv
    import backend.db.repositories.listings_repo as lr
    import backend.main as main

    def boom(*_a, **_k):
        raise AssertionError("whole-fleet grid built on a request path")

    for mod in (inv, lr, main):
        monkeypatch.setattr(mod, "listings_grid_serialized_cars", boom, raising=False)
    monkeypatch.setattr(lr, "_build_grid_cars_uncached", boom)


# ── endpoint contract ───────────────────────────────────────────────────


def test_endpoint_without_zip_is_400_not_the_fleet(scoped, client):
    r = client.get("/api/listings/cars")
    assert r.status_code == 400
    assert r.get_json() == {"ok": False, "error": "zip_required"}
    assert client.get("/api/listings/cars?radius=50").status_code == 400
    assert client.get("/api/listings/cars?zip=12").status_code == 400


def test_unknown_zip_is_a_clean_400(scoped, client):
    r = client.get("/api/listings/cars?zip=00000&radius=50")
    assert r.status_code == 400
    assert r.get_json()["error"] == "zip_not_found"


def test_radius_is_clamped_to_5_250_and_defaults_to_50():
    from backend.routes.listings_api import clamp_listings_radius

    assert clamp_listings_radius(None) == 50
    assert clamp_listings_radius("") == 50
    assert clamp_listings_radius("abc") == 50
    assert clamp_listings_radius("nan") == 50
    assert clamp_listings_radius("1") == 5
    assert clamp_listings_radius("-40") == 5
    assert clamp_listings_radius("25") == 25
    assert clamp_listings_radius("9000") == 250


def test_only_cars_within_the_radius_are_served(scoped, client, monkeypatch):
    _forbid_whole_fleet(monkeypatch)
    data = _cars(client.get("/api/listings/cars?zip=92694&radius=5", headers={"Accept-Encoding": "gzip"}))
    # 5 mi snaps up to the 10 mi option; the client narrows by distance_miles.
    assert data["ok"] is True and data["radius"] == 10
    titles = sorted(c["title"] for c in data["cars"])
    # near-motors (2 cars) + the dealer located only via dealer_geopoints.
    assert titles == ["2022 Toyota Camry #1", "2022 Toyota Camry #2", "2022 Toyota Camry #5"]
    assert all(c["distance_miles"] <= 10 for c in data["cars"])
    # Grid order is the same price order the whole-fleet grid used.
    assert [c["price"] for c in data["cars"]] == sorted(c["price"] for c in data["cars"])
    # The car with no coordinates anywhere is counted, never silently dropped.
    assert data["missing_coords"] == 1

    wide = _cars(client.get("/api/listings/cars?zip=92694&radius=9000"))
    assert wide["radius"] == 250
    assert "2022 Toyota Camry #4" in {c["title"] for c in wide["cars"]}  # Las Vegas, ~230 mi


def test_cards_have_the_grid_serializer_keys(scoped, client):
    from backend.db.repositories.listings_repo import serialize_car_for_listings_grid

    data = _cars(client.get("/api/listings/cars?zip=92694&radius=50"))
    card = next(c for c in data["cars"] if c["title"].endswith("#1"))
    reference = serialize_car_for_listings_grid(
        {"id": card["id"], "title": "x", "price": 1, "gallery": "[]"}
    )
    assert set(reference) <= set(card)
    assert set(card) - set(reference) == {"distance_miles"}


def test_hidden_dealers_are_excluded(scoped, client, monkeypatch):
    import backend.main as main

    monkeypatch.setattr(main, "hidden_dealer_ids_for_user", lambda _uid: {"near-motors-test"})
    r = client.get("/api/listings/cars?zip=92694&radius=50")
    data = _cars(r)
    assert data["cars"], "other dealers must still be served"
    assert not [c for c in data["cars"] if c["dealer_id"] == "near-motors-test"]
    # A per-user body must never be cached by a shared cache.
    assert r.headers["Cache-Control"].startswith("private")


def test_etag_round_trips_to_304(scoped, client):
    r1 = client.get("/api/listings/cars?zip=92694&radius=50", headers={"Accept-Encoding": "gzip"})
    assert r1.status_code == 200
    assert r1.headers["Content-Encoding"] == "gzip"
    etag = r1.headers["ETag"]
    assert etag.startswith('W/"lc-92694-50-')
    r2 = client.get("/api/listings/cars?zip=92694&radius=50", headers={"If-None-Match": etag})
    assert r2.status_code == 304
    assert r2.get_data() == b""
    # A different scope never shares a validator.
    r3 = client.get("/api/listings/cars?zip=92694&radius=25", headers={"If-None-Match": etag})
    assert r3.status_code == 200


def test_etag_moves_when_a_served_car_changes(scoped, client):
    r1 = client.get("/api/listings/cars?zip=92694&radius=50")
    conn = sqlite3.connect(str(scoped.path))
    conn.execute("UPDATE cars SET price = 12345 WHERE title = '2022 Toyota Camry #1'")
    conn.commit()
    conn.close()
    r2 = client.get("/api/listings/cars?zip=92694&radius=50", headers={"If-None-Match": r1.headers["ETag"]})
    assert r2.status_code == 200
    card = next(c for c in _cars(r2)["cars"] if c["title"].endswith("#1"))
    assert card["price"] == 12345


def test_scope_cache_is_bounded(scoped, client, monkeypatch):
    from backend.routes import listings_api

    monkeypatch.setattr(listings_api, "_CARS_SCOPE_MAX_ENTRIES", 3)
    for radius in (10, 25, 50, 100, 250):
        assert client.get(f"/api/listings/cars?zip=92694&radius={radius}").status_code == 200
    assert len(listings_api._cars_scope_cache) == 3
    assert list(k[1] for k in listings_api._cars_scope_cache) == [50, 100, 250]

    listings_api.clear_cars_scope_cache()
    monkeypatch.setattr(listings_api, "_CARS_SCOPE_MAX_BYTES", 1)
    client.get("/api/listings/cars?zip=92694&radius=10")
    client.get("/api/listings/cars?zip=92694&radius=20")
    assert len(listings_api._cars_scope_cache) <= 1


def test_radius_snaps_up_to_the_offered_options():
    from backend.routes.listings_api import snap_listings_radius

    assert snap_listings_radius(None) == 50
    assert snap_listings_radius("1") == 10
    assert snap_listings_radius("10") == 10
    assert snap_listings_radius("10.01") == 25
    assert snap_listings_radius("49.99") == 50
    assert snap_listings_radius("50.01") == 100
    assert snap_listings_radius("101") == 250
    assert snap_listings_radius("9000") == 250


def test_float_radii_share_one_scope_cache_entry(scoped, client):
    from backend.routes import listings_api

    for radius in ("26", "30.5", "49.99", "50"):
        assert client.get(f"/api/listings/cars?zip=92694&radius={radius}").status_code == 200
    assert [k[1] for k in listings_api._cars_scope_cache] == [50.0]


def test_scope_build_locks_are_a_fixed_stripe_array(scoped, client):
    """The per-scope build lock never comes from a structure that evicts: the same
    key always maps to the same lock object, however many scopes are requested."""
    from backend.routes import listings_api

    key = ("92694", 50.0, ())
    lock = listings_api._cars_scope_build_lock(key)
    before = listings_api._cars_scope_build_locks
    for i in range(10, 10 + 4 * listings_api._CARS_SCOPE_MAX_ENTRIES + 5):
        listings_api._cars_scope_build_lock((f"{i:05d}", 50.0, ()))
    assert listings_api._cars_scope_build_locks is before
    assert len(before) == listings_api._CARS_SCOPE_BUILD_STRIPES
    assert listings_api._cars_scope_build_lock(key) is lock
    # A held lock is still the one the next request for that scope waits on.
    with lock:
        assert listings_api._cars_scope_build_lock(key).locked()
    assert client.get("/api/listings/cars?zip=92694&radius=50").status_code == 200


# ── card store ──────────────────────────────────────────────────────────


def test_stored_card_equals_a_fresh_serialization(scoped):
    from backend.db.geo import zip_to_coords
    from backend.db.repositories import grid_cards_repo as gc

    lat, lon = zip_to_coords("92694")
    first = gc.cards_near(lat, lon, 50)
    assert first.stats["changed"] == len(first.entries) > 0  # built and stored
    second = gc.cards_near(lat, lon, 50)
    assert second.stats["changed"] == 0                       # served from the store
    assert second.cards_json() == first.cards_json()


def test_dealer_scope_reads_only_that_dealer(scoped):
    from backend.db.repositories import grid_cards_repo as gc

    cards = [json.loads(c) for c in gc.cards_for_dealer("near-motors-test").cards_json()]
    assert {c["dealer_id"] for c in cards} == {"near-motors-test"}
    assert len(cards) == 2


# ── no whole-fleet build on any request path ────────────────────────────


def test_request_paths_never_build_the_whole_fleet(scoped, client, monkeypatch):
    _forbid_whole_fleet(monkeypatch)
    assert client.get("/listings").status_code == 200
    assert client.get("/listings?make=Toyota&zip_code=92694&radius=25").status_code == 200
    assert client.get("/api/listings/cars?zip=92694&radius=50").status_code == 200
    assert client.get("/api/listings/filter-options").status_code == 200
    assert client.get("/dealership/near-motors-test").status_code == 200
    assert client.get("/api/dealership/near-motors-test/cars").status_code == 200


def test_listings_page_ships_no_cars(scoped, client):
    html = client.get("/listings").get_data(as_text=True)
    assert 'id="ds-listings-bootstrap-grid"' not in html
    assert 'id="ds-listings-all-cars"' not in html
    assert "2022 Toyota Camry" not in html
    assert 'data-search-started="0"' in html
    assert 'id="listings-start-prompt"' in html
    # Nothing fetches inventory from <head>.
    head = html.split("</head>", 1)[0]
    assert "/api/listings/cars" not in head


def test_deep_link_counts_as_a_started_search(scoped, client):
    html = client.get("/listings?make=Toyota&zip_code=92694&radius=25").get_data(as_text=True)
    assert 'data-search-started="1"' in html
    assert 'value="92694"' in html
    assert '<option value="25" selected>' in html
    # Sorting alone is not a search.
    html = client.get("/listings?sort=price_asc").get_data(as_text=True)
    assert 'data-search-started="0"' in html


def test_radius_defaults_to_50_on_the_page(scoped, client):
    html = client.get("/listings").get_data(as_text=True)
    assert '<option value="50" selected>' in html


def test_smart_search_is_scoped_to_zip_and_radius(scoped, client, monkeypatch):
    import backend.utils.hybrid_search as hs

    seen = {}

    def fake_search(q, filters, **kw):
        seen.update(kw)
        return [], {"mode": "sql"}

    monkeypatch.setattr(hs, "hybrid_smart_search", fake_search)
    csrf = "scoped-grid-test-csrf-token-32-chars-x"
    with client.session_transaction() as sess:
        sess["_csrf_token"] = csrf
    headers = {"X-CSRF-Token": csrf}
    r = client.post("/api/search/smart", json={"query": "camry"}, headers=headers)
    assert r.status_code == 400 and r.get_json()["error"] == "zip_required"

    r = client.post(
        "/api/search/smart", json={"query": "camry", "zip_code": "92694", "radius": 900}, headers=headers
    )
    assert r.status_code == 200
    assert seen["listing_geo_kwargs"] == {"zip_code": "92694", "radius_miles": 250}


def test_offline_builder_fills_then_reuses_then_prunes(scoped):
    from backend.db.repositories import grid_cards_repo as gc

    first = gc.build_all_cards(batch=2)
    assert first["active"] == 6 and first["rebuilt"] == 6 and first["fresh"] == 0
    again = gc.build_all_cards(batch=2)
    assert again["rebuilt"] == 0 and again["fresh"] == 6

    conn = sqlite3.connect(str(scoped.path))
    conn.execute("UPDATE cars SET listing_active = 0 WHERE title = '2022 Toyota Camry #4'")
    conn.commit()
    conn.close()
    pruned = gc.build_all_cards(batch=2)
    assert pruned["active"] == 5 and pruned["pruned"] == 1
