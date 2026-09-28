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
    assert r.get_json() == {
        "ok": False,
        "error": "zip_required",
        "area_required": True,
        "api_version": 2,
        "cars": [],
    }
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


# ── card freshness: inputs outside the cars row ─────────────────────────


def test_card_revision_covers_deal_score_and_public_incomplete_sources(monkeypatch):
    from pathlib import Path

    from backend.db.repositories import grid_cards_repo as gc

    root = Path(gc.__file__).resolve().parents[3]
    for spec in (
        "backend/intelligence/deal_score_cache.py",
        "backend/utils/market_price.py",
        "backend/utils/price_plausibility.py",
        "backend/db/repositories/listings_repo.py::serialize_car_for_listings_grid",
    ):
        assert spec in gc._REV_SOURCES
    for spec in gc._REV_SOURCES:
        rel, _, func = spec.partition("::")
        assert (root / rel).exists(), spec
        if func:
            assert gc._function_source((root / rel).read_bytes(), func), spec

    base = gc._compute_rev()
    real = Path.read_bytes

    def edited(self):
        data = real(self)
        return data + b"\n# edited\n" if self.name == "deal_score_cache.py" else data

    monkeypatch.setattr(Path, "read_bytes", edited)
    assert gc._compute_rev() != base


def _market_stats(path, computed_at):
    conn = sqlite3.connect(str(path))
    conn.execute(
        "CREATE TABLE IF NOT EXISTS market_price_stats (id INTEGER PRIMARY KEY, computed_at TEXT)"
    )
    conn.execute("INSERT INTO market_price_stats (computed_at) VALUES (?)", (computed_at,))
    conn.commit()
    conn.close()


def test_market_band_recompute_invalidates_stored_cards(scoped, monkeypatch):
    import backend.intelligence.deal_score_cache as dsc
    from backend.db.geo import zip_to_coords
    from backend.db.repositories import grid_cards_repo as gc

    reloads = []
    monkeypatch.setattr(dsc, "refresh_cache", lambda: reloads.append(1))
    _market_stats(scoped.path, "2026-09-27T03:00:00")
    lat, lon = zip_to_coords("92694")
    first = gc.cards_near(lat, lon, 50)
    assert first.stats["changed"] > 0
    assert gc.cards_near(lat, lon, 50).stats["changed"] == 0
    token_before = gc.grid_scope_token()

    # Nightly step 6 recomputes the bands; nothing in ``cars`` moved.
    _market_stats(scoped.path, "2026-09-28T03:00:00")
    gc._market_gen_cache = None  # past the 60 s resample
    after = gc.cards_near(lat, lon, 50)
    assert after.stats["changed"] == len(first.entries)
    assert reloads, "deal-score band cache must reload before cards are rebuilt"
    assert gc.grid_scope_token() != token_before


def test_market_generation_read_failure_keeps_the_last_generation(scoped, monkeypatch):
    from backend.db.repositories import grid_cards_repo as gc

    _market_stats(scoped.path, "2026-09-27T03:00:00")
    good = gc.market_bands_generation()
    assert good != "-"
    gc._market_gen_cache = None

    class Boom:
        def __enter__(self):
            raise RuntimeError("connection reset")

        def __exit__(self, *a):
            return False

    monkeypatch.setattr(gc, "db_conn", lambda *a, **k: Boom())
    assert gc.market_bands_generation() == good
    assert gc._market_gen_cache is None  # a failure is never cached


def test_include_incomplete_toggle_is_part_of_the_aux_key(scoped, monkeypatch):
    from backend.db.geo import zip_to_coords
    from backend.db.repositories import grid_cards_repo as gc

    monkeypatch.setenv("LISTINGS_INCLUDE_INCOMPLETE_CARS", "1")
    lat, lon = zip_to_coords("92694")
    gc.cards_near(lat, lon, 50)
    assert gc.cards_near(lat, lon, 50).stats["changed"] == 0
    token_on = gc.grid_scope_token()

    ctx_on = gc._Ctx()
    monkeypatch.setenv("LISTINGS_INCLUDE_INCOMPLETE_CARS", "0")
    ctx_off = gc._Ctx()
    assert ctx_on.include_incomplete and not ctx_off.include_incomplete
    for pub_inc in (False, True):
        assert gc._aux_key(ctx_on, pub_inc, None) != gc._aux_key(ctx_off, pub_inc, None)
    assert gc.grid_scope_token() != token_on


# ── attribution read failures ───────────────────────────────────────────

_VERDICT = {
    "status": "mismatch",
    "observed_rooftop": "Some Other Rooftop",
    "location_unconfirmed": True,
    "group_feed": False,
}


def _car_id(path, title):
    conn = sqlite3.connect(str(path))
    try:
        return conn.execute("SELECT id FROM cars WHERE title = ?", (title,)).fetchone()[0]
    finally:
        conn.close()


def test_attribution_read_failure_keeps_stored_cards(scoped, monkeypatch, caplog):
    import logging

    from backend.db.geo import zip_to_coords
    from backend.db.repositories import cars_repo
    from backend.db.repositories import grid_cards_repo as gc

    cid = _car_id(scoped.path, "2022 Toyota Camry #1")
    monkeypatch.setattr(cars_repo, "car_attribution_states", lambda *a, **k: {cid: dict(_VERDICT)})
    lat, lon = zip_to_coords("92694")
    first = gc.cards_near(lat, lon, 50)
    card1 = next(json.loads(c) for c in first.cards_json() if json.loads(c)["id"] == cid)
    assert card1.get("location_confirmed") is False

    def broken(*_a, fail_open=True, **_k):
        assert fail_open is False, "the card store must see read failures"
        raise RuntimeError("connection reset by peer")

    monkeypatch.setattr(cars_repo, "car_attribution_states", broken)
    gc._attr_cache = None  # past the 60 s TTL
    caplog.set_level(logging.WARNING, logger=gc.__name__)
    second = gc.cards_near(lat, lon, 50)
    assert second.stats["changed"] == 0, "no mass rebuild on a failed attribution read"
    assert second.cards_json() == first.cards_json()  # caveats kept
    assert gc._attr_cache is None, "a failed read is never cached"
    assert any("car_attribution read failed" in r.getMessage() for r in caplog.records)


def test_attribution_failure_with_no_prior_read_still_keeps_stored_cards(scoped, monkeypatch):
    from backend.db.geo import zip_to_coords
    from backend.db.repositories import cars_repo
    from backend.db.repositories import grid_cards_repo as gc

    cid = _car_id(scoped.path, "2022 Toyota Camry #1")
    monkeypatch.setattr(cars_repo, "car_attribution_states", lambda *a, **k: {cid: dict(_VERDICT)})
    lat, lon = zip_to_coords("92694")
    first = gc.cards_near(lat, lon, 50)
    gc.reset_grid_cards_state()  # e.g. a freshly started worker

    def broken(*_a, **_k):
        raise RuntimeError("timeout")

    monkeypatch.setattr(cars_repo, "car_attribution_states", broken)
    second = gc.cards_near(lat, lon, 50)
    assert second.stats["changed"] == 0
    assert second.cards_json() == first.cards_json()


def test_attribution_drop_to_zero_is_logged(scoped, monkeypatch, caplog):
    import logging

    from backend.db.repositories import cars_repo
    from backend.db.repositories import grid_cards_repo as gc

    monkeypatch.setattr(cars_repo, "car_attribution_states", lambda *a, **k: {1: dict(_VERDICT), 2: dict(_VERDICT)})
    gc._attribution_read()
    monkeypatch.setattr(cars_repo, "car_attribution_states", lambda *a, **k: {})
    gc._attr_cache = None
    caplog.set_level(logging.WARNING, logger=gc.__name__)
    states, _gen, ok = gc._attribution_read()
    assert ok and states == {}
    assert any("returned no verdicts but the previous read had 2" in r.getMessage() for r in caplog.records)


def test_car_attribution_states_fail_open_is_opt_out(monkeypatch):
    from backend.db.repositories import cars_repo

    class Boom:
        def __enter__(self):
            raise RuntimeError("down")

        def __exit__(self, *a):
            return False

    monkeypatch.setattr(cars_repo, "db_conn", lambda *a, **k: Boom())
    assert cars_repo.car_attribution_states() == {}
    with pytest.raises(RuntimeError):
        cars_repo.car_attribution_states(fail_open=False)


# ── bounded inline work ─────────────────────────────────────────────────


def test_thin_store_request_serializes_at_most_the_cap(scoped, client, monkeypatch):
    """Fresh deploy (empty store): a request builds at most _INLINE_REBUILD_MAX cards,
    nearest first, serves them, queues the rest and says the body is partial."""
    from backend.db.repositories import grid_cards_repo as gc

    monkeypatch.setattr(gc, "_INLINE_REBUILD_MAX", 2)
    queued = []
    monkeypatch.setattr(gc, "_enqueue_refresh", lambda ids: queued.extend(ids))
    serialized = []
    real = gc._serialize_rows

    def counting(rows, ctx, **kw):
        serialized.append(len(rows))
        return real(rows, ctx, **kw)

    monkeypatch.setattr(gc, "_serialize_rows", counting)
    r = client.get("/api/listings/cars?zip=92694&radius=250")
    data = _cars(r)
    assert sum(serialized) == 2, "inline work is bounded by the cap"
    assert data["partial"] is True and data["count"] == 2
    assert r.headers["X-Listings-Partial"] == "1"
    assert "s-maxage" not in r.headers["Cache-Control"]
    # Nearest first: near-motors (~1 mi) before anything farther.
    assert {c["dealer_id"] for c in data["cars"]} == {"near-motors-test"}
    assert len(queued) == 5 - 2  # every other car in range is queued, none dropped


def test_many_changes_with_a_servable_store_serve_stale_and_build_nothing(scoped, monkeypatch):
    from backend.db.geo import zip_to_coords
    from backend.db.repositories import grid_cards_repo as gc

    lat, lon = zip_to_coords("92694")
    gc.cards_near(lat, lon, 250)  # fill the store
    conn = sqlite3.connect(str(scoped.path))
    conn.execute("UPDATE cars SET price = price + 1")
    conn.commit()
    conn.close()
    monkeypatch.setattr(gc, "_INLINE_REBUILD_MAX", 2)
    queued = []
    monkeypatch.setattr(gc, "_enqueue_refresh", lambda ids: queued.extend(ids))
    res = gc.cards_near(lat, lon, 250)
    # 5 cars have coordinates (#6 is counted in missing_coords).
    assert res.stats["inline"] == 0 and res.stats["deferred"] == 5
    assert res.partial and len(res.entries) == 5 and len(set(queued)) == 5


def test_offline_builder_is_not_capped(scoped, monkeypatch):
    from backend.db.repositories import grid_cards_repo as gc

    monkeypatch.setattr(gc, "_INLINE_REBUILD_MAX", 1)
    assert gc.build_all_cards(batch=10)["rebuilt"] == 6


# ── background refresh failures never leave a partial body looking current ──


def test_failed_background_refresh_bumps_the_generation(scoped, monkeypatch, caplog):
    import logging
    import queue as _queue
    import time as _time

    from backend.db.repositories import grid_cards_repo as gc

    def boom(ids, **_k):
        raise RuntimeError("db went away")

    monkeypatch.setattr(gc, "refresh_cards", boom)
    monkeypatch.setattr(gc, "_bg_queue", _queue.Queue(maxsize=4))
    monkeypatch.setattr(gc, "_bg_thread", None)
    caplog.set_level(logging.WARNING, logger=gc.__name__)
    gen = gc.store_generation()
    gc._enqueue_refresh([101, 102])
    deadline = _time.monotonic() + 5
    while gc.store_generation() == gen and _time.monotonic() < deadline:
        _time.sleep(0.01)
    assert gc.store_generation() > gen
    with gc._bg_pending_lock:
        assert not ({101, 102} & gc._bg_pending)
    assert any("background grid-card refresh failed" in r.getMessage() for r in caplog.records)


def test_queue_full_drop_bumps_the_generation(scoped, monkeypatch, caplog):
    import logging
    import queue as _queue

    from backend.db.repositories import grid_cards_repo as gc

    class Alive:
        def is_alive(self):
            return True

    full = _queue.Queue(maxsize=1)
    full.put_nowait([1])
    monkeypatch.setattr(gc, "_bg_queue", full)
    monkeypatch.setattr(gc, "_bg_thread", Alive())
    caplog.set_level(logging.WARNING, logger=gc.__name__)
    gen = gc.store_generation()
    gc._enqueue_refresh([201, 202, 203])
    assert gc.store_generation() == gen + 1
    with gc._bg_pending_lock:
        assert not ({201, 202, 203} & gc._bg_pending)  # re-queueable next request
    assert any("queue full; dropped 3 cars" in r.getMessage() for r in caplog.records)


def test_partial_body_is_rebuilt_after_a_lost_refresh(scoped, client, monkeypatch):
    import queue as _queue

    from backend.db.repositories import grid_cards_repo as gc
    from backend.routes import listings_api

    class Alive:
        def is_alive(self):
            return True

    full = _queue.Queue(maxsize=1)
    full.put_nowait([1])
    monkeypatch.setattr(gc, "_bg_queue", full)
    monkeypatch.setattr(gc, "_bg_thread", Alive())
    monkeypatch.setattr(gc, "_INLINE_REBUILD_MAX", 1)
    assert _cars(client.get("/api/listings/cars?zip=92694&radius=250"))["partial"] is True
    (entry,) = listings_api._cars_scope_cache.values()
    token = gc.grid_scope_token()
    assert not listings_api._cars_scope_entry_valid(entry, token)


# ── dealer_url coordinate fallback ──────────────────────────────────────


@pytest.mark.parametrize("id_batch", [800, 1])
def test_fallback_includes_cars_without_dealer_id_and_survives_batching(
    sqlite_inventory, client, monkeypatch, id_batch
):
    from backend.db.repositories import grid_cards_repo as gc
    from backend.db.repositories import listings_repo as lr
    from backend.routes import listings_api

    lr.clear_inventory_listings_cache()
    gc.reset_grid_cards_state()
    listings_api.clear_cars_scope_cache()

    def geo_car(i, dealer, dealer_id):
        return {
            "title": f"2021 Honda Civic #{i}",
            "year": 2021,
            "make": "Honda",
            "model": "Civic",
            "price": 15000 + i,
            "gallery": json.dumps([]),
            "dealer_name": dealer,
            "dealer_url": f"https://{dealer}.test",
            "dealer_id": dealer_id,
        }

    _seed(
        sqlite_inventory,
        extra_cars=(
            geo_car(1, "geo-only", None),        # NULL dealer_id
            geo_car(2, "geo-only", ""),          # empty dealer_id
            geo_car(3, "geo-two", "geo-two-a"),  # second located URL, two dealer_ids
            geo_car(4, "geo-two", "geo-two-b"),
            geo_car(5, "no-url", "no-url-test") | {"dealer_url": None},
        ),
    )
    conn = sqlite3.connect(str(sqlite_inventory.path))
    conn.execute(
        "INSERT INTO dealer_geopoints (dealer_url, lat, lon) VALUES (?, ?, ?)",
        ("https://geo-two.test", NEAR[0] - 0.02, NEAR[1]),
    )
    conn.commit()
    conn.close()
    # 800: one batch (the old dealer_id IN (...) filter applied and dropped NULL/'').
    # 1: two URL batches, past the size where the old filter was silently skipped.
    monkeypatch.setattr(gc, "_ID_BATCH", id_batch)

    data = _cars(client.get("/api/listings/cars?zip=92694&radius=10"))
    titles = {c["title"] for c in data["cars"]}
    for i in (1, 2, 3, 4):
        assert f"2021 Honda Civic #{i}" in titles
    assert "2022 Toyota Camry #5" in titles
    assert "2021 Honda Civic #5" not in titles
    # nowhere-motors (#6) and the URL-less car: counted, never silently dropped.
    assert data["missing_coords"] == 2


# ── dealership page: one card build per view, 304 without a build ───────


@pytest.fixture
def dealer_builds(scoped, monkeypatch):
    from backend.db.repositories import grid_cards_repo as gc
    from backend.routes import dealership_page as dp

    # Steady state: the store is warm. (On SQLite the data token is the DB file's
    # mtime, so a cold store's own first write moves it -- one extra build, dev only;
    # on Postgres the write fingerprint does not include the card store.)
    gc.build_all_cards()
    dp.clear_dealer_cards_cache()
    calls = []
    real = gc.cards_for_dealer

    def counting(dealer_id, **kw):
        calls.append(dealer_id)
        return real(dealer_id, **kw)

    monkeypatch.setattr(gc, "cards_for_dealer", counting)
    yield calls
    dp.clear_dealer_cards_cache()


def test_dealership_page_view_builds_the_dealer_cards_once(dealer_builds, client):
    from backend.routes import dealership_page as dp

    # Warm-up view: on SQLite the first render's own writes move the DB-mtime token.
    client.get("/dealership/near-motors-test")
    dp.clear_dealer_cards_cache()
    dealer_builds.clear()

    assert client.get("/dealership/near-motors-test").status_code == 200
    assert client.get("/api/dealership/near-motors-test/cars").status_code == 200
    assert client.get("/api/dealership/near-motors-test/filter-options").status_code == 200
    assert dealer_builds == ["near-motors-test"]


def test_dealership_cars_304_skips_the_card_build(dealer_builds, client):
    from backend.routes import dealership_page as dp

    r1 = client.get("/api/dealership/near-motors-test/cars")
    etag = r1.headers["ETag"]
    assert len(r1.get_json()["cars"]) == 2
    dp.clear_dealer_cards_cache()
    dealer_builds.clear()
    r2 = client.get("/api/dealership/near-motors-test/cars", headers={"If-None-Match": etag})
    assert r2.status_code == 304
    assert dealer_builds == [], "a matching validator must not build the cards"


def test_dealership_cars_etag_and_cache_move_when_a_car_changes(dealer_builds, client, scoped):
    r1 = client.get("/api/dealership/near-motors-test/cars")
    conn = sqlite3.connect(str(scoped.path))
    conn.execute("UPDATE cars SET price = 11111 WHERE title = '2022 Toyota Camry #1'")
    conn.commit()
    conn.close()
    r2 = client.get("/api/dealership/near-motors-test/cars", headers={"If-None-Match": r1.headers["ETag"]})
    assert r2.status_code == 200
    assert r2.headers["ETag"] != r1.headers["ETag"]
    assert 11111 in {c["price"] for c in r2.get_json()["cars"]}
    assert len(dealer_builds) == 2


def test_dealership_cards_cache_expires_after_its_ttl(dealer_builds, monkeypatch):
    from backend.routes import dealership_page as dp

    dp._dealer_grid_cards_json("near-motors-test")
    dp._dealer_grid_cards_json("near-motors-test")
    assert len(dealer_builds) == 1
    monkeypatch.setattr(dp, "_DEALER_CARDS_TTL_S", 0.0)
    dp._dealer_grid_cards_json("near-motors-test")
    assert len(dealer_builds) == 2
