"""Listings performance helpers (ETag, batched make/model search).

Client listings page debounces ZIP input (~280ms) and defers geo-coords,
nearby-dealers, and market-stats work until idle so partial ZIP keystrokes
do not replay full-grid radius filters.
"""

import json

from backend.db.inventory_db import search_cars_by_make_model_pairs
from backend.main import app


def test_api_listings_geo_coords():
    with app.test_client() as client:
        r = client.get("/api/listings/geo-coords")
        assert r.status_code == 200
        data = r.get_json()
        assert data["ok"] is True
        assert isinstance(data.get("zip_coords"), dict)
        assert isinstance(data.get("dealer_coords"), dict)
        assert isinstance(data.get("registry_id_by_host"), dict)


def test_filter_options_omit_inline_geo_maps():
    from backend.db.inventory_db import get_filter_options

    opts = get_filter_options()
    assert opts.get("zip_coords") == {}
    assert opts.get("dealer_coords") == {}


def test_api_listings_cars_304_when_etag_matches():
    # Radius-scoped since 2026-09-28: a bare /api/listings/cars is a 400, never the
    # fleet (see test_listings_scoped_grid.py for the seeded contract tests).
    with app.test_client() as client:
        r1 = client.get("/api/listings/cars?zip=92694&radius=50")
        assert r1.status_code == 200
        etag = r1.headers.get("ETag")
        assert etag
        assert r1.get_json()["ok"] is True
        assert isinstance(r1.get_json()["cars"], list)
        r2 = client.get("/api/listings/cars?zip=92694&radius=50", headers={"If-None-Match": etag})
        assert r2.status_code == 304
        assert r2.get_data(as_text=True) == ""


def test_search_cars_by_make_model_pairs_empty():
    assert search_cars_by_make_model_pairs([]) == []


def test_search_cars_by_make_model_pairs_warm_under_200ms():
    import time

    from backend.db.inventory_db import _incomplete_car_ids_for_listings

    pairs = [("Toyota", "Camry"), ("Honda", "Accord")]
    _incomplete_car_ids_for_listings()
    t0 = time.perf_counter()
    cars = search_cars_by_make_model_pairs(pairs, sql_limit=60)
    elapsed = time.perf_counter() - t0
    assert isinstance(cars, list)
    assert elapsed < 0.2, f"warm make/model search too slow: {elapsed:.2f}s"


def test_listings_grid_cold_build_under_one_second():
    import time

    from backend.db.inventory_db import clear_inventory_listings_cache, listings_grid_serialized_cars

    clear_inventory_listings_cache()
    t0 = time.perf_counter()
    cars = listings_grid_serialized_cars()
    elapsed = time.perf_counter() - t0
    assert isinstance(cars, list)
    assert elapsed < 1.0, f"cold grid build too slow: {elapsed:.2f}s"


def test_listings_grid_photo_count_matches_full_gallery() -> None:
    from backend.utils.car_serialize import serialize_car_for_listings_grid

    urls = [f"https://cdn.example.com/inventory/{i}.jpg" for i in range(27)]
    car = {
        "id": 99,
        "title": "2022 Toyota Tacoma",
        "make": "Toyota",
        "model": "Tacoma",
        "trim": "SR5",
        "price": 28397,
        "mileage": 77007,
        "fuel_type": "Gasoline",
        "drivetrain": "4WD",
        "body_style": "Truck Double Cab",
        "gallery": json.dumps(urls),
        "image_url": urls[0],
    }
    out = serialize_car_for_listings_grid(car)
    assert len(out["gallery"]) <= 4
    assert out["photo_count"] == 27


def test_listings_grid_serializer_has_filter_fields():
    from backend.utils.car_serialize import serialize_car_for_listings_grid

    car = {
        "id": 1,
        "title": "2024 Test Car",
        "make": "Toyota",
        "model": "Camry",
        "trim": "LE",
        "price": 25000,
        "mileage": 1000,
        "fuel_type": "Gas",
        "cylinders": 4,
        "transmission": "Automatic",
        "drivetrain": "FWD",
        "body_style": "Sedan",
        "exterior_color": "Red",
        "interior_color": "Black",
        "packages": '{"packages_normalized":[{"canonical_name":"Premium Pkg"}]}',
        "gallery": '["https://example.com/a.jpg","https://example.com/b.jpg"]',
    }
    out = serialize_car_for_listings_grid(car)
    assert out["package_names"] == ["Premium Pkg"]
    assert "packages" not in out
    assert len(out["gallery"]) <= 4
    assert out["photo_count"] == 2
    assert out["exterior_color_families"]
    assert out["interior_color_families"]
    assert out["condition"] == "Used"


def test_listings_inventory_condition_helpers() -> None:
    from backend.utils.car_serialize import (
        listings_inventory_is_new,
        listings_inventory_is_pre_owned,
        serialize_car_for_listings_grid,
    )

    assert listings_inventory_is_new("New")
    assert not listings_inventory_is_pre_owned("New")

    assert listings_inventory_is_pre_owned("Used")
    assert listings_inventory_is_pre_owned("Certified Pre-Owned")
    assert listings_inventory_is_pre_owned("Pre-Owned")
    assert not listings_inventory_is_new("Used")

    new_row = serialize_car_for_listings_grid(
        {
            "id": 2,
            "title": "New 2025 Toyota Camry",
            "year": 2025,
            "make": "Toyota",
            "model": "Camry",
            "price": 30000,
            "mileage": 0,
            "source_url": "https://dealer.example.com/new-inventory/index.htm",
        }
    )
    assert new_row["condition"] == "New"
    assert listings_inventory_is_new(new_row["condition"])


def test_listings_grid_serializer_includes_dealership_registry_id():
    from backend.utils.car_serialize import serialize_car_for_listings_grid

    out = serialize_car_for_listings_grid(
        {"id": 1, "title": "Test", "price": 1, "dealership_registry_id": 42}
    )
    assert out["dealership_registry_id"] == 42


# ---------------------------------------------------------------------------
# Grid rebuild policy. The cache used to key on a 60s time bucket, so the whole
# fleet was re-serialized every minute forever whether or not anything changed.
# ---------------------------------------------------------------------------


def _seed_grid_fleet(sqlite_inventory, n: int = 12) -> None:
    sqlite_inventory.add_cars(
        [
            {
                "title": f"2022 Toyota Camry #{i}",
                "year": 2022,
                "make": "Toyota",
                "model": "Camry",
                "trim": "LE",
                "price": 20000 + i,
                "mileage": 1000 + i,
                "image_url": f"https://cdn.example.com/{i}.jpg",
                "gallery": json.dumps([f"https://cdn.example.com/{i}-{k}.jpg" for k in range(3)]),
                "dealer_name": "Test Motors",
                "dealer_url": "https://dealer.example.com",
                "body_style": "Sedan",
                "fuel_type": "Gasoline",
                "exterior_color": "White",
                "interior_color": "Black",
            }
            for i in range(n)
        ]
    )


def test_grid_memo_reproduces_a_fresh_serialization(sqlite_inventory):
    """A memo hit must be indistinguishable from serializing the row again."""
    from backend.db.repositories import listings_repo as lr

    _seed_grid_fleet(sqlite_inventory)

    lr._clear_grid_serialize_memo()
    fresh = lr._build_grid_cars_uncached()          # cold memo: everything serialized
    memoized = lr._build_grid_cars_uncached()       # warm memo: everything reused
    lr._clear_grid_serialize_memo()
    fresh_again = lr._build_grid_cars_uncached()    # cold memo again

    assert len(fresh) == len(memoized) == len(fresh_again) > 0
    assert memoized == fresh_again
    # Reuse must be by reference, otherwise the memo is not saving the work.
    assert all(a is b for a, b in zip(fresh, memoized))


def test_grid_memo_key_tracks_every_stored_column():
    """Changing any stored value must produce a different memo key."""
    from backend.db.repositories import listings_repo as lr

    row = {"id": 1, "price": 100.0, "make": "Toyota", "gallery": "[]", "trim": None}
    base = lr._row_memo_digest(row)
    assert base is not None
    for col in row:
        mutated = dict(row)
        mutated[col] = "CHANGED"
        assert lr._row_memo_digest(mutated) != base, col


def test_grid_is_not_rebuilt_while_nothing_changes(sqlite_inventory):
    """
    The treadmill regression test.

    Simulates the Postgres shape: a data fingerprint that never moves (nothing
    written) alongside a legacy token that rolls over every 60 seconds. An hour
    of reads must not produce 60 full-fleet rebuilds.
    """
    from backend.db.repositories import listings_repo as lr

    _seed_grid_fleet(sqlite_inventory)
    lr.clear_inventory_listings_cache()

    builds = []
    real_build = lr._build_grid_cars_uncached
    real_fingerprint = lr._pg_grid_write_fingerprint
    real_legacy_token = lr._listings_cache_token
    real_monotonic = lr.time.monotonic
    fake = [real_monotonic()]

    def counted():
        builds.append(1)
        return real_build()

    lr._build_grid_cars_uncached = counted
    lr._pg_grid_write_fingerprint = lambda: (("cars", 1234),)
    # If the grid ever goes back to keying on a clock, this rolls once a minute.
    lr._listings_cache_token = lambda: (float(int(fake[0]) // 60), 0.0)
    try:
        lr.listings_grid_serialized_cars()
        assert len(builds) == 1, "cold start must build once"

        for _ in range(3600):
            fake[0] += 1.0
            lr.time.monotonic = lambda: fake[0]
            lr.listings_grid_serialized_cars()
            thread = lr._grid_cars_rebuild_thread
            if thread is not None:
                thread.join(timeout=30)
    finally:
        lr.time.monotonic = real_monotonic
        lr._build_grid_cars_uncached = real_build
        lr._pg_grid_write_fingerprint = real_fingerprint
        lr._listings_cache_token = real_legacy_token
        lr.clear_inventory_listings_cache()

    # Only the wall-clock ceiling may fire (price_drop_days_ago / deal-score
    # bands), never the old once-a-minute rebuild.
    ceiling_rebuilds = int(3600 // lr._GRID_MAX_CACHE_AGE_S)
    assert len(builds) - 1 <= ceiling_rebuilds, builds
    assert len(builds) - 1 < 60, "grid is back on the 60s rebuild treadmill"


def test_grid_rebuilds_when_the_data_fingerprint_moves(sqlite_inventory):
    """Invalidate-on-write: a changed fingerprint must produce exactly one rebuild."""
    from backend.db.repositories import listings_repo as lr

    _seed_grid_fleet(sqlite_inventory)
    lr.clear_inventory_listings_cache()

    builds = []
    real_build = lr._build_grid_cars_uncached
    real_fingerprint = lr._pg_grid_write_fingerprint
    real_monotonic = lr.time.monotonic
    counter = [1]

    def counted():
        builds.append(1)
        return real_build()

    lr._build_grid_cars_uncached = counted
    lr._pg_grid_write_fingerprint = lambda: (("cars", counter[0]),)
    try:
        lr.listings_grid_serialized_cars()
        assert len(builds) == 1

        # A write lands, and enough time has passed for the throttle to allow it.
        counter[0] = 2
        lr.time.monotonic = lambda: real_monotonic() + lr._GRID_MIN_REBUILD_INTERVAL_S + 1
        lr.listings_grid_serialized_cars()
        thread = lr._grid_cars_rebuild_thread
        if thread is not None:
            thread.join(timeout=30)
        assert len(builds) == 2, "a data change must trigger a rebuild"
    finally:
        lr.time.monotonic = real_monotonic
        lr._build_grid_cars_uncached = real_build
        lr._pg_grid_write_fingerprint = real_fingerprint
        lr.clear_inventory_listings_cache()


def test_grid_write_fingerprint_falls_back_off_postgres(sqlite_inventory):
    """Without the Postgres write counters the token must revert to the old one."""
    from backend.db.repositories import listings_repo as lr
    from backend.db.repositories.data_quality_repo import _listings_cache_token

    assert lr._pg_grid_write_fingerprint() is None
    assert lr._grid_cache_token() == _listings_cache_token()


def test_upsert_holds_no_connection_during_per_car_backfill(sqlite_inventory):
    """
    The scanner leak: ``upsert_vehicles`` used to resolve VIN->id on a connection
    it then held open for the whole per-car backfill loop (which does NHTSA
    network calls). That connection sat ``idle in transaction`` for as long as
    the loop ran, which is what parked the web app behind a queued CREATE INDEX.
    """
    import backend.db.incomplete_listings_db as ild
    import backend.scanner.database as scanner_db

    open_conns = []
    real_get_conn = scanner_db.get_conn

    class _Tracked:
        def __init__(self, inner):
            self._inner = inner
            open_conns.append(self)

        def close(self):
            if self in open_conns:
                open_conns.remove(self)
            return self._inner.close()

        def cursor(self, *a, **kw):
            return self._inner.cursor(*a, **kw)

        def __getattr__(self, name):
            return getattr(self._inner, name)

    def tracked_get_conn():
        return _Tracked(real_get_conn())

    open_during_backfill = []

    def fake_sync(car_id):
        open_during_backfill.append(len(open_conns))

    real_sync = ild.sync_incomplete_listing_for_car_id
    scanner_db.get_conn = tracked_get_conn
    ild.sync_incomplete_listing_for_car_id = fake_sync
    try:
        n = scanner_db.upsert_vehicles(
            [
                {
                    "vin": f"LEAKTESTVIN{i:06d}"[:17],
                    "title": f"2021 Honda Accord #{i}",
                    "year": 2021,
                    "make": "Honda",
                    "model": "Accord",
                    "price": 21000 + i,
                    "mileage": 500 + i,
                    "dealer_name": "Leak Motors",
                    "dealer_url": "https://leak.example.com",
                    "dealer_id": "leak",
                }
                for i in range(5)
            ]
        )
    finally:
        scanner_db.get_conn = real_get_conn
        ild.sync_incomplete_listing_for_car_id = real_sync

    assert n == 5
    assert open_during_backfill, "post-upsert per-car loop did not run"
    assert set(open_during_backfill) == {0}, (
        f"upsert_vehicles held {max(open_during_backfill)} scanner connection(s) open "
        "across the per-car backfill loop"
    )
    assert open_conns == [], "upsert_vehicles leaked a connection"
