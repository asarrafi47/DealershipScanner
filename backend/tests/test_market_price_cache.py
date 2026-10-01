"""Cohort cache freshness and size bound (audit 2026-10-01, B5)."""
from __future__ import annotations

import pytest

from backend.utils import market_price as mp


def _row(price: float) -> dict:
    return {
        "make": "Toyota",
        "model": "Camry",
        "trim": "SE",
        "year": 2022,
        "mileage": 10000,
        "price": price,
        "zip_code": "",
        "dealer_url": "",
    }


@pytest.fixture
def loads(monkeypatch):
    """Count full scans and let the test swap the inventory between them."""
    mp._cohort_cache.clear()
    state = {"calls": 0, "rows": [_row(30000), _row(31000), _row(32000)]}

    def fake_load():
        state["calls"] += 1
        return list(state["rows"])

    monkeypatch.setattr(mp, "_load_active_listing_rows", fake_load)
    monkeypatch.setattr(mp, "_filter_cars_by_geo", lambda cars, **_: (cars, "test region"))
    clock = {"now": 1000.0}
    monkeypatch.setattr(mp.time, "monotonic", lambda: clock["now"])
    state["clock"] = clock
    yield state
    mp._cohort_cache.clear()


def _postgres_mode(monkeypatch, mtime_values):
    import backend.db.inventory_db as inv

    monkeypatch.setattr(inv, "is_inventory_postgres", lambda: True)
    it = iter(mtime_values)
    monkeypatch.setattr(mp, "_inventory_db_mtime", lambda: next(it))


def test_expired_entry_refreshes_under_postgres(monkeypatch, loads):
    monkeypatch.setenv("MARKET_PRICE_CACHE_TTL_SEC", "60")
    # The SQLite file mtime must not matter in Postgres mode: make it raise if read.
    _postgres_mode(monkeypatch, [])

    first = mp.get_cohort_index()
    assert mp.get_cohort_index() is first
    assert loads["calls"] == 1

    loads["rows"] = [_row(40000), _row(41000), _row(42000)]
    loads["clock"]["now"] += 61
    fresh = mp.get_cohort_index()
    assert loads["calls"] == 2
    assert fresh is not first
    assert fresh.groups[("toyota", "camry", "se", 2022, "0-25k")] == [40000, 41000, 42000]


def test_postgres_mode_ignores_sqlite_mtime(monkeypatch, loads):
    import backend.db.inventory_db as inv

    monkeypatch.setattr(inv, "is_inventory_postgres", lambda: True)
    mtimes = iter([1.0, 2.0, 3.0])
    monkeypatch.setattr(mp, "_inventory_db_mtime", lambda: next(mtimes))
    mp.get_cohort_index()
    mp.get_cohort_index()
    assert loads["calls"] == 1
    assert mp._inventory_version() is None


def test_sqlite_mtime_change_still_invalidates(monkeypatch, loads):
    import backend.db.inventory_db as inv

    monkeypatch.setattr(inv, "is_inventory_postgres", lambda: False)
    mtime = {"v": 1.0}
    monkeypatch.setattr(mp, "_inventory_db_mtime", lambda: mtime["v"])
    mp.get_cohort_index()
    mp.get_cohort_index()
    assert loads["calls"] == 1
    mtime["v"] = 2.0
    mp.get_cohort_index()
    assert loads["calls"] == 2


def test_cache_size_is_bounded_lru(monkeypatch, loads):
    import backend.db.inventory_db as inv

    monkeypatch.setattr(inv, "is_inventory_postgres", lambda: True)
    monkeypatch.setenv("MARKET_PRICE_CACHE_MAX", "2")

    mp.get_cohort_index(zip_code="28202", radius_miles=25)
    mp.get_cohort_index(zip_code="30301", radius_miles=25)
    mp.get_cohort_index(zip_code="28202", radius_miles=25)  # touch: now most recent
    mp.get_cohort_index(zip_code="37402", radius_miles=25)  # evicts 30301
    assert len(mp._cohort_cache) == 2
    assert set(mp._cohort_cache) == {("28202", 25.0), ("37402", 25.0)}
    assert loads["calls"] == 3

    mp.get_cohort_index(zip_code="30301", radius_miles=25)
    assert loads["calls"] == 4
    assert len(mp._cohort_cache) == 2


def test_cache_settings_fall_back_on_bad_env(monkeypatch):
    monkeypatch.setenv("MARKET_PRICE_CACHE_TTL_SEC", "soon")
    monkeypatch.setenv("MARKET_PRICE_CACHE_MAX", "lots")
    assert mp._cache_ttl_sec() == mp._DEFAULT_CACHE_TTL_SEC
    assert mp._cache_max_entries() == mp._DEFAULT_CACHE_MAX_ENTRIES
