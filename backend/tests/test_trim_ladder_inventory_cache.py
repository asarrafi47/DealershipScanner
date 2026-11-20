"""Car-page trim ladder query: spelling narrowing + TTL cache (2026-11-20).

``_inventory_trim_rows`` read every active row (LOWER(TRIM(make)) defeats every index:
120-175 ms per car page locally). On Postgres it now adds ``make IN / model IN`` with the
literal spellings of the pair (served by ``idx_cars_active_facet_combo``) and caches the
rows for a TTL. The narrowing must never change the rows: the reference is the
unnarrowed query on the same data.
"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager

import pytest

from backend.enrichment.trim_ladder import inventory as inv


@pytest.fixture()
def sample(monkeypatch):
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE cars (make TEXT, model TEXT, year INT, trim TEXT, price REAL, listing_active INT)"
    )
    rows = []
    for make in ("Honda", "HONDA", "honda ", "Toyota"):
        for model in ("Civic", "CIVIC", "Accord"):
            for i, trim in enumerate(("LX", "lx", "EX", "Sport", " ", None)):
                rows.append((make, model, 2018 + i, trim, 20000 + 1000 * i, 1))
                rows.append((make, model, 2020, trim, 0, 1))
                rows.append((make, model, 2021, trim, 30000, 0))
    conn.executemany("INSERT INTO cars VALUES (?,?,?,?,?,?)", rows)

    class _W:
        def cursor(self):
            return conn.cursor()

    @contextmanager
    def fake_db_conn(**_kw):
        yield _W()

    monkeypatch.setattr("backend.db.inventory_db.db_conn", fake_db_conn)
    inv.clear_inventory_trim_rows_cache()
    yield conn
    inv.clear_inventory_trim_rows_cache()


@pytest.mark.parametrize(
    ("make", "model", "lo", "hi"),
    [("Honda", "Civic", 0, 9999), ("honda", "civic", 2018, 2021), ("Toyota", "Accord", 2019, 2022), ("BMW", "X5", 0, 9999)],
)
def test_narrowed_query_returns_the_same_rows(sample, make, model, lo, hi):
    spellings = inv._active_pair_spellings(60.0).get(inv._pair_key(make, model))
    plain = inv._query_inventory_trim_rows(make, model, lo, hi)
    narrowed = inv._query_inventory_trim_rows(make, model, lo, hi, spellings)
    assert narrowed == plain


def test_spelling_map_collects_every_literal_spelling(sample):
    m = inv._active_pair_spellings(60.0)
    makes, models = m[("honda", "civic")]
    assert set(makes) == {"Honda", "HONDA", "honda "} and set(models) == {"Civic", "CIVIC"}


def test_postgres_path_caches_by_normalised_arguments(sample, monkeypatch):
    monkeypatch.setattr("backend.db.inventory_pg.is_inventory_postgres", lambda: True)
    monkeypatch.delenv("TRIM_LADDER_INVENTORY_CACHE_TTL_S", raising=False)
    calls = []
    real = inv._query_inventory_trim_rows

    def counting(*a):
        calls.append(a)
        return real(*a)

    monkeypatch.setattr(inv, "_query_inventory_trim_rows", counting)
    first = inv._inventory_trim_rows("Honda", "Civic", 2018, 2022)
    again = inv._inventory_trim_rows("HONDA ", "civic", 2018, 2022)
    assert first == again and len(calls) == 1
    assert calls[0][4] is not None  # the spelling narrowing was used
    again.append("caller mutation")
    assert inv._inventory_trim_rows("Honda", "Civic", 2018, 2022) == first  # callers get copies


def test_cache_is_bounded(sample, monkeypatch):
    monkeypatch.setattr("backend.db.inventory_pg.is_inventory_postgres", lambda: True)
    monkeypatch.setattr(inv, "_TRIM_ROWS_CACHE_MAX", 3)
    monkeypatch.setattr(inv, "_query_inventory_trim_rows", lambda *a: [])
    for y in range(10):
        inv._inventory_trim_rows("Honda", "Civic", y, y)
    assert len(inv._trim_rows_cache) == 3


def test_ttl_zero_and_sqlite_never_cache(sample, monkeypatch):
    calls = []
    monkeypatch.setattr(inv, "_query_inventory_trim_rows", lambda *a: calls.append(a) or [])
    inv._inventory_trim_rows("Honda", "Civic", 0, 1)
    inv._inventory_trim_rows("Honda", "Civic", 0, 1)
    assert len(calls) == 2  # not Postgres
    monkeypatch.setattr("backend.db.inventory_pg.is_inventory_postgres", lambda: True)
    monkeypatch.setenv("TRIM_LADDER_INVENTORY_CACHE_TTL_S", "0")
    inv._inventory_trim_rows("Honda", "Civic", 0, 1)
    inv._inventory_trim_rows("Honda", "Civic", 0, 1)
    assert len(calls) == 4 and all(len(a) == 4 for a in calls)  # and no narrowing


def test_spelling_map_failure_falls_back_to_the_plain_query(sample, monkeypatch):
    monkeypatch.setattr("backend.db.inventory_pg.is_inventory_postgres", lambda: True)

    def boom(ttl):
        raise RuntimeError("db hiccup")

    monkeypatch.setattr(inv, "_active_pair_spellings", boom)
    rows = inv._inventory_trim_rows("Honda", "Civic", 0, 9999)
    assert rows == inv._query_inventory_trim_rows("Honda", "Civic", 0, 9999)
