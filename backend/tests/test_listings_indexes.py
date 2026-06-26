"""Listings-oriented cars table indexes and grid column projection."""

from __future__ import annotations

import sqlite3

import pytest

from backend.db.inventory_db import (
    DB_PATH,
    LISTINGS_GRID_CAR_COLUMNS,
    ensure_cars_listings_indexes,
    get_conn,
    init_inventory_db,
    is_inventory_postgres,
)


@pytest.mark.skipif(is_inventory_postgres(), reason="SQLite index names")
def test_listings_indexes_created() -> None:
    init_inventory_db()
    conn = get_conn()
    cur = conn.cursor()
    ensure_cars_listings_indexes(cur)
    conn.commit()
    names = {
        r[0]
        for r in cur.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='cars'"
        ).fetchall()
    }
    conn.close()
    for expected in (
        "idx_cars_active_price",
        "idx_cars_active_make",
        "idx_cars_active_facet_combo",
        "idx_cars_active_registry",
        "idx_cars_active_zip",
        "idx_cars_active_packages",
    ):
        assert expected in names


@pytest.mark.skipif(is_inventory_postgres(), reason="SQLite EXPLAIN")
def test_active_price_query_uses_partial_index() -> None:
    init_inventory_db()
    conn = get_conn()
    cur = conn.cursor()
    ensure_cars_listings_indexes(cur)
    conn.commit()
    plan = " ".join(
        r[3]
        for r in cur.execute(
            """
            EXPLAIN QUERY PLAN
            SELECT id, price FROM cars
            WHERE COALESCE(listing_active, 1) = 1
            ORDER BY price ASC
            LIMIT 50
            """
        ).fetchall()
    )
    conn.close()
    assert "idx_cars_active_price" in plan or "USING INDEX" in plan


def test_listings_grid_columns_exclude_heavy_blobs() -> None:
    assert "spec_source_json" not in LISTINGS_GRID_CAR_COLUMNS
    assert "kbb_fetched_at" not in LISTINGS_GRID_CAR_COLUMNS
    assert "description" not in LISTINGS_GRID_CAR_COLUMNS
    assert "id" in LISTINGS_GRID_CAR_COLUMNS
    assert "packages" in LISTINGS_GRID_CAR_COLUMNS

    conn = sqlite3.connect(DB_PATH)
    existing = {r[1] for r in conn.execute("PRAGMA table_info(cars)").fetchall()}
    conn.close()
    missing = [c for c in LISTINGS_GRID_CAR_COLUMNS if c not in existing]
    assert missing == [], f"unknown columns: {missing}"
