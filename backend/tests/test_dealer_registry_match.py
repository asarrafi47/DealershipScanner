"""Dealership registry resolution for listings dealer filter."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

import backend.db.inventory_db as inventory_db
from backend.db.dealerships_db import ensure_dealerships_table
from backend.db.inventory_db import init_inventory_db, search_cars
from backend.listings.dealer_registry_match import (
    collect_registry_ids_from_cars,
    dealer_registry_sql_filter,
    registry_id_by_dealer_host,
    resolve_car_dealership_registry_id,
)


def test_resolve_car_registry_id_from_host() -> None:
    host_map = {"testtoyota.com": 1}
    rid = resolve_car_dealership_registry_id(
        {"dealer_url": "https://www.testtoyota.com/x", "dealership_registry_id": None},
        host_to_registry=host_map,
    )
    assert rid == 1
    assert (
        resolve_car_dealership_registry_id(
            {"dealer_url": "https://other.com/x", "dealership_registry_id": 2},
            host_to_registry=host_map,
        )
        == 2
    )


def test_collect_registry_ids_includes_host_only_cars() -> None:
    host_map = {"dealer-a.com": 1}
    cars = [
        {"dealership_registry_id": None, "dealer_url": "https://www.dealer-a.com/a"},
        {"dealership_registry_id": 99, "dealer_url": "https://other.com/b"},
    ]
    assert collect_registry_ids_from_cars(cars, host_map) == {1, 99}


def test_dealer_registry_sql_filter_shape() -> None:
    clause, params = dealer_registry_sql_filter(
        [1, 2],
        {"foo.com": 1, "bar.com": 2},
        placeholders_fn=lambda xs: ",".join("?" * len(xs)),
    )
    assert "dealership_registry_id" in clause
    assert "dealer_url" in clause
    assert 1 in params and 2 in params


def test_search_cars_dealer_filter_matches_host_only(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    dbp = tmp_path / "inv_reg_filter.db"
    monkeypatch.setattr(inventory_db, "DB_PATH", str(dbp))
    inventory_db._registry_backfill_ran = True
    init_inventory_db()
    conn = sqlite3.connect(str(dbp))
    cur = conn.cursor()
    ensure_dealerships_table(cur)
    cur.execute(
        """
        INSERT INTO dealerships (id, name, website_url, city, state, is_active)
        VALUES (1, 'Test Toyota', 'https://www.testtoyota.com', 'C', 'NC', 1)
        """
    )
    now = "2026-01-01T00:00:00Z"
    for vid, reg in (("AAAAAAAAAAAAAAAAA", None), ("BBBBBBBBBBBBBBBBB", 1)):
        cur.execute(
            """
            INSERT INTO cars (
                vin, title, year, make, model, trim, price, mileage,
                image_url, dealer_name, dealer_url, dealer_id, scraped_at,
                fuel_type, cylinders, transmission, drivetrain,
                exterior_color, interior_color, stock_number, gallery,
                listing_active, dealership_registry_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                vid,
                "t",
                2022,
                "Toyota",
                "Camry",
                "LE",
                25000,
                10000,
                "https://h/x.jpg",
                "Test Toyota",
                "https://www.testtoyota.com/inventory/" + vid[-1],
                "test-toyota",
                now,
                "G",
                4,
                "A",
                "F",
                "Black",
                "Black",
                "S1",
                "[]",
                1,
                reg,
            ),
        )
    conn.commit()
    conn.close()

    rows = search_cars(dealer_registry_ids=[1])
    assert len(rows) == 2
    assert {r["vin"] for r in rows} == {"AAAAAAAAAAAAAAAAA", "BBBBBBBBBBBBBBBBB"}


def test_registry_id_by_dealer_host_reads_dealerships_table(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    dbp = tmp_path / "inv_reg_host.db"
    monkeypatch.setattr(inventory_db, "DB_PATH", str(dbp))
    init_inventory_db()
    conn = sqlite3.connect(str(dbp))
    cur = conn.cursor()
    ensure_dealerships_table(cur)
    cur.execute(
        """
        INSERT INTO dealerships (id, name, website_url, city, state, is_active)
        VALUES (42, 'Demo', 'https://demo-dealer.example', 'X', 'NC', 1)
        """
    )
    conn.commit()
    host_map = registry_id_by_dealer_host(conn)
    conn.close()
    assert host_map.get("demo-dealer.example") == 42
