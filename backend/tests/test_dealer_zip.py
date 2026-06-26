"""Tests for dealership ZIP normalization and car backfill."""

from __future__ import annotations

import sqlite3

from backend.utils.dealer_zip import (
    coalesce_car_zip,
    enrich_vehicle_zip_from_dealership,
    lookup_dealership_zip,
    normalize_us_zip,
)


def test_normalize_us_zip() -> None:
    assert normalize_us_zip("29714-8803") == "29714"
    assert normalize_us_zip("NC 28212") == "28212"
    assert normalize_us_zip("nan") is None
    assert normalize_us_zip("") is None


def test_coalesce_car_zip_prefers_vehicle() -> None:
    assert coalesce_car_zip(car_zip="90210", dealer_zip="28173") == "90210"
    assert coalesce_car_zip(car_zip=None, dealer_zip="28173-1234") == "28173"


def test_enrich_vehicle_zip_from_registry(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "inv.db"
    monkeypatch.setenv("INVENTORY_DB_PATH", str(db_path))
    monkeypatch.setenv("INVENTORY_SQLITE_TESTS", "1")
    from backend.db import inventory_db as inv

    inv.DB_PATH = str(db_path)
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE dealerships (
            id INTEGER PRIMARY KEY,
            name TEXT,
            website_url TEXT,
            dealer_website_url TEXT,
            zip_code TEXT
        )
        """
    )
    conn.execute(
        "INSERT INTO dealerships (id, name, website_url, zip_code) VALUES (1, 'Test Dealer', ?, ?)",
        ("https://example-dealer.com", "28173"),
    )
    conn.execute(
        """
        CREATE TABLE cars (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vin TEXT UNIQUE,
            dealer_url TEXT,
            dealership_registry_id INTEGER,
            zip_code TEXT
        )
        """
    )
    conn.commit()
    cur = conn.cursor()

    vehicle = {
        "vin": "1" * 17,
        "dealer_url": "https://www.example-dealer.com",
        "dealership_registry_id": 1,
    }
    enrich_vehicle_zip_from_dealership(vehicle, cur)
    assert vehicle["zip_code"] == "28173"

    z = lookup_dealership_zip(
        cur,
        registry_id=1,
        dealer_url="https://www.example-dealer.com",
    )
    assert z == "28173"


def test_upsert_stamps_dealer_zip(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "inv.db"
    monkeypatch.setenv("INVENTORY_DB_PATH", str(db_path))
    monkeypatch.setenv("INVENTORY_SQLITE_TESTS", "1")
    from backend.db import inventory_db as inv
    from backend.scanner.database import upsert_vehicles

    inv.DB_PATH = str(db_path)
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE dealerships (
            id INTEGER PRIMARY KEY,
            name TEXT,
            website_url TEXT,
            dealer_website_url TEXT,
            zip_code TEXT
        )
        """
    )
    conn.execute(
        "INSERT INTO dealerships (id, name, website_url, zip_code) VALUES (5, 'Lot', ?, ?)",
        ("https://lot.example.com", "90210"),
    )
    conn.commit()
    conn.close()

    vin = "2" * 17
    upsert_vehicles(
        [
            {
                "vin": vin,
                "title": "2024 Test Car",
                "year": 2024,
                "make": "Test",
                "model": "Car",
                "price": 10000,
                "dealer_url": "https://lot.example.com",
                "dealership_registry_id": 5,
            }
        ]
    )

    conn = sqlite3.connect(str(db_path))
    row = conn.execute("SELECT zip_code FROM cars WHERE vin=?", (vin,)).fetchone()
    assert row is not None
    assert row[0] == "90210"
