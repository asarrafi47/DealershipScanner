"""Radius filtering must not surface distant dealers via URL mismatch or missing geo."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

import backend.db.inventory_db as inventory_db
from backend.db.dealer_geo import (
    build_dealer_geo_index,
    dealer_coords_client_map,
    load_dealer_geo_index,
    lookup_dealer_coords,
    normalize_dealer_host,
)
from backend.db.inventory_db import init_inventory_db, listings_geo_coords_maps, search_cars


def _ensure_dealer_geopoints(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS dealer_geopoints (
            dealer_url TEXT PRIMARY KEY,
            dealer_name TEXT,
            lat REAL,
            lon REAL,
            zip_code TEXT,
            city TEXT,
            state TEXT,
            geocode_source TEXT,
            geocoded_at TEXT
        )
        """
    )


def test_normalize_dealer_host_strips_www() -> None:
    assert normalize_dealer_host("https://WWW.tonychevrolethilo.com/") == "tonychevrolethilo.com"


def test_lookup_dealer_coords_host_fallback() -> None:
    geo = build_dealer_geo_index(
        [("https://www.tonychevrolethilo.com", 19.72, -155.09)]
    )
    assert lookup_dealer_coords("https://tonychevrolethilo.com", geo) == (19.72, -155.09)
    assert lookup_dealer_coords("https://www.tonychevrolethilo.com/inventory", geo) == (
        19.72,
        -155.09,
    )


def test_search_cars_radius_excludes_hawaii_dealer_from_california_zip(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """92694 (CA) + 100 mi must not return Hilo HI inventory when geopoints exist."""
    dbp = tmp_path / "inv_geo_radius.db"
    monkeypatch.setattr(inventory_db, "DB_PATH", str(dbp))
    inventory_db.clear_inventory_listings_cache()
    init_inventory_db()
    conn = sqlite3.connect(str(dbp))
    _ensure_dealer_geopoints(conn)
    cur = conn.cursor()
    now = "2026-01-01T00:00:00Z"
    cur.execute(
        """
        INSERT INTO dealer_geopoints (dealer_url, dealer_name, lat, lon, geocode_source)
        VALUES (?, ?, ?, ?, ?)
        """,
        ("https://www.tonychevrolethilo.com", "Tony Chevrolet Hilo", 19.72, -155.09, "test"),
    )
    cur.execute(
        """
        INSERT INTO cars (
            vin, title, year, make, model, trim, price, mileage,
            image_url, dealer_name, dealer_url, dealer_id, scraped_at,
            fuel_type, cylinders, transmission, drivetrain,
            exterior_color, interior_color, interior_color_buckets, stock_number, gallery,
            listing_active, listing_removed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "HAWAIIHIHIHIHIHIHI",
            "Used 2024 Chevy Silverado",
            2024,
            "Chevrolet",
            "Silverado",
            "LT",
            45000,
            5000,
            "https://example.com/a.jpg",
            "Tony Chevrolet Hilo",
            "https://tonychevrolethilo.com",
            "tonychevrolethilo-com",
            now,
            "Gas",
            8,
            "Automatic",
            "4WD",
            "Black",
            "Black",
            "[]",
            "S1",
            "[]",
            1,
            None,
        ),
    )
    cur.execute(
        """
        INSERT INTO cars (
            vin, title, year, make, model, trim, price, mileage,
            image_url, dealer_name, dealer_url, dealer_id, scraped_at,
            fuel_type, cylinders, transmission, drivetrain,
            exterior_color, interior_color, interior_color_buckets, stock_number, gallery,
            listing_active, listing_removed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "CACACACACACACACACA",
            "Used 2024 Toyota Camry",
            2024,
            "Toyota",
            "Camry",
            "LE",
            28000,
            8000,
            "https://example.com/b.jpg",
            "Local Toyota",
            "https://www.toyotaoforange.com",
            "toyotaoforange-com",
            now,
            "Gas",
            4,
            "Automatic",
            "FWD",
            "White",
            "Black",
            "[]",
            "S2",
            "[]",
            1,
            None,
        ),
    )
    cur.execute(
        """
        INSERT INTO dealer_geopoints (dealer_url, dealer_name, lat, lon, geocode_source)
        VALUES (?, ?, ?, ?, ?)
        """,
        ("https://www.toyotaoforange.com", "Toyota of Orange", 33.79, -117.85, "test"),
    )
    conn.commit()
    conn.close()

    rows = search_cars(zip_code="92694", radius_miles=100)
    vins = {c["vin"] for c in rows}
    assert "HAWAIIHIHIHIHIHIHI" not in vins
    assert "CACACACACACACACACA" in vins


def test_listings_geo_coords_includes_host_keys(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    dbp = tmp_path / "inv_geo_maps.db"
    monkeypatch.setattr(inventory_db, "DB_PATH", str(dbp))
    inventory_db.clear_inventory_listings_cache()
    init_inventory_db()
    conn = sqlite3.connect(str(dbp))
    _ensure_dealer_geopoints(conn)
    conn.execute(
        """
        INSERT INTO dealer_geopoints (dealer_url, dealer_name, lat, lon, geocode_source)
        VALUES (?, ?, ?, ?, ?)
        """,
        ("https://www.tonychevrolethilo.com", "Tony Chevrolet Hilo", 19.72, -155.09, "test"),
    )
    conn.commit()
    conn.close()

    maps = listings_geo_coords_maps()
    dc = maps["dealer_coords"]
    assert dc.get("https://www.tonychevrolethilo.com") == [19.72, -155.09]
    assert dc.get("host:tonychevrolethilo.com") == [19.72, -155.09]


def test_dealer_coords_client_map_roundtrip() -> None:
    geo = build_dealer_geo_index([("https://dealer.test", 1.0, 2.0)])
    cm = dealer_coords_client_map(geo)
    assert cm["host:dealer.test"] == [1.0, 2.0]


def test_load_dealer_geo_index_from_connection(tmp_path: Path) -> None:
    dbp = tmp_path / "geo_load.db"
    conn = sqlite3.connect(str(dbp))
    conn.execute(
        """
        CREATE TABLE dealer_geopoints (
            dealer_url TEXT PRIMARY KEY,
            dealer_name TEXT,
            lat REAL,
            lon REAL,
            geocode_source TEXT
        )
        """
    )
    conn.execute(
        "INSERT INTO dealer_geopoints VALUES (?, ?, ?, ?, ?)",
        ("https://www.example-dealer.com", "Ex", 40.0, -75.0, "test"),
    )
    conn.commit()
    geo = load_dealer_geo_index(conn)
    conn.close()
    assert lookup_dealer_coords("https://example-dealer.com", geo) == (40.0, -75.0)
