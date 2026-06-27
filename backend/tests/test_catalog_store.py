"""Tests for catalog_store option-row adapter."""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager

import pytest

from backend.db import inventory_db as idb
from backend.db.catalog_schema import ensure_catalog_tables
from backend.enrichment import catalog_store as cs


def test_fetch_catalog_option_rows_empty_without_db(monkeypatch) -> None:
    monkeypatch.setattr(cs, "_fetch_rows", lambda *args, **kwargs: [])
    assert cs.fetch_catalog_option_rows(2024, "Toyota", "Camry") == []


def test_fetch_catalog_option_rows_builds_csv_shape(monkeypatch) -> None:
    def fake_fetch(sql: str, params: tuple) -> list[dict]:
        if "catalog_trims" in sql:
            return [{"id": 1, "trim": "LE", "make": "Toyota", "model": "Camry"}]
        if "catalog_packages" in sql:
            return [{"package_name": "Tech Pkg", "feature_name": "Blind spot monitor"}]
        if "catalog_options" in sql:
            return [{"option_name": "Floor mats", "description": "All-weather"}]
        if "catalog_exterior_colors" in sql:
            return [{"color_name": "White"}]
        return []

    monkeypatch.setattr(cs, "_fetch_rows", fake_fetch)
    rows = cs.fetch_catalog_option_rows(2024, "Toyota", "Camry")
    assert len(rows) == 1
    assert rows[0]["Trim"] == "LE"
    assert "Tech Pkg" in rows[0]["Packages"]
    assert "Blind spot" in rows[0]["packageDetails"]


def test_fetch_catalog_option_rows_orders_options_and_colors_by_id(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    """Options/colors have no sort_order column; queries must ORDER BY id only."""
    db_path = tmp_path / "catalog_store.db"
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    ensure_catalog_tables(cur, postgres=False)
    cur.execute(
        "INSERT INTO catalog_trims (year, make, model, trim) VALUES (?, ?, ?, ?)",
        (2024, "Toyota", "Camry", "LE"),
    )
    vehicle_id = int(cur.lastrowid or 0)
    cur.execute(
        "INSERT INTO catalog_options (vehicle_id, option_name, description) VALUES (?, ?, ?)",
        (vehicle_id, "Second option", "Inserted first"),
    )
    cur.execute(
        "INSERT INTO catalog_options (vehicle_id, option_name, description) VALUES (?, ?, ?)",
        (vehicle_id, "First option", "Inserted second"),
    )
    cur.execute(
        "INSERT INTO catalog_exterior_colors (vehicle_id, color_name) VALUES (?, ?)",
        (vehicle_id, "Blue"),
    )
    cur.execute(
        "INSERT INTO catalog_exterior_colors (vehicle_id, color_name) VALUES (?, ?)",
        (vehicle_id, "Red"),
    )
    conn.commit()
    conn.close()

    @contextmanager
    def fake_db_conn(*, row_factory=None):
        c = sqlite3.connect(str(db_path))
        if row_factory is not None:
            c.row_factory = row_factory
        try:
            yield c
        finally:
            c.close()

    monkeypatch.setattr(idb, "db_conn", fake_db_conn)

    rows = cs.fetch_catalog_option_rows(2024, "Toyota", "Camry")
    assert len(rows) == 1
    assert rows[0]["Options"] == "Second option, First option"
    assert rows[0]["exteriorColors"] == "Blue, Red"
