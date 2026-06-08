"""Persist inventory card lot location into spec_source_json on upsert."""

from __future__ import annotations

import json
import sqlite3

from backend.scanner.database import upsert_vehicles


def test_upsert_persists_inventory_lot_location(tmp_path, monkeypatch):
    db_path = tmp_path / "test.db"
    monkeypatch.setenv("INVENTORY_DB_PATH", str(db_path))
    from backend.db import inventory_db as inv

    inv.DB_PATH = str(db_path)

    vin = "1" * 17
    upsert_vehicles(
        [
            {
                "vin": vin,
                "title": "2024 BMW X5",
                "year": 2024,
                "make": "BMW",
                "model": "X5",
                "price": 65000,
                "_lot_location": "Parks Luxury of Roanoke",
            }
        ]
    )

    conn = sqlite3.connect(str(db_path))
    row = conn.execute("SELECT spec_source_json FROM cars WHERE vin=?", (vin,)).fetchone()
    assert row is not None
    spec = json.loads(row[0] or "{}")
    lot = spec.get("inventory_lot_location") or {}
    assert lot.get("value") == "Parks Luxury of Roanoke"
