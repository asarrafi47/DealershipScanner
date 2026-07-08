"""360 spin assets: spin_frames / interior_pano round-trip upsert -> read -> serialize."""

from __future__ import annotations

import json
import sqlite3

import pytest

from backend.scanner.database import upsert_vehicles
from backend.utils.car_serialize import serialize_car_for_api


@pytest.fixture(autouse=True)
def _no_dotenv(monkeypatch):
    """Keep this test hermetic: lazy imports inside upsert_vehicles (e.g. the
    dictionary enrichment path) call load_project_dotenv(), which restores
    INVENTORY_DATABASE_URL from .env mid-test and silently points subsequent
    reads at the production Postgres. No-op it so every connection stays on
    the tmp SQLite DB."""
    monkeypatch.setattr(
        "backend.utils.project_env.load_project_dotenv", lambda *a, **k: None
    )

SPIN_FRAMES = [
    f"https://cdn.impel.io/swipetospin-viewers/dealer/VIN/123/frames/sp-{i}.jpg"
    for i in range(1, 9)
]
INTERIOR_PANO = "https://cdn.impel.io/swipetospin-viewers/dealer/VIN/123/pano/pano.jpg"


def _isolated_db(tmp_path, monkeypatch):
    db_path = tmp_path / "test.db"
    monkeypatch.setenv("INVENTORY_DB_PATH", str(db_path))
    from backend.db import inventory_db as inv

    # inv.DB_PATH is resolved once at import time; patch the module attribute so
    # every read/write in this test hits the tmp file, never the real DB.
    monkeypatch.setattr(inv, "DB_PATH", str(db_path))
    return db_path, inv


def test_spin_assets_roundtrip_upsert_read_serialize(tmp_path, monkeypatch):
    db_path, inv = _isolated_db(tmp_path, monkeypatch)
    vin = "5" * 17
    upsert_vehicles(
        [
            {
                "vin": vin,
                "title": "2024 BMW X5",
                "year": 2024,
                "make": "BMW",
                "model": "X5",
                "price": 65000,
                "spin_frames": SPIN_FRAMES,
                "interior_pano": INTERIOR_PANO,
            }
        ]
    )

    # Stored as JSON text in the column.
    conn = sqlite3.connect(str(db_path))
    row = conn.execute(
        "SELECT id, spin_frames, interior_pano FROM cars WHERE vin=?", (vin,)
    ).fetchone()
    conn.close()
    assert row is not None
    assert json.loads(row[1]) == SPIN_FRAMES
    assert row[2] == INTERIOR_PANO

    # Row read decodes spin_frames to a list.
    car = inv.get_car_by_id(row[0])
    assert car is not None
    assert car["spin_frames"] == SPIN_FRAMES
    assert car["interior_pano"] == INTERIOR_PANO

    # API serialization emits the contract fields.
    out = serialize_car_for_api(car, include_verified=False)
    assert out["spin_frames"] == SPIN_FRAMES
    assert out["interior_pano"] == INTERIOR_PANO


def test_spin_assets_defaults_when_absent(tmp_path, monkeypatch):
    db_path, inv = _isolated_db(tmp_path, monkeypatch)
    vin = "6" * 17
    upsert_vehicles(
        [{"vin": vin, "title": "2023 Kia EV6", "year": 2023, "make": "Kia", "model": "EV6"}]
    )

    car = inv.get_car_by_vin(vin)
    assert car is not None
    assert car["spin_frames"] == []
    assert car["interior_pano"] is None

    out = serialize_car_for_api(car, include_verified=False)
    # Must be contract defaults, not the display em-dash.
    assert out["spin_frames"] == []
    assert out["interior_pano"] is None


def test_spin_assets_kept_when_reupsert_lacks_them(tmp_path, monkeypatch):
    db_path, inv = _isolated_db(tmp_path, monkeypatch)
    vin = "7" * 17
    base = {"vin": vin, "title": "2024 Volvo XC90", "year": 2024, "make": "Volvo", "model": "XC90"}
    upsert_vehicles([{**base, "spin_frames": SPIN_FRAMES, "interior_pano": INTERIOR_PANO}])
    # Second scan without spin assets must not wipe the stored ones.
    upsert_vehicles([base])

    car = inv.get_car_by_vin(vin)
    assert car is not None
    assert car["spin_frames"] == SPIN_FRAMES
    assert car["interior_pano"] == INTERIOR_PANO
