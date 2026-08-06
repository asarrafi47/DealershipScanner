"""An SRP-only rescan must not demote a row's ``data_quality_score``.

``compute_data_quality_score`` is called on the INCOMING scanner payload, and the
result used to be assigned straight from ``excluded.*``. Every column the score
reads is keep-if-nonempty in the same ON CONFLICT clause, so the stored row keeps
its colors / transmission / drivetrain / mpg after a sparse rescan -- but the
score written over them was computed as if those fields were gone.

Measured on a seeded row: 95.37 -> 54.63 while exterior_color and transmission
were both still present in the row. ``backend/utils/hybrid_search.py`` sorts
search results on ``data_quality_score`` (three call sites), so the row is
demoted for data it still has. A live sample of 8,000 active rows found 182
(2.3%) whose stored score was lower than a re-computation from their own stored
columns, 14 of them by 20 points or more.
"""

from __future__ import annotations

import sqlite3

from backend.scanner.database import upsert_vehicles
from backend.utils.field_clean import compute_data_quality_score

VIN = "1FTFW1ET5DFA00003"

RICH = {
    "vin": VIN,
    "title": "2022 Ford F-150 XLT",
    "year": 2022,
    "make": "Ford",
    "model": "F-150",
    "trim": "XLT",
    "price": 45000,
    "mileage": 12000,
    "condition": "Used",
    "exterior_color": "Oxford White",
    "interior_color": "Black",
    "transmission": "10-Speed Automatic",
    "drivetrain": "4WD",
    "fuel_type": "Gasoline",
    "cylinders": 6,
    "engine_description": "3.5L V6 EcoBoost",
    "body_style": "Pickup",
    "mpg_city": 18,
    "mpg_highway": 24,
    "image_url": "https://example.com/hero.jpg",
}

SPARSE = {
    "vin": VIN,
    "title": "2022 Ford F-150 XLT",
    "year": 2022,
    "make": "Ford",
    "model": "F-150",
    "price": 44000,
}


def _setup(tmp_path, monkeypatch):
    db_path = tmp_path / "inventory.db"
    monkeypatch.setenv("INVENTORY_DB_PATH", str(db_path))
    from backend.db import inventory_db as inv

    monkeypatch.setattr(inv, "DB_PATH", str(db_path))
    return db_path


def _row(db_path):
    conn = sqlite3.connect(str(db_path))
    try:
        return conn.execute(
            "SELECT data_quality_score, exterior_color, transmission, drivetrain, "
            "mpg_city, mpg_highway FROM cars WHERE vin=?",
            (VIN,),
        ).fetchone()
    finally:
        conn.close()


def test_sparse_rescan_keeps_the_quality_score_the_row_earned(tmp_path, monkeypatch):
    db_path = _setup(tmp_path, monkeypatch)
    upsert_vehicles([dict(RICH)])
    rich_score = _row(db_path)[0]
    assert rich_score > 90

    upsert_vehicles([dict(SPARSE)])
    score, ext, trans, drive, mpg_c, mpg_h = _row(db_path)

    # The fields the score is computed from are all still stored...
    assert (ext, trans, drive, mpg_c, mpg_h) == ("Oxford White", "10-Speed Automatic", "4WD", 18, 24)
    # ...so the score must not fall.
    assert score == rich_score, (
        f"data_quality_score fell {rich_score} -> {score} on a rescan that kept every "
        "field the score is computed from"
    )


def test_score_still_rises_when_the_row_gets_richer(tmp_path, monkeypatch):
    db_path = _setup(tmp_path, monkeypatch)
    upsert_vehicles([dict(SPARSE)])
    sparse_score = _row(db_path)[0]

    upsert_vehicles([dict(RICH)])
    score = _row(db_path)[0]

    assert score > sparse_score
    assert score == compute_data_quality_score(RICH)
