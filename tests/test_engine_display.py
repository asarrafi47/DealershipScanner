"""Engine display (liters + layout) and displacement range search."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

import backend.db.inventory_db as inventory_db
from backend.db.inventory_db import init_inventory_db, search_cars
from backend.enrichment_service import ensure_enrichment_columns
from backend.utils.car_serialize import (
    build_engine_display,
    car_matches_engine_displacement_l_range,
    infer_engine_l_for_db,
    parse_engine_displacement_liters,
)


def test_build_engine_display_combines_liters_and_layout() -> None:
    car = {
        "engine_description": None,
        "engine_l": "4.4",
        "cylinders": 8,
        "fuel_type": "Gas",
    }
    assert build_engine_display(car, {}) == "4.4L V8"


def test_displacement_only_description_merges_verified_cylinders() -> None:
    """BMW listings often store ``2.0`` / ``2`` without layout — fold in EPA/decoder cylinders."""
    car = {
        "engine_description": "2.0",
        "engine_l": None,
        "cylinders": None,
        "fuel_type": "Gasoline",
    }
    vs = {"cylinders": 4, "cylinders_display": 4}
    assert build_engine_display(car, vs) == "2.0L I4"


def test_displacement_only_numeric_engine_l_uses_verified_cylinders() -> None:
    car = {
        "engine_description": None,
        "engine_l": "2",
        "cylinders": None,
        "fuel_type": "Gas",
    }
    vs = {"cylinders_display": 4}
    assert build_engine_display(car, vs) == "2.0L I4"


def test_rich_engine_description_still_wins() -> None:
    car = {
        "engine_description": "2.0L BMW TwinPower Turbo inline 4-cylinder",
        "engine_l": None,
        "cylinders": None,
        "fuel_type": "Gas",
    }
    out = build_engine_display(car, {"cylinders": 4})
    assert "TwinPower" in out or "inline" in out.lower()


def test_build_engine_display_respects_i_in_description() -> None:
    """Dealer ``engine_description`` wins when present (full string); layout token still used when absent."""
    car = {
        "engine_l": "3.0",
        "cylinders": 6,
        "engine_description": None,
        "fuel_type": "Gas",
    }
    vs = {"master_engine_string": "3.0L I6 (EPA mode aggregate)"}
    out = build_engine_display(car, vs)
    assert out == "3.0L I6 (EPA mode aggregate)"


def test_parse_engine_displacement_from_engine_l() -> None:
    assert parse_engine_displacement_liters({"engine_l": "2.0", "engine_description": None}) == 2.0


def test_parse_engine_displacement_from_description() -> None:
    assert (
        parse_engine_displacement_liters(
            {"engine_l": None, "engine_description": "Intercooled Turbo Premium Unleaded 4.4L V8"}
        )
        == 4.4
    )


def test_parse_engine_displacement_displacement_only_no_l_suffix() -> None:
    assert parse_engine_displacement_liters({"engine_l": None, "engine_description": "2.0"}) == 2.0


def test_infer_engine_l_for_db_from_text() -> None:
    out = infer_engine_l_for_db(
        {"engine_description": "2.0L I4", "cylinders": 4, "fuel_type": "Gas", "engine_l": None}
    )
    assert out is not None and float(out) == 2.0
    assert infer_engine_l_for_db({"cylinders": 0, "fuel_type": "Electric", "engine_l": None}) == "Electric"
    assert infer_engine_l_for_db({"cylinders": 0, "fuel_type": "Plug-in Hybrid", "engine_l": None}) == "PHEV"
    assert infer_engine_l_for_db({"engine_l": "3.0", "engine_description": "ignored"}) == "3.0"


def test_car_matches_engine_displacement_l_range() -> None:
    car = {"engine_l": "5.0", "engine_description": None}
    assert car_matches_engine_displacement_l_range(car, 4.0, 6.0)
    assert not car_matches_engine_displacement_l_range(car, 1.0, 2.0)


def test_search_cars_engine_displacement_range(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    dbp = tmp_path / "inv_engine_l.db"
    monkeypatch.setattr(inventory_db, "DB_PATH", str(dbp))
    init_inventory_db()
    conn = sqlite3.connect(str(dbp))
    ensure_enrichment_columns(conn)
    cur = conn.cursor()
    now = "2026-01-01T00:00:00Z"
    base_sql = """
        INSERT INTO cars (
            vin, title, year, make, model, trim, price, mileage,
            image_url, dealer_name, dealer_url, dealer_id, scraped_at,
            zip_code, fuel_type, cylinders, transmission, drivetrain,
            exterior_color, interior_color, stock_number, gallery,
            engine_l, engine_description,
            listing_active, listing_removed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    def row(vin: str, eng_l: str | None, desc: str | None) -> tuple:
        return (
            vin,
            f"Used 2020 Test {vin[-4:]}",
            2020,
            "TestMake",
            "ModelX",
            "Base",
            30000,
            10000,
            "https://example.com/a.jpg",
            "Dealer",
            "https://dealer.test/",
            "d1",
            now,
            "90210",
            "Gas",
            8,
            "Automatic",
            "RWD",
            "Black",
            "Black",
            "S1",
            "[]",
            eng_l,
            desc,
            1,
            None,
        )

    cur.execute(base_sql, row("EEEEEEEEEEEEEEEEE", "4.4", None))
    cur.execute(base_sql, row("FFFFFFFFFFFFFFFFF", "2.0", None))
    cur.execute(base_sql, row("GGGGGGGGGGGGGGGGG", None, "Turbo 3.5L V6 engine"))
    conn.commit()
    conn.close()

    hits = search_cars(
        makes=["TestMake"],
        engine_displacement_l_min=3.0,
        engine_displacement_l_max=5.0,
    )
    vins = {r["vin"] for r in hits}
    assert "EEEEEEEEEEEEEEEEE" in vins
    assert "FFFFFFFFFFFFFFFFF" not in vins
    assert "GGGGGGGGGGGGGGGGG" in vins
