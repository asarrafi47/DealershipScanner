"""Engine display (displacement + layout) and displacement range search."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

import backend.db.inventory_db as inventory_db
from backend.db.inventory_db import init_inventory_db, search_cars
from backend.enrichment.service import ensure_enrichment_columns
from backend.utils.car_serialize import (
    build_engine_display,
    car_matches_engine_displacement_l_range,
    infer_engine_l_for_db,
    parse_engine_displacement_liters,
)


def test_build_engine_display_displacement_only_from_engine_l() -> None:
    car = {
        "engine_description": None,
        "engine_l": "4.4",
        "cylinders": 8,
        "fuel_type": "Gas",
    }
    assert build_engine_display(car, {}) == "4.4L V8"


def test_displacement_only_description_merges_verified_cylinders_for_parse() -> None:
    """Verified cylinders do not add layout text — only help infer liters when missing."""
    car = {
        "engine_description": "2.0",
        "engine_l": None,
        "cylinders": None,
        "fuel_type": "Gasoline",
    }
    vs = {"cylinders": 4, "cylinders_display": 4}
    assert build_engine_display(car, vs) == "2.0L I4"


def test_displacement_only_numeric_engine_l() -> None:
    car = {
        "engine_description": None,
        "engine_l": "2",
        "cylinders": None,
        "fuel_type": "Gas",
    }
    vs = {"cylinders_display": 4}
    assert build_engine_display(car, vs) == "2.0L I4"


def test_rich_engine_description_yields_displacement_only() -> None:
    car = {
        "engine_description": "2.0L BMW TwinPower Turbo inline 4-cylinder",
        "engine_l": None,
        "cylinders": None,
        "fuel_type": "Gas",
    }
    assert build_engine_display(car, {"cylinders": 4}) == "2.0L I4"


def test_master_engine_string_epa_aggregate_stripped_to_liters() -> None:
    car = {
        "engine_l": "3.0",
        "cylinders": 6,
        "engine_description": None,
        "fuel_type": "Gas",
    }
    vs = {"master_engine_string": "3.0L I6 (EPA mode aggregate)"}
    assert build_engine_display(car, vs) == "3.0L I6"


def test_build_engine_display_prefers_sticker_packages() -> None:
    car = {
        "engine_l": "3.6",
        "cylinders": 6,
        "packages": '{"sticker_engine_display": "6.4L V8"}',
    }
    assert build_engine_display(car, {}) == "6.4L V8"


def test_build_engine_display_missing_cylinders_no_crash() -> None:
    car = {
        "engine_l": "2.0",
        "cylinders": None,
        "engine_description": None,
        "fuel_type": "Gas",
    }
    assert build_engine_display(car, {}) == "2.0L"


def test_build_engine_display_layout_from_description_without_cylinder_count() -> None:
    car = {
        "engine_l": "3.5",
        "cylinders": None,
        "engine_description": "3.5L V6 EcoBoost",
        "fuel_type": "Gas",
    }
    assert build_engine_display(car, {}) == "3.5L V6"


def test_build_engine_display_jeep_wrangler_392() -> None:
    car = {
        "make": "Jeep",
        "model": "Wrangler",
        "trim": "Unlimited Rubicon 392",
        "engine_l": "3.6",
        "cylinders": 6,
        "fuel_type": "Gas",
    }
    assert build_engine_display(car, {}) == "6.4L V8"


def test_known_oem_engine_jeep_392() -> None:
    from backend.scanner.window_sticker import known_oem_engine_from_car

    hit = known_oem_engine_from_car(
        {"make": "Jeep", "model": "Wrangler", "trim": "Rubicon 392"}
    )
    assert hit.get("engine_display") == "6.4L V8"
    assert hit.get("engine_l") == 6.4


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
