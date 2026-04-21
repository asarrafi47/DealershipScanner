"""Inventory filter plumbing (body_style, hybrid kwargs)."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

import backend.db.inventory_db as inventory_db
from backend.db.inventory_db import init_inventory_db, search_cars
from backend.hybrid_inventory_search import filters_dict_to_search_cars_kwargs
from backend.utils import query_parser as qp


def test_filters_dict_maps_body_style_list() -> None:
    kw = filters_dict_to_search_cars_kwargs({"body_style": ["SUV", "Sedan"]})
    assert kw.get("body_styles") == ["SUV", "Sedan"]
    kw2 = filters_dict_to_search_cars_kwargs({"body_style": "Coupe"})
    assert kw2.get("body_styles") == ["Coupe"]


def test_match_body_style_exact_token() -> None:
    got = qp._match_body_style_filters("show me SUV inventory", ["SUV", "Sedan"])
    assert got == ["SUV"]


def test_match_body_style_cue_fuzzy() -> None:
    distinct = ["Sport Utility Vehicle", "Sedan"]
    got = qp._match_body_style_filters("awd crossover under 40k", distinct)
    assert got and "Sport Utility Vehicle" in got


def test_filters_dict_package_contains_kwarg() -> None:
    kw = filters_dict_to_search_cars_kwargs({"package_contains": "Heated seats"})
    assert kw.get("packages_json_contains") == "Heated seats"
    kw2 = filters_dict_to_search_cars_kwargs({"packages_json_contains": "premium audio"})
    assert kw2.get("packages_json_contains") == "premium audio"


def test_filters_dict_interior_color_buckets_kwarg() -> None:
    kw = filters_dict_to_search_cars_kwargs({"interior_color_buckets": ["black", "tan"]})
    assert kw.get("interior_color_bucket_filters") == ["black", "tan"]
    kw2 = filters_dict_to_search_cars_kwargs({"interior_color_bucket": "gray"})
    assert kw2.get("interior_color_bucket_filters") == ["gray"]


def test_filters_dict_engine_displacement_kwarg() -> None:
    kw = filters_dict_to_search_cars_kwargs(
        {"engine_displacement_l_min": 2.5, "engine_displacement_l_max": 4.5}
    )
    assert kw.get("engine_displacement_l_min") == 2.5
    assert kw.get("engine_displacement_l_max") == 4.5


def test_search_cars_exterior_color_uses_paint_family_not_raw_string(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Facets pass bucket ids (e.g. red); raw dealer strings like Tacora Red still match."""
    dbp = tmp_path / "inv_paint_family.db"
    monkeypatch.setattr(inventory_db, "DB_PATH", str(dbp))
    init_inventory_db()
    conn = sqlite3.connect(str(dbp))
    cur = conn.cursor()
    now = "2026-01-01T00:00:00Z"
    cur.execute(
        """
        INSERT INTO cars (
            vin, title, year, make, model, trim, price, mileage,
            image_url, dealer_name, dealer_url, dealer_id, scraped_at,
            zip_code, fuel_type, cylinders, transmission, drivetrain,
            exterior_color, interior_color, interior_color_buckets, stock_number, gallery,
            listing_active, listing_removed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "TTTTTTTTTTTTTTTTT",
            "Used 2024 Example X",
            2024,
            "Example",
            "X",
            "Base",
            28000,
            1000,
            "https://example.com/a.jpg",
            "Test Dealer",
            "https://dealer.test/",
            "t-dealer",
            now,
            "90210",
            "Gas",
            4,
            "Automatic",
            "FWD",
            "Tacora Red",
            "Ebony / Mocha",
            "[]",
            "S1",
            "[]",
            1,
            None,
        ),
    )
    conn.commit()
    conn.close()

    red_rows = search_cars(makes=["Example"], exterior_colors=["red"])
    assert len(red_rows) == 1
    assert red_rows[0]["exterior_color"] == "Tacora Red"

    no_hit = search_cars(makes=["Example"], exterior_colors=["blue"])
    assert no_hit == []
