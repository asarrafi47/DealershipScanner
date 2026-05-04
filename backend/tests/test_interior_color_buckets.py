"""Interior color bucket lexicon + search_cars bucket filter."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

import backend.db.inventory_db as inventory_db
from backend.db.inventory_db import init_inventory_db, search_cars
from backend.utils.interior_color_buckets import (
    infer_interior_color_buckets,
    interior_color_buckets_json,
    merge_bucket_lists,
    row_matches_interior_bucket_filter,
)
from backend.vision.interior_vision_merge import build_updates_from_llava_result


def test_infer_cognac_maps_to_brown_tan() -> None:
    assert infer_interior_color_buckets("Cognac leather") == ["brown", "tan"]


def test_infer_empty_returns_empty_list() -> None:
    assert infer_interior_color_buckets(None) == []
    assert infer_interior_color_buckets("   ") == []


def test_infer_unknown_non_empty_is_other() -> None:
    assert infer_interior_color_buckets("zzzz-not-a-color-xyz123") == ["other"]


def test_interior_color_buckets_json_always_array() -> None:
    assert interior_color_buckets_json(None, None) == "[]"
    assert interior_color_buckets_json("Black", None) == '["black"]'


def test_merge_bucket_lists_order_unique() -> None:
    assert merge_bucket_lists(["tan", "black"], ["black", "red"]) == ["tan", "black", "red"]


def test_row_matches_interior_bucket_filter() -> None:
    assert row_matches_interior_bucket_filter(
        {"interior_color_buckets": '["black","gray"]'},
        {"black"},
    )
    assert not row_matches_interior_bucket_filter(
        {"interior_color_buckets": '["tan"]'},
        {"black"},
    )


def test_build_updates_respects_dealer_interior_without_overwrite(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("INTERIOR_VISION_OVERWRITE", raising=False)
    row = {
        "interior_color": "Ebony",
        "make": "Ford",
        "spec_source_json": None,
        "packages": "{}",
    }
    llava = {
        "interior_buckets": ["black"],
        "interior_guess_text": "Black",
        "confidence": 0.95,
        "evidence": "dash",
        "model": "llava:13b",
    }
    patch = build_updates_from_llava_result(row=row, llava=llava)
    assert "interior_color" not in patch
    assert "interior_color_buckets" in patch


def test_build_updates_fills_placeholder_interior(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("INTERIOR_VISION_OVERWRITE", raising=False)
    row = {
        "interior_color": None,
        "make": "Honda",
        "spec_source_json": None,
        "packages": None,
    }
    llava = {
        "interior_buckets": ["tan"],
        "interior_guess_text": "Tan leather",
        "confidence": 0.92,
        "evidence": "seats",
        "model": "llava:13b",
    }
    patch = build_updates_from_llava_result(row=row, llava=llava)
    assert patch.get("interior_color") == "Tan leather"
    assert "interior_color_buckets" in patch


def test_search_cars_interior_color_bucket_filters(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    dbp = tmp_path / "inv_interior_buckets.db"
    monkeypatch.setattr(inventory_db, "DB_PATH", str(dbp))
    init_inventory_db()
    conn = sqlite3.connect(str(dbp))
    cur = conn.cursor()
    now = "2026-01-01T00:00:00Z"

    def insert_row(vin: str, buckets: str) -> None:
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
                vin,
                "Used 2022 Honda Civic LX",
                2022,
                "Honda",
                "Civic",
                "LX",
                22000,
                3000,
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
                "Blue",
                "Black",
                buckets,
                "S1",
                "[]",
                1,
                None,
            ),
        )

    insert_row("AAAAAAAAAAAAAAAAA", '["black","gray"]')
    insert_row("BBBBBBBBBBBBBBBBB", '["tan"]')
    conn.commit()
    conn.close()

    black_rows = search_cars(
        makes=["Honda"],
        interior_color_bucket_filters=["black"],
    )
    assert {r["vin"] for r in black_rows} == {"AAAAAAAAAAAAAAAAA"}

    either = search_cars(
        makes=["Honda"],
        interior_color_bucket_filters=["black", "tan"],
    )
    assert {r["vin"] for r in either} == {"AAAAAAAAAAAAAAAAA", "BBBBBBBBBBBBBBBBB"}
