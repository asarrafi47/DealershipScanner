"""Inventory filter plumbing (body_style, hybrid kwargs)."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

import backend.db.inventory_db as inventory_db
from backend.db.inventory_db import init_inventory_db, link_cars_to_dealership_registry, search_cars
from backend.hybrid_inventory_search import (
    _normalize_listings_vin_query,
    _parse_listings_car_id_query,
    filters_dict_to_search_cars_kwargs,
    hybrid_search_with_kwargs,
)
from backend.utils import query_parser as qp


def test_listings_vin_and_id_query_parsing() -> None:
    assert _normalize_listings_vin_query("5ux53g p00s9062118") == "5UX53GP00S9062118"
    assert _normalize_listings_vin_query("5UX53GP0") is None
    assert _parse_listings_car_id_query("#42") == [42]
    assert _parse_listings_car_id_query("id: 99") == [99]
    assert _parse_listings_car_id_query("carid:7") == [7]
    assert _parse_listings_car_id_query("12345") == [12345]
    assert _parse_listings_car_id_query("0") is None
    assert _parse_listings_car_id_query("#0") is None


def test_hybrid_search_resolves_vin_and_car_id_short_circuit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict] = []

    def fake_search_cars(**kw: object) -> list[dict]:
        calls.append(kw)
        if kw.get("vin") == "5UX53GP00S9062118":
            return [{"id": 7, "vin": "5UX53GP00S9062118", "title": "t", "data_quality_score": 1.0, "price": 1.0}]
        if kw.get("candidate_ids") == [7]:
            return [{"id": 7, "vin": "V", "title": "t", "data_quality_score": 1.0, "price": 1.0}]
        return []

    monkeypatch.setattr("backend.hybrid_inventory_search.search_cars", fake_search_cars)
    r1, m1 = hybrid_search_with_kwargs("5UX53GP00S9062118", {}, vector_top_k=10)
    assert m1.get("mode") == "vin_exact"
    assert r1 and r1[0]["id"] == 7

    r2, m2 = hybrid_search_with_kwargs("7", {}, vector_top_k=10)
    assert m2.get("mode") == "car_id"
    assert r2 and r2[0]["id"] == 7


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


def test_search_cars_max_price_zero_is_not_ignored(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """``max_price=0`` must apply; falsy 0 is a real bound, not "no limit"."""
    dbp = tmp_path / "inv_maxp0.db"
    monkeypatch.setattr(inventory_db, "DB_PATH", str(dbp))
    init_inventory_db()
    conn = sqlite3.connect(str(dbp))
    cur = conn.cursor()
    now = "2026-01-01T00:00:00Z"
    def row(
        vin: str,
        title: str,
        price: int,
    ) -> tuple:
        return (
            vin,
            title,
            2024,
            "Acme",
            "A",
            "L",
            price,
            0,
            "https://x/a.jpg",
            "Dealer",
            "https://d.test/",
            "acme",
            now,
            "90210",
            "Gas",
            4,
            "Automatic",
            "FWD",
            "Black",
            "Black",
            "[]",
            "S1",
            "[]",
            1,
            None,
        )

    for r in (row("ZZZZZZZZZZZZZZZZ1", "T1", 0), row("ZZZZZZZZZZZZZZZZ2", "T2", 25_000)):
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
            r,
        )
    conn.commit()
    conn.close()

    free_only = search_cars(makes=["Acme"], max_price=0)
    assert {c["vin"] for c in free_only} == {"ZZZZZZZZZZZZZZZZ1"}


def test_search_cars_packages_substring_is_literal_not_like_wildcard(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """``%`` in the needle is a normal character, not a SQL ``LIKE`` wildcard."""
    dbp = tmp_path / "inv_pkgp.db"
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
            exterior_color, interior_color, packages, stock_number, gallery,
            listing_active, listing_removed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "PPPPPPPPPPPPPPPPP",
            "t",
            2023,
            "Prco",
            "M",
            "B",
            1,
            0,
            "https://h/x.jpg",
            "D",
            "https://d.test/",
            "d1",
            now,
            "90210",
            "G",
            4,
            "A",
            "F",
            "x",
            "i",
            '{"x": "stated 50% State of Health"}',
            "S",
            "[]",
            1,
            None,
        ),
    )
    conn.commit()
    conn.close()
    found = search_cars(makes=["Prco"], packages_json_contains="50%")
    assert len(found) == 1
    assert "50%" in (found[0].get("packages") or "")


def test_link_cars_falls_back_to_dealer_id_slug(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """
    When ``dealer_url`` does not match the registry website string (e.g. different
    working-copy DB or URL normalization), linking by ``dealer_id`` (manifest slug) still works.
    """
    dbp = tmp_path / "inv_link_did.db"
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
            exterior_color, interior_color, stock_number, gallery,
            listing_active, listing_removed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "QQQQQQQQQQQQQQQQQ",
            "t",
            2024,
            "X",
            "Y",
            "Z",
            1,
            0,
            "https://h/x.jpg",
            "D",
            "https://unrelated-wrong.example/",
            "hendrick-example-com",
            now,
            "28027",
            "G",
            4,
            "A",
            "F",
            "Black",
            "Black",
            "S1",
            "[]",
            1,
            None,
        ),
    )
    conn.commit()
    conn.close()
    n = link_cars_to_dealership_registry(
        99, "https://right.example", dealer_id_slug="hendrick-example-com"
    )
    assert n == 1
    conn2 = sqlite3.connect(str(dbp))
    r = conn2.execute("SELECT dealership_registry_id FROM cars WHERE vin = ?", ("QQQQQQQQQQQQQQQQQ",)).fetchone()
    conn2.close()
    assert r and r[0] == 99
