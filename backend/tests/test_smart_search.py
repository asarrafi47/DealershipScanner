"""Smart search: SQL-first when structured filters, model expansion, body-style precision."""

from __future__ import annotations

import json
import sqlite3

import pytest

import backend.db.inventory_db as inventory_db
from backend.db.inventory_db import init_inventory_db
from backend.utils.hybrid_search import (
    _expand_inventory_models_cached,
    expand_inventory_models,
    filters_dict_to_search_cars_kwargs,
    hybrid_smart_search,
)
from backend.utils.query_parser import parse_natural_query

# parse_natural_query() learns makes/models/trims/body styles/colors/packages from the
# inventory DB, so these tests seed a small fixture inventory instead of depending on
# whatever the developer's local inventory.db happens to contain.
_X5_PACKAGES = json.dumps(
    {
        "packages_normalized": [
            {"name": "M Sport Package"},
            {"name": "Bowers & Wilkins Diamond Surround Sound"},
        ],
        "detected_adas": ["head_up_display", "360_cameras"],
    }
)

_SEED_CARS: list[dict] = [
    {
        "vin": "5UXCR6C05N9A00001",
        "title": "2023 BMW X5 xDrive40i",
        "year": 2023,
        "make": "BMW",
        "model": "X5",
        "trim": "xDrive40i",
        "price": 62500,
        "mileage": 12000,
        "cylinders": 6,
        "drivetrain": "AWD",
        "exterior_color": "Alpine White",
        "interior_color": "Black",
        "body_style": "Sport Utility Vehicle",
        "packages": _X5_PACKAGES,
    },
    {
        "vin": "5UXCR6C05N9A00002",
        "title": "2022 BMW X5 xDrive40i",
        "year": 2022,
        "make": "BMW",
        "model": "X5",
        "trim": "xDrive40i",
        "price": 58990,
        "mileage": 24000,
        "cylinders": 6,
        "drivetrain": "AWD",
        "exterior_color": "Alpine White",
        "interior_color": "Black",
        "body_style": "Sport Utility Vehicle",
        "packages": _X5_PACKAGES,
    },
    {
        "vin": "5UX43DP05N9B00003",
        "title": "2022 BMW X3 sDrive30i",
        "year": 2022,
        "make": "BMW",
        "model": "X3",
        "trim": "sDrive30i",
        "price": 41000,
        "mileage": 18000,
        "cylinders": 4,
        "drivetrain": "RWD",
        "exterior_color": "Phytonic Blue",
        "interior_color": "White",
        "body_style": "Sport Utility Vehicle",
    },
    {
        "vin": "1HGCV1F55MA000004",
        "title": "2021 Honda Accord EX-L",
        "year": 2021,
        "make": "Honda",
        "model": "Accord",
        "trim": "EX-L",
        "price": 27500,
        "mileage": 30000,
        "cylinders": 4,
        "drivetrain": "FWD",
        "exterior_color": "Platinum White Pearl",
        "interior_color": "Black",
        "body_style": "Sedan",
    },
    {
        "vin": "W1K6G7GB5NA000005",
        "title": "2022 Mercedes-Benz S-Class S 580 4MATIC",
        "year": 2022,
        "make": "Mercedes-Benz",
        "model": "S-Class",
        "trim": "S 580",
        "price": 114900,
        "mileage": 9000,
        "cylinders": 8,
        "drivetrain": "AWD",
        "exterior_color": "Obsidian Black Metallic",
        "interior_color": "Macchiato Beige",
        "body_style": "Sedan",
    },
    {
        "vin": "3GCUDDED5PG000006",
        "title": "2023 Chevrolet Silverado 1500 LT Crew Cab",
        "year": 2023,
        "make": "Chevrolet",
        "model": "Silverado 1500",
        "trim": "LT",
        "price": 48990,
        "mileage": 15000,
        "cylinders": 8,
        "drivetrain": "4WD",
        "exterior_color": "Summit White",
        "interior_color": "Jet Black",
        "body_style": "Crew Cab Pickup",
    },
]


@pytest.fixture(scope="module")
def _smart_search_inventory_db(tmp_path_factory: pytest.TempPathFactory) -> str:
    """Build one seeded SQLite inventory for the whole module."""
    dbp = str(tmp_path_factory.mktemp("smart_search_inv") / "inventory.db")
    with pytest.MonkeyPatch.context() as mp:
        # Module-scoped: runs before the function-scoped sqlite-mode fixture in conftest.
        mp.delenv("INVENTORY_DATABASE_URL", raising=False)
        mp.delenv("DATABASE_URL", raising=False)
        mp.setenv("INVENTORY_SQLITE_TESTS", "1")
        mp.setattr(inventory_db, "DB_PATH", dbp)
        init_inventory_db()
    conn = sqlite3.connect(dbp)
    for car in _SEED_CARS:
        row = {
            "fuel_type": "Gasoline",
            "transmission": "Automatic",
            "image_url": "https://example.com/car.jpg",
            "dealer_name": "Test Dealer",
            "dealer_url": "https://example.com",
            "dealer_id": "test-dealer",
            "scraped_at": "2026-07-01T00:00:00Z",
            "stock_number": car["vin"][-6:],
            "gallery": '["https://example.com/car.jpg"]',
            "listing_active": 1,
            **car,
        }
        cols = list(row)
        conn.execute(
            f"INSERT INTO cars ({', '.join(cols)}) VALUES ({', '.join('?' for _ in cols)})",
            [row[c] for c in cols],
        )
    conn.commit()
    conn.close()
    return dbp


@pytest.fixture(autouse=True)
def _use_seeded_inventory(
    _smart_search_inventory_db: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Point inventory reads at the seeded fixture DB (root conftest resets DB_PATH per test)."""
    monkeypatch.setattr(inventory_db, "DB_PATH", _smart_search_inventory_db)
    _expand_inventory_models_cached.cache_clear()
    yield
    _expand_inventory_models_cached.cache_clear()


def test_expand_inventory_models_x5_prefix() -> None:
    models = expand_inventory_models("BMW", "X5")
    assert models
    assert any("X5" in m for m in models)
    assert any(m.strip() == "X5" or m.lower().startswith("x5 ") for m in models)


def test_body_style_crew_cab_not_all_trucks() -> None:
    f = parse_natural_query("silverado 1500 crew cab")
    bs = f.get("body_style") or []
    assert len(bs) <= 10
    joined = " ".join(bs).lower()
    assert "crew" in joined or "cab" in joined


def test_hybrid_smart_search_structured_sql_first() -> None:
    q = "white BMW X5 under 70000"
    filters = parse_natural_query(q)
    rows, meta = hybrid_smart_search(q, filters, vector_top_k=50)
    assert meta.get("mode", "").startswith("sql_first")
    assert len(rows) > 0
    for r in rows[:15]:
        assert str(r.get("make", "")).lower() == "bmw"
        assert "x5" in str(r.get("model", "")).lower()


def test_filters_kwargs_expand_model() -> None:
    kw = filters_dict_to_search_cars_kwargs({"make": "BMW", "model": "X5"})
    models = kw.get("models") or []
    assert len(models) >= 1
    assert all("x5" in m.lower() for m in models)


def test_parse_mercedes_s580_line_code() -> None:
    f = parse_natural_query("Mercedes benz S580")
    assert f.get("make") == "Mercedes-Benz"
    assert f.get("model") == "S-Class"
    assert f.get("trim_contains") == "580"


def test_parse_multi_make_or() -> None:
    f = parse_natural_query("BMW or Mercedes")
    assert f.get("make") == ["BMW", "Mercedes-Benz"]


def test_parse_multi_make_model_or() -> None:
    f = parse_natural_query("BMW X5 or Honda Accord")
    assert f.get("vehicle_or") == [
        {"make": "BMW", "model": "X5"},
        {"make": "Honda", "model": "Accord"},
    ]


def test_parse_same_make_multi_model_or() -> None:
    f = parse_natural_query("BMW X5 or X3")
    v_or = f.get("vehicle_or") or []
    assert len(v_or) == 2
    assert all(b.get("make") == "BMW" for b in v_or)
    models = {b.get("model") for b in v_or}
    assert "X5" in models and "X3" in models


def test_hybrid_smart_search_multi_make_model_or() -> None:
    q = "BMW X5 or Honda Accord"
    filters = parse_natural_query(q)
    rows, meta = hybrid_smart_search(q, filters, vector_top_k=50)
    assert meta.get("mode", "").startswith("sql_first")
    assert len(rows) > 0
    for r in rows:
        mk = str(r.get("make", "")).lower()
        md = str(r.get("model", "")).lower()
        assert (mk == "bmw" and "x5" in md) or (mk == "honda" and "accord" in md)


def test_filters_kwargs_vehicle_or() -> None:
    kw = filters_dict_to_search_cars_kwargs(
        {"vehicle_or": [{"make": "BMW", "model": "X5"}, {"make": "Honda", "model": "Accord"}]}
    )
    assert "vehicle_or" in kw
    assert len(kw["vehicle_or"]) == 2
    assert "makes" not in kw
    assert "models" not in kw


def test_parse_feature_keyword_packages_json_contains() -> None:
    f = parse_natural_query("Honda Accord with heated seats under 30k")
    assert f.get("packages_json_contains") == "heated"
    assert "packages_json_contains_all" not in f
    assert f.get("make") == "Honda"
    assert f.get("model") == "Accord"


def test_parse_named_package_phrase() -> None:
    f = parse_natural_query("BMW X5 with M Sport package")
    pkg_needles: list[str] = []
    if f.get("packages_json_contains"):
        pkg_needles.append(str(f["packages_json_contains"]))
    pkg_needles.extend(f.get("packages_json_contains_all") or [])
    joined = " ".join(pkg_needles).lower()
    assert "m sport" in joined
    assert f.get("make") == "BMW"


def test_parse_complex_bmw_equipment_query() -> None:
    q = (
        "bmw x5 v8 bowers wilkins heads up display 360 camera "
        "white inside and blue outside fully loaded under 50k miles"
    )
    f = parse_natural_query(q)
    assert f.get("make") == "BMW"
    assert f.get("model") == "X5"
    assert f.get("cylinders") == 8
    assert f.get("max_mileage") == 50000
    assert f.get("max_price") is None
    assert f.get("interior_color") == ["white"]
    assert f.get("exterior_color") == ["blue"]
    assert f.get("fully_loaded") is True
    assert f.get("trim_contains") != "and"
    pkg = f.get("packages_json_contains_all") or []
    joined = " ".join(pkg).lower()
    assert "bowers" in joined
    assert "head-up" in joined
    assert "360" in joined


def test_filters_kwargs_package_checkbox_list() -> None:
    kw = filters_dict_to_search_cars_kwargs(
        {"packages_json_contains_list": ["Premium Package", "M Sport"]}
    )
    assert kw.get("packages_json_contains_list") == ["Premium Package", "M Sport"]


def test_rank_smart_search_prefers_more_equipment_matches() -> None:
    from backend.utils.hybrid_search import _rank_smart_search_results

    rows = [
        {"id": 1, "packages": '{"packages_normalized":[{"name":"Head-Up Display"}]}', "title": "BMW X5"},
        {
            "id": 2,
            "packages": (
                '{"packages_normalized":[{"name":"Bowers & Wilkins"}'
                '], "detected_adas":["360_cameras","head_up_display"]}'
            ),
            "title": "BMW X5",
        },
    ]
    filters = {"packages_json_contains_all": ["bowers", "head-up", "360"], "fully_loaded": True}
    ranked = _rank_smart_search_results(rows, "bmw x5 bowers head-up 360", filters, vector_top_k=10)
    assert [r["id"] for r in ranked][:2] == [2, 1]


def test_api_search_smart_parse_route() -> None:
    from backend.main import app

    client = app.test_client()
    r = client.get("/api/search/smart/parse?query=AWD+Accord+under+30000")
    assert r.status_code == 200
    data = r.get_json()
    assert data.get("ok") is True
    assert data.get("filters", {}).get("max_price") == 30000
    assert "AWD" in (data.get("filters", {}).get("drivetrain") or [])


def test_parse_awd_not_body_style_trim_label() -> None:
    f = parse_natural_query("Apple CarPlay, AWD, and leather seats under $25k")
    assert f.get("drivetrain") == ["AWD", "4WD"]
    assert "body_style" not in f
    assert f.get("packages_json_contains_all") == ["carplay", "leather"]
    assert f.get("max_price") == 25000


def test_parse_multi_feature_filters_kwargs_and() -> None:
    f = parse_natural_query("AWD with carplay and leather under 30k")
    kw = filters_dict_to_search_cars_kwargs(f)
    assert kw.get("packages_json_contains_all") == ["carplay", "leather"]
    assert kw.get("drivetrains") == ["AWD", "4WD"]
    assert "packages_json_contains_list" not in kw


def test_hybrid_smart_search_mercedes_s580() -> None:
    q = "Mercedes benz S580"
    filters = parse_natural_query(q)
    rows, meta = hybrid_smart_search(q, filters, vector_top_k=50)
    assert meta.get("mode", "").startswith("sql_first")
    assert len(rows) > 0
    for r in rows:
        assert str(r.get("make", "")).lower() == "mercedes-benz"
        assert str(r.get("model", "")).lower() == "s-class"
        trim_title = f"{r.get('trim', '')} {r.get('title', '')}".lower()
        assert "580" in trim_title


def test_gibberish_query_returns_no_parse_match() -> None:
    q = "notarealbrand xyz999"
    filters = parse_natural_query(q)
    rows, meta = hybrid_smart_search(q, filters, vector_top_k=50)
    assert meta.get("mode") == "no_parse_match"
    assert rows == []


def test_query_is_actionable_vin_and_facets() -> None:
    from backend.utils.hybrid_search import query_is_actionable, sql_kwargs_has_facet_filters

    assert query_is_actionable("1HGBH41JXMN109186", {}, {}) is True
    assert query_is_actionable("notarealbrand xyz999", {}, {}) is False
    assert query_is_actionable("notarealbrand xyz999", {}, {"makes": ["BMW"]}) is True
    assert sql_kwargs_has_facet_filters({"zip_code": "90210", "radius_miles": 50}) is False
