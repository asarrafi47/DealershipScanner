"""
Smart-search parse bug (2026-09-28): "used toyota under 30k" parsed "used" as
``trim_contains`` instead of the condition filter. new / used / pre-owned /
certified / cpo now set ``inventory_condition`` (the listings UI vocabulary)
and never reach make/model/trim matching; search_cars filters on it.
"""

import pytest

import backend.utils.query_parser as qp
from backend.utils.hybrid_search import filters_dict_to_search_cars_kwargs


@pytest.fixture(autouse=True)
def _fixture_inventory(monkeypatch):
    pairs = (("Toyota", "Camry"), ("Toyota", "RAV4"), ("Honda", "CR-V"), ("Volkswagen", "New Beetle"))
    # "USED" / "Certified" really occur in feed trim columns; that is how the bug happened.
    trims = ("Certified", "USED", "XLE", "LE")
    monkeypatch.setattr(qp, "_load_inventory_keywords", lambda key: (pairs, (), (), (), trims))
    monkeypatch.setattr(qp, "_load_package_names", lambda key: ())


def test_used_toyota_under_30k():
    f = qp.parse_natural_query("used toyota under 30k")
    assert f.get("inventory_condition") == "pre_owned"
    assert f.get("make") == "Toyota"
    assert f.get("max_price") == 30000
    assert "trim_contains" not in f


@pytest.mark.parametrize(
    "query,expected",
    [
        ("new honda cr-v", "new"),
        ("brand new camry", "new"),
        ("pre-owned rav4", "pre_owned"),
        ("preowned rav4", "pre_owned"),
        ("certified pre-owned camry", "cpo"),
        ("certified camry", "cpo"),
        ("cpo camry xle", "cpo"),
    ],
)
def test_condition_words(query, expected):
    f = qp.parse_natural_query(query)
    assert f.get("inventory_condition") == expected
    assert str(f.get("trim_contains") or "").lower() not in ("used", "certified", "new", "pre-owned")


def test_new_beetle_is_a_model_not_a_condition():
    f = qp.parse_natural_query("volkswagen new beetle")
    assert "inventory_condition" not in f


def test_trim_still_parses_next_to_a_condition():
    f = qp.parse_natural_query("used camry xle")
    assert f.get("inventory_condition") == "pre_owned"
    assert f.get("trim_contains") == "xle"


def test_condition_maps_to_search_kwargs():
    assert filters_dict_to_search_cars_kwargs({"inventory_condition": "pre_owned"}) == {"inventory_condition": "pre_owned"}
    assert filters_dict_to_search_cars_kwargs({"inventory_condition": "cpo"}) == {"inventory_condition": "cpo", "cpo_only": True}
    assert filters_dict_to_search_cars_kwargs({"inventory_condition": "bogus"}) == {}


def test_search_cars_condition_filter(tmp_path, monkeypatch):
    import sqlite3

    import backend.db.inventory_db as inventory_db
    from backend.db.inventory_db import init_inventory_db
    from backend.db.repositories.search_repo import search_cars

    dbp = str(tmp_path / "inventory.db")
    monkeypatch.setattr(inventory_db, "DB_PATH", dbp)
    init_inventory_db()
    conn = sqlite3.connect(dbp)
    rows = [
        ("4T1B11HK1KU000001", "New", 0, 0),
        ("4T1B11HK1KU000002", "Used", 0, 30000),
        ("4T1B11HK1KU000003", "Certified Pre-Owned", 1, 12000),
        ("4T1B11HK1KU000004", None, 0, 5000),
    ]
    for vin, cond, cpo, mi in rows:
        conn.execute(
            "INSERT INTO cars (vin, title, make, model, year, price, mileage, condition, is_cpo, dealer_id,"
            " dealer_name, dealer_url, image_url, gallery, scraped_at, listing_active)"
            " VALUES (?, 'Toyota Camry', 'Toyota', 'Camry', 2024, 25000, ?, ?, ?, 'd', 'D', 'https://d.example',"
            " 'https://d.example/a.jpg', '[\"https://d.example/a.jpg\"]', '2026-09-01T00:00:00Z', 1)",
            (vin, mi, cond, cpo),
        )
    conn.commit()
    conn.close()

    def vins(**kw):
        return sorted(r["vin"][-1] for r in search_cars(makes=["Toyota"], include_incomplete=True, **kw))

    assert vins(inventory_condition="new") == ["1"]
    assert vins(inventory_condition="pre_owned") == ["2", "3"]
    assert vins(inventory_condition="cpo") == ["3"]
    assert vins() == ["1", "2", "3", "4"]
