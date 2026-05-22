"""Smart search: SQL-first when structured filters, model expansion, body-style precision."""

from __future__ import annotations

from backend.utils.hybrid_search import (
    expand_inventory_models,
    filters_dict_to_search_cars_kwargs,
    hybrid_smart_search,
)
from backend.utils.query_parser import parse_natural_query


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
