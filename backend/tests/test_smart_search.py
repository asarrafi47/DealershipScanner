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
    pkg = f.get("packages_json_contains_list") or []
    assert "heated" in pkg
    assert f.get("make") == "Honda"
    assert f.get("model") == "Accord"


def test_parse_named_package_phrase() -> None:
    f = parse_natural_query("BMW X5 with M Sport package")
    pkg = f.get("packages_json_contains_list") or []
    assert any("m sport" in str(p).lower() for p in pkg)
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
    pkg = f.get("packages_json_contains_list") or []
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
    filters = {"packages_json_contains_list": ["bowers", "head-up", "360"], "fully_loaded": True}
    ranked = _rank_smart_search_results(rows, "bmw x5 bowers head-up 360", filters, vector_top_k=10)
    assert [r["id"] for r in ranked][:2] == [2, 1]


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
