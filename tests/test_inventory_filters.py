"""Inventory filter plumbing (body_style, hybrid kwargs)."""

from __future__ import annotations

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
