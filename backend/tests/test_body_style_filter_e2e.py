"""Body-style filter end to end (P8B.6b): the facet value, the filter expansion and the
SQL clause agree on every spelling the vocabulary folds into a preset."""

from __future__ import annotations

from backend.db.repositories.facets.queries import _clean_relationship_row
from backend.db.repositories.search.query import SearchQuery
from backend.db.repositories.search.where import SqlWhere, add_spec_columns
from backend.utils.field_clean import BODY_STYLE_PRESETS, coerce_body_style_stored, sort_body_style_presets
from backend.utils.hybrid_search import filters_dict_to_search_cars_kwargs


def _stored_matches(stored: str, filter_value: str) -> bool:
    """What the SQL ``LOWER(TRIM(body_style)) IN (...)`` clause does for one stored value."""
    q = SearchQuery(body_styles=filters_dict_to_search_cars_kwargs({"body_style": [filter_value]})["body_styles"])
    w = SqlWhere("")
    add_spec_columns(w, q)
    assert "LOWER(TRIM(IFNULL(body_style" in w.sql
    return stored.lower().strip() in w.params


def test_facet_value_finds_every_folded_spelling() -> None:
    stored = ["Extended Cab", "Pickup", "Sport Utility", "Sedan/Saloon", "Hatchback/Liftback/Notchback", "SUV"]
    facet = sort_body_style_presets(stored)
    assert set(facet) <= set(BODY_STYLE_PRESETS)
    for s in stored:
        shown = coerce_body_style_stored(s)
        assert _stored_matches(s, shown), (s, shown)


def test_relationship_row_emits_preset() -> None:
    row = ("Ford", "F-150", "XL", None, None, None, "Extended Cab", None, 2022, None, None)
    assert _clean_relationship_row(row)[6] == "Truck"
