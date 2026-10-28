"""P10C.6: consumers treat 'Naturally Aspirated' as an explicit, not-boosted label."""
from __future__ import annotations

import json

from backend.enrichment import knowledge_engine_specs as kes
from backend.enrichment.generated_spec_sheet import _forced_induction_source
from backend.utils.car_serialize.api_specs import apply_forced_induction

NA = "Naturally Aspirated"


def test_has_forced_induction_na_is_false_even_with_badge_text():
    assert kes._has_forced_induction({"forced_induction": NA, "trim": "GT Turbo"}) is False
    assert kes._has_forced_induction({"forced_induction": "Turbocharged"}) is True
    assert kes._has_forced_induction({"forced_induction": "Twin Turbocharged"}) is True
    assert kes._has_forced_induction({"trim": "GT Turbo"}) is True  # blank label still reads text


def test_serializer_gives_na_car_no_turbo():
    out: dict = {}
    apply_forced_induction(out, {"forced_induction": NA, "make": "BMW", "model": "M3", "year": 2022,
                                 "title": "2022 BMW M3 Competition"})
    assert out["forced_induction"] == NA
    # a stored NA is never re-guessed into a boost label
    out = {}
    apply_forced_induction(out, {"forced_induction": NA, "make": "Audi", "model": "RS 7", "year": 2022})
    assert out["forced_induction"] == NA


def test_spec_sheet_shows_stored_source():
    prov = json.dumps({"forced_induction": {"source": "nhtsa_vpic", "value": NA,
                                            "fetched_at": "2026-10-01T00:00:00+00:00"}})
    assert _forced_induction_source({"forced_induction": NA, "spec_source_json": prov}) == "nhtsa_vpic"
    assert _forced_induction_source({"forced_induction": NA}) == "listing"


def test_heavy_body_na_below_floor_flagged_but_turbo_is_not():
    base = {"make": "Ford", "model": "Expedition", "body_style": "SUV", "trim": "XLT", "year": 2022,
            "title": "2022 Ford Expedition XLT SUV", "engine_description": "3.5L V6"}
    assert kes._zero_to_60_implausible({**base, "forced_induction": NA}, {}, 4.5) is True
    assert kes._zero_to_60_implausible({**base, "forced_induction": "Turbocharged"}, {}, 4.5) is False


def test_facet_lists_na_as_a_value():
    """D-ET2 (a): the Engine type facet shows 'Naturally Aspirated' like any other label."""
    from unittest import mock

    from backend.db.repositories.facets import queries

    cur = mock.Mock()
    cur.fetchall.return_value = [(NA,), ("Turbocharged",)]
    assert queries.distinct_values(cur, "forced_induction") == [NA, "Turbocharged"]
