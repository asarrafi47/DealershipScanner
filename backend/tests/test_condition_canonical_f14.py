"""F14 (docs/DATA_COMPLETENESS_FINDINGS_2026_09_28.md): condition was stored in nine
raw spellings (New, Used, Certified, Pre-Owned, Pre-owned, Certified Pre-Owned,
CTP, CarBravo, Demo) and is_cpo was NULL on 2,210 certified rows. Every upsert
row passes clean_car_row_dict, which now canonicalises to New / Used /
Certified and sets is_cpo; the storage repair normaliser agrees."""
from __future__ import annotations

import pytest

from backend.utils.car_serialize.condition import normalize_condition_for_storage
from backend.utils.field_clean import canonicalize_condition, clean_car_row_dict


@pytest.mark.parametrize("raw,is_cpo_in,want,want_cpo,program", [
    ("New", None, "New", 0, None),
    ("Used", None, "Used", 0, None),
    ("Certified", None, "Certified", 1, None),
    ("Certified Pre-Owned", None, "Certified", 1, None),
    ("certified pre-owned", None, "Certified", 1, None),
    ("CPO", None, "Certified", 1, None),
    ("Pre-Owned", None, "Used", 0, None),
    ("Pre-owned", None, "Used", 0, None),
    ("Used", 1, "Certified", 1, None),          # feed says used, its own cpo flag says certified
    ("CTP", None, "New", 0, "CTP"),             # GM Courtesy Transportation loaner, titled new
    ("Demo", None, "New", 0, "Demo"),
    ("CarBravo", None, "Used", 0, "CarBravo"),
    ("CarBravo", 1, "Certified", 1, "CarBravo"),
])
def test_canonical_condition_and_is_cpo(raw, is_cpo_in, want, want_cpo, program):
    row = clean_car_row_dict({"vin": "1HGBH41JXMN109186", "condition": raw, "is_cpo": is_cpo_in})
    assert row["condition"] == want
    assert row["is_cpo"] == want_cpo
    assert row.get("_condition_program") == program


def test_unknown_spelling_and_blank_left_alone():
    row = {"condition": "Fleet Return", "is_cpo": None}
    canonicalize_condition(row)
    assert row["condition"] == "Fleet Return" and row["is_cpo"] is None
    assert clean_car_row_dict({"condition": ""})["condition"] is None
    assert clean_car_row_dict({"condition": "false"})["condition"] is None
    assert "is_cpo" not in clean_car_row_dict({"vin": "1HGBH41JXMN109186"})


def test_storage_repair_normaliser_returns_canonical():
    assert normalize_condition_for_storage({"condition": "Certified Pre-Owned"}) == "Certified"
    assert normalize_condition_for_storage({"condition": "Pre-Owned"}) == "Used"
    assert normalize_condition_for_storage({"condition": "Certified", "title": "2019 Lexus ES 350 CPO"}) is None
    assert normalize_condition_for_storage({"condition": "Used", "is_cpo": 1}) == "Certified"
    assert normalize_condition_for_storage({"condition": ""}) is None
