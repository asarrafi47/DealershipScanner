"""Permanent epa_master key (P10A.2)."""
from __future__ import annotations

from backend.catalog.epa_key import (
    assign_natural_keys,
    base_key,
    match_rows,
    natural_key,
    split_key,
)

CSV_ROW = {
    "id": "12345", "year": "2013", "make": "Ford", "model": "F150 Pickup 2WD",
    "trany": "Automatic 6-spd", "drive": "Rear-Wheel Drive", "displ": "3.5",
    "cylinders": "6", "fuelType": "Regular", "eng_dscr": "(FFS,TRBO)", "VClass": "Pickup",
    "city08": "16", "highway08": "22",
}
DB_ROW = {
    "epa_vehicle_id": 12345, "year": 2013, "make": "Ford", "model": "F150 Pickup 2WD",
    "trim": None, "trany": "Automatic 6-spd", "drive": "Rear-Wheel Drive",
    "displacement": 3.5, "cylinders": 6, "fuel_type": "Regular",
    "engine_description": "(FFS,TRBO)", "body_style": "Pickup",
    "city08": 16.0, "highway08": 22.0,
}


def _no_vid(row):
    return {k: v for k, v in row.items() if k not in ("id", "epa_vehicle_id")}


def test_vid_key():
    assert natural_key(CSV_ROW) == "vid:12345"
    assert natural_key(DB_ROW) == "vid:12345"


def test_csv_and_db_typed_rows_share_base_key():
    assert base_key(CSV_ROW) == base_key(DB_ROW)
    assert natural_key(_no_vid(CSV_ROW)) == natural_key(_no_vid(DB_ROW)) == base_key(DB_ROW) + "#1"


def test_normalization_and_none():
    a = {"year": 2020, "make": " Audi ", "model": "A4", "displacement": 2.0}
    b = {"year": "2020", "make": "audi", "model": "A4", "displ": "2.0000", "trim": ""}
    assert base_key(a) == base_key(b)
    assert base_key(a).split("|")[3] == ""  # None/blank trim -> ''


def test_ordinal_assignment_deterministic_and_sticky():
    rows = [_no_vid(DB_ROW), _no_vid(DB_ROW), dict(_no_vid(DB_ROW), model="Other")]
    keys = assign_natural_keys(rows)
    assert keys == assign_natural_keys(rows)
    assert [split_key(k)[1] for k in keys] == [1, 2, 1]
    # stored keys are kept; a new row never reuses a retired ordinal
    stored = dict(_no_vid(DB_ROW), natural_key=keys[1])  # only #2 survives
    new = _no_vid(DB_ROW)
    out = assign_natural_keys([stored, new])
    assert out[0] == keys[1]
    assert split_key(out[1])[1] == 3
    # taken keys count too
    assert split_key(assign_natural_keys([new], taken=[keys[0], keys[1]])[0])[1] == 3


def test_vid_rows_in_assign():
    assert assign_natural_keys([DB_ROW]) == ["vid:12345"]


def test_match_by_value_not_position():
    base = _no_vid(DB_ROW)
    db = [
        dict(base, engine_display="3.5L V6 Turbo"),
        dict(base, engine_display="3.5L V6 Twin Turbo"),
    ]
    csv_rows = [
        dict(base, engine_display="3.5L V6 Twin Turbo"),
        dict(base, engine_display="3.5L V6 Turbo"),
    ]
    pairs, cu, du = match_rows(csv_rows, db)
    assert pairs == [(0, 1), (1, 0)] and not cu and not du


def test_match_nearest_values_and_leftovers():
    base = _no_vid(DB_ROW)
    db = [dict(base, city08=16, highway08=22), dict(base, city08=30, highway08=40)]
    csv_rows = [dict(base, city08=29, highway08=40), dict(base, city08=16, highway08=23),
                dict(base, model="Unmatched")]
    pairs, cu, du = match_rows(csv_rows, db)
    assert pairs == [(0, 1), (1, 0)]
    assert cu == [2] and du == []


def test_db_surrogate_pk_is_not_the_epa_id():
    row = dict(_no_vid(DB_ROW), id=777, epa_vehicle_id=None)
    base = base_key(row)
    assert natural_key(row) == base + "#1"
    assert assign_natural_keys([row]) == [base + "#1"]
    assert not natural_key(row).startswith("vid:")
