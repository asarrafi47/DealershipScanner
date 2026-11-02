"""N6.5: junk values from the dealer.com VDP parser are gaps, not fills.

Covers the parser (_clean), the upsert (a stored junk value is dropped or
replaced on rescan, a junk incoming value never overwrites a real one), the
completeness invariant and the heal script's selection.
"""
from __future__ import annotations

import sqlite3

import pytest

from backend.scanner.database import upsert_vehicles
from backend.scanner.utils.vdp_extras_parse import (
    _clean,
    junk_text_value,
    parse_vdp_extras_from_html,
)

PAD = "<!-- " + "x" * 500 + " -->"
VIN = "3MW69CW01T8G81446"
DICT_EMPTY = {"_type": "s", "en_US": ""}


@pytest.mark.parametrize(
    "raw,expected",
    [
        ({"_type": "s", "en_US": "xDrive40i"}, "xDrive40i"),
        (DICT_EMPTY, None),
        ({"_type": "s"}, None),
        ("{'_type': 's', 'en_US': ''}", None),
        ("[[exteriorColor]]", None),
        ("[extra_cost_color]", None),
        ("null", None),
        ("", None),
        (None, None),
        (["a"], None),
        ("SR5", "SR5"),
        ("[Black] Crystal Bl", "[Black] Crystal Bl"),
    ],
)
def test_clean_trim(raw, expected):
    assert _clean(raw, 80) == expected


@pytest.mark.parametrize(
    "raw,expected",
    [("X9", None), ("21", None), ("T2g", None), ("Red", "Red"), ("Ice", "Ice"), ("Black", "Black"),
     ("White", "White"), ({"_type": "s", "en_US": "Mineral White"}, "Mineral White")],
)
def test_clean_color_option_codes(raw, expected):
    assert _clean(raw, color=True) == expected


def test_option_codes_only_apply_to_colors():
    assert _clean("X9") == "X9"  # e.g. a stock number or a trim stays
    assert junk_text_value("X9", color=True) and not junk_text_value("X9")


def _ddc_html(trim, ext):
    import json
    import html as h

    blob = json.dumps({"vin": VIN, "trim": trim, "exteriorColor": ext, "interiorColor": "Black", "stockNumber": "U1"})
    return f"<html><body class='vdp_main' data-vehicle='{h.escape(blob, quote=True)}'></body></html>" + PAD


def test_fixture_dict_trim_parses_to_real_trim_or_none():
    x = parse_vdp_extras_from_html(_ddc_html(DICT_EMPTY, "[[exteriorColor]]"), VIN)
    assert "trim" not in x and "exterior_color" not in x
    assert x["interior_color"] == "Black"
    x = parse_vdp_extras_from_html(_ddc_html({"_type": "s", "en_US": "M340i"}, "X9"), VIN)
    assert x["trim"] == "M340i" and "exterior_color" not in x


def _db(tmp_path, monkeypatch):
    db_path = tmp_path / "inventory.db"
    monkeypatch.setenv("INVENTORY_DB_PATH", str(db_path))
    from backend.db import inventory_db as inv

    monkeypatch.setattr(inv, "DB_PATH", str(db_path))
    return db_path


def _vehicle(**kw):
    v = {"vin": VIN, "title": "2025 BMW X3", "year": 2025, "make": "BMW", "model": "X3", "price": 50000,
         "condition": "New"}
    v.update(kw)
    return v


def _read(db_path):
    c = sqlite3.connect(str(db_path))
    try:
        return c.execute("SELECT trim, exterior_color, interior_color FROM cars WHERE vin=?", (VIN,)).fetchone()
    finally:
        c.close()


def _seed_junk(db_path, trim, ext, intc):
    c = sqlite3.connect(str(db_path))
    c.execute("UPDATE cars SET trim=?, exterior_color=?, interior_color=? WHERE vin=?", (trim, ext, intc, VIN))
    c.commit()
    c.close()


JUNK = ("{'_type': 's', 'en_US': ''}", "[[exteriorColor]]", "X9")


def test_upsert_real_value_replaces_stored_junk(tmp_path, monkeypatch):
    p = _db(tmp_path, monkeypatch)
    upsert_vehicles([_vehicle(trim="x")])
    _seed_junk(p, *JUNK)
    upsert_vehicles([_vehicle(trim="xDrive30i", exterior_color="Alpine White", interior_color="Black")])
    assert _read(p) == ("xDrive30i", "Alpine White", "Black")


def test_upsert_empty_incoming_nulls_stored_junk(tmp_path, monkeypatch):
    p = _db(tmp_path, monkeypatch)
    upsert_vehicles([_vehicle(trim="x")])
    _seed_junk(p, *JUNK)
    upsert_vehicles([_vehicle()])
    assert _read(p) == (None, None, None)


def test_upsert_keeps_stored_real_value_on_empty_or_junk_incoming(tmp_path, monkeypatch):
    p = _db(tmp_path, monkeypatch)
    upsert_vehicles([_vehicle(trim="xDrive30i", exterior_color="Alpine White", interior_color="Black")])
    upsert_vehicles([_vehicle()])
    assert _read(p) == ("xDrive30i", "Alpine White", "Black")
    upsert_vehicles([_vehicle(trim=JUNK[0], exterior_color=JUNK[1], interior_color=JUNK[2])])
    assert _read(p) == ("xDrive30i", "Alpine White", "Black")


def _car(**kw):
    c = {"vin": VIN, "title": "2025 BMW X3 xDrive30i", "year": 2025, "make": "BMW", "model": "X3",
         "trim": "xDrive30i", "price": 50000, "mileage": 5, "condition": "New", "fuel_type": "Gasoline",
         "transmission": "Automatic", "drivetrain": "AWD", "body_style": "SUV", "cylinders": 4,
         "engine_description": "2.0L I4", "exterior_color": "Alpine White", "interior_color": "Black",
         "image_url": "https://x.example/a.jpg"}
    c.update(kw)
    return c


def test_completeness_does_not_count_junk_as_filled():
    from backend.utils.listing_completeness import listing_missing_field_codes as codes

    ok = codes(_car(), for_public_filter=True)
    assert "trim" not in ok and "exterior_color" not in ok
    bad = codes(_car(trim=JUNK[0], exterior_color=JUNK[1]), for_public_filter=True)
    assert "trim" in bad and "exterior_color" in bad
    assert "exterior_color" in codes(_car(exterior_color="X9"), for_public_filter=True)


def test_heal_selection_categories():
    from backend.scripts.heal_vdp_junk_values import junk_columns

    assert junk_columns({"trim": JUNK[0], "exterior_color": JUNK[1], "interior_color": JUNK[2]}) == {
        "trim": "trim_dict", "exterior_color": "color_dbl", "interior_color": "color_option_code"}
    assert junk_columns({"trim": "[extra_cost_color]", "exterior_color": "[extra_cost_color]"}) == {
        "trim": "trim_token", "exterior_color": "color_single"}
    assert junk_columns({"trim": "SR5", "exterior_color": "Red", "interior_color": None}) == {}


def test_stored_junk_sql_avoids_negative_substr():
    from backend.scanner.upsert.sql import _stored_junk_sql
    for color in (False, True):
        assert "-1" not in _stored_junk_sql("cars.trim", color=color)
