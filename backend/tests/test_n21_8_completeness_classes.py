"""N21.8: Mercedes designation (F1), explicit single-trim list (M1), non-road field
sets (F3), make/model fill from a clean decode (2.24), and the dead recovery script's
removal (2.18, consumer-audited)."""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from backend.enrichment import vpic_facts as vf
from backend.utils.listing_completeness import listing_missing_field_codes
from backend.vehicle_facts import listing_classes as lc

pg = pytest.mark.pg
REPO = Path(__file__).resolve().parents[2]


def _row(**kw):
    base = {
        "vin": "1HGBH41JXMN109186", "title": "2020 Honda Civic LX", "year": 2020, "make": "Honda",
        "model": "Civic", "trim": "LX", "price": 22000, "mileage": 5000, "transmission": "CVT",
        "drivetrain": "FWD", "fuel_type": "Gasoline", "exterior_color": "Crystal Black",
        "interior_color": "Gray", "image_url": "https://example.com/hero.jpg", "gallery": [],
        "body_style": "Sedan", "engine_description": "1.5L turbo I4", "condition": "Used", "cylinders": 4,
    }
    base.update(kw)
    return base


def _codes(row):
    with patch("backend.utils.listing_completeness.prepare_car_detail_context", return_value={"verified_specs": {}}), \
         patch.object(lc, "vpic_vehicle_type", return_value=row.pop("_vt", "")):
        return listing_missing_field_codes(row, for_public_filter=True)


# -- F1 ----------------------------------------------------------------------------

@pytest.mark.parametrize("model", ["C 300", "GLC 300", "GLA 250", "CLA 220", "E 450", "GLB 250", "EQS 580", "S 580", "GLE 450e"])
def test_mercedes_designation_models(model):
    assert lc.mercedes_model_is_designation("Mercedes-Benz", model)


@pytest.mark.parametrize("make,model", [
    ("Mercedes-Benz", "GLC"), ("Mercedes-Benz", "CLA"), ("Mercedes-Benz", "Sprinter 2500"),
    ("Mercedes-Benz", "C-Class"), ("Mercedes-Benz", ""), ("Lexus", "RX 350"), ("BMW", "X5 40"),
])
def test_mercedes_family_or_other_make_is_not_a_designation(make, model):
    assert not lc.mercedes_model_is_designation(make, model)


def test_blank_trim_on_mercedes_designation_is_not_a_gap_but_family_is():
    assert "trim" not in _codes(_row(make="Mercedes-Benz", model="GLC 300", trim=None))
    assert "trim" in _codes(_row(make="Mercedes-Benz", model="GLC", trim=None))
    assert "trim" in _codes(_row(make="Mercedes-Benz", model="Sprinter 2500", trim=None))


# -- M1 ----------------------------------------------------------------------------

def test_single_trim_list_is_year_bounded():
    assert lc.model_is_single_trim(2026, "Honda", "Prelude")
    assert lc.model_is_single_trim(2023, "BMW", "XM")
    assert not lc.model_is_single_trim(2024, "BMW", "XM")
    assert not lc.model_is_single_trim(None, "BMW", "XM")
    assert lc.model_is_single_trim(2021, "Toyota", "Mirai")
    assert lc.model_is_single_trim(2020, "Audi", "RS Q8") and lc.model_is_single_trim(2023, "Audi", "RS-Q8")


@pytest.mark.parametrize("year,make,model", [
    (2027, "Toyota", "Land Cruiser"), (2026, "Honda", "Odyssey"), (2019, "Subaru", "WRX"), (2024, "BMW", "XM"),
])
def test_blank_trim_off_the_list_is_a_gap_whatever_the_catalog_says(year, make, model):
    # The catalog says the model name is the only trim; the explicit list is the authority now.
    with patch("backend.enrichment.knowledge_engine.catalog_model_is_trimless", return_value=True, create=True):
        assert "trim" in _codes(_row(year=year, make=make, model=model, trim=None))


def test_blank_trim_on_listed_single_trim_model_is_not_a_gap():
    assert "trim" not in _codes(_row(year=2026, make="Honda", model="Prelude", trim=None))


# -- F3 ----------------------------------------------------------------------------

_BARE = dict(engine_description=None, transmission=None, drivetrain=None, fuel_type=None, cylinders=None)


def test_trailer_is_not_judged_for_powertrain_fields():
    codes = _codes(_row(_vt="TRAILER", **_BARE))
    assert not {"engine", "transmission", "drivetrain", "fuel_type", "cylinders"} & set(codes)


def test_trailer_keeps_real_gaps():
    assert "price" in _codes(_row(_vt="TRAILER", price=None, msrp=None, **_BARE))


def test_motorcycle_exempt_from_drivetrain_only():
    codes = _codes(_row(_vt="MOTORCYCLE", **_BARE))
    assert "drivetrain" not in codes and "transmission" in codes and "engine" in codes


def test_cart_and_boat_by_body_style_without_a_decode():
    assert "drivetrain" not in _codes(_row(body_style="Golf Cart", vin="J0A0000000Y106625", **{**_BARE}))
    assert "engine" not in _codes(_row(body_style="Boat", **_BARE))


def test_road_car_still_flags_its_powertrain_gaps():
    codes = _codes(_row(_vt="PASSENGER CAR", **_BARE))
    assert {"engine", "transmission", "drivetrain", "fuel_type"} <= set(codes)


def test_non_road_class_prefers_vpic_then_body_style():
    assert lc.non_road_class({"body_style": "Sedan"}, vehicle_type="TRAILER") == "trailer"
    assert lc.non_road_class({"body_style": "Sedan"}, vehicle_type="LOW SPEED VEHICLE (LSV)") == "cart"
    assert lc.non_road_class({"body_style": "Motorcycle"}, vehicle_type="") == "motorcycle"
    assert lc.non_road_class({"body_style": "SUV"}, vehicle_type="MULTIPURPOSE PASSENGER VEHICLE (MPV)") is None


# -- 2.24 make/model from a clean decode -------------------------------------------

@pytest.mark.parametrize("flat,expected", [
    ({"ErrorCode": "0", "Make": "TOYOTA", "Model": "Crown Signia"}, {"make": "Toyota", "model": "Crown Signia"}),
    ({"ErrorCode": "0", "Make": "MORGAN", "Model": "Super 3"}, {"make": "Morgan", "model": "Super 3"}),
    ({"ErrorCode": "0", "Make": "BMW", "Model": "750i, Alpina B7"}, {"make": "BMW"}),
    ({"ErrorCode": "0,6", "Make": "TOYOTA", "Model": "Crown"}, {}),
    ({"ErrorCode": "", "Make": "TOYOTA", "Model": "Crown"}, {}),
    ({"ErrorCode": "0", "Make": "Not Applicable", "Model": "Crown"}, {"model": "Crown"}),
    (None, {}),
])
def test_clean_vpic_make_model(flat, expected):
    assert vf.clean_vpic_make_model(flat) == expected


V1, V2, V3 = "JTDAGACF9V3000896", "SA9M3TAA1PP202041", "WBA13FK05VCX97005"


@pytest.fixture
def conn(pg_db, monkeypatch):
    c = pg_db.get_conn()
    flat = {
        V1: {"VIN": V1, "ErrorCode": "0", "Make": "TOYOTA", "Model": "Crown"},
        V2: {"VIN": V2, "ErrorCode": "0", "Make": "MORGAN", "Model": "Super 3"},
        V3: {"VIN": V3, "ErrorCode": "0", "Make": "BMW", "Model": "750i, Alpina B7"},
    }
    monkeypatch.setattr("backend.enrichment.knowledge_engine.prime_vpic_cache", lambda vins: None)
    monkeypatch.setattr("backend.enrichment.knowledge_engine.clear_vpic_lookup_cache", lambda: None)
    monkeypatch.setattr("backend.enrichment.knowledge_engine.lookup_vpic_from_cache", lambda vin: {})
    monkeypatch.setattr(vf, "_vpic_flat_rows", lambda _c, vins: {v: flat[v] for v in vins if v in flat})
    rows = [(1, V1, "Toyota", ""), (2, V2, "", None), (3, V3, "BMW", ""), (4, "9ABCD2F63TA000004", "Honda", "Civic")]
    for i, vin, mk, md in rows:
        c.execute("INSERT INTO cars (id, vin, dealer_id, title, listing_active, make, model) VALUES (?, ?, 'd', 't', 1, ?, ?)",
                  (i, vin, mk, md))
    c.commit()
    return c


def _mm(conn, i):
    mk, md, raw = conn.execute("SELECT make, model, spec_source_json FROM cars WHERE id=?", (i,)).fetchone()
    prov = (raw if isinstance(raw, dict) else json.loads(raw)) if raw else {}
    return mk, md, prov


@pg
def test_blank_make_model_filled_from_clean_single_valued_decode(conn):
    stats = vf.heal_rows(conn)
    assert stats["model"] == 2 and stats["make"] == 1
    assert _mm(conn, 1)[:2] == ("Toyota", "Crown")
    mk, md, prov = _mm(conn, 2)
    assert (mk, md) == ("Morgan", "Super 3")
    assert prov["model"]["source"] == "nhtsa_vpic" and prov["model"]["previous"] in (None, "")


@pg
def test_list_valued_model_stays_blank_and_filled_values_are_never_overwritten(conn):
    vf.heal_rows(conn)
    assert _mm(conn, 3)[:2] == ("BMW", "")
    assert _mm(conn, 4)[:2] == ("Honda", "Civic")


@pg
def test_dry_run_writes_nothing(conn):
    vf.heal_rows(conn, dry_run=True)
    assert _mm(conn, 1)[1] == ""


# -- 2.18 consumer audit ----------------------------------------------------------

def test_dead_recovery_script_and_helper_are_gone_with_no_consumer_left():
    assert not (REPO / "backend/scripts/recover_incomplete_listings.py").exists()
    assert not (REPO / "backend/utils/incomplete_recovery.py").exists()
    hits = []
    for path in (REPO / "backend").rglob("*.py"):
        if path.name == Path(__file__).name:
            continue
        text = path.read_text(errors="ignore")
        if "incomplete_recovery" in text or "recover_incomplete_listings" in text:
            hits.append(str(path.relative_to(REPO)))
    assert hits == []


def test_recovery_columns_stay_until_a_migration_drops_them():
    # DDL is split off N21.8: the six columns remain in the schema and the cars_repo lists.
    src = (REPO / "backend/db/repositories/cars_repo.py").read_text()
    for col in ("recovery_status", "recovery_attempted_at", "recoverability_score"):
        assert col in src
