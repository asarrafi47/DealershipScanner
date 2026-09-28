"""F06 (docs/DATA_COMPLETENESS_FINDINGS_2026_09_28.md): the vPIC cache already holds
DisplacementL + EngineCylinders (6,338 engine-null rows), BodyClass (1,219
body-null rows) and the EV fields; heal_rows now fills engine_description /
body_style from it, fill-only, never over the dealer's own engine text."""
from __future__ import annotations

import json
import sqlite3

import pytest

from backend.enrichment import vpic_facts as vf

TACOMA = {"VIN": "3TYLB5JNXRT022195", "DisplacementL": "2.4", "EngineCylinders": "4", "EngineConfiguration": "In-Line",
          "EngineModel": "T24A-FTS", "Turbo": "Yes", "BodyClass": "Pickup", "DriveType": "4WD/4-Wheel Drive/4x4",
          "FuelTypePrimary": "Gasoline", "ElectrificationLevel": ""}
LYRIQ = {"VIN": "1GYKPMRL5RZ100001", "DisplacementL": "", "EngineCylinders": "", "EngineConfiguration": "",
         "EngineModel": "", "BodyClass": "Sport Utility Vehicle (SUV)/Multi-Purpose Vehicle (MPV)",
         "DriveType": "AWD/All-Wheel Drive", "FuelTypePrimary": "Electric", "ElectrificationLevel": "BEV (Battery Electric Vehicle)",
         "EVDriveUnit": "Dual Motor", "EngineHP": "500", "EngineKW": "373"}
BLANK = {"VIN": "5J6RS4H29VL003929", "DisplacementL": "", "EngineCylinders": "", "BodyClass": "", "DriveType": "",
         "FuelTypePrimary": "", "ElectrificationLevel": ""}


def _vp(flat: dict) -> dict:
    el = flat.get("ElectrificationLevel", "").lower()
    return {"drivetrain": "4WD" if "4WD" in flat.get("DriveType", "") else ("AWD" if "AWD" in flat.get("DriveType", "") else None),
            "electrification": "ev" if "bev" in el else None,
            "fuel_type": {"Gasoline": "Gasoline", "Electric": "Electric"}.get(flat.get("FuelTypePrimary", "")),
            "cylinders": int(flat["EngineCylinders"]) if flat.get("EngineCylinders", "").isdigit() else None,
            "body_style": {"Pickup": "Truck"}.get(flat.get("BodyClass", ""), "SUV" if "SUV" in flat.get("BodyClass", "") else None),
            "engine_l": None, "transmission": None, "trim": None}


@pytest.fixture
def conn(monkeypatch):
    c = sqlite3.connect(":memory:")
    c.executescript(
        "CREATE TABLE cars (id INTEGER PRIMARY KEY, vin TEXT, dealer_id TEXT, listing_active INTEGER, drivetrain TEXT,"
        " fuel_type TEXT, cylinders INTEGER, engine_description TEXT, body_style TEXT, spec_source_json TEXT, epa_master_id INTEGER);"
        "CREATE TABLE epa_master (id INTEGER PRIMARY KEY, drive TEXT);"
        "CREATE TABLE nhtsa_vpic_cache (vin TEXT PRIMARY KEY, response_json TEXT, fetched_at TEXT);"
    )
    flats = {f["VIN"]: f for f in (TACOMA, LYRIQ, BLANK)}
    for vin, f in flats.items():
        c.execute("INSERT INTO nhtsa_vpic_cache VALUES (?,?,?)", (vin, json.dumps({"Results": [f]}), "2026-09-28T00:00:00Z"))
    monkeypatch.setattr("backend.enrichment.knowledge_engine.prime_vpic_cache", lambda vins: None)
    monkeypatch.setattr("backend.enrichment.knowledge_engine.clear_vpic_lookup_cache", lambda: None)
    monkeypatch.setattr("backend.enrichment.knowledge_engine.lookup_vpic_from_cache",
                        lambda vin: _vp(flats[vin]) if vin in flats else {})
    return c


def _row(conn, vin):
    r = conn.execute("SELECT engine_description, body_style, drivetrain, cylinders, spec_source_json FROM cars WHERE vin=?", (vin,)).fetchone()
    return {"engine_description": r[0], "body_style": r[1], "drivetrain": r[2], "cylinders": r[3],
            "prov": json.loads(r[4]) if r[4] else {}}


def test_engine_and_body_filled_from_decode_with_provenance(conn):
    conn.execute("INSERT INTO cars VALUES (1,?, 'd1', 1, 'RWD', 'Gasoline', NULL, NULL, NULL, NULL, NULL)", (TACOMA["VIN"],))
    stats = vf.heal_rows(conn)
    row = _row(conn, TACOMA["VIN"])
    assert row["engine_description"] == "2.4L I4 T24A-FTS Turbo"
    assert row["body_style"] == "Truck"
    assert row["drivetrain"] == "4WD" and row["cylinders"] == 4  # the existing overrides still apply
    assert row["prov"]["engine_description"]["source"] == "nhtsa_vpic"
    assert row["prov"]["body_style"]["source"] == "nhtsa_vpic"
    assert stats["engine_description"] == 1 and stats["body_style"] == 1


def test_dealer_engine_text_and_body_are_never_overwritten(conn):
    conn.execute("INSERT INTO cars VALUES (1,?, 'd1', 1, '4WD', 'Gasoline', 4, 'i-FORCE 2.4L 4-Cyl Turbo', 'Crew Cab', NULL, NULL)",
                 (TACOMA["VIN"],))
    stats = vf.heal_rows(conn)
    row = _row(conn, TACOMA["VIN"])
    assert row["engine_description"] == "i-FORCE 2.4L 4-Cyl Turbo" and row["body_style"] == "Crew Cab"
    assert stats["engine_description"] == 0 and stats["body_style"] == 0


def test_ev_gets_motor_line_not_a_cylinder_engine(conn):
    conn.execute("INSERT INTO cars VALUES (1,?, 'd1', 1, 'AWD', 'Electric', 0, '', NULL, NULL, NULL)", (LYRIQ["VIN"],))
    vf.heal_rows(conn)
    row = _row(conn, LYRIQ["VIN"])
    assert row["engine_description"] == "Electric motor, Dual Motor, 500 hp / 373 kW"
    assert row["body_style"] == "SUV"


def test_blank_decode_changes_nothing(conn):
    conn.execute("INSERT INTO cars VALUES (1,?, 'd1', 1, NULL, NULL, NULL, NULL, NULL, NULL, NULL)", (BLANK["VIN"],))
    stats = vf.heal_rows(conn)
    row = _row(conn, BLANK["VIN"])
    assert row["engine_description"] is None and row["body_style"] is None
    assert stats["engine_description"] == 0 and stats["body_style"] == 0


def test_dry_run_writes_nothing(conn):
    conn.execute("INSERT INTO cars VALUES (1,?, 'd1', 1, 'RWD', 'Gasoline', NULL, NULL, NULL, NULL, NULL)", (TACOMA["VIN"],))
    stats = vf.heal_rows(conn, dry_run=True)
    assert stats["engine_description"] == 1
    assert _row(conn, TACOMA["VIN"])["engine_description"] is None


def test_derived_fills_is_pure_and_ev_without_fields_stays_empty():
    assert vf.derived_fills({"engine_description": "", "body_style": ""}, {"electrification": "ev", "body_style": None},
                            {"BodyClass": "Not Applicable"}) == {}
    assert vf.derived_fills({"engine_description": "3.5L V6", "body_style": None}, {"body_style": "Sedan"}, {}) == \
        {"body_style": (None, "Sedan")}
