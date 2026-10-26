"""N6.9: length != 17 and model year 0/NULL never reach cars; used-mileage coverage (D-N16, D-N17)."""
from __future__ import annotations

import sqlite3

import pytest

from backend.parsers import dealer_eprocess
from backend.scanner.database import upsert_vehicles
from backend.scanner.identifier_gate import gate_reason, year_from_vin
from backend.scanner.phases.dealer_run_steps.enrich import _capture_coverage, used_mileage_coverage

GOOD = "1HGCM82633A004352"  # 2003 (7th char digit, 10th char 3)
BAD1 = "1HGCM8263UA004352"  # 10th char U decodes to no year
BAD2 = "2HGCM8263UA004352"
GOOD_2023 = "1FTFW1ET5PFA00001"  # 7th char letter, 10th char P = 2023


def _car(vin, year=2024, **kw):
    return {"vin": vin, "title": "t", "year": year, "make": "Honda", "model": "Accord", "price": 20000, **kw}


@pytest.fixture
def db(tmp_path, monkeypatch):
    path = tmp_path / "t.db"
    monkeypatch.setenv("INVENTORY_DB_PATH", str(path))
    from backend.db import inventory_db as inv

    monkeypatch.setattr(inv, "DB_PATH", str(path))
    return path


def _vins(path):
    c = sqlite3.connect(str(path))
    try:
        return {r[0]: r[1] for r in c.execute("SELECT vin, year FROM cars")}
    finally:
        c.close()


def test_year_from_vin():
    assert year_from_vin(GOOD) == 2003
    assert year_from_vin(GOOD_2023) == 2023
    assert year_from_vin("SHORT") is None


def test_upsert_refuses_non_17_identifiers(db):
    stats: dict = {}
    n = upsert_vehicles(
        [_car("A1234567"), _car("12345678901"), _car("MH-ORDER-0042"), _car(GOOD_2023 + "X"), _car(GOOD_2023)],
        stats,
    )
    assert n == 1
    assert list(_vins(db)) == [GOOD_2023]
    assert stats["identifier_gate_refused"] == {"vin_length_not_17": 4}


def test_upsert_refuses_model_year_zero_and_null_unless_recoverable(db):
    stats: dict = {}
    upsert_vehicles([_car(BAD1, year=0), _car(BAD2, year=None), _car(GOOD_2023, year=0)], stats)
    got = _vins(db)
    assert BAD1 not in got and BAD2 not in got
    assert got == {GOOD_2023: 2023}  # recovered from the VIN, never written as 0
    assert stats["identifier_gate_refused"] == {"model_year_missing": 2}
    assert 0 not in got.values() and None not in got.values()


def test_gate_keeps_i_o_q_17_char_ids():
    # D-N17b: 17-character IDs with I/O/Q are decided in N21, not refused here.
    assert gate_reason(_car("1HGCM8263IA004352")) is None


def _ld(vin, **kw):
    return {"@type": "Vehicle", "vehicleIdentificationNumber": vin, "model": "Camry", "vehicleModelDate": "2024", **kw}


def test_dealer_eprocess_parse_refuses_short_and_order_numbers():
    rows = [dealer_eprocess._map(_ld(v), "https://d.example", "d", "D", "https://d.example")
            for v in ("K0123456", "ORDER-123456", "12345678901", GOOD_2023.lower())]
    assert rows[:3] == [None, None, None]
    assert rows[3]["vin"] == GOOD_2023


def test_used_mileage_coverage_reports_zero_and_exempts_f4():
    cars = [
        {"condition": "Used", "mileage": 30000, "year": 2020},
        {"condition": "Used", "mileage": 0, "year": 2019},
        {"condition": "Certified", "mileage": 0, "year": 2026},  # F4: data question
        {"condition": "New", "mileage": 0, "year": 2026},
    ]
    out = used_mileage_coverage(cars)
    assert out == {"used_n": 3, "used_zero_mi": 2, "used_zero_mi_my2026": 1, "used_mileage": 0.5}
    assert _capture_coverage(cars)["used_zero_mi"] == 2


def test_assess_gate_marks_thin_on_used_mileage():
    import json

    from backend.scripts import dealer_pipeline as dp
    from backend.tests.test_assess_stamped_rows import SINCE, WALK, _db

    for used_mileage, verdict_thin in ((0.4, True), (0.95, False)):
        conn = _db(stamped_rows=40, old_rows=40, feed_rows=40)
        cc = {"n": 40, "price": 1.0, "trim": 1.0, "exterior_color": 1.0, "used_mileage": used_mileage}
        conn.execute("UPDATE scan_runs SET summary_json = ?", (json.dumps({"capture_coverage": cc, "recipe_replay": WALK}),))
        out = dp.assess(conn, "mb-com", SINCE, known_before=40, recipe_info={"had_recipes": 1})
        assert (out["verdict"] == "thin") is verdict_thin
        if verdict_thin:
            assert "used_mileage" in out["reason"]
