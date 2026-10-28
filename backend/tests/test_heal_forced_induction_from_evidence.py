"""P10C.9: forced-induction heal tool (dry run, backup-gated apply, restore)."""
from __future__ import annotations

import csv
import json
import sqlite3

import pytest

from backend.catalog.resolver import _CANDIDATE_COLS
from backend.db.inventory_compat import InventoryConnection
from backend.scripts import heal_forced_induction_from_evidence as heal

_CAR_COLS = (
    "id", "vin", "dealer_id", "listing_active", "year", "make", "model", "trim",
    "cylinders", "engine_l", "engine_description", "drivetrain", "fuel_type", "title",
    "description", "epa_master_id", "forced_induction", "spec_source_json",
)


def _epa(id_, disp, cyl, t=None):
    row = dict.fromkeys(_CANDIDATE_COLS)
    row.update(id=id_, year=2022, make="Ford", model="F150", trim="Pickup", cylinders=cyl,
               displacement=disp, drive="Rear-Wheel Drive", fuel_type="Regular Gasoline", t_charger=t)
    return tuple(row[c] for c in _CANDIDATE_COLS)


def _car(id_, engine_l, cyl, desc, fi, prov=None, trim="XLT"):
    row = dict.fromkeys(_CAR_COLS)
    row.update(id=id_, vin=f"1FTFW1E80TFA{id_:05d}", dealer_id="d1", listing_active=1, year=2022,
               make="Ford", model="F150", trim=trim, cylinders=cyl, engine_l=engine_l,
               engine_description=desc, fuel_type="Gasoline", title="2022 Ford F150",
               forced_induction=fi, spec_source_json=prov)
    return tuple(row[c] for c in _CAR_COLS)


@pytest.fixture
def conn(monkeypatch):
    raw = sqlite3.connect(":memory:")
    raw.execute(f"CREATE TABLE cars ({', '.join(_CAR_COLS)})")
    raw.execute(f"CREATE TABLE epa_master ({', '.join(_CANDIDATE_COLS)})")
    ph = ", ".join("?" * len(_CANDIDATE_COLS))
    raw.execute(f"INSERT INTO epa_master VALUES ({ph})", _epa(1, 5.0, 8))        # NA-only V8
    raw.execute(f"INSERT INTO epa_master VALUES ({ph})", _epa(2, 3.5, 6, t="Y"))  # boosted V6
    cph = ", ".join("?" * len(_CAR_COLS))
    dealer = json.dumps({"forced_induction": {"source": "dealer_feed", "value": "Turbocharged"}})
    for c in (
        _car(1, 5.0, 8, "5.0L V8", None),                       # blank -> NA
        _car(2, 3.5, 6, "3.5L V6", None),                       # blank -> boost
        _car(3, 5.0, 8, "5.0L V8", "Twin Turbocharged",
             json.dumps({"forced_induction": {"source": "trim_heuristic", "value": "Twin Turbocharged"}})),  # flip
        _car(4, 5.0, 8, "5.0L V8 Turbo", "Turbocharged", dealer),  # dealer text kept, conflicts with family
    ):
        raw.execute(f"INSERT INTO cars VALUES ({cph})", c)
    raw.commit()
    monkeypatch.setattr("backend.enrichment.knowledge_engine.prime_vpic_cache", lambda vins: None)
    monkeypatch.setattr("backend.enrichment.knowledge_engine.lookup_vpic_from_cache", lambda vin: {})
    yield InventoryConnection(raw, backend="sqlite", shared=True)
    raw.close()


def _state(conn):
    cur = conn.cursor()
    cur.execute("SELECT id, forced_induction, spec_source_json FROM cars ORDER BY id")
    return cur.fetchall()


def test_dry_run_reports_transitions_and_writes_nothing(conn, tmp_path) -> None:
    before = _state(conn)
    res = heal.heal(conn, out_dir=tmp_path, today="2026-10-27")
    assert _state(conn) == before
    assert res["mode"] == "dry_run"
    assert res["by_kind"] == {"na_write": 1, "boost_fill": 1, "boost_to_na": 1}
    assert res["matrix"]["(blank) -> Naturally Aspirated"] == 1
    assert res["matrix"]["boosted -> Naturally Aspirated"] == 1
    assert res["by_source"] == {"epa_catalog": 3}
    rows = list(csv.DictReader(open(res["report_path"])))
    assert {r["id"] for r in rows} == {"1", "2", "3"}  # dealer-text row 4 untouched


def test_conflict_sample_lists_dealer_text_vs_family(conn, tmp_path) -> None:
    res = heal.heal(conn, out_dir=tmp_path, sample=10, today="2026-10-27")
    assert res["conflicts"] == 1
    rows = list(csv.DictReader(open(res["sample_path"])))
    assert [r["id"] for r in rows] == ["4"]


def test_apply_refuses_without_backup(conn, tmp_path) -> None:
    before = _state(conn)
    with pytest.raises(heal.BackupRequired):
        heal.heal(conn, apply=True, out_dir=tmp_path)
    assert _state(conn) == before


def test_apply_then_restore_round_trips(conn, tmp_path) -> None:
    before = _state(conn)
    bk = tmp_path / "bk.csv"
    res = heal.heal(conn, apply=True, backup_path=bk, out_dir=tmp_path)
    assert res["changes"] == 3 and bk.exists()
    after = {r[0]: r[1] for r in _state(conn)}
    assert after == {1: "Naturally Aspirated", 2: "Turbocharged", 3: "Naturally Aspirated", 4: "Turbocharged"}
    assert heal.restore(conn, bk) == 3
    assert _state(conn) == before
