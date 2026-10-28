"""P10C.9: forced-induction heal tool (dry run, backup-gated apply, restore) on Postgres."""
from __future__ import annotations

import csv
import json

import pytest

from backend.scripts import heal_forced_induction_from_evidence as heal

pytestmark = pytest.mark.pg

DEALER = json.dumps({"forced_induction": {"source": "dealer_feed", "value": "Turbocharged"}})
HEUR = json.dumps({"forced_induction": {"source": "trim_heuristic", "value": "Twin Turbocharged"}})


def _vin(i: int) -> str:
    return f"1FTFW1E80TFA{i:05d}"


@pytest.fixture
def env(pg_db, monkeypatch):
    with pg_db.connect() as c:
        for disp, cyl, t in ((5.0, 8, None), (3.5, 6, "Y")):
            c.execute(
                "INSERT INTO epa_master (year, make, model, trim, cylinders, displacement, "
                "t_charger, fuel_type) VALUES (2022, 'Ford', 'F150', 'Pickup', %s, %s, %s, 'Regular Gasoline')",
                (cyl, disp, t),
            )
        c.commit()
    base = dict(dealer_id="d1", year=2022, make="Ford", model="F150", trim="XLT",
                fuel_type="Gasoline", title="2022 Ford F150")
    pg_db.add_cars([
        {**base, "vin": _vin(1), "engine_l": 5.0, "cylinders": 8, "engine_description": "5.0L V8"},
        {**base, "vin": _vin(2), "engine_l": 3.5, "cylinders": 6, "engine_description": "3.5L V6"},
        {**base, "vin": _vin(3), "engine_l": 5.0, "cylinders": 8, "engine_description": "5.0L V8",
         "forced_induction": "Twin Turbocharged", "spec_source_json": HEUR},
        {**base, "vin": _vin(4), "engine_l": 5.0, "cylinders": 8, "engine_description": "5.0L V8 Turbo",
         "forced_induction": "Turbocharged", "spec_source_json": DEALER},
    ])
    monkeypatch.setattr("backend.enrichment.knowledge_engine.prime_vpic_cache", lambda vins: None)
    monkeypatch.setattr("backend.enrichment.knowledge_engine.lookup_vpic_from_cache", lambda vin: {})
    return pg_db


def _state(env):
    with env.connect() as c:
        return c.execute("SELECT vin, forced_induction, spec_source_json FROM cars ORDER BY vin").fetchall()


def _run(env, **kw):
    conn = env.get_conn()
    try:
        return heal.heal(conn, **kw)
    finally:
        conn.close()


def test_dry_run_reports_transitions_and_writes_nothing(env, tmp_path) -> None:
    before = _state(env)
    res = _run(env, out_dir=tmp_path, today="2026-10-27")
    assert _state(env) == before
    assert res["mode"] == "dry_run"
    assert res["by_kind"] == {"na_write": 1, "boost_fill": 1, "boost_to_na": 1}
    assert res["matrix"]["(blank) -> Naturally Aspirated"] == 1
    assert res["matrix"]["boosted -> Naturally Aspirated"] == 1
    assert res["by_source"] == {"epa_catalog": 3}
    rows = list(csv.DictReader(open(res["report_path"])))
    assert {r["vin"] for r in rows} == {_vin(1), _vin(2), _vin(3)}  # dealer-text row untouched


def test_conflict_sample_lists_dealer_text_vs_family(env, tmp_path) -> None:
    res = _run(env, out_dir=tmp_path, sample=10, today="2026-10-27")
    assert res["conflicts"] == 1
    assert [r["vin"] for r in csv.DictReader(open(res["sample_path"]))] == [_vin(4)]


def test_apply_refuses_without_backup(env, tmp_path) -> None:
    before = _state(env)
    with pytest.raises(heal.BackupRequired):
        _run(env, apply=True, out_dir=tmp_path)
    assert _state(env) == before


def test_apply_then_restore_round_trips(env, tmp_path) -> None:
    before = _state(env)
    bk = tmp_path / "bk.csv"
    res = _run(env, apply=True, backup_path=bk, out_dir=tmp_path)
    assert res["changes"] == 3 and bk.exists()
    assert {v: fi for v, fi, _ in _state(env)} == {
        _vin(1): "Naturally Aspirated", _vin(2): "Turbocharged",
        _vin(3): "Naturally Aspirated", _vin(4): "Turbocharged",
    }
    conn = env.get_conn()
    try:
        assert heal.restore(conn, bk) == 3
    finally:
        conn.close()
    assert _state(env) == before
