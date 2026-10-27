"""model_specs fills go through fact precedence at the lowest rank (P8B.8)."""
from __future__ import annotations

import json
from typing import Any

import pytest

from backend.scanner import database as sdb

VIN = "1FTEW1EP5KFA00001"


class _Cur:
    def __init__(self, rows_by_chunk, log):
        self.rows, self.log, self._last = rows_by_chunk, log, []

    def execute(self, sql, params=()):
        self.log.append((sql, list(params)))
        vins = {str(p) for p in params}
        self._last = [r for r in self.rows if r[1] in vins] if params else list(self.rows)

    def fetchall(self):
        return list(self._last)


class _Conn:
    def __init__(self, rows):
        self.log: list = []
        self._cur = _Cur(rows, self.log)

    def cursor(self):
        return self._cur

    def commit(self):
        pass

    def close(self):
        pass


def _row(vin=VIN, cyl=None, trans=None, drive=None, body="SUV", fuel="Gasoline", eng=None, prov=None, id_=7):
    # id, vin, make, model, trim, title, cylinders, transmission, drivetrain, body_style, fuel, year, engine, prov
    return (id_, vin, "Ford", "Explorer", "XLT", "2019 Ford Explorer XLT", cyl, trans, drive, body, fuel, 2019, eng, prov)


@pytest.fixture()
def env(monkeypatch):
    writes: list[tuple[int, dict]] = []
    monkeypatch.setattr(
        "backend.db.repositories.cars_repo.update_car_row_partial",
        lambda cid, patch: writes.append((cid, patch)),
    )
    spec = {"cylinders": 6, "transmission": "Automatic", "drivetrain": "AWD", "body_style": "SUV", "fuel_type": "Gasoline"}
    monkeypatch.setattr(
        "backend.enrichment.model_specs_dictionary.lookup_model_specs_dictionary", lambda make, model: dict(spec)
    )
    ctxs: dict[str, dict] = {}
    monkeypatch.setattr(
        "backend.dictionary.enrich_from_dictionary.vpic_contexts_for_vins",
        lambda vins: {v: ctxs[v] for v in vins if v in ctxs},
    )
    return writes, spec, ctxs


def test_blank_drivetrain_with_vpic_4x2_is_not_filled_with_awd(env):
    writes, spec, ctxs = env
    ctxs[VIN] = {"vpic": {"DriveType": "4x2", "EngineCylinders": "6"}, "vpic_drive": "4x2"}
    n = sdb.apply_model_specs_corrections(vins=[VIN], conn=_Conn([_row(trans="Automatic", cyl=6)]))
    assert n == 0 and writes == []


def test_cylinder_fill_skipped_when_text_says_v6_and_specs_say_4(env):
    writes, spec, ctxs = env
    spec.update(cylinders=4, drivetrain=None, transmission=None, body_style=None, fuel_type=None)
    n = sdb.apply_model_specs_corrections(vins=[VIN], conn=_Conn([_row(eng="3.5L V6", drive="AWD", trans="Auto")]))
    assert n == 0 and writes == []


def test_cylinder_fill_skipped_when_vpic_states_another_count(env):
    writes, spec, ctxs = env
    spec.update(cylinders=4, drivetrain=None, transmission=None, body_style=None, fuel_type=None)
    ctxs[VIN] = {"vpic": {"EngineCylinders": "6"}}
    assert sdb.apply_model_specs_corrections(vins=[VIN], conn=_Conn([_row()])) == 0
    assert writes == []


def test_fill_without_a_decode_is_tagged_model_specs_per_field(env):
    writes, spec, ctxs = env
    n = sdb.apply_model_specs_corrections(vins=[VIN], conn=_Conn([_row()]))
    assert n == 1
    (cid, patch), = writes
    assert cid == 7 and patch["drivetrain"] == "AWD" and patch["cylinders"] == 6
    prov = json.loads(patch["spec_source_json"])
    for f in ("drivetrain", "cylinders", "transmission"):
        assert prov[f]["source"] == "model_specs"


def test_a_later_decode_overrides_a_model_specs_drive():
    from backend.vehicle_facts import precedence as prec

    row = {"drivetrain": "AWD", "spec_source_json": json.dumps({"drivetrain": {"source": "model_specs", "value": "AWD"}})}
    ch, _p, _d = prec.plan_updates(row, [("drivetrain", "2WD", "nhtsa_vpic")], {"vpic_drive": "4x2"})
    assert ch.get("drivetrain") == "2WD"


def test_dry_run_counts_and_writes_nothing(env):
    writes, _s, _c = env
    assert sdb.apply_model_specs_corrections(vins=[VIN], conn=_Conn([_row()]), dry_run=True) == 1
    assert writes == []


def test_stored_higher_ranked_value_is_kept(env):
    writes, spec, _c = env
    prov = json.dumps({"cylinders": {"source": "dealer_feed", "value": "4"}})
    spec.update(drivetrain=None, transmission=None, body_style=None, fuel_type=None)
    # cylinders is not blank, so nothing is proposed at all
    assert sdb.apply_model_specs_corrections(vins=[VIN], conn=_Conn([_row(cyl=4, prov=prov)])) == 0


def test_two_thousand_vins_use_in_lists_of_at_most_900_binds(env):
    vins = [f"1FTEW1EP5KFA{i:05d}" for i in range(2000)]
    conn = _Conn([])
    sdb.apply_model_specs_corrections(vins=vins, conn=conn)
    selects = [p for sql, p in conn.log if "IN (" in sql]
    assert len(selects) == 3 and max(len(p) for p in selects) == 900
    assert sum(len(p) for p in selects) == 2000
    for sql, p in conn.log:
        assert sql.count("?") == len(p)


def test_fleet_script_defaults_to_dry_run(monkeypatch, capsys):
    from backend.scripts import apply_model_specs as script

    seen: dict[str, Any] = {}

    def fake(vins=None, conn=None, *, dry_run=False):
        seen.update(vins=vins, dry_run=dry_run)
        return 3

    monkeypatch.setattr(sdb, "apply_model_specs_corrections", fake)
    assert script.main(["--all"]) == 0
    assert seen == {"vins": None, "dry_run": True}
    script.main(["--all", "--apply"])
    assert seen["dry_run"] is False
