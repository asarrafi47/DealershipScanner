"""Exit-gate invariants for N16 (FP08, M5): fact writers never store

* a "Hybrid" fuel label on a mild hybrid (vPIC says MHEV, or the evidence-backed
  eTorque family), whichever source proposes it, or
* an engine layout (cylinders) on an electric vehicle, and the NHTSA vPIC decode
  still outranks the dealer feed for electrification and drivetrain.

The writers are exercised through ``apply_fact_proposals`` and the column-level
primitive it ends in (``update_car_row_partial``), with the DB layer faked so the
test needs no server.
"""
from __future__ import annotations

import contextlib

import pytest

from backend.vehicle_facts import precedence as prec
from backend.vehicle_facts.apply import apply_fact_proposals

_MHEV = {"ElectrificationLevel": "Mild Hybrid Electric Vehicle (MHEV)", "FuelTypePrimary": "Gasoline"}
_BEV = {"ElectrificationLevel": "Battery Electric Vehicle (BEV)", "FuelTypePrimary": "Electric"}
_WRITING_SOURCES = [s for s in prec.SOURCES
                    if s not in (prec.UNKNOWN, prec.LLM, prec.STICKER_UNVERIFIED, prec.TRIM_HEURISTIC,
                                 prec.EPA_TIEBREAK)]

_RAM = {"id": 7, "vin": "", "make": "Ram", "model": "1500", "year": 2022, "title": "2022 Ram 1500 Big Horn",
        "engine_description": "5.7L V8 eTorque", "fuel_type": "Gasoline"}


class _FakeConn:
    def __init__(self, sink):
        self.sink = sink

    def execute(self, sql, vals=()):
        self.sink.append((sql, list(vals)))

    def commit(self):
        pass


@pytest.fixture
def captured(monkeypatch):
    sink: list = []
    from backend.db.repositories import cars_repo
    from backend.utils import fuel_type_normalize as ftn

    @contextlib.contextmanager
    def fake_db_conn(*a, **k):
        yield _FakeConn(sink)

    monkeypatch.setattr(cars_repo, "db_conn", fake_db_conn)
    monkeypatch.setattr(cars_repo, "get_car_by_id", lambda cid: dict(_RAM))
    monkeypatch.setattr(ftn, "catalog_fuel_type_for_car", lambda car: None)
    return sink


def _written_columns(sink) -> dict:
    cols: dict = {}
    for sql, vals in sink:
        names = [p.split("=")[0].strip() for p in sql.split("SET", 1)[1].rsplit("WHERE", 1)[0].split(",")]
        cols.update(dict(zip(names, vals)))
    return cols


@pytest.mark.parametrize("source", _WRITING_SOURCES)
def test_no_writer_stores_hybrid_on_a_mild_hybrid(captured, source):
    row = {**_RAM, "fuel_type": None}
    apply_fact_proposals(None, row, [("fuel_type", "Hybrid", source)], vpic=_MHEV)
    cols = _written_columns(captured)
    if "fuel_type" in cols:
        assert cols["fuel_type"] != "Hybrid", (source, cols)


def test_hybrid_label_on_mild_hybrid_is_stored_as_gasoline(captured):
    row = {**_RAM, "fuel_type": None}
    res = apply_fact_proposals(None, row, [("fuel_type", "Hybrid", prec.DEALER_FEED)], vpic=_MHEV)
    assert res.written
    assert _written_columns(captured)["fuel_type"] == "Gasoline"


def test_vpic_mhev_decode_never_writes_hybrid_over_gasoline():
    res = apply_fact_proposals(None, {"id": 1, "fuel_type": "Gasoline"},
                               [("fuel_type", "Mild Hybrid", prec.NHTSA_VPIC)], vpic=_MHEV, dry_run=True)
    assert "fuel_type" not in res.applied


@pytest.mark.parametrize("source", _WRITING_SOURCES)
def test_no_source_writes_cylinders_on_a_bev(source):
    row = {"id": 1, "fuel_type": "Electric", "cylinders": None, "engine_description": None}
    for n in (4, 6, 8):
        res = apply_fact_proposals(None, row, [("cylinders", n, source)], vpic=_BEV, dry_run=True)
        assert res.applied.get("cylinders") in (None, 0), (source, n, res.applied)


def test_bev_dealer_label_on_a_gasoline_decode_is_vetoed():
    res = apply_fact_proposals(None, {"id": 1, "fuel_type": "Gasoline"},
                               [("fuel_type", "Electric", prec.DEALER_FEED)],
                               vpic={"FuelTypePrimary": "Gasoline"}, dry_run=True)
    assert "fuel_type" not in res.applied


def test_vpic_outranks_dealer_feed_for_electrification_and_drivetrain():
    r = apply_fact_proposals(None, {"id": 1, "fuel_type": "Gasoline"},
                             [("fuel_type", "Electric", prec.NHTSA_VPIC)], vpic=_BEV, dry_run=True)
    assert r.applied.get("fuel_type") == "Electric"
    assert prec.rank_of("drivetrain", prec.NHTSA_VPIC) > prec.rank_of("drivetrain", prec.DEALER_FEED)
    assert prec.rank_of("fuel_type", prec.NHTSA_VPIC) > prec.rank_of("fuel_type", prec.DEALER_FEED)
    assert prec.rank_of("drivetrain", prec.EPA_CATALOG) < prec.rank_of("drivetrain", prec.DEALER_FEED)


def test_catalog_never_outranks_dealer_engine_text_or_decode():
    for f in ("engine_description", "engine_l", "cylinders"):
        assert prec.rank_of(f, prec.EPA_CATALOG) < prec.rank_of(f, prec.DEALER_FEED)
    assert prec.rank_of("cylinders", prec.EPA_CATALOG) < prec.rank_of("cylinders", prec.NHTSA_VPIC)


def test_model_specs_never_overwrites_vpic_4x2():
    row = {"id": 1, "drivetrain": "RWD"}
    res = apply_fact_proposals(None, row, [("drivetrain", "4WD", prec.MODEL_SPECS)],
                               vpic={"drivetrain": "4x2", "drive_type_raw": "4x2"}, dry_run=True)
    assert "drivetrain" not in res.applied


def test_spec_backfill_proposes_no_engine_layout_for_a_bev():
    from backend.enrichment.spec_backfill import _conservative_updates

    bev = {"id": 1, "fuel_type": "Electric", "cylinders": None, "engine_description": None}
    updates, _prov = _conservative_updates(bev, {"cylinders": 4, "engine_description": "2.0L I4"}, {})
    assert updates.get("cylinders") in (None, 0)
    assert "engine_description" not in updates and "engine_l" not in updates


def test_structured_backfill_leaves_bev_cylinders_alone():
    from backend.enrichment.spec_structured_backfill import slot_fillable_for_vpic

    bev = {"id": 1, "fuel_type": "Electric", "cylinders": None}
    assert not slot_fillable_for_vpic(bev, "cylinders", allow_overwrite_dealer=False)
