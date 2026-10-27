"""InventoryEnricher catalog fills go through fact precedence; Haiku is opt-in and never a fact (P8B.8)."""
from __future__ import annotations

import json
import threading

import pytest

from backend.enrichment import service as svc
from backend.enrichment.service import InventoryEnricher


class _Catalog:
    def __init__(self, result):
        self.result = result

    def lookup_car(self, car, n_results=5):
        return self.result


def _enricher(result) -> InventoryEnricher:
    e = object.__new__(InventoryEnricher)
    e.catalog = _Catalog(result)
    e._catalog_lock = threading.Lock()
    e._db_write_lock = threading.Lock()
    e._write_buffer = []
    return e


def _row(**kw):
    r = {"id": 5, "vin": "1FTEW1EP5KFA00001", "year": 2019, "make": "Ford", "model": "Explorer", "trim": "XLT",
         "cylinders": None, "engine_l": 0, "transmission": None, "drivetrain": None, "fuel_type": None,
         "mpg_city": None, "mpg_highway": None, "spec_source_json": None}
    r.update(kw)
    return r


BEST = {"ok": True, "best": {"cylinders": 6, "engine_l": 3.5, "transmission": "10-Speed Automatic",
                              "drivetrain": "AWD", "mpg_city": 18, "mpg_highway": 25, "fuel_type": "Gasoline"}}


@pytest.fixture(autouse=True)
def _no_vpic(monkeypatch):
    ctxs = {}
    monkeypatch.setattr("backend.dictionary.enrich_from_dictionary.vpic_contexts_for_vins",
                        lambda vins: {v: ctxs[v] for v in vins if v in ctxs})
    return ctxs


def test_catalog_proposals_carry_epa_catalog_provenance():
    out, heal = _enricher(BEST).apply_catalog_best(_row())
    assert out["drivetrain"] == "AWD" and out["cylinders"] == 6 and out["mpg_city"] == 18
    prov = out["spec_provenance"]
    for f in ("drivetrain", "cylinders", "transmission"):
        assert prov[f]["source"] == "epa_catalog"
    assert prov["mpg_city"]["source"] == "epa_catalog"
    assert "drivetrain" in heal


def test_catalog_awd_not_filled_where_vpic_says_4x2(_no_vpic):
    _no_vpic["1FTEW1EP5KFA00001"] = {"vpic": {"DriveType": "4x2"}, "vpic_drive": "4x2"}
    out, heal = _enricher(BEST).apply_catalog_best(_row())
    assert "drivetrain" not in out and "drivetrain" not in heal
    assert out["cylinders"] == 6


def test_catalog_cylinders_refused_against_the_engine_text():
    out, _ = _enricher(BEST).apply_catalog_best(_row(engine_description="2.0L I4 Turbo"))
    assert "cylinders" not in out


def test_haiku_is_off_by_default(monkeypatch):
    monkeypatch.delenv("ENRICHMENT_HAIKU_FALLBACK", raising=False)
    called = []
    monkeypatch.setattr(svc, "_ask_haiku_specs", lambda *a: called.append(a) or {"cylinders": 4})
    out, heal = _enricher({"ok": False}).apply_catalog_best(_row())
    assert out == {} and heal == [] and called == []


def test_haiku_only_proposals_never_reach_fact_columns(monkeypatch):
    monkeypatch.setenv("ENRICHMENT_HAIKU_FALLBACK", "1")
    monkeypatch.setattr(svc, "_ask_haiku_specs", lambda *a: dict(BEST["best"]))
    out, _ = _enricher({"ok": False}).apply_catalog_best(_row())
    fact = {"cylinders", "engine_l", "transmission", "drivetrain", "fuel_type"}
    assert not fact & set(out)
    assert out["mpg_city"] == 18 and out["spec_provenance"]["mpg_city"]["source"] == "llm"


def test_batch_write_merges_provenance_into_the_stored_object(monkeypatch):
    executed: list = []

    class Conn:
        def execute(self, sql, params=()):
            executed.append((sql, list(params)))
            class R:
                def fetchone(s):
                    return (json.dumps({"color": {"source": "dealer_feed"}}),)
            return R()
        def commit(self): pass
        def close(self): pass

    monkeypatch.setattr(svc, "get_conn", lambda: Conn())
    monkeypatch.setattr(svc, "get_car_by_id", lambda cid: {"spec_source_json": json.dumps({"color": {"source": "dealer_feed"}})})
    e = _enricher({"ok": False})
    e._do_batch_write([(5, {"cylinders": 6, "spec_provenance": {"cylinders": {"source": "epa_catalog", "value": "6"}}})])
    sql, params = [x for x in executed if x[0].startswith("UPDATE")][0]
    assert "spec_source_json = ?" in sql and "spec_provenance" not in sql
    merged = json.loads(params[1])
    assert merged["color"]["source"] == "dealer_feed" and merged["cylinders"]["source"] == "epa_catalog"


def test_save_enriched_data_returns_only_columns():
    e = _enricher({"ok": False})
    cols = e.save_enriched_data(5, {"cylinders": 6, "spec_provenance": {"cylinders": {"source": "epa_catalog"}}})
    assert cols == ["cylinders"]
    assert e._write_buffer[0][1]["spec_provenance"]
