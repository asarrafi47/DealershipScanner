"""P10C.4: enrich_car takes forced induction from the resolver, never the CSV."""
from __future__ import annotations

import json

from backend.dictionary import enrich_from_dictionary as efd

ROWS = [{"Trim": "Base", "transmissionOptions": "Automatic", "cylinders": "4", "displacement": "2.0",
         "engineOptions": "2.0L I4", "forcedInduction": "Turbocharged"}]


def _car(**kw):
    car = {"id": 5, "vin": "1HGCV1F30MA000001", "year": 2021, "make": "Honda", "model": "Accord",
           "trim": "EX", "title": "2021 Honda Accord EX", "fuel_type": "Gasoline",
           "engine_l": 2.0, "cylinders": 4, "transmission": "CVT", "forced_induction": None}
    car.update(kw)
    return car


def _run(monkeypatch, car, **kw):
    monkeypatch.setattr(efd, "_load_epa_csv", lambda *a: ROWS)
    return efd.enrich_car(car, use_vpic=False, **kw)


def test_dealer_turbo_text_replaces_stored_na_under_fill_all(monkeypatch):
    car = _car(engine_description="2.0L Turbo I4", forced_induction="Naturally Aspirated")
    up = _run(monkeypatch, car, fill_all=True)
    assert up["forced_induction"] == "Turbocharged"
    assert up.provenance["forced_induction"]["source"] == "dealer_feed"


def test_no_text_keeps_na_row_na(monkeypatch):
    car = _car(engine_description="2.0L I4", forced_induction="Naturally Aspirated")
    up = _run(monkeypatch, car, fill_all=True)
    assert "forced_induction" not in up


def test_blank_filled_from_vpic_not_csv(monkeypatch):
    up = _run(monkeypatch, _car(engine_description="2.0L I4"), vpic_ctx={"vpic": {"Turbo": "No"}})
    assert up["forced_induction"] == "Naturally Aspirated"
    assert up.provenance["forced_induction"]["source"] == "nhtsa_vpic"


def test_csv_label_alone_never_fills(monkeypatch):
    up = _run(monkeypatch, _car(engine_description="2.0L I4", model="Accord", make="Mazda", year=2012))
    assert "forced_induction" not in up


def test_existing_value_untouched_without_fill_all(monkeypatch):
    car = _car(engine_description="2.0L Turbo I4", forced_induction="Naturally Aspirated")
    assert "forced_induction" not in _run(monkeypatch, car)


def test_fill_all_keeps_strong_text_backed_value(monkeypatch):
    prov = json.dumps({"forced_induction": {"source": "dealer_feed", "value": "Turbocharged",
                                            "fetched_at": "2026-10-01T00:00:00+00:00"}})
    car = _car(engine_description="2.0L Turbo I4", forced_induction="Turbocharged", spec_source_json=prov)
    assert "forced_induction" not in _run(monkeypatch, car, fill_all=True)
