"""N15.5 / N15.5b: vPIC fills plain Gasoline (blank / "F" / injection junk) through
precedence, never over a stated fuel; the injection repair script is dry-run by
default. Pure tests run everywhere; the repair tests need the Postgres harness."""
from __future__ import annotations

import json

import pytest

from backend.enrichment import vpic_facts as vf
from backend.scripts import repair_injection_fuel as rep
from backend.utils.field_clean import coerce_fuel_type_stored

pg = pytest.mark.pg
GAS = {"fuel_type": "Gasoline"}


@pytest.mark.parametrize("stored", [None, "", "F", "Direct Injection", "G", "Regular Unleaded"])
def test_gasoline_decode_fills_blank_or_noncanonical(stored):
    out = vf.vin_overrides({"fuel_type": stored}, GAS)
    assert out == {"fuel_type": (stored.strip() if stored else None, "Gasoline")} or out == {"fuel_type": (None, "Gasoline")}


@pytest.mark.parametrize("stored", ["Hybrid", "Plug-In Hybrid", "Diesel", "Electric"])
def test_gasoline_decode_never_replaces_stated_fuel(stored):
    assert vf.vin_overrides({"fuel_type": stored}, GAS) == {}


def test_canonical_gasoline_unchanged_and_electric_secondary_ignored():
    assert vf.vin_overrides({"fuel_type": "Gasoline"}, GAS) == {}
    assert vf.vin_overrides({"fuel_type": None}, {"fuel_type": "Gasoline / Electric"}) == {}


def test_phev_still_beats_gasoline_label():
    out = vf.vin_overrides({"fuel_type": "Gasoline"}, {"electrification": "phev", "fuel_type": "Gasoline"})
    assert out == {"fuel_type": ("Gasoline", "Plug-In Hybrid")}


def test_single_letter_f_maps_to_gasoline():
    assert coerce_fuel_type_stored("F") == "Gasoline"


def test_scan_path_stamps_provenance_for_gasoline_fill(monkeypatch):
    monkeypatch.setattr(vf, "vin_facts_enabled", lambda: True)
    monkeypatch.setattr("backend.enrichment.knowledge_engine.prime_vpic_cache", lambda vins: None)
    monkeypatch.setattr("backend.enrichment.knowledge_engine.lookup_vpic_from_cache", lambda vin: GAS)
    cars = [{"vin": "1HGCY2F60PA000001", "fuel_type": None}]
    vf.override_vehicles(cars)
    assert cars[0]["fuel_type"] == "Gasoline"
    assert json.loads(cars[0]["spec_source_json"])["fuel_type"]["value"] == "Gasoline"


@pytest.fixture
def conn(pg_db):
    c = pg_db.get_conn()
    yield c
    c.close()


def _car(conn, i, vin, fuel):
    conn.execute("INSERT INTO cars (id, vin, dealer_id, title, listing_active, fuel_type) VALUES (?, ?, 'd1', 't', 1, ?)",
                 (i, vin, fuel))
    conn.commit()


def _fuel(conn, i):
    return conn.execute("SELECT fuel_type FROM cars WHERE id=?", (i,)).fetchone()[0]


@pg
def test_repair_dry_run_writes_nothing_then_apply(conn):
    decodes = {"1HGCY2F60PA000001": GAS, "1HGCY2F60PA000002": {"electrification": "phev"}}
    _car(conn, 1, "1HGCY2F60PA000001", "Direct Injection")
    _car(conn, 2, "1HGCY2F60PA000002", "Direct Injection")
    _car(conn, 3, "1HGCY2F60PA000003", "Port/Direct Injection")  # undecodable
    _car(conn, 4, "1HGCY2F60PA000004", "Hybrid")  # untouched
    look = lambda v: decodes.get(v, {})  # noqa: E731
    s = rep.repair_injection_fuel(conn, lookup=look)
    assert s["dry_run"] and s["rows"] == 3 and s["healed"] == 2 and s["left"] == 1
    assert [_fuel(conn, i) for i in (1, 2, 3, 4)] == ["Direct Injection", "Direct Injection", "Port/Direct Injection", "Hybrid"]
    s = rep.repair_injection_fuel(conn, apply=True, clear_undecodable=True, lookup=look)
    assert s["healed"] == 2 and s["cleared"] == 1
    assert [_fuel(conn, i) for i in (1, 2, 3, 4)] == ["Gasoline", "Plug-In Hybrid", None, "Hybrid"]
    assert rep.repair_injection_fuel(conn, lookup=look)["rows"] == 0
