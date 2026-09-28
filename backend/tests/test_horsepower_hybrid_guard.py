"""
DC-1 (visual review 2026-09-28): "Horsepower 145 hp" on a 2027 CR-V Hybrid
Sport-L was the gasoline engine alone (vPIC ``EngineHP``), rendered with no
qualifier and no source. Every hp value now carries ``horsepower_source``,
and a vPIC fill on an electrified VIN carries ``horsepower_note``.
"""

import json
from pathlib import Path

from backend.utils.car_serialize import serialize_car_for_api
from backend.utils.vpic_specs import specs_from_decode, vpic_is_hybrid

_CAR_HTML = Path(__file__).resolve().parents[2] / "frontend" / "templates" / "car.html"

_CRV = {"id": 1470314, "vin": "7FARS6H97TE000000", "make": "Honda", "model": "CR-V Hybrid",
        "year": 2027, "trim": "Sport-L", "price": 41297, "fuel_type": "Gasoline"}


def _flat(**fields) -> str:
    return json.dumps({"Count": 1, "Results": [dict(fields)]})


def test_decode_carries_electrification_fields():
    got = specs_from_decode(_flat(
        EngineHP="145", ElectrificationLevel="Strong Hybrid Electric Vehicle (HEV)",
        FuelTypeSecondary="Electric",
    ))
    assert got["horsepower"] == 145
    assert got["electrification_level"] == "Strong Hybrid Electric Vehicle (HEV)"
    assert got["fuel_type_secondary"] == "Electric"
    assert vpic_is_hybrid(got)
    assert vpic_is_hybrid({"electrification_level": "Plug-in Hybrid Electric Vehicle (PHEV)"})
    assert vpic_is_hybrid({"fuel_type_secondary": "Electric"})
    assert not vpic_is_hybrid({"electrification_level": "Not Applicable"})
    assert not vpic_is_hybrid({"horsepower": 300})
    # A BEV's motor hp is the whole output; not a "hybrid" note.
    assert not vpic_is_hybrid({"electrification_level": "BEV (Battery Electric Vehicle)"})


def test_vpic_fill_on_hybrid_is_noted_and_sourced(monkeypatch):
    monkeypatch.setattr(
        "backend.utils.vpic_specs.vpic_specs_for_vin",
        lambda vin: {"horsepower": 145, "electrification_level": "Strong Hybrid Electric Vehicle (HEV)"},
    )
    out = serialize_car_for_api(dict(_CRV), verified_specs={})
    assert out["horsepower"] == 145
    assert out["horsepower_source"] == "NHTSA vPIC"
    assert out["horsepower_note"] == "engine only; hybrid system output not filed"


def test_vpic_fill_on_conventional_car_has_source_and_no_note(monkeypatch):
    monkeypatch.setattr("backend.utils.vpic_specs.vpic_specs_for_vin", lambda vin: {"horsepower": 300})
    out = serialize_car_for_api(
        {"id": 1, "vin": "WBA5B3C5XED539263", "make": "BMW", "model": "5 Series", "year": 2014, "fuel_type": "Gasoline"},
        verified_specs={},
    )
    assert out["horsepower"] == 300
    assert out["horsepower_source"] == "NHTSA vPIC"
    assert out["horsepower_note"] is None


def test_hybrid_known_only_from_feed_text_still_gets_the_note(monkeypatch):
    # Decode lacks ElectrificationLevel; the listing's own fuel type says hybrid.
    monkeypatch.setattr("backend.utils.vpic_specs.vpic_specs_for_vin", lambda vin: {"horsepower": 145})
    out = serialize_car_for_api(dict(_CRV, fuel_type="Hybrid"), verified_specs={})
    assert out["horsepower"] == 145
    assert out["horsepower_note"] is not None


def test_gated_trim_page_value_wins_and_is_tagged(monkeypatch):
    monkeypatch.setattr(
        "backend.utils.vpic_specs.vpic_specs_for_vin",
        lambda vin: {"horsepower": 145, "electrification_level": "Strong Hybrid Electric Vehicle (HEV)"},
    )
    out = serialize_car_for_api(dict(_CRV), verified_specs={"horsepower": 204})
    assert out["horsepower"] == 204
    assert out["horsepower_source"] == "trim page"
    assert out["horsepower_note"] is None


def test_no_horsepower_no_source(monkeypatch):
    monkeypatch.setattr("backend.utils.vpic_specs.vpic_specs_for_vin", lambda vin: {})
    out = serialize_car_for_api(dict(_CRV), verified_specs={})
    assert out["horsepower"] is None
    assert out["horsepower_source"] is None


def test_template_renders_note_and_source():
    src = _CAR_HTML.read_text(encoding="utf-8")
    assert "car.horsepower_note" in src
    assert "per {{ car.horsepower_source }}" in src
