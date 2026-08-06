"""Per-VIN vPIC specs: the fill-behind for model-level-contaminated catalog data.

``epa_extended_specs`` stamps one scraped value across a nameplate's every trim
and model year (BMW 3 Series: a single 0-60 of 3.7s across 579 rows spanning
1984-2026). ``knowledge_engine._extended_family_suspicious`` correctly refuses
horsepower from such a family, and nothing filled in behind it, so the field
rendered blank. vPIC is keyed on the individual VIN, so it cannot carry
model-level contamination by construction.
"""
from __future__ import annotations

import json

from backend.utils.car_serialize import serialize_car_for_api
from backend.utils.vpic_specs import specs_from_decode, vpic_specs_for_vin


def _flat(**fields) -> str:
    """A stored decode in vPIC's ``DecodeVinValues`` (flat) shape."""
    return json.dumps({"Count": 1, "Message": "Results returned successfully",
                       "Results": [dict(fields)]})


# ── shape ───────────────────────────────────────────────────────────────────

def test_reads_the_flat_decodevinvalues_shape():
    """The cache stores ONE flat object per VIN, not Variable/Value pairs."""
    got = specs_from_decode(_flat(
        VIN="WBA5B3C5XED539263", EngineHP="300", EngineCylinders="6",
        DisplacementL="3.0", DriveType="AWD/All-Wheel Drive", BodyClass="Sedan/Saloon",
        FuelTypePrimary="Gasoline", Trim="xDrive",
    ))
    assert got["horsepower"] == 300
    assert got["cylinders"] == 6
    assert got["displacement_l"] == 3.0
    assert got["drive_type"] == "AWD/All-Wheel Drive"


def test_variable_value_shape_yields_nothing_rather_than_wrong_values():
    """vPIC's OTHER response shape (``DecodeVin``) is a list of Variable/Value
    pairs. Reading it as the flat shape must return nothing, not a partial or
    mistyped result — misreading it this way reported 0% coverage on a fleet
    that actually had 76%, which is a silent-wrong-answer failure mode."""
    pairs = json.dumps({"Results": [
        {"Variable": "Engine Brake (hp) From", "Value": "300"},
        {"Variable": "Engine Number of Cylinders", "Value": "6"},
    ]})
    assert specs_from_decode(pairs) == {}


def test_malformed_or_empty_payloads_are_survivable():
    for payload in (None, "", "{", "[]", json.dumps({"Results": []}),
                    json.dumps({"Results": [None]})):
        assert specs_from_decode(payload) == {}


# ── bounds: a decode may not smuggle in a value the catalog gate would reject ─

def test_out_of_range_horsepower_is_dropped():
    assert "horsepower" not in specs_from_decode(_flat(EngineHP="4"))
    assert "horsepower" not in specs_from_decode(_flat(EngineHP="9999"))
    assert specs_from_decode(_flat(EngineHP="60"))["horsepower"] == 60
    assert specs_from_decode(_flat(EngineHP="1600"))["horsepower"] == 1600


def test_placeholder_values_are_not_treated_as_data():
    got = specs_from_decode(_flat(
        EngineHP="0", BodyClass="Not Applicable", DriveType="", FuelTypePrimary="Gasoline",
    ))
    assert "horsepower" not in got
    assert "body_class" not in got
    assert "drive_type" not in got
    assert got["fuel_type_primary"] == "Gasoline"


def test_bad_vin_lengths_never_hit_the_database():
    for vin in (None, "", "TOOSHORT", "X" * 18):
        assert vpic_specs_for_vin(vin) == {}


# ── the fill-behind contract ────────────────────────────────────────────────

def test_vpic_fills_horsepower_only_when_the_gated_value_is_absent(monkeypatch):
    """A real per-trim horsepower that survived the catalog gate must WIN over
    the decode; vPIC is a fallback, not an override."""
    monkeypatch.setattr(
        "backend.utils.vpic_specs.vpic_specs_for_vin",
        lambda vin: {"horsepower": 300},
    )

    car = {"id": 1, "vin": "WBA5B3C5XED539263", "make": "BMW", "model": "5 Series",
           "year": 2014, "price": 9579}

    blank = serialize_car_for_api(dict(car), verified_specs={})
    assert blank["horsepower"] == 300, "should fall back to the per-VIN decode"

    gated = serialize_car_for_api(dict(car), verified_specs={"horsepower": 315})
    assert gated["horsepower"] == 315, "a surviving per-trim value must win"


def test_zero_to_60_and_curb_weight_are_not_filled_from_vpic(monkeypatch):
    """vPIC carries neither. They stay suppressed rather than being estimated
    from power-to-weight, because the curb weight such an estimate needs is
    itself the contaminated field (2,042 rows lighter than the lightest US car).
    """
    monkeypatch.setattr(
        "backend.utils.vpic_specs.vpic_specs_for_vin",
        lambda vin: {"horsepower": 300, "curb_weight_lb": 4000, "zero_to_60_sec": 5.5},
    )
    out = serialize_car_for_api(
        {"id": 1, "vin": "WBA5B3C5XED539263", "make": "BMW", "model": "5 Series", "year": 2014},
        verified_specs={},
    )
    assert out["curb_weight_lb"] is None
    assert out["zero_to_60_sec"] is None
