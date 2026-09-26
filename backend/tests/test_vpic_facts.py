"""NHTSA vPIC drivetrain / electrification outrank the dealer feed (2026-09-23)."""
from __future__ import annotations

from backend.enrichment import vpic_facts as vf


def test_drivetrain_override_only_on_real_disagreement():
    assert vf.vin_overrides({"drivetrain": "RWD"}, {"drivetrain": "4WD"}) == {"drivetrain": ("RWD", "4WD")}
    assert vf.vin_overrides({"drivetrain": "FWD"}, {"drivetrain": "AWD"}) == {"drivetrain": ("FWD", "AWD")}
    assert vf.vin_overrides({"drivetrain": "AWD"}, {"drivetrain": "4WD"}) == {}  # same wheels driven
    assert vf.vin_overrides({"drivetrain": ""}, {"drivetrain": "4WD"}) == {"drivetrain": ("", "4WD")}
    assert vf.vin_overrides({"drivetrain": "FWD"}, {"drivetrain": None}) == {}


def test_fuel_override_follows_stated_electrification_only():
    assert vf.vin_overrides({"fuel_type": "Gasoline"}, {"electrification": "hybrid"}) == {"fuel_type": ("Gasoline", "Hybrid")}
    assert vf.vin_overrides({"fuel_type": "Gasoline"}, {"electrification": "phev"}) == {"fuel_type": ("Gasoline", "Plug-In Hybrid")}
    assert vf.vin_overrides({"fuel_type": "Gasoline"}, {"electrification": "ev"}) == {"fuel_type": ("Gasoline", "Electric")}
    assert vf.vin_overrides({"fuel_type": "Hybrid"}, {"electrification": "hybrid"}) == {}
    # 48V mild hybrid / blank decode: the dealer's "Hybrid" stands
    assert vf.vin_overrides({"fuel_type": "Hybrid"}, {"electrification": None, "fuel_type": "Gasoline"}) == {}
    # diesel stated, feed says gas
    assert vf.vin_overrides({"fuel_type": "Gasoline"}, {"electrification": None, "fuel_type": "Diesel"}) == {"fuel_type": ("Gasoline", "Diesel")}


def test_apply_marks_provenance_and_mutates():
    row = {"drivetrain": "RWD", "fuel_type": "Gasoline"}
    ch = vf.apply_vin_facts_to_row(row, {"drivetrain": "4WD", "electrification": "hybrid"})
    assert set(ch) == {"drivetrain", "fuel_type"}
    assert row["drivetrain"] == "4WD" and row["fuel_type"] == "Hybrid"
    assert row["_drivetrain_source"] == "nhtsa_vpic" and row["_fuel_type_source"] == "nhtsa_vpic"


def test_override_vehicles_uses_cache(monkeypatch):
    seen = {}
    monkeypatch.setattr("backend.enrichment.knowledge_engine.prime_vpic_cache", lambda vins: seen.setdefault("primed", list(vins)))
    monkeypatch.setattr(
        "backend.enrichment.knowledge_engine.lookup_vpic_from_cache",
        lambda vin: {"drivetrain": "4WD", "electrification": None, "fuel_type": "Gasoline"} if vin.startswith("3TY") else {},
    )
    cars = [{"vin": "3TYLB5JNXRT022195", "drivetrain": "RWD", "fuel_type": "Gasoline"},
            {"vin": "1HGCY2F63TA066331", "drivetrain": "FWD", "fuel_type": "Gasoline"},
            {"vin": "bad", "drivetrain": "FWD"}]
    stats = vf.override_vehicles(cars)
    assert stats == {"vehicles": 3, "cached": 1, "drivetrain": 1, "fuel_type": 0}
    assert cars[0]["drivetrain"] == "4WD" and cars[1]["drivetrain"] == "FWD"
    assert seen["primed"] == ["3TYLB5JNXRT022195", "1HGCY2F63TA066331"]


def test_cylinders_override_follows_the_decode():
    # 2027 Buick Enclave 2.5T stored 6 from a model-level backfill; vPIC says 4 (2026-09-25)
    assert vf.vin_overrides({"cylinders": 6}, {"cylinders": "4"}) == {"cylinders": (6, 4)}
    assert vf.vin_overrides({"cylinders": None}, {"cylinders": 8}) == {"cylinders": (None, 8)}
    assert vf.vin_overrides({"cylinders": 4}, {"cylinders": "4"}) == {}
    assert vf.vin_overrides({"cylinders": 4}, {"cylinders": None}) == {}
    assert vf.vin_overrides({"cylinders": 4, "fuel_type": "Electric"}, {"cylinders": "4", "electrification": "ev"}) == {}
    assert vf.vin_overrides({"cylinders": 4}, {"cylinders": "0"}) == {}

