"""override_vehicles must survive a cylinder override (KeyError 'cylinders' since 2026-09-25)."""
from __future__ import annotations

from backend.enrichment import vpic_facts


def test_override_vehicles_counts_cylinders(monkeypatch):
    monkeypatch.setattr(vpic_facts, "vin_facts_enabled", lambda: True)
    import backend.enrichment.knowledge_engine as ke

    decoded = {"drivetrain": "AWD", "fuel_type": "Gasoline", "cylinders": "4", "electrification": ""}
    monkeypatch.setattr(ke, "prime_vpic_cache", lambda vins: None)
    monkeypatch.setattr(ke, "lookup_vpic_from_cache", lambda vin: decoded)

    vehicles = [{"vin": "1GKS2JKL5RR123456", "drivetrain": "AWD", "fuel_type": "Gasoline", "cylinders": 6}]
    stats = vpic_facts.override_vehicles(vehicles)

    assert stats["cached"] == 1
    assert stats["cylinders"] == 1
    assert vehicles[0]["cylinders"] == 4
    assert vehicles[0]["_cylinders_source"] == "nhtsa_vpic"


def test_override_vehicles_unknown_field_does_not_raise(monkeypatch):
    monkeypatch.setattr(vpic_facts, "vin_facts_enabled", lambda: True)
    import backend.enrichment.knowledge_engine as ke

    monkeypatch.setattr(ke, "prime_vpic_cache", lambda vins: None)
    monkeypatch.setattr(ke, "lookup_vpic_from_cache", lambda vin: {"drivetrain": "RWD"})
    monkeypatch.setattr(vpic_facts, "vin_overrides", lambda row, vp: {"body_style": (None, "Coupe")})

    vehicles = [{"vin": "1GKS2JKL5RR123456"}]
    stats = vpic_facts.override_vehicles(vehicles)
    assert stats.get("body_style") == 1
