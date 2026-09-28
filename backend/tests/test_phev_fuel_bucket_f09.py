"""F09 (docs/DATA_COMPLETENESS_FINDINGS_2026_09_28.md): dealer "Hybrid" must be
upgraded to "Plug-In Hybrid" when vPIC says PHEV; the buckets used to fold both
into 'hybrid' so vin_overrides never fired. vPIC wins on electrification."""
from __future__ import annotations

from backend.catalog.resolver import _fuel_bucket
from backend.enrichment import vpic_facts as vf


def test_phev_is_its_own_bucket():
    assert _fuel_bucket("Plug-In Hybrid") == "phev"
    assert _fuel_bucket("PHEV") == "phev"
    assert _fuel_bucket("Plug-in Hybrid Electric Vehicle") == "phev"
    assert _fuel_bucket("Hybrid") == "hybrid"
    assert _fuel_bucket("Gas/Electric Hybrid") == "hybrid"
    assert _fuel_bucket("Electric") == "ev"
    assert _fuel_bucket("Gasoline") == "gas" and _fuel_bucket("Diesel") == "diesel"


def test_hybrid_row_upgraded_to_phev_when_decode_says_phev():
    # BMW 550e xDrive stored "Hybrid"; vPIC ElectrificationLevel = PHEV
    assert vf.vin_overrides({"fuel_type": "Hybrid"}, {"electrification": "phev"}) == {"fuel_type": ("Hybrid", "Plug-In Hybrid")}
    assert vf.vin_overrides({"fuel_type": "Plug-In Hybrid"}, {"electrification": "phev"}) == {}
    # decode says plain HEV, dealer says plug-in: the VIN wins the other way too
    assert vf.vin_overrides({"fuel_type": "Plug-In Hybrid"}, {"electrification": "hybrid"}) == {"fuel_type": ("Plug-In Hybrid", "Hybrid")}
    # silent decode changes nothing
    assert vf.vin_overrides({"fuel_type": "Hybrid"}, {"electrification": None}) == {}
