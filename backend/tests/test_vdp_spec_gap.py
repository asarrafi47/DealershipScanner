"""VDP spec-gap queue for missing engine/transmission fields."""

from __future__ import annotations

from backend.scanner.vdp import _vehicle_needs_spec_gap_vdp


def test_vehicle_needs_spec_gap_when_engine_missing():
    assert _vehicle_needs_spec_gap_vdp({"engine_description": "", "transmission": "Auto"})
    assert not _vehicle_needs_spec_gap_vdp(
        {
            "engine_description": "5.7L V8",
            "transmission": "Auto",
            "drivetrain": "AWD",
            "fuel_type": "Gas",
            "body_style": "Truck",
        }
    )
