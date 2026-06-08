"""VDP description-gap queue."""

from __future__ import annotations

from backend.scanner.vdp import _vehicle_needs_description_vdp


def test_vehicle_needs_description_when_missing_or_short():
    assert _vehicle_needs_description_vdp({})
    assert _vehicle_needs_description_vdp({"description": "Short"})
    assert not _vehicle_needs_description_vdp(
        {"description": "Irvine BMW proudly presents this exclusive service loaner with many features."}
    )
