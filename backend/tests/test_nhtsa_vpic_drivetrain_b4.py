"""B4 (monolith audit 2026-10-01): vPIC "4x2" must never reach cars.drivetrain."""

from __future__ import annotations

import pytest

from backend.enrichment.nhtsa_vpic import _normalize_drivetrain, flat_vpic_result_to_car_patch


@pytest.mark.parametrize(
    "raw, expected",
    [
        # Every DriveType value seen in the local nhtsa_vpic_cache (2026-10-01).
        ("4WD/4-Wheel Drive/4x4", "4WD"),
        ("AWD/All-Wheel Drive", "AWD"),
        ("FWD/Front-Wheel Drive", "FWD"),
        ("RWD/Rear-Wheel Drive", "RWD"),
        ("4x2", None),
        ("2WD/4WD", None),
        ("Not Applicable", None),
        ("", None),
        # Other spellings / ambiguous two-wheel forms.
        ("4x2/2-Wheel Drive", None),
        ("2WD", None),
        ("Front-Wheel Drive (FWD)", "FWD"),
        ("Rear Wheel Drive", "RWD"),
        ("Four-Wheel Drive", "4WD"),
        ("6x4", None),
    ],
)
def test_normalize_drivetrain_canonical_or_none(raw: str, expected: str | None) -> None:
    assert _normalize_drivetrain(raw) == expected


@pytest.mark.parametrize("raw", ["4x2", "4x2/2-Wheel Drive", "2WD/4WD", "6x4"])
def test_ambiguous_drive_type_left_out_of_car_patch(raw: str) -> None:
    patch = flat_vpic_result_to_car_patch({"Make": "TOYOTA", "Model": "Tacoma", "DriveType": raw})
    assert "drivetrain" not in patch
