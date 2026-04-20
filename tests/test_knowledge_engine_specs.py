"""Verified spec merge: placeholders must not block EPA/regex inference."""

from __future__ import annotations

import pytest

from backend.knowledge_engine import _is_na_spec, merge_verified_specs


@pytest.mark.parametrize(
    "raw",
    ["--", "—", "-", "N/A", "na", "  ", "unknown", "tbd"],
)
def test_is_na_spec_placeholders(raw: str) -> None:
    assert _is_na_spec(raw) is True


def test_is_na_spec_real_values() -> None:
    assert _is_na_spec("AWD") is False
    assert _is_na_spec("8-Speed Automatic") is False
    assert _is_na_spec(4) is False


def test_merge_verified_specs_does_not_use_double_dash_as_drivetrain() -> None:
    car = {
        "make": "Toyota",
        "model": "Camry",
        "year": 2020,
        "trim": "LE",
        "title": "Used 2020 Toyota Camry LE",
        "drivetrain": "--",
        "transmission": "--",
        "cylinders": None,
        "fuel_type": None,
        "body_style": None,
    }
    vs = merge_verified_specs(car)
    assert vs.get("drivetrain") != "--"
    assert vs.get("transmission_display") != "--"
    td = vs.get("transmission_display")
    if isinstance(td, str):
        assert td.strip().lower() not in ("--", "-", "n/a", "na")
