"""Transmission bucket normalization (Automatic / Manual / CVT)."""

from __future__ import annotations

import pytest

from backend.utils.transmission_normalize import normalize_transmission_standard


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("A", "Automatic"),
        ("a", "Automatic"),
        ("Auto", "Automatic"),
        ("8-Speed", "Automatic"),
        ("10-Speed Automatic", "Automatic"),
        ("6-Speed Automatic", "Automatic"),
        ("1-Speed Automatic", "Automatic"),
        ("Transmission w/Dual Shift Mode", "Automatic"),
        ("Single-Speed Fixed Gear", "Automatic"),
        ("Single-Speed Fixed", "Automatic"),
        ("1-Speed", "Automatic"),
        ("direct drive", "Automatic"),
        ("M", "Manual"),
        ("Manual", "Manual"),
        ("6-Speed Manual", "Manual"),
        ("5-Speed Manual", "Manual"),
        ("CVT", "CVT"),
        ("cvt", "CVT"),
        ("Continuously Variable Transmission", "CVT"),
        ("eCVT", "CVT"),
        ("Lineartronic Variable Transmission", "CVT"),
        ("Continuous Duty Transmission", "CVT"),
    ],
)
def test_normalize_transmission_explicit_buckets(raw: str, expected: str) -> None:
    label, weak = normalize_transmission_standard(raw, log_weak=False)
    assert label == expected
    assert weak is False


def test_speed_without_manual_is_automatic() -> None:
    label, weak = normalize_transmission_standard("8-Speed", log_weak=False)
    assert label == "Automatic"
    assert weak is False


def test_manual_wins_over_speed_phrase() -> None:
    label, weak = normalize_transmission_standard("6-Speed Manual", log_weak=False)
    assert label == "Manual"


def test_automated_manual_is_automatic_bucket() -> None:
    label, weak = normalize_transmission_standard("Automated Manual", log_weak=False)
    assert label == "Automatic"


def test_placeholder_returns_none() -> None:
    assert normalize_transmission_standard("--", log_weak=False) == (None, False)
    assert normalize_transmission_standard(None, log_weak=False) == (None, False)


def test_unknown_string_defaults_automatic_and_weak() -> None:
    label, weak = normalize_transmission_standard("Totally Unknown Gearbox XYZ", log_weak=False)
    assert label == "Automatic"
    assert weak is True
