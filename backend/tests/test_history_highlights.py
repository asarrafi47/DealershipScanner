"""Tests for dealer-text history highlight extraction."""
from __future__ import annotations

from backend.utils.history_highlights import (
    coalesce_history_highlights_for_storage,
    extract_history_highlights_from_badges,
    extract_history_highlights_from_dealer_text,
    history_highlights_json,
    merge_history_highlights,
)


def test_extract_from_carfax_one_owner_description() -> None:
    desc = (
        "CARFAX One-Owner. Clean CARFAX. Black Raven 2019 Cadillac CTS 2.0L Turbo "
        "4D Sedan AWD 2.0L Turbo I4 DI DOHC VVT 8-Speed Automatic AWD."
    )
    got = extract_history_highlights_from_dealer_text(desc)
    assert "CARFAX One-Owner" in got
    assert "Clean CARFAX" in got


def test_extract_from_dom_badges() -> None:
    got = extract_history_highlights_from_badges(["1-Owner", "Certified Pre-Owned", "No Accidents Reported"])
    assert "1 Owner" in got or "One Owner" in got
    assert "No Accidents Reported" in got
    assert "Certified Pre-Owned" not in got


def test_merge_dedupes() -> None:
    merged = merge_history_highlights(
        ["Clean CARFAX"],
        extract_history_highlights_from_dealer_text("Clean CARFAX. CARFAX One-Owner."),
    )
    assert merged == ["Clean CARFAX", "CARFAX One-Owner"]


def test_coalesce_for_storage_from_description() -> None:
    vehicle = {"history_highlights": [], "description": "Clean CARFAX one owner dealer serviced."}
    got = coalesce_history_highlights_for_storage(vehicle)
    assert got is not None
    assert "Clean CARFAX" in got
    assert "CARFAX One-Owner" in got or "One Owner" in got


def test_history_highlights_json_null_when_empty() -> None:
    assert history_highlights_json(None) is None
    assert history_highlights_json([]) is None
    assert history_highlights_json(["Clean CARFAX"]) == '["Clean CARFAX"]'
