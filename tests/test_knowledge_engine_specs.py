"""Verified spec merge: placeholders must not block EPA/regex inference."""

from __future__ import annotations

import json

import pytest

from backend.knowledge_engine import (
    _is_na_spec,
    decode_trim_logic,
    lookup_epa_aggregate,
    merge_verified_specs,
    prepare_car_detail_context,
)


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


def test_lookup_epa_ford_f150_hyphen_model_matches_pickup_rows() -> None:
    """Dealer ``F-150`` must resolve to EPA ``F150 Pickup *`` rows (transmission + trany formatting)."""
    epa = lookup_epa_aggregate(2018, "Ford", "F-150")
    if not epa.get("transmission"):
        pytest.skip("epa_master has no Ford F-150 aggregate for this environment")
    vs = merge_verified_specs(
        {
            "make": "Ford",
            "model": "F-150",
            "year": 2018,
            "trim": "XLT",
            "title": "Used 2018 Ford F-150 XLT",
            "transmission": None,
            "drivetrain": "4WD",
            "cylinders": 6,
            "fuel_type": "Gasoline",
            "body_style": None,
        }
    )
    assert vs.get("transmission_display")
    assert "Speed" in (vs.get("transmission_display") or "")


def test_decode_trim_ford_ten_speed_from_title() -> None:
    hints = decode_trim_logic("Ford", "F-150", "Lariat", "2020 Ford F-150 Lariat Electronic Ten-Speed Automatic")
    assert hints.get("transmission_hint") == "10-Speed Automatic"
    assert hints.get("gears") == 10


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


def test_prepare_car_detail_packages_uses_name_fallbacks_and_extras() -> None:
    """Car detail Packages panel: canonical_name / vision lists / standalone features."""
    car = {
        "gallery": [],
        "packages": json.dumps(
            {
                "packages_normalized": [
                    {
                        "name": "",
                        "canonical_name": "Cold Weather Package",
                        "name_verbatim": "Winter Pkg",
                        "features": ["Heated seats"],
                    }
                ],
                "standalone_features_from_description": ["Panoramic roof"],
                "possible_packages": ["Tech Package"],
                "observed_features": ["Roof rails"],
            }
        ),
    }
    ctx = prepare_car_detail_context(car)
    sections = ctx.get("listing_packages_sections") or []
    titles = [s["name"] for s in sections]
    assert "Cold Weather Package" in titles
    assert "Tech Package" in titles
    assert ctx.get("listing_standalone_features") == ["Panoramic roof"]
    assert ctx.get("listing_observed_features") == ["Roof rails"]
    assert ctx.get("packages_panel_has_content") is True


def test_prepare_car_detail_llava_interior_section() -> None:
    car = {
        "gallery": [],
        "packages": json.dumps(
            {
                "llava_interior_cabin": {
                    "interior_guess_text": "Black cabin",
                    "interior_buckets": ["black"],
                    "evidence": "dash photo",
                    "confidence": 0.77,
                }
            }
        ),
        "spec_source_json": json.dumps({"interior_cabin_vision": {"source": "llava_vision"}}),
    }
    ctx = prepare_car_detail_context(car)
    sec = ctx.get("llava_interior_section")
    assert isinstance(sec, dict)
    assert sec.get("guess") == "Black cabin"
    assert sec.get("buckets") == ["black"]
    assert ctx.get("interior_from_llava_vision") is True
    assert ctx.get("packages_panel_has_content") is True


def test_prepare_car_detail_packages_empty_state() -> None:
    car = {"gallery": [], "packages": None, "spec_source_json": None}
    ctx = prepare_car_detail_context(car)
    assert ctx.get("packages_panel_has_content") is False
