"""Tests for multi-image equipment vision helpers."""
from __future__ import annotations

import json

from backend.vision.equipment_vision import (
    collect_photo_detected_equipment,
    humanize_adas_token,
    packages_lacks_photo_equipment,
    pick_equipment_vision_urls,
)


def test_pick_equipment_vision_urls_diverse() -> None:
    urls = [
        "https://cdn.example.com/hero.jpg",
        "https://cdn.example.com/2.jpg",
        "https://cdn.example.com/3.jpg",
        "https://cdn.example.com/interior-dash.jpg",
        "https://cdn.example.com/wheel-brake.jpg",
        "https://cdn.example.com/rear-exhaust.jpg",
        "https://cdn.example.com/screen.jpg",
    ]
    picked = pick_equipment_vision_urls(urls, max_urls=5)
    assert picked[0] == urls[0]
    assert any("interior" in u for u in picked)
    assert len(picked) <= 5


def test_packages_lacks_photo_equipment() -> None:
    assert packages_lacks_photo_equipment(None) is True
    assert packages_lacks_photo_equipment("{}") is True
    assert packages_lacks_photo_equipment(
        json.dumps({"observed_features": ["Bang & Olufsen audio"]})
    ) is True
    assert packages_lacks_photo_equipment(
        json.dumps(
            {
                "observed_features": ["Bang & Olufsen audio"],
                "photo_equipment_scan_version": 4,
                "photo_equipment_scanned_at": 123,
            }
        )
    ) is False
    assert packages_lacks_photo_equipment(
        json.dumps({"photo_equipment_scanned_at": 123, "photo_equipment_scan_version": 4})
    ) is False
    assert packages_lacks_photo_equipment(
        json.dumps({"photo_equipment_scanned_at": 123, "photo_equipment_scan_version": 3})
    ) is True


def test_filter_vision_feature_list_drops_generic_trim_guesswork() -> None:
    from backend.vision.equipment_vision import filter_vision_feature_list, is_meaningful_vision_feature

    assert is_meaningful_vision_feature("Chrome grille") is False
    assert is_meaningful_vision_feature("Alloy wheels") is False
    assert is_meaningful_vision_feature("Panoramic sunroof") is True
    assert is_meaningful_vision_feature("Aftermarket side step rails") is True
    filtered = filter_vision_feature_list(
        [
            "Chrome grille",
            "LED headlights",
            "Roof rails",
            "Dark tinted rear windows",
            "Tow hitch",
            "SEL or Titanium trim level",
        ]
    )
    assert "Chrome grille" not in filtered
    assert "LED headlights" not in filtered
    assert "SEL or Titanium trim level" not in filtered
    assert "Roof rails" in filtered
    assert "Dark tinted rear windows" in filtered
    assert "Tow hitch" in filtered


def test_collect_photo_detected_equipment_includes_aftermarket_steps() -> None:
    pj = {
        "observed_features": [
            "Aftermarket side step rails",
            "Aftermarket wheels",
        ],
    }
    items = collect_photo_detected_equipment(pj)
    assert "Aftermarket side step rails" in items
    assert "Aftermarket wheels" in items

    pj = {
        "observed_features": ["Red brake calipers", "Tow hitch"],
        "possible_packages": ["ST Performance Package"],
        "detected_adas": ["360_cameras", "blind_spot"],
    }
    items = collect_photo_detected_equipment(pj)
    assert "Red brake calipers" in items
    assert "Tow hitch" in items
    assert "ST Performance Package" in items
    assert "360° surround-view cameras" in items
    assert "Blind spot monitoring" in items


def test_humanize_adas_token() -> None:
    assert humanize_adas_token("360_cameras") == "360° surround-view cameras"


def test_prepare_car_detail_ford_hides_photo_equipment() -> None:
    from backend.enrichment.knowledge_engine import prepare_car_detail_context

    car = {
        "vin": "1FM5K8HT9HGD95470",
        "make": "Ford",
        "gallery": [],
        "packages": json.dumps(
            {
                "observed_features": ["Bang & Olufsen audio system", "Chrome grille"],
                "detected_adas": ["360_cameras"],
                "possible_packages": ["ST Performance Package"],
            }
        ),
    }
    ctx = prepare_car_detail_context(car)
    assert ctx.get("hide_photo_analysis") is True
    assert ctx.get("listing_photo_detected_equipment") == []
    assert ctx.get("listing_possible_packages") == ["ST Performance Package"]
