"""Defaults for ``python scanner.py`` vision-related env toggles."""

from __future__ import annotations

import pytest

import backend.scanner_post_pipeline as scanner_post_pipeline
from backend.scanner_post_pipeline import (
    candidate_urls_for_interior_vision,
    gallery_vision_filter_env_enabled,
    monroney_vision_env_enabled,
    pick_listing_image_for_interior_vision,
    post_interior_vision_env_enabled,
)


def test_vision_toggles_default_on(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in (
        "SCANNER_GALLERY_VISION_FILTER",
        "SCANNER_MONRONEY_VISION",
        "SCANNER_POST_INTERIOR_VISION",
    ):
        monkeypatch.delenv(key, raising=False)
    assert gallery_vision_filter_env_enabled() is True
    assert monroney_vision_env_enabled() is True
    assert post_interior_vision_env_enabled() is True


def test_vision_toggles_opt_out_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SCANNER_GALLERY_VISION_FILTER", "0")
    monkeypatch.setenv("SCANNER_MONRONEY_VISION", "false")
    monkeypatch.setenv("SCANNER_POST_INTERIOR_VISION", "off")
    assert gallery_vision_filter_env_enabled() is False
    assert monroney_vision_env_enabled() is False
    assert post_interior_vision_env_enabled() is False


def test_pick_interior_vision_uses_hero_when_no_cabin(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("INTERIOR_VISION_NO_EXTERIOR_FALLBACK", raising=False)
    monkeypatch.setenv("INTERIOR_VISION_FALLBACK_THROUGH_WINDOWS", "1")
    monkeypatch.setattr(scanner_post_pipeline, "select_url_for_cabin_vision", lambda urls: None)
    u, ctx = pick_listing_image_for_interior_vision(
        ["https://dealer/a.jpg", "https://dealer/b.jpg"]
    )
    assert u == "https://dealer/a.jpg"
    assert ctx == "through_windows"


def test_pick_interior_vision_respects_no_exterior_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("INTERIOR_VISION_NO_EXTERIOR_FALLBACK", "1")
    monkeypatch.setattr(scanner_post_pipeline, "select_url_for_cabin_vision", lambda urls: None)
    u, _ctx = pick_listing_image_for_interior_vision(["https://dealer/a.jpg"])
    assert u is None


def test_pick_interior_vision_prefers_cabin_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        scanner_post_pipeline,
        "select_url_for_cabin_vision",
        lambda urls: "https://dealer/cabin.jpg",
    )
    u, ctx = pick_listing_image_for_interior_vision(["https://dealer/ext.jpg"])
    assert u == "https://dealer/cabin.jpg"
    assert ctx == "cabin"


def test_candidate_urls_include_multiple_fallbacks(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("INTERIOR_VISION_NO_EXTERIOR_FALLBACK", raising=False)
    monkeypatch.setenv("INTERIOR_VISION_FALLBACK_THROUGH_WINDOWS", "1")
    monkeypatch.setenv("INTERIOR_VISION_MAX_URL_TRIES", "3")
    monkeypatch.setattr(scanner_post_pipeline, "select_url_for_cabin_vision", lambda urls: None)
    cands = candidate_urls_for_interior_vision(
        ["https://dealer/1.jpg", "https://dealer/2.jpg", "https://dealer/3.jpg", "https://dealer/4.jpg"]
    )
    assert cands[:3] == [
        ("https://dealer/1.jpg", "through_windows"),
        ("https://dealer/2.jpg", "through_windows"),
        ("https://dealer/3.jpg", "through_windows"),
    ]
