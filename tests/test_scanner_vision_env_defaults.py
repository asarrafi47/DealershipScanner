"""Defaults for ``python scanner.py`` vision-related env toggles."""

from __future__ import annotations

import pytest

from backend.scanner_post_pipeline import (
    gallery_vision_filter_env_enabled,
    monroney_vision_env_enabled,
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
