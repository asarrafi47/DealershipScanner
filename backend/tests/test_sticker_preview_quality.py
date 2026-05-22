"""Window sticker PNG preview render settings."""

from __future__ import annotations

from backend.enrichment.window_sticker_service import sticker_preview_dpi


def test_sticker_preview_dpi_default(monkeypatch) -> None:
    monkeypatch.delenv("WINDOW_STICKER_PREVIEW_DPI", raising=False)
    assert sticker_preview_dpi() == 200


def test_sticker_preview_dpi_env_clamped(monkeypatch) -> None:
    monkeypatch.setenv("WINDOW_STICKER_PREVIEW_DPI", "400")
    assert sticker_preview_dpi() == 300
    monkeypatch.setenv("WINDOW_STICKER_PREVIEW_DPI", "50")
    assert sticker_preview_dpi() == 120
