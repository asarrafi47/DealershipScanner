"""VDP throughput helpers (concurrency + uncapped gallery)."""

from __future__ import annotations

from backend.scanner.scan_efficiency import (
    effective_vdp_concurrency,
    vdp_gallery_carousel_only,
    vdp_gallery_url_max,
)


def test_effective_vdp_concurrency_default_four(monkeypatch):
    monkeypatch.delenv("SCANNER_MAX_VDP_CONCURRENCY", raising=False)
    assert effective_vdp_concurrency() == 4


def test_effective_vdp_concurrency_env_override(monkeypatch):
    monkeypatch.setenv("SCANNER_MAX_VDP_CONCURRENCY", "8")
    assert effective_vdp_concurrency() == 8


def test_vdp_gallery_url_max_uncapped_by_default(monkeypatch):
    monkeypatch.delenv("SCANNER_VDP_GALLERY_MAX_URLS", raising=False)
    assert vdp_gallery_url_max() is None


def test_vdp_gallery_url_max_env_cap(monkeypatch):
    monkeypatch.setenv("SCANNER_VDP_GALLERY_MAX_URLS", "120")
    assert vdp_gallery_url_max() == 120


def test_vdp_gallery_carousel_only_default_on(monkeypatch):
    monkeypatch.delenv("SCANNER_VDP_GALLERY_CAROUSEL_ONLY", raising=False)
    assert vdp_gallery_carousel_only() is True


def test_vdp_gallery_carousel_only_can_disable(monkeypatch):
    monkeypatch.setenv("SCANNER_VDP_GALLERY_CAROUSEL_ONLY", "0")
    assert vdp_gallery_carousel_only() is False
