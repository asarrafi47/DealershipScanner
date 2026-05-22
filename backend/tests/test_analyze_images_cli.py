"""Smoke tests for unified image analysis entrypoint."""

from __future__ import annotations

from pathlib import Path

import pytest

import backend.vision.analyze_images as ai


def test_repo_root_points_at_inventory_parent() -> None:
    assert (ai.REPO_ROOT / "backend").is_dir()
    assert (ai.REPO_ROOT / "backend" / "vision" / "analyze_images.py").is_file()


def test_apply_inventory_db_defaults_uses_repo_inventory(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("INVENTORY_DB_PATH", raising=False)
    fake_root = tmp_path / "proj"
    (fake_root / "backend" / "vision").mkdir(parents=True)
    monkeypatch.setattr(ai, "REPO_ROOT", fake_root)
    p = ai.apply_inventory_db_defaults(None)
    assert p == str(fake_root / "inventory.db")
    assert Path(p).parent == fake_root


def test_main_filter_gallery_invokes_filter(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.setattr(
        ai,
        "filter_gallery_urls_for_vehicle_listing",
        lambda urls, max_workers=1: list(urls),
    )
    ai.main(["filter-gallery", "https://example.com/a.jpg", "https://example.com/b.jpg"])
    out = capsys.readouterr().out
    assert "https://example.com/a.jpg" in out
    assert "https://example.com/b.jpg" in out
