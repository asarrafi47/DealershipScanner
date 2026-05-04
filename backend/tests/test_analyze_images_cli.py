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


def test_main_interior_url_invokes_analyze(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.setattr(
        ai,
        "analyze_interior_from_image_url",
        lambda url, inference_context="cabin": {"ok": True, "url": url, "ctx": inference_context},
    )
    ai.main(["interior", "--url", "https://example.com/x.jpg"])
    out = capsys.readouterr().out
    assert "ok" in out
    assert "https://example.com/x.jpg" in out
