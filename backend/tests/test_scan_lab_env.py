"""Scan-lab subprocess must not inherit the Postgres DSN (audit B-030)."""
from __future__ import annotations

from pathlib import Path

from backend.dev import scan_lab as sl


def test_lab_env_strips_dsn(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("DATABASE_URL", "postgresql://prod/cars")
    monkeypatch.setenv("INVENTORY_DATABASE_URL", "postgresql://prod/cars")
    monkeypatch.delenv("SCAN_LAB_DATABASE_URL", raising=False)
    env = sl._scan_lab_subprocess_env(tmp_path / "m.json")
    assert "DATABASE_URL" not in env
    assert "INVENTORY_DATABASE_URL" not in env
    assert env["SCANNER_SCAN_ONLY"] == "1"


def test_lab_env_uses_explicit_lab_dsn(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("INVENTORY_DATABASE_URL", "postgresql://prod/cars")
    monkeypatch.setenv("SCAN_LAB_DATABASE_URL", "postgresql://localhost/scratch")
    env = sl._scan_lab_subprocess_env(tmp_path / "m.json")
    assert env["INVENTORY_DATABASE_URL"] == "postgresql://localhost/scratch"
    assert "SCAN_LAB_DATABASE_URL" not in env
