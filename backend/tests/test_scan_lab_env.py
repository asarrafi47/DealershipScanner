"""Scan-lab subprocess must not inherit the Postgres DSN (audit B-030)."""
from __future__ import annotations

from pathlib import Path

import pytest

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


def test_lab_env_disables_dotenv_refill(monkeypatch, tmp_path: Path):
    env = sl._scan_lab_subprocess_env(tmp_path / "m.json")
    assert env["PROJECT_DOTENV_DISABLE"] == "1"


def test_lab_job_refuses_without_lab_dsn(monkeypatch, tmp_path: Path):
    monkeypatch.delenv("SCAN_LAB_DATABASE_URL", raising=False)
    monkeypatch.setenv("INVENTORY_DATABASE_URL", "postgresql://prod/cars")

    def boom(*a, **k):
        raise AssertionError("scanner must not be spawned")

    monkeypatch.setattr(sl.subprocess, "Popen", boom)
    sl.scan_lab_jobs["j1"] = {"log": ""}
    sl._run_manifest_scan_job("j1", tmp_path / "m.json")
    job = sl.scan_lab_jobs.pop("j1")
    assert job["done"] and job["exit_code"] == 2
    assert "SCAN_LAB_DATABASE_URL" in job["log"]


def test_irvinebmw_script_refuses_without_scratch_dsn():
    import os
    import subprocess

    root = Path(__file__).resolve().parents[2]
    env = {k: v for k, v in os.environ.items() if k not in ("SCAN_TEST_DATABASE_URL", "SCAN_LAB_DATABASE_URL")}
    r = subprocess.run(["bash", str(root / "scripts/scan_irvinebmw_test.sh")], env=env,
                       capture_output=True, text=True, timeout=30)
    assert r.returncode == 2


@pytest.mark.parametrize(
    "lab_dsn",
    [
        "postgresql://localhost/cars",
        "postgresql://localhost:5432/cars?sslmode=disable",
        "postgresql://localhost/cars_cleanroom",
        "postgresql://localhost/cars_cr_batch2",
        "postgresql://localhost/postgres",
        "mysql://localhost/cars_lab",
    ],
)
def test_lab_refuses_real_inventory_databases(monkeypatch, tmp_path: Path, lab_dsn):
    monkeypatch.setenv("SCAN_LAB_DATABASE_URL", lab_dsn)
    with pytest.raises(sl.ScanLabRefused):
        sl.scan_lab_dsn()
    env = sl._scan_lab_subprocess_env(tmp_path / "m.json")
    assert "INVENTORY_DATABASE_URL" not in env


def test_lab_refuses_dsn_equal_to_inventory_dsn(monkeypatch):
    monkeypatch.setenv("INVENTORY_DATABASE_URL", "postgresql://u:p@db.internal:5432/prodcars")
    monkeypatch.setenv("SCAN_LAB_DATABASE_URL", "postgresql://other@db.internal:5432/prodcars")
    with pytest.raises(sl.ScanLabRefused):
        sl.scan_lab_dsn()


def test_lab_accepts_cars_lab(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("INVENTORY_DATABASE_URL", "postgresql://localhost/cars")
    monkeypatch.setenv("SCAN_LAB_DATABASE_URL", "postgresql://localhost/cars_lab")
    assert sl.scan_lab_database_name() == "cars_lab"
    env = sl._scan_lab_subprocess_env(tmp_path / "m.json")
    assert env["INVENTORY_DATABASE_URL"] == "postgresql://localhost/cars_lab"


def test_lab_job_refuses_prod_looking_lab_dsn(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("SCAN_LAB_DATABASE_URL", "postgresql://localhost/cars")

    def boom(*a, **k):
        raise AssertionError("scanner must not be spawned")

    monkeypatch.setattr(sl.subprocess, "Popen", boom)
    sl.scan_lab_jobs["j2"] = {"log": ""}
    sl._run_manifest_scan_job("j2", tmp_path / "m.json")
    job = sl.scan_lab_jobs.pop("j2")
    assert job["exit_code"] == 2
    assert "protected" in job["log"]


def test_lab_db_conn_refuses_without_dsn(monkeypatch):
    monkeypatch.delenv("SCAN_LAB_DATABASE_URL", raising=False)
    with pytest.raises(sl.ScanLabRefused):
        with sl.scan_lab_db_conn():
            pass
