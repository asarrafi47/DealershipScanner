"""``dealer_pipeline --no-reconcile`` reaches the scanner subprocesses (P1A.1).

Before: the flag only dry-ran the pipeline's own reconcile (run.py, lifecycle.py),
while every ``scanner.py`` it spawned kept ``SCANNER_RECONCILE`` on and retired
rows in inventory_reconcile. ``run()`` now sets ``SCANNER_RECONCILE=0`` in its
environment before the batches (runner.run_http_only_scan copies os.environ) and
restores the caller's value on return. ``subprocess.run`` is a recorder here: no
scanner runs, no network.
"""
from __future__ import annotations

import json
import os
import sqlite3
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from backend.scripts import dealer_pipeline as dp
from backend.tests.pipeline_patch import patch_pipeline

D = "noreconcile-dealer-com"


@pytest.fixture()
def log_root(tmp_path, monkeypatch):
    root = tmp_path / "dealer_logs"
    patch_pipeline(monkeypatch, "LOG_ROOT", root)
    monkeypatch.setenv("DEALER_LOGS_ROOT", str(root))
    return root


def _scan_runs_row(db: Path, n: int) -> None:
    """What a finished scanner run leaves behind for assess (n rows captured)."""
    now = (datetime.now(timezone.utc) + timedelta(seconds=1)).replace(microsecond=0).isoformat()
    c = sqlite3.connect(str(db))
    try:
        summary = {"capture_coverage": {"n": n, "price": 1.0, "trim": 1.0, "exterior_color": 1.0}, "recipe_fetch": "hit"}
        c.execute("INSERT INTO scan_runs (dealer_id, finished_at, duration_seconds, upserted, inventory_rows, error, provider, summary_json) "
                  "VALUES (?,?,?,?,?,?,?,?)", (D, now, 30, n, n, None, "carscommerce", json.dumps(summary)))
        c.commit()
    finally:
        c.close()


def _setup(monkeypatch, tmp_path, sqlite_inventory, *, flag: bool, lifecycle: bool = False) -> list[dict]:
    """Drive ``dealer_pipeline.main()`` for one dealer with a recipe on file; every
    ``scanner.py`` subprocess is recorded (its env) instead of run."""
    scans: list[dict] = []

    def _run(cmd, **kw):
        if "scanner.py" in cmd:
            scans.append(dict(kw["env"]))
            _scan_runs_row(sqlite_inventory.path, 0)  # replay yielded nothing: verdict no_rows
            return subprocess.CompletedProcess(cmd, 0)
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

    monkeypatch.setattr(subprocess, "run", _run)
    patch_pipeline(monkeypatch, "ensure_recipe", lambda dealer, force=False: {"had_recipes": 1, "synth": None})
    patch_pipeline(monkeypatch, "wait_for_db", lambda *a, **k: True)
    patch_pipeline(monkeypatch, "wait_for_scanner_lock", lambda *a, **k: True)
    patch_pipeline(monkeypatch, "chromium_process_count", lambda: 0)
    patch_pipeline(monkeypatch, "record_timing", lambda did, run: {"minutes": 0.5, "flags": []})
    patch_pipeline(monkeypatch, "_scan_hints", lambda did: {})
    patch_pipeline(monkeypatch, "run_lifecycle", lambda d, r, **k: {"lifecycle": "resynth_ok", "steps": [], "recipe": {"had_recipes": 1, "synth": "saved_1"}})
    patch_pipeline(monkeypatch, "platform_cluster_lines", lambda ids: [])
    manifest = tmp_path / "dealers.json"
    manifest.write_text(json.dumps({"dealers": [{"dealer_id": D, "url": f"https://www.{D}.com", "name": "No Reconcile Store"}]}),
                        encoding="utf-8")
    argv = ["dealer_pipeline", "--dealers", D, "--manifest", str(manifest), "--out", str(tmp_path / "out"), "--no-vpic"]
    if flag:
        argv.append("--no-reconcile")
    if not lifecycle:
        argv.append("--no-lifecycle")
    monkeypatch.setattr("sys.argv", argv)
    return scans


def test_no_reconcile_reaches_the_scanner_subprocess(monkeypatch, tmp_path, log_root, sqlite_inventory):
    monkeypatch.setenv("SCANNER_RECONCILE", "1")
    scans = _setup(monkeypatch, tmp_path, sqlite_inventory, flag=True)
    assert dp.main() == 0
    assert len(scans) == 1
    assert scans[0]["SCANNER_RECONCILE"] == "0"
    assert scans[0]["SCANNER_HTTP_ONLY"] == "1"  # the rest of the scanner env is unchanged
    assert os.environ["SCANNER_RECONCILE"] == "1", "the caller's environment is restored"


def test_no_reconcile_reaches_the_lifecycle_retry_scan_too(monkeypatch, tmp_path, log_root, sqlite_inventory):
    monkeypatch.delenv("SCANNER_RECONCILE", raising=False)
    scans = _setup(monkeypatch, tmp_path, sqlite_inventory, flag=True, lifecycle=True)
    assert dp.main() == 0
    triage = json.loads((tmp_path / "out" / "triage.json").read_text(encoding="utf-8"))
    assert triage["lifecycle"]["retried"] == {D: "retried: no_rows → no_rows"}
    assert len(scans) == 2, "main batch + lifecycle retry batch"
    assert [s.get("SCANNER_RECONCILE") for s in scans] == ["0", "0"]
    assert "SCANNER_RECONCILE" not in os.environ


def test_without_the_flag_the_scanner_env_is_untouched(monkeypatch, tmp_path, log_root, sqlite_inventory):
    monkeypatch.setenv("SCANNER_RECONCILE", "1")
    scans = _setup(monkeypatch, tmp_path, sqlite_inventory, flag=False)
    assert dp.main() == 0
    assert [s.get("SCANNER_RECONCILE") for s in scans] == ["1"]
    assert os.environ["SCANNER_RECONCILE"] == "1"


def test_environment_restored_when_the_run_fails(monkeypatch, tmp_path, log_root, sqlite_inventory):
    monkeypatch.delenv("SCANNER_RECONCILE", raising=False)
    _setup(monkeypatch, tmp_path, sqlite_inventory, flag=True)

    def _boom(*a, **k):
        raise RuntimeError("roster exploded")

    patch_pipeline(monkeypatch, "dealers_from_db", _boom)
    patch_pipeline(monkeypatch, "dealer_from_manifest", lambda did, manifest: None)
    with pytest.raises(RuntimeError, match="roster exploded"):
        dp.main()
    assert "SCANNER_RECONCILE" not in os.environ
