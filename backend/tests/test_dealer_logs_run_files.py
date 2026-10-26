"""N9.12: every run leaves all five dealer log files; log lines are never truncated (IL-22)."""
from __future__ import annotations

import json

import pytest

from backend.scanner.pipeline import dealer_logs as dl

FILES = {"discovery.md", "location.md", "scan_runs.md", "summary.md", "scan_instructions.md"}


@pytest.fixture()
def root(tmp_path, monkeypatch):
    monkeypatch.setattr(dl, "LOG_ROOT", tmp_path)
    return tmp_path


def _result(verdict, did):
    return {"dealer_id": did, "verdict": verdict, "reason": "r", "rows": 100, "rows_captured": 100, "rows_refused_vin_owner": 10,
            "rows_refused_rooftop": 0, "rows_written": 90, "rows_new": 40, "rows_used": 50, "known_before": 90, "active_after": 90,
            "inactive_after": 1, "run_finished_at": "2026-10-13T15:00:00+00:00", "provider": "p",
            "http_first": {"statuses": {str(c): c for c in range(200, 260)}, "fetched": 12, "candidates": 99},
            "vin_owner_conflict_owners": {"sis": 10}, "recipe": {"had_recipes": 1}}


@pytest.mark.parametrize("verdict", ["ok", "thin", "inaccurate"])
def test_all_five_files_written(root, verdict, monkeypatch):
    monkeypatch.setattr("backend.scanner.recipes.load_recipes", lambda did: [])
    dl.log_run_files(_result(verdict, "d1"), "2026-10-13 15:00 UTC")
    assert {p.name for p in (root / "d1").iterdir()} == FILES
    runs = (root / "d1" / "scan_runs.md").read_text()
    assert "captured 100" in runs and "VIN-owner guard 10" in runs and "written 90" in runs and "2026-10-13T15:00:00" in runs
    # every http_first line parses (43 of 434 did not: the 300-character cut)
    lines = [ln for ln in runs.splitlines() if ln.startswith("- http_first: ")]
    assert lines and all(json.loads(ln[len("- http_first: "):]) for ln in lines)


def test_ok_run_replaces_the_provisional_instructions(root, monkeypatch):
    monkeypatch.setattr("backend.scanner.recipes.load_recipes", lambda did: [])
    dl.log_run_files(_result("thin", "d2"), "t1")
    assert dl.PROVISIONAL_MARK in (root / "d2" / "scan_instructions.md").read_text()
    dl.log_run_files(_result("ok", "d2"), "t2")
    assert dl.PROVISIONAL_MARK not in (root / "d2" / "scan_instructions.md").read_text()
    assert "t1" in (root / "d2" / "summary.md").read_text()
