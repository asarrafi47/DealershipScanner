"""N9.15: ``fleet_scan --resume <run_dir>`` scans only the dealers a killed pass has not finished.

The fake ``run_step`` stands in for ``dealer_pipeline`` and applies the pipeline's own resume
plan (backend/scanner/pipeline/resume.py) against a SQLite scan_runs table."""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from backend.scanner.pipeline.resume import plan_resume, prior_triage_dealers
from backend.scripts import fleet_scan

START = datetime(2026, 10, 20, 1, 0, 0, tzinfo=timezone.utc)
SHARDS = {"s0": ["a-com", "b-com", "c-com"], "s1": ["d-com", "e-com", "f-com"], "s2": ["g-com", "h-com"]}
ALL = sorted(d for ids in SHARDS.values() for d in ids)


def _conn():
    c = sqlite3.connect(":memory:", check_same_thread=False)
    c.execute("CREATE TABLE scan_runs (id INTEGER PRIMARY KEY, dealer_id TEXT, finished_at TEXT)")
    return c


def _entry(did):
    return {"dealer_id": did, "verdict": "ok", "rows": 1}


def _write_triage(d: Path, dealers):
    d.mkdir(parents=True, exist_ok=True)
    (d / "triage.json").write_text(json.dumps({"dealers": [_entry(x) for x in dealers]}))


def _killed_run(tmp_path) -> Path:
    """3 shards; s0 finished (triage), s1 died after d-com's scan_runs row, s2 never started."""
    run = tmp_path / "fleet_20261020T010000Z"
    run.mkdir()
    for tag, ids in SHARDS.items():
        (run / f"{tag}.txt").write_text("\n".join(ids) + "\n")
    (run / "run_state.json").write_text(json.dumps({"started": START.isoformat()}))
    _write_triage(run / "s0", SHARDS["s0"])
    return run


def test_plan_splits_done_assess_only_and_todo(tmp_path):
    conn = _conn()
    conn.execute("INSERT INTO scan_runs (dealer_id, finished_at) VALUES ('d-com', '2026-10-20T03:00:00+00:00')")
    conn.execute("INSERT INTO scan_runs (dealer_id, finished_at) VALUES ('e-com', '2026-10-19T03:00:00+00:00')")  # before the start
    _write_triage(tmp_path / "s1", ["f-com"])
    plan = plan_resume(SHARDS["s1"], tmp_path / "s1", conn, START.isoformat())
    assert [d["dealer_id"] for d in plan["prior"]] == ["f-com"]
    assert plan["assess_only"] == ["d-com"]
    assert plan["todo"] == ["e-com"]


def test_missing_or_corrupt_triage_means_nothing_done(tmp_path):
    assert prior_triage_dealers(tmp_path / "nope") == []
    (tmp_path / "triage.json").write_text("{not json")
    assert prior_triage_dealers(tmp_path) == []


def test_resume_scans_only_remaining_and_merges_one_entry_per_dealer(tmp_path, monkeypatch):
    run = _killed_run(tmp_path)
    conn = _conn()
    conn.execute("INSERT INTO scan_runs (dealer_id, finished_at) VALUES ('d-com', '2026-10-20T03:00:00+00:00')")
    scanned: list[str] = []
    cmds: dict[str, list[str]] = {}

    def fake_run_step(tag, cmd, env, sink):
        cmds[tag] = cmd
        assert "--resume" in cmd and cmd[cmd.index("--resume-since") + 1] == START.isoformat()
        ids = cmd[cmd.index("--dealers") + 1].split(",")
        out = Path(cmd[cmd.index("--out") + 1])
        plan = plan_resume(ids, out, conn, START.isoformat())
        scanned.extend(plan["todo"])
        merged = {d["dealer_id"]: d for d in plan["prior"]}
        for did in plan["assess_only"] + plan["todo"]:
            merged[did] = _entry(did)
        _write_triage(out, list(merged))
        return 0, 0.1

    monkeypatch.setattr(fleet_scan, "run_step", fake_run_step)
    monkeypatch.setattr(fleet_scan, "active_rows", lambda ids: pytest.fail("a resume keeps the shard rosters"))
    monkeypatch.setattr(fleet_scan, "load_roster", lambda: pytest.fail("a resume keeps the shard rosters"))
    monkeypatch.setattr(fleet_scan.time, "sleep", lambda s: None)
    monkeypatch.setattr(fleet_scan, "rooftop_candidate_report", lambda since: {})
    monkeypatch.setenv("SCAN_POST_STEPS", "0")
    monkeypatch.setenv("SCAN_OUT_ROOT", str(tmp_path))
    monkeypatch.setattr("sys.argv", ["fleet_scan", "--resume", str(run)])

    assert fleet_scan.main() == 0
    assert sorted(scanned) == ["e-com", "f-com", "g-com", "h-com"]  # not s0's three, not d-com
    assert sorted(cmds) == ["s0", "s1", "s2"]
    summary = json.loads((run / "fleet_summary.json").read_text())
    assert summary["assessed"] == len(ALL) and summary["dealers"] == len(ALL)
    ids = [d["dealer_id"] for tag in SHARDS for d in prior_triage_dealers(run / tag)]
    assert sorted(ids) == ALL  # one entry per dealer
    assert (run / "s0.txt").read_text().split() == SHARDS["s0"]  # rosters untouched


def test_resume_dir_from_env_and_missing_rosters(tmp_path, monkeypatch):
    monkeypatch.setenv("SCAN_RESUME_DIR", str(tmp_path))
    assert fleet_scan.resume_dir_arg([]) == tmp_path
    assert fleet_scan.resume_dir_arg(["--resume=/x/y"]) == Path("/x/y")
    with pytest.raises(SystemExit):
        fleet_scan.load_resume_state(tmp_path)


def test_start_falls_back_to_directory_name(tmp_path):
    run = tmp_path / "fleet_20261020T010000Z"
    run.mkdir()
    (run / "s0.txt").write_text("a-com\n")
    started, shards = fleet_scan.load_resume_state(run)
    assert started == START and shards == [["a-com"]]
