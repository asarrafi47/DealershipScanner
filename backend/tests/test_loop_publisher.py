"""Card L18: the Railway publisher (``ops_publisher.py``, ``ops_publish.py publish|sync``, the fleet hook)."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from backend.scripts import fleet_scan, ops_publish, ops_publisher

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "ops_publish"
SHARDS = {"s0": "scan_2026_11_03_s0", "s1": "scan_2026_11_03_s3", "s2": "scan_2026_11_03_s6"}
TOKEN = "tok_" + "Zk3Qm9xV2LpR7wYd8NaB5cEh"
GIT_ENV = {
    "GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_SYSTEM": os.devnull,
    "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@example.invalid",
    "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@example.invalid",
}


def _git(cwd, *args) -> str:
    r = subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True, env={**os.environ, **GIT_ENV})
    assert r.returncode == 0, r.stderr
    return r.stdout


@pytest.fixture()
def remote(tmp_path):
    seed = tmp_path / "seed"
    seed.mkdir()
    _git(seed, "init", "-q", "-b", "main")
    (seed / "README.md").write_text("data repo\n", encoding="utf-8")
    _git(seed, "add", "-A")
    _git(seed, "commit", "-q", "-m", "init")
    bare = tmp_path / "data.git"
    _git(tmp_path, "clone", "-q", "--bare", str(seed), str(bare))
    return bare


@pytest.fixture()
def env(tmp_path, remote, monkeypatch):
    monkeypatch.setenv("PUBLISH_SLEEP", "0")
    values = {
        **os.environ, **GIT_ENV, "LOOP_DATA_REPO": str(remote), "LOOP_DATA_TOKEN": TOKEN,
        "LOOP_DATA_DIR": str(tmp_path / "loop-data"), "LOOP_TARGET": "prod", "PUBLISH_SLEEP": "0",
    }
    return values


def _run_dir(tmp_path: Path, stamp: str) -> Path:
    run = tmp_path / "pipeline" / f"fleet_{stamp}"
    run.mkdir(parents=True)
    for tag, fixture in SHARDS.items():
        shutil.copytree(FIXTURES / fixture, run / tag)
    return run


def _branch_files(bare: Path, branch: str) -> list:
    return _git(bare, "ls-tree", "-r", "--name-only", branch).split()


def test_publish_puts_the_bundle_gates_and_queue_on_the_runs_branch(tmp_path, remote, env):
    run = _run_dir(tmp_path, "20261103T090000Z")
    result = ops_publisher.publish(run, env, "2026-11-03T12:00:00Z", catch_up=False)
    assert result["bundles"] == [{"run_id": run.name, "state": "published"}]
    files = _branch_files(remote, "runs")
    base = f"runs/{run.name}/"
    for name in ("MANIFEST.json", "digest.md", "dealers.tsv", "triage.json.gz", "gate_report.json", "queue.json"):
        assert base + name in files, name
    assert "runs/LATEST.json" in files


def test_publishing_twice_adds_no_commit(tmp_path, remote, env):
    run = _run_dir(tmp_path, "20261103T090000Z")
    ops_publisher.publish(run, env, "2026-11-03T12:00:00Z", catch_up=False)
    before = _git(remote, "rev-parse", "runs")
    again = ops_publisher.publish(run, env, "2026-11-03T12:00:00Z", catch_up=False)
    assert again["bundles"][0]["state"] == "unchanged"
    assert _git(remote, "rev-parse", "runs") == before


def test_catch_up_publishes_a_run_directory_the_branch_lacks(tmp_path, remote, env):
    older = _run_dir(tmp_path, "20261102T090000Z")
    newer = _run_dir(tmp_path, "20261103T090000Z")
    result = ops_publisher.publish(newer, env, "2026-11-03T12:00:00Z")
    assert {b["run_id"] for b in result["bundles"]} == {older.name, newer.name}
    files = _branch_files(remote, "runs")
    assert f"runs/{older.name}/MANIFEST.json" in files and f"runs/{newer.name}/MANIFEST.json" in files
    latest = json.loads(_git(remote, "show", "runs:runs/LATEST.json"))
    assert newer.name in json.dumps(latest)


def test_retention_rebuilds_the_branch_as_an_orphan_with_only_the_newest_runs(tmp_path, remote, env, monkeypatch):
    monkeypatch.setattr(ops_publisher, "RETAIN_FLEET_RUNS", 2)
    for stamp in ("20261101T090000Z", "20261102T090000Z", "20261103T090000Z"):
        ops_publisher.publish(_run_dir(tmp_path, stamp), env, "2026-11-03T12:00:00Z", catch_up=False)
    dirs = {f.split("/")[1] for f in _branch_files(remote, "runs") if f.startswith("runs/fleet_")}
    assert dirs == {"fleet_20261102T090000Z", "fleet_20261103T090000Z"}
    assert _git(remote, "rev-list", "--count", "runs").strip() == "1"  # an orphan: one commit


def test_dealer_logs_go_to_main_scrubbed(tmp_path, remote, env):
    run = _run_dir(tmp_path, "20261103T090000Z")
    clone = ops_publisher.sync_main(env)
    log = clone / "workspace" / "dealer_logs" / "abc-com" / "scan_runs.md"
    log.parent.mkdir(parents=True)
    log.write_text(f"## run\nfetched https://feed.example/api?api_key=abcdef123456&page=2 with {TOKEN}\n", encoding="utf-8")
    result = ops_publisher.publish(run, env, "2026-11-03T12:00:00Z", catch_up=False)
    assert result["logs"] == 1
    text = _git(remote, "show", "main:workspace/dealer_logs/abc-com/scan_runs.md")
    assert "abcdef123456" not in text and TOKEN not in text and "## run" in text
    assert _git(remote, "log", "-1", "--format=%s", "main").strip() == "logs: " + run.name


def test_the_token_is_in_no_commit_no_git_config_and_no_argv(tmp_path, remote, env, monkeypatch):
    seen = []
    real = subprocess.run

    def spy(cmd, *a, **kw):
        seen.append(list(cmd))
        return real(cmd, *a, **kw)

    monkeypatch.setattr(ops_publisher.subprocess, "run", spy)
    ops_publisher.publish(_run_dir(tmp_path, "20261103T090000Z"), env, "2026-11-03T12:00:00Z", catch_up=False)
    assert seen and not any(TOKEN in part for cmd in seen for part in cmd)
    for clone in (Path(env["LOOP_DATA_DIR"]), ops_publisher.runs_dir(env)):
        assert TOKEN not in (clone / ".git" / "config").read_text(encoding="utf-8")
    for branch in ("main", "runs"):
        for name in _branch_files(remote, branch):
            assert TOKEN not in _git(remote, "show", f"{branch}:{name}") if name.endswith((".md", ".json", ".tsv", ".txt")) else True


def test_a_rejecting_remote_a_missing_token_and_a_bad_run_each_fail_without_raising_out_of_the_cli(tmp_path, remote, env, capsys):
    run = _run_dir(tmp_path, "20261103T090000Z")
    hook = remote / "hooks" / "pre-receive"
    hook.write_text("#!/bin/sh\nexit 1\n", encoding="utf-8")
    hook.chmod(0o755)
    args = ops_publish._parser().parse_args(["publish", "--run-dir", str(run), "--no-catch-up"])
    assert ops_publish.publish_command(args, env) == 1
    rc = json.loads((run / "publisher.rc").read_text(encoding="utf-8"))
    assert rc["rc"] == 1 and TOKEN not in json.dumps(rc)
    no_token = {k: v for k, v in env.items() if k != "LOOP_DATA_TOKEN"}
    assert ops_publish.publish_command(args, no_token) == 1
    bad = ops_publish._parser().parse_args(["publish", "--run-dir", str(tmp_path / "nope")])
    assert ops_publish.publish_command(bad, env) == 1
    assert TOKEN not in capsys.readouterr().err


def test_the_fleet_hook_is_skipped_without_config_and_never_raises(tmp_path, monkeypatch, capsys):
    calls = []
    monkeypatch.delenv("LOOP_DATA_REPO", raising=False)
    monkeypatch.delenv("LOOP_DATA_TOKEN", raising=False)
    monkeypatch.setattr(fleet_scan.subprocess, "run", lambda *a, **k: calls.append(a))
    fleet_scan.publish_run(tmp_path)
    assert calls == []
    monkeypatch.setenv("LOOP_DATA_REPO", "x")
    monkeypatch.setenv("LOOP_DATA_TOKEN", "y")

    def boom(*a, **k):
        raise subprocess.TimeoutExpired("git", 1)

    monkeypatch.setattr(fleet_scan.subprocess, "run", boom)
    fleet_scan.publish_run(tmp_path)  # does not raise
    assert "publish skipped" in capsys.readouterr().out


def test_the_scanner_image_has_git_and_the_job_syncs_before_the_fleet():
    root = Path(__file__).resolve().parents[2]
    assert " git " in (root / "Dockerfile.scanner").read_text(encoding="utf-8")
    script = (root / "scripts" / "railway_scan_fleet.sh").read_text(encoding="utf-8")
    assert script.index("ops_publish sync") < script.index("exec python3 -m backend.scripts.fleet_scan")
    assert "DEALER_LOGS_ROOT" in script


def test_ops_publish_cli_runs_as_a_script(tmp_path):
    root = Path(__file__).resolve().parents[2]
    r = subprocess.run([sys.executable, str(root / "backend" / "scripts" / "ops_publish.py"), "publish", "--help"],
                       capture_output=True, text=True, cwd=root)
    assert r.returncode == 0 and "--run-dir" in r.stdout
