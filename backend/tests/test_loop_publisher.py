"""Card L18: the Railway publisher (``ops_publisher.py``, ``ops_publish.py publish``, the fleet hook) on the store."""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from backend.scanner.loop import store as loop_store
from backend.scripts import fleet_scan, ops_publish, ops_publisher

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "ops_publish"
SHARDS = {"s0": "scan_2026_11_03_s0", "s1": "scan_2026_11_03_s3", "s2": "scan_2026_11_03_s6"}
SECRET = "tok_" + "Zk3Qm9xV2LpR7wYd8NaB5cEh"
NOW = "2026-11-03T12:00:00Z"


@pytest.fixture()
def store():
    return loop_store.MemStore()


@pytest.fixture()
def env(tmp_path):
    return {"LOOP_STORE": "db", "LOOP_TARGET": "prod", "DEALER_LOGS_ROOT": str(tmp_path / "logs"), "PATH": "/usr/bin"}


def _run_dir(tmp_path: Path, stamp: str) -> Path:
    run = tmp_path / "pipeline" / f"fleet_{stamp}"
    run.mkdir(parents=True)
    for tag, fixture in SHARDS.items():
        shutil.copytree(FIXTURES / fixture, run / tag)
    return run


def _paths(store, prefix="") -> list:
    return [e.path for e in store.list(prefix)]


def test_publish_puts_the_bundle_gates_and_queue_in_the_store(tmp_path, store, env):
    run = _run_dir(tmp_path, "20261103T090000Z")
    result = ops_publisher.publish(run, env, NOW, catch_up=False, store=store)
    assert result["bundles"] == [{"run_id": run.name, "state": "published"}]
    files = _paths(store)
    base = f"runs/{run.name}/"
    for name in ("MANIFEST.json", "digest.md", "dealers.tsv", "triage.json.gz", "gate_report.json", "queue.json"):
        assert base + name in files, name
    assert "runs/LATEST.json" in files


def test_publishing_twice_changes_nothing(tmp_path, store, env):
    run = _run_dir(tmp_path, "20261103T090000Z")
    ops_publisher.publish(run, env, NOW, catch_up=False, store=store)
    before = {e.path: e.sha256 for e in store.list()}
    again = ops_publisher.publish(run, env, NOW, catch_up=False, store=store)
    assert again["bundles"][0]["state"] == "unchanged"
    assert {e.path: e.sha256 for e in store.list()} == before


def test_catch_up_publishes_a_run_directory_the_store_lacks(tmp_path, store, env):
    older = _run_dir(tmp_path, "20261102T090000Z")
    newer = _run_dir(tmp_path, "20261103T090000Z")
    result = ops_publisher.publish(newer, env, NOW, store=store)
    assert {b["run_id"] for b in result["bundles"]} == {older.name, newer.name}
    assert f"runs/{older.name}/MANIFEST.json" in _paths(store) and f"runs/{newer.name}/MANIFEST.json" in _paths(store)
    assert newer.name in store.get("runs/LATEST.json").decode()


def test_retention_deletes_the_oldest_runs_and_keeps_latest_on_the_newest(tmp_path, store, env, monkeypatch):
    monkeypatch.setattr(ops_publisher, "RETAIN_FLEET_RUNS", 2)
    for stamp in ("20261101T090000Z", "20261102T090000Z", "20261103T090000Z"):
        ops_publisher.publish(_run_dir(tmp_path, stamp), env, NOW, catch_up=False, store=store)
    dirs = {p.split("/")[1] for p in _paths(store, "runs/fleet_")}
    assert dirs == {"fleet_20261102T090000Z", "fleet_20261103T090000Z"}
    assert "fleet_20261103T090000Z" in store.get("runs/LATEST.json").decode()


def test_dealer_logs_are_stored_scrubbed_and_session_notes_are_never_overwritten(tmp_path, store, env):
    run = _run_dir(tmp_path, "20261103T090000Z")
    log = tmp_path / "logs" / "abc-com" / "scan_runs.md"
    log.parent.mkdir(parents=True)
    log.write_text(f"## run\nfetched https://feed.example/api?api_key=abcdef123456&page=2\n", encoding="utf-8")
    (log.parent / "summary.md").write_text("railway summary\n", encoding="utf-8")
    store.put("workspace/dealer_logs/abc-com/summary.md", b"session summary\n", "cloud")
    result = ops_publisher.publish(run, env, NOW, catch_up=False, store=store)
    assert result["logs"] == 1
    text = store.get("workspace/dealer_logs/abc-com/scan_runs.md").decode()
    assert "abcdef123456" not in text and "## run" in text
    assert store.get("workspace/dealer_logs/abc-com/summary.md") == b"session summary\n"


def test_an_oversize_log_keeps_its_newest_part(tmp_path, store, env):
    log = tmp_path / "logs" / "abc-com" / "scan_runs.md"
    log.parent.mkdir(parents=True)
    limit = loop_store.file_limit("workspace/dealer_logs/abc-com/scan_runs.md")
    log.write_text("old\n" * limit + "NEWEST\n", encoding="utf-8")
    ops_publisher.publish_logs(tmp_path / "logs", store, env)
    data = store.get("workspace/dealer_logs/abc-com/scan_runs.md")
    assert len(data) <= limit and data.endswith(b"NEWEST\n")


def test_a_full_store_a_missing_flag_and_a_bad_run_each_fail_without_raising_out_of_the_cli(tmp_path, env, monkeypatch, capsys):
    run = _run_dir(tmp_path, "20261103T090000Z")
    tiny = loop_store.MemStore(max_total=10)
    monkeypatch.setattr(ops_publisher, "open_store", lambda e: tiny)
    args = ops_publish._parser().parse_args(["publish", "--run-dir", str(run), "--no-catch-up"])
    assert ops_publish.publish_command(args, env) == 1
    rc = json.loads((run / "publisher.rc").read_text(encoding="utf-8"))
    assert rc["rc"] == 1
    assert ops_publish.publish_command(args, {k: v for k, v in env.items() if k != "LOOP_STORE"}) == 1
    bad = ops_publish._parser().parse_args(["publish", "--run-dir", str(tmp_path / "nope")])
    assert ops_publish.publish_command(bad, env) == 1


def test_the_fleet_hook_is_skipped_without_the_flag_and_never_raises(tmp_path, monkeypatch, capsys):
    calls = []
    monkeypatch.delenv("LOOP_STORE", raising=False)
    monkeypatch.setattr(fleet_scan.subprocess, "run", lambda *a, **k: calls.append(a))
    fleet_scan.publish_run(tmp_path)
    assert calls == []
    monkeypatch.setenv("LOOP_STORE", "db")

    def boom(*a, **k):
        raise subprocess.TimeoutExpired("python", 1)

    monkeypatch.setattr(fleet_scan.subprocess, "run", boom)
    fleet_scan.publish_run(tmp_path)  # does not raise
    assert "publish skipped" in capsys.readouterr().out


def test_the_job_script_no_longer_syncs_a_git_clone():
    root = Path(__file__).resolve().parents[2]
    script = (root / "scripts" / "railway_scan_fleet.sh").read_text(encoding="utf-8")
    assert "ops_publish sync" not in script and "LOOP_DATA_TOKEN" not in script
    assert "exec python3 -m backend.scripts.fleet_scan" in script


def test_ops_publish_cli_runs_as_a_script(tmp_path):
    root = Path(__file__).resolve().parents[2]
    r = subprocess.run([sys.executable, str(root / "backend" / "scripts" / "ops_publish.py"), "publish", "--help"],
                       capture_output=True, text=True, cwd=root)
    assert r.returncode == 0 and "--run-dir" in r.stdout


# --- card L12: the recipe mirror -----------------------------------------------------------------------------------


def _rows(url="https://feed.abc.example/api", **extra):
    row = {"dealer_id": "abc-com", "url": url, "method": "GET", "content_type": "application/json", "post_template": None,
           "auth_headers": {"x-api-key": "k1"}, "pagination": "page_query", "vehicle_rows": 3, "total_count": 3,
           "provider_hint": "p", "stale": False, "stale_reason": "", "saved_at": 1.0, "last_ok_at": 2.0,
           "field_coverage": {"price": 1.0}}
    row.update(extra)
    return row


def test_canonical_hash_ignores_stamps_and_key_order_but_not_a_url():
    from backend.scanner.loop import bundle

    base = [_rows()]
    sha = bundle.canonical_sha256(base)
    assert bundle.canonical_sha256([_rows(saved_at=99.0, last_ok_at=98.0, field_coverage={})]) == sha
    assert bundle.canonical_sha256([dict(reversed(list(_rows().items())))]) == sha
    assert bundle.canonical_sha256([_rows(url="https://feed.abc.example/other")]) != sha
    assert bundle.canonical_recipe_bytes(base).endswith(b"]\n") and b"saved_at" not in bundle.canonical_recipe_bytes(base)


def test_mirror_files_are_identical_across_runs_and_dealers_without_recipes_get_none(tmp_path):
    from backend.scanner.loop import bundle

    sets = {"abc-com": [_rows(), _rows(url="https://feed.abc.example/2", stale=True, stale_reason="401")],
            "def-com": [_rows(dealer_id="def-com")], "empty-com": []}
    hints = {"abc-com": {"price_source": "list", "secret": "no"}, "def-com": {}}
    out = tmp_path / "m"
    first = ops_publisher.write_mirror(out, sets, hints)
    snapshot = {p.name: p.read_bytes() for p in (out / "mirror").rglob("*") if p.is_file()}
    second = ops_publisher.write_mirror(out, sets, hints)
    assert first["written"] == 4 and second["written"] == 0 and second["unchanged"] == 4
    assert snapshot == {p.name: p.read_bytes() for p in (out / "mirror").rglob("*") if p.is_file()}
    assert not (out / "mirror" / "recipes" / "empty-com.json").exists()
    index = (out / "mirror" / "index.tsv").read_text(encoding="utf-8").splitlines()
    assert index[0] == "dealer_id\tcanonical_sha256\trecipe_count\tstale_count"
    assert index[1].split("\t")[0] == "abc-com" and index[1].split("\t")[2:] == ["2", "1"]
    assert json.loads((out / "mirror" / "scan_hints.json").read_text(encoding="utf-8")) == {"abc-com": {"price_source": "list"}}
    assert bundle.canonical_sha256(sets["abc-com"]) == index[1].split("\t")[1]
    # a dealer that lost its recipes loses its file
    del sets["def-com"]
    assert ops_publisher.write_mirror(out, sets, hints)["deleted"] == 1
    assert not (out / "mirror" / "recipes" / "def-com.json").exists()
    # the optional full backup keeps the stamps
    ops_publisher.write_mirror(out, sets, hints, full=True)
    import gzip
    assert json.loads(gzip.decompress((out / "recipes_full.json.gz").read_bytes()))["abc-com"][0]["saved_at"] == 1.0


def test_the_mirror_cli_refuses_an_output_inside_this_repository(tmp_path):
    root = Path(__file__).resolve().parents[2]
    args = ops_publish._parser().parse_args(["mirror", "--out", str(root / "workspace" / "mirror_out")])
    assert ops_publish.mirror_command(args, {}) == 2
    assert not (root / "workspace" / "mirror_out").exists()


def test_publish_stores_the_mirror_and_a_source_failure_only_skips_it(store, env):
    state = ops_publisher.publish_mirror(store, "fleet_20261103T090000Z", env, source=lambda: ({"abc-com": [_rows()]}, {}))
    assert state == "published"
    assert "mirror/recipes/abc-com.json" in _paths(store)
    assert ops_publisher.publish_mirror(store, "x", env, source=lambda: ({"abc-com": [_rows()]}, {})) == "unchanged"
    assert ops_publisher.publish_mirror(store, "x", env, source=lambda: ({}, {})) == "published"  # recipes gone: files go
    assert "mirror/recipes/abc-com.json" not in _paths(store)

    def boom():
        raise RuntimeError("db down")

    assert ops_publisher.publish_mirror(store, "x", env, source=boom).startswith("skipped:")
