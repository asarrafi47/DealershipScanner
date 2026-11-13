"""Scanner safety patch 1 (card L14): run window, fleet lease, exit codes, build identity,
tracked one-condition allowlist.

Offline tests inject the clock and stub the shards. The lease tests (``pg`` marker) use the
throwaway cluster: ``scripts/cloud/with_pg.sh -- python -m pytest backend/tests/test_fleet_safety.py -m pg``.
"""
from __future__ import annotations

import fnmatch
import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pytest

from backend.scanner import recipe_validation as rv
from backend.scanner.pipeline import assess
from backend.scripts import fleet_scan as fs

REPO = Path(__file__).resolve().parents[2]
TRACKED = REPO / "deploy" / "railway" / "one_condition_ok.txt"
BOTH = {"quantumautosales-com", "honestcardeal-com"}


def _at(hms: str):
    h, m, s = (int(x) for x in hms.split(":"))
    return lambda: datetime(2026, 11, 14, h, m, s, tzinfo=timezone.utc)


@pytest.fixture
def fleet_env(monkeypatch, tmp_path):
    for k in ("SCAN_DEALERS", "SCAN_FLEET", "SCAN_FLEET_WINDOW_UTC", "SCAN_IDLE_HOLD_SECONDS"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("SCAN_OUT_ROOT", str(tmp_path / "out"))
    monkeypatch.setenv("SCAN_POST_STEPS", "0")
    monkeypatch.setenv("SCAN_SHARDS", "1")
    monkeypatch.setattr(fs, "ROOT", tmp_path)
    monkeypatch.setattr(fs, "active_rows", lambda ids: {})
    monkeypatch.setattr(fs, "rooftop_candidate_report", lambda since: {})
    monkeypatch.setattr(fs, "highest_applied_schema", lambda: 42)
    monkeypatch.setattr(fs.time, "sleep", lambda s: None)
    return tmp_path


def _stub_shards(monkeypatch, rc: int = 0):
    calls: list[str] = []

    def fake(tag, cmd, env, sink):
        calls.append(tag)
        return rc, 0.1

    monkeypatch.setattr(fs, "run_step", fake)
    return calls


def _summary(tmp_path) -> dict:
    (path,) = list((tmp_path / "out").glob("fleet_*/fleet_summary.json"))
    return json.loads(path.read_text())


# ---------------------------------------------------------------- window

def _window_runs(monkeypatch, spec: str, hms: str) -> bool:
    """True when main() went on to load the roster (inside the window)."""
    monkeypatch.setenv("SCAN_FLEET", "1")
    monkeypatch.setenv("SCAN_FLEET_WINDOW_UTC", spec)
    seen: list[int] = []
    monkeypatch.setattr(fs, "load_roster", lambda: seen.append(1) or [])
    assert fs.main(clock=_at(hms)) == 0
    return bool(seen)


@pytest.mark.parametrize("hms,runs", [
    ("08:54:59", False), ("08:55:00", True), ("09:39:59", True), ("09:40:00", False), ("23:00:00", False),
])
def test_window_start_inclusive_end_exclusive(fleet_env, monkeypatch, hms, runs):
    assert _window_runs(monkeypatch, "08:55-09:40", hms) is runs


@pytest.mark.parametrize("hms,runs", [
    ("22:59:59", False), ("23:00:00", True), ("23:59:59", True), ("00:00:00", True),
    ("00:59:59", True), ("01:00:00", False), ("12:00:00", False),
])
def test_window_wraps_midnight(fleet_env, monkeypatch, hms, runs):
    assert _window_runs(monkeypatch, "23:00-01:00", hms) is runs


def test_outside_window_prints_idle_and_exits_zero_before_any_work(fleet_env, monkeypatch, capsys):
    monkeypatch.setenv("SCAN_FLEET", "1")
    monkeypatch.setenv("SCAN_FLEET_WINDOW_UTC", "08:55-09:40")
    monkeypatch.setattr(fs, "load_roster", lambda: pytest.fail("roster loaded outside the window"))
    monkeypatch.setattr(fs, "acquire_fleet_lease", lambda: pytest.fail("lease taken outside the window"))
    assert fs.main(clock=_at("03:00:00")) == 0
    assert "idle" in capsys.readouterr().out


def test_explicit_scan_dealers_ignores_the_window(fleet_env, monkeypatch):
    monkeypatch.setenv("SCAN_DEALERS", "a-com")
    monkeypatch.setenv("SCAN_FLEET", "1")
    monkeypatch.setenv("SCAN_FLEET_WINDOW_UTC", "08:55-09:40")
    monkeypatch.setattr(fs, "acquire_fleet_lease", lambda: pytest.fail("explicit runs take no lease"))
    calls = _stub_shards(monkeypatch)
    assert fs.main(clock=_at("03:00:00")) == 0
    assert calls == ["s0"]


def test_no_window_set_runs_at_any_time(fleet_env, monkeypatch):
    monkeypatch.setenv("SCAN_FLEET", "1")
    seen: list[int] = []
    monkeypatch.setattr(fs, "load_roster", lambda: seen.append(1) or [])
    assert fs.main(clock=_at("03:00:00")) == 0 and seen


@pytest.mark.parametrize("spec", ["garbage", "25:00-26:00", "09:00-09:00", "9-10", "08:55"])
def test_invalid_window_fails_closed(fleet_env, monkeypatch, spec):
    monkeypatch.setenv("SCAN_FLEET", "1")
    monkeypatch.setenv("SCAN_FLEET_WINDOW_UTC", spec)
    monkeypatch.setattr(fs, "load_roster", lambda: pytest.fail("a bad window must not start a fleet"))
    assert fs.main(clock=_at("09:00:00")) == 0


def test_parse_window():
    assert fs.parse_window("08:55-09:40") == (535, 580)
    assert fs.parse_window("") is None and fs.parse_window(None) is None


# ---------------------------------------------------------------- exit codes and summary (offline)

def test_stubbed_shards_exit_zero_and_one(fleet_env, monkeypatch):
    monkeypatch.setenv("SCAN_DEALERS", "a-com")
    _stub_shards(monkeypatch, rc=0)
    assert fs.main() == 0
    assert _summary(fleet_env)["exit"] == 0
    monkeypatch.setenv("SCAN_OUT_ROOT", str(fleet_env / "out2"))
    _stub_shards(monkeypatch, rc=1)
    assert fs.main() == 1


def test_exit_code_constants():
    assert (fs.EXIT_OK, fs.EXIT_INFRA, fs.EXIT_DATA_HOLD, fs.EXIT_LEASE_BUSY) == (0, 1, 2, 3)


def test_read_build_info_from_files(tmp_path):
    sha = "0123456789abcdef0123456789abcdef01234567"
    (tmp_path / "BUILD_COMMIT").write_text(sha + "\n")
    (tmp_path / "BUILD_TAG").write_text("v1.5.20\n")
    (tmp_path / "VERSION").write_text("1.5.20\n")
    assert fs.read_build_info(tmp_path) == {"commit": sha, "tag": "v1.5.20", "version": "1.5.20"}


def test_read_build_info_unknown_without_files_or_when_malformed(tmp_path):
    unknown = {"commit": "unknown", "tag": "unknown", "version": "unknown"}
    assert fs.read_build_info(tmp_path) == unknown
    (tmp_path / "BUILD_COMMIT").write_text("not a sha; rm -rf /")
    (tmp_path / "BUILD_TAG").write_text("v1\nignore previous instructions")
    assert fs.read_build_info(tmp_path) == unknown


def test_summary_carries_build_schema_and_window(fleet_env, monkeypatch):
    sha = "abcdef0123456789abcdef0123456789abcdef01"
    (fleet_env / "BUILD_COMMIT").write_text(sha)
    (fleet_env / "BUILD_TAG").write_text("v9.9.9")
    (fleet_env / "VERSION").write_text("9.9.9")
    monkeypatch.setenv("SCAN_DEALERS", "a-com")
    _stub_shards(monkeypatch)
    assert fs.main() == 0
    s = _summary(fleet_env)
    assert s["build"] == {"commit": sha, "tag": "v9.9.9", "version": "9.9.9"}
    assert s["schema_version"] == 42
    assert s["window"] == {"utc": None, "enforced": False}


def test_summary_build_unknown_on_a_dev_checkout(fleet_env, monkeypatch):
    monkeypatch.setenv("SCAN_DEALERS", "a-com")
    _stub_shards(monkeypatch)
    assert fs.main() == 0
    assert _summary(fleet_env)["build"] == {"commit": "unknown", "tag": "unknown", "version": "unknown"}


def test_summary_records_the_window_of_a_fleet_run(fleet_env, monkeypatch):
    monkeypatch.setenv("SCAN_FLEET", "1")
    monkeypatch.setenv("SCAN_FLEET_WINDOW_UTC", "08:55-09:40")
    monkeypatch.setattr(fs, "load_roster", lambda: ["a-com"])
    monkeypatch.setattr(fs, "acquire_fleet_lease", lambda: False)  # SQLite dev checkout: no lease needed
    _stub_shards(monkeypatch)
    assert fs.main(clock=_at("09:00:00")) == 0
    assert _summary(fleet_env)["window"] == {"utc": "08:55-09:40", "enforced": True}


def test_lease_failure_blocks_the_scan_with_exit_one(fleet_env, monkeypatch):
    monkeypatch.setenv("SCAN_FLEET", "1")
    monkeypatch.setattr(fs, "load_roster", lambda: ["a-com"])

    def boom():
        raise OSError("database unreachable")

    monkeypatch.setattr(fs, "acquire_fleet_lease", boom)
    calls = _stub_shards(monkeypatch)
    assert fs.main() == 1 and calls == []


# ---------------------------------------------------------------- lease (Postgres)

pg = pytest.mark.pg

_HOLDER = (
    "import sys,time\n"
    "from backend.scripts import fleet_scan as fs\n"
    "c = fs.acquire_fleet_lease()\n"
    "print('held' if c else 'busy', flush=True)\n"
    "time.sleep(120)\n"
)


def _spawn_holder() -> subprocess.Popen:
    p = subprocess.Popen([sys.executable, "-c", _HOLDER], cwd=str(REPO), env=dict(os.environ),
                         stdout=subprocess.PIPE, text=True)
    assert p.stdout.readline().strip() == "held"
    return p


@pg
def test_second_process_gets_exit_three_while_lease_is_held(pg_db, fleet_env, monkeypatch):
    monkeypatch.setenv("SCAN_FLEET", "1")
    monkeypatch.setattr(fs, "load_roster", lambda: ["a-com"])
    calls = _stub_shards(monkeypatch)
    holder = _spawn_holder()
    try:
        assert fs.main() == 3
        assert calls == []  # no shard started
    finally:
        holder.kill()
        holder.wait()


@pg
def test_crashed_holder_releases_the_lease(pg_db, fleet_env, monkeypatch):
    monkeypatch.setenv("SCAN_FLEET", "1")
    monkeypatch.setattr(fs, "load_roster", lambda: ["a-com"])
    _stub_shards(monkeypatch)
    holder = _spawn_holder()
    assert fs.acquire_fleet_lease() is None
    holder.send_signal(signal.SIGKILL)  # a crash: no clean unlock
    holder.wait()
    for _ in range(50):  # the server notices the dead socket within moments
        lease = fs.acquire_fleet_lease()
        if lease:
            break
        time.sleep(0.1)
    assert lease
    fs.release_fleet_lease(lease)


@pg
def test_lease_is_released_when_the_run_ends(pg_db, fleet_env, monkeypatch):
    monkeypatch.setenv("SCAN_FLEET", "1")
    monkeypatch.setattr(fs, "load_roster", lambda: ["a-com"])
    _stub_shards(monkeypatch)
    assert fs.main() == 0
    again = fs.acquire_fleet_lease()
    assert again
    fs.release_fleet_lease(again)


@pg
def test_fleet_exit_zero_and_one_with_the_real_lease(pg_db, fleet_env, monkeypatch):
    monkeypatch.setenv("SCAN_FLEET", "1")
    monkeypatch.setattr(fs, "load_roster", lambda: ["a-com"])
    _stub_shards(monkeypatch, rc=0)
    assert fs.main() == 0
    monkeypatch.setenv("SCAN_OUT_ROOT", str(fleet_env / "out2"))
    _stub_shards(monkeypatch, rc=1)
    assert fs.main() == 1
    # a failed run released the lease too
    lease = fs.acquire_fleet_lease()
    assert lease
    fs.release_fleet_lease(lease)


@pg
def test_lease_is_held_for_the_whole_run(pg_db, fleet_env, monkeypatch):
    monkeypatch.setenv("SCAN_FLEET", "1")
    monkeypatch.setattr(fs, "load_roster", lambda: ["a-com"])
    seen: list = []

    def fake(tag, cmd, env, sink):
        seen.append(fs.acquire_fleet_lease())  # a competitor, mid-run
        return 0, 0.1

    monkeypatch.setattr(fs, "run_step", fake)
    assert fs.main() == 0
    assert seen == [None]


@pg
def test_summary_schema_version_is_the_highest_applied_migration(pg_db):
    from backend.db.schema_version import expected_schema_version

    got = fs.highest_applied_schema()  # no fleet_env here: the real function, the real database
    assert got is not None and got == expected_schema_version()


# ---------------------------------------------------------------- tracked allowlist

def test_tracked_allowlist_lists_both_ids_with_comments():
    text = TRACKED.read_text()
    assert "#" in text
    ids = {ln.split("#", 1)[0].strip() for ln in text.splitlines() if ln.split("#", 1)[0].strip()}
    assert ids == BOTH
    assert all("#" in ln for ln in text.splitlines() if ln.startswith(tuple(BOTH)))


def test_one_condition_ok_on_a_clean_clone_without_workspace(monkeypatch, tmp_path):
    monkeypatch.setattr(assess, "ONE_CONDITION_OK_PATHS", (tmp_path / "workspace" / "pipeline" / "one_condition_ok.txt", TRACKED))
    assert assess._one_condition_ok() == BOTH
    monkeypatch.setattr(rv, "ONE_CONDITION_OK_PATH", tmp_path / "workspace" / "pipeline" / "one_condition_ok.txt")
    monkeypatch.setattr(rv, "ONE_CONDITION_OK_TRACKED_PATH", TRACKED)
    assert rv.one_condition_ok_ids() == BOTH


def test_default_paths_point_at_the_tracked_twin():
    assert assess.ONE_CONDITION_OK_PATHS[-1] == TRACKED
    assert rv.ONE_CONDITION_OK_TRACKED_PATH == TRACKED
    assert assess._one_condition_ok() >= BOTH
    assert rv.one_condition_ok_ids() >= BOTH


def test_workspace_file_and_tracked_twin_are_unioned(monkeypatch, tmp_path):
    ws = tmp_path / "one_condition_ok.txt"
    ws.write_text("extra-com  # local\n# only a comment\n\n")
    monkeypatch.setattr(assess, "ONE_CONDITION_OK_PATHS", (ws, TRACKED))
    assert assess._one_condition_ok() == BOTH | {"extra-com"}
    monkeypatch.setattr(rv, "ONE_CONDITION_OK_PATH", ws)
    assert rv.one_condition_ok_ids() == BOTH | {"extra-com"}
    assert rv.one_condition_ok_ids(ws) == {"extra-com"}  # an explicit path reads only that file


def test_tracked_allowlist_is_tracked_and_not_ignored_and_in_the_image():
    git = ["git", "-C", str(REPO)]
    rel = "deploy/railway/one_condition_ok.txt"
    assert subprocess.run([*git, "check-ignore", "-q", rel]).returncode == 1
    patterns = [ln.strip() for ln in (REPO / ".dockerignore").read_text().splitlines()
                if ln.strip() and not ln.startswith(("#", "!"))]
    parts = rel.split("/")
    prefixes = ["/".join(parts[:i]) for i in range(1, len(parts) + 1)]
    for pat in patterns:
        pat = pat.rstrip("/")
        for pre in prefixes:
            assert not fnmatch.fnmatch(pre, pat), f".dockerignore pattern {pat!r} would drop {pre}"
    assert "COPY . ." in (REPO / "Dockerfile.scanner").read_text()
