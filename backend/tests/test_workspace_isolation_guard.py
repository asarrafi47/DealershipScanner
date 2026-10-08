"""The root conftest keeps tests out of the real workspace/ (P1B.1).

Three layers, each pinned here:

* ``DEALER_LOGS_ROOT`` is a session tmp dir, set before any backend import, so the
  import-time ``LOG_ROOT`` constants of the pipeline, the platform-candidates report,
  the discovery probe and the recipe validator never name workspace/dealer_logs.
* ``backend.scanner.recipes.RECIPES_DIR`` and ``vdp_recipes.VDP_RECIPES_DIR`` point at
  a fresh tmp dir per test (opt out with ``@pytest.mark.real_recipes_dir``).
* ``pytest_sessionstart`` / ``pytest_sessionfinish`` snapshot workspace/dealer_logs and
  workspace/recipes and fail the session when a path changed, unless a scanner or
  pipeline was live (then a warning) or ``WORKSPACE_GUARD=0``.

The session-level cases run pytest in a subprocess against a scratch ROOT that holds a
copy of the root conftest, so the guard under test watches the scratch workspace/ and
never the real tree.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

import conftest as root_conftest

REPO = Path(root_conftest.__file__).resolve().parent
SESSION_WS = Path(root_conftest.TEST_DEALER_LOGS_ROOT).parent


def _under(path: Path, parent: Path) -> bool:
    try:
        Path(path).resolve().relative_to(Path(parent).resolve())
        return True
    except ValueError:
        return False


# ── dealer-log root ──────────────────────────────────────────────────────────


def test_dealer_logs_root_env_is_the_session_tmp_dir():
    root = Path(os.environ["DEALER_LOGS_ROOT"])
    assert root == Path(root_conftest.TEST_DEALER_LOGS_ROOT)
    assert root.is_dir()
    assert not _under(root, REPO)


def test_import_time_log_roots_never_name_the_checkout():
    # In this session a module may first have been imported under a test's own
    # DEALER_LOGS_ROOT (still a tmp dir); the fresh-process equality is pinned by
    # test_guard_is_silent_when_tests_use_the_redirected_dirs below.
    from backend.scanner import recipe_validation
    from backend.scanner.pipeline import constants
    from backend.scripts import discovery_probe, platform_candidates

    for mod in (constants, platform_candidates, discovery_probe, recipe_validation):
        assert not _under(mod.LOG_ROOT, REPO), (mod.__name__, mod.LOG_ROOT)


# ── per-test recipe dirs ─────────────────────────────────────────────────────

_SEEN_RECIPE_DIRS: list[Path] = []


@pytest.mark.parametrize("n", [1, 2])
def test_recipe_dirs_are_fresh_tmp_dirs_per_test(n):
    from backend.scanner import recipes
    from backend.scanner.vdp import vdp_recipes

    rd = Path(recipes.RECIPES_DIR)
    assert _under(rd, root_conftest.TEST_RECIPES_ROOT) and not _under(rd, REPO)
    assert Path(vdp_recipes.VDP_RECIPES_DIR) == rd / "vdp"
    assert not rd.exists()  # created on demand by the writers
    assert rd not in _SEEN_RECIPE_DIRS
    _SEEN_RECIPE_DIRS.append(rd)


def test_recipe_writes_land_in_the_per_test_dir():
    from backend.scanner import recipes
    from backend.scanner.vdp import vdp_recipes

    vdp_recipes.save_vdp_recipes("guard-check-com", [])
    assert (Path(vdp_recipes.VDP_RECIPES_DIR) / "guard-check-com.json").is_file()
    assert recipes._recipe_path("guard-check-com").parent == Path(recipes.RECIPES_DIR)


def test_a_test_own_patch_wins(monkeypatch, tmp_path):
    from backend.scanner import recipes

    monkeypatch.setattr(recipes, "RECIPES_DIR", tmp_path / "mine")
    assert recipes._recipe_path("x-com") == tmp_path / "mine" / "x-com.json"


def test_a_value_set_before_the_fixture_is_left_alone(tmp_path):
    from backend.scanner import recipes
    from backend.scanner.vdp import vdp_recipes

    mp = pytest.MonkeyPatch()
    try:
        # Back to the import-time defaults, then a value a broader fixture would set.
        defaults = dict(root_conftest._PRISTINE_RECIPE_DIRS)
        mp.setattr(vdp_recipes, "VDP_RECIPES_DIR", defaults["backend.scanner.vdp.vdp_recipes.VDP_RECIPES_DIR"])
        mp.setattr(recipes, "RECIPES_DIR", tmp_path / "set-by-module-fixture")
        changed = root_conftest.redirect_recipe_dirs(mp)
        assert recipes.RECIPES_DIR == tmp_path / "set-by-module-fixture"
        assert list(changed) == ["backend.scanner.vdp.vdp_recipes.VDP_RECIPES_DIR"]
        assert _under(vdp_recipes.VDP_RECIPES_DIR, root_conftest.TEST_RECIPES_ROOT)
    finally:
        mp.undo()


@pytest.mark.real_recipes_dir
def test_marker_keeps_the_real_dirs():
    from backend.scanner import recipes
    from backend.scanner.vdp import vdp_recipes

    # Read-only: the opted-out test sees the module defaults, not a per-test tmp dir.
    for mod, attr in ((recipes, "RECIPES_DIR"), (vdp_recipes, "VDP_RECIPES_DIR")):
        assert not _under(getattr(mod, attr), root_conftest.TEST_RECIPES_ROOT), attr
        seen = root_conftest._PRISTINE_RECIPE_DIRS.get(f"{mod.__name__}.{attr}")
        assert seen is None or getattr(mod, attr) == seen


# ── snapshot and diff ────────────────────────────────────────────────────────


def _bump(p: Path, ns: int = 5_000_000_000) -> None:
    st = p.stat()
    os.utime(p, ns=(st.st_atime_ns, st.st_mtime_ns + ns))


def test_snapshot_diff_lists_added_removed_and_modified(tmp_path):
    ws = tmp_path / "workspace"
    (ws / "dealer_logs" / "_learning").mkdir(parents=True)
    (ws / "dealer_logs" / "old-com").mkdir()
    (ws / "recipes" / "vdp").mkdir(parents=True)
    idx = ws / "dealer_logs" / "_learning" / "errors_index.md"
    idx.write_text("# errors_index\n")
    (ws / "dealer_logs" / "old-com" / "scan_runs.md").write_text("x\n")
    (ws / "recipes" / "a-com.json").write_text("[]")
    (ws / "elsewhere.txt").write_text("not guarded")

    before = root_conftest.workspace_snapshot(tmp_path)
    assert "workspace/dealer_logs/_learning/errors_index.md" in before
    assert "workspace/recipes/vdp/" in before
    assert not any("elsewhere" in k for k in before)
    assert root_conftest.workspace_touched(before, root_conftest.workspace_snapshot(tmp_path)) == []

    with idx.open("a") as fh:
        fh.write("- leaked line\n")
    _bump(idx)
    (ws / "dealer_logs" / "new-com").mkdir()
    (ws / "dealer_logs" / "new-com" / "summary.md").write_text("x")
    (ws / "recipes" / "vdp" / "b-com.json").write_text("[]")
    (ws / "recipes" / "a-com.json").unlink()
    (ws / "elsewhere.txt").write_text("changed, but not guarded")

    touched = root_conftest.workspace_touched(before, root_conftest.workspace_snapshot(tmp_path))
    assert touched == [
        "modified workspace/dealer_logs/_learning/errors_index.md",
        "added    workspace/dealer_logs/new-com/",
        "added    workspace/dealer_logs/new-com/summary.md",
        "removed  workspace/recipes/a-com.json",
        "added    workspace/recipes/vdp/b-com.json",
    ]


def test_snapshot_of_a_missing_workspace_is_empty(tmp_path):
    assert root_conftest.workspace_snapshot(tmp_path) == {}


# ── live-writer probe ────────────────────────────────────────────────────────


def _no_processes(monkeypatch, scanner_pids=()):
    from backend.scripts import scanner_liveness

    monkeypatch.setattr(scanner_liveness, "scanner_pids", lambda: list(scanner_pids))
    monkeypatch.setattr(root_conftest, "_process_table", lambda: {})


def test_probe_is_quiet_without_writers(tmp_path, monkeypatch):
    _no_processes(monkeypatch)
    monkeypatch.setenv("SCANNER_LOCK_PATH", "")
    assert root_conftest.live_workspace_writers(tmp_path) == []


def test_probe_reports_scanner_pids_but_not_itself_or_pytest(tmp_path, monkeypatch):
    from backend.scripts import scanner_liveness

    monkeypatch.setenv("SCANNER_LOCK_PATH", "")
    monkeypatch.setattr(scanner_liveness, "scanner_pids", lambda: [424242, os.getpid(), 515151])
    monkeypatch.setattr(root_conftest, "_process_table",
                        lambda: {515151: "python -m pytest backend/tests/test_scanner.py"})
    assert root_conftest.live_workspace_writers(tmp_path) == ["scanner pid 424242"]


def test_probe_reports_pipeline_processes(tmp_path, monkeypatch):
    from backend.scripts import scanner_liveness

    monkeypatch.setenv("SCANNER_LOCK_PATH", "")
    monkeypatch.setattr(scanner_liveness, "scanner_pids", lambda: [])
    monkeypatch.setattr(root_conftest, "_process_table", lambda: {
        111: "python -m backend.scripts.dealer_pipeline --dealers a,b",
        222: "python -m pytest backend/tests/test_dealer_pipeline_surface.py",
        333: "/usr/bin/python3 backend/scripts/discovery_probe.py --dealer x",
        444: "grep -rn dealer_pipeline docs",
    })
    got = root_conftest.live_workspace_writers(tmp_path)
    assert [g.split(" (")[0] for g in got] == ["pipeline pid 111", "pipeline pid 333"]


def test_probe_reports_a_live_lock_and_ignores_a_dead_one(tmp_path, monkeypatch):
    _no_processes(monkeypatch)
    monkeypatch.setenv("SCANNER_LOCK_PATH", "")
    ws = tmp_path / "workspace"
    (ws / "pipeline" / "fleet_20261008T000000Z").mkdir(parents=True)
    dead = subprocess.Popen([sys.executable, "-c", "pass"])
    dead.wait()
    (ws / "scanner.lock").write_text(str(dead.pid))
    assert root_conftest.live_workspace_writers(tmp_path) == []

    live = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
    try:
        shard = ws / "pipeline" / "fleet_20261008T000000Z" / "s0.lock"
        shard.write_text(str(live.pid))
        assert root_conftest.live_workspace_writers(tmp_path) == [
            f"scanner lock {shard} held by live pid {live.pid}"]
        # SCANNER_LOCK_PATH (relative to the guarded root) is checked too.
        shard.unlink()
        monkeypatch.setenv("SCANNER_LOCK_PATH", "locks/shard7.lock")
        (tmp_path / "locks").mkdir()
        (tmp_path / "locks" / "shard7.lock").write_text(str(live.pid))
        assert root_conftest.live_workspace_writers(tmp_path) == [
            f"scanner lock {tmp_path / 'locks' / 'shard7.lock'} held by live pid {live.pid}"]
    finally:
        live.kill()
        live.wait()


def test_probe_ignores_a_lock_this_process_holds(tmp_path, monkeypatch):
    _no_processes(monkeypatch)
    monkeypatch.setenv("SCANNER_LOCK_PATH", "")
    (tmp_path / "workspace").mkdir()
    (tmp_path / "workspace" / "scanner.lock").write_text(str(os.getpid()))
    assert root_conftest.live_workspace_writers(tmp_path) == []


# ── the guard, end to end, against a scratch ROOT ────────────────────────────

# Puts the probe in a known state from inside the scratch session: the real machine may
# be running a scanner, which would downgrade the failure this file means to prove.
_QUIET_PROBE = """
import conftest
from backend.scripts import scanner_liveness
conftest._WORKSPACE_GUARD["live_at_start"] = []
conftest._process_table = lambda: {}
scanner_liveness.scanner_pids = lambda: []
"""

_LEAK = """
from pathlib import Path
ROOT = Path(__file__).resolve().parent

def test_leaks():
    with (ROOT / "workspace/dealer_logs/_learning/errors_index.md").open("a") as fh:
        fh.write("- 2026-09-28 12:00 UTC leaked -> lifecycle-dealer-com\\n")
    (ROOT / "workspace/dealer_logs/leaky-com").mkdir()
    (ROOT / "workspace/dealer_logs/leaky-com/scan_runs.md").write_text("leak\\n")
    (ROOT / "workspace/recipes/vdp/leaky-com.json").write_text("[]")
"""

_LEAKED = (
    "modified workspace/dealer_logs/_learning/errors_index.md",
    "added    workspace/dealer_logs/leaky-com/",
    "added    workspace/dealer_logs/leaky-com/scan_runs.md",
    "added    workspace/recipes/vdp/leaky-com.json",
)


def _scratch_session(tmp_path: Path, body: str, **env_extra: str) -> tuple[subprocess.CompletedProcess, Path]:
    root = tmp_path / "root"
    (root / "workspace" / "dealer_logs" / "_learning").mkdir(parents=True)
    (root / "workspace" / "recipes" / "vdp").mkdir(parents=True)
    idx = root / "workspace" / "dealer_logs" / "_learning" / "errors_index.md"
    idx.write_text("# errors_index\n")
    # Back-date the seeded files so an append inside the same mtime tick still shows.
    for p in (idx, root / "workspace" / "dealer_logs" / "_learning"):
        os.utime(p, (1_700_000_000, 1_700_000_000))
    (root / "workspace" / "recipes" / "kept-com.json").write_text("[]")
    shutil.copyfile(REPO / "conftest.py", root / "conftest.py")
    (root / "pytest.ini").write_text("[pytest]\n")
    (root / "test_scratch.py").write_text(textwrap.dedent(body))

    env = {k: v for k, v in os.environ.items()
           if k not in ("WORKSPACE_GUARD", "PYTEST_ADDOPTS", "PYTEST_CURRENT_TEST", "PYTHONPATH")}
    env["PYTHONPATH"] = str(REPO)  # `import backend` resolves to this checkout, read-only
    env["SCANNER_LOCK_PATH"] = ""
    env.update(env_extra)
    proc = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", "test_scratch.py"],
        cwd=root, env=env, capture_output=True, text=True, timeout=110,
    )
    return proc, root


def test_guard_fails_the_session_on_a_deliberate_write(tmp_path):
    proc, _ = _scratch_session(tmp_path, _QUIET_PROBE + _LEAK)
    out = proc.stdout + proc.stderr
    assert "1 passed" in out, out
    assert proc.returncode == 1, out
    assert "WORKSPACE GUARD: this test session wrote 4 path(s)" in out, out
    assert "session marked FAILED" in out
    for row in _LEAKED:
        assert row in out, (row, out)
    assert "kept-com" not in out


def test_guard_is_silent_when_tests_use_the_redirected_dirs(tmp_path):
    body = _QUIET_PROBE + """
import os
from pathlib import Path

def test_writes_only_where_conftest_points():
    from backend.scanner import recipe_validation, recipes
    from backend.scanner.pipeline import constants, dealer_logs
    from backend.scanner.vdp import vdp_recipes
    from backend.scripts import discovery_probe, platform_candidates

    root = Path(__file__).resolve().parent
    logs = Path(os.environ["DEALER_LOGS_ROOT"])
    assert logs == Path(conftest.TEST_DEALER_LOGS_ROOT)
    for mod in (constants, platform_candidates, discovery_probe, recipe_validation):
        assert mod.LOG_ROOT == logs, mod.__name__
    assert root not in logs.parents
    assert root not in Path(recipes.RECIPES_DIR).resolve().parents
    dealer_logs._log_append("clean-com", "scan_runs.md", "a run")
    vdp_recipes.save_vdp_recipes("clean-com", [])
    assert (constants.LOG_ROOT / "clean-com" / "scan_runs.md").is_file()
"""
    proc, _ = _scratch_session(tmp_path, body)
    out = proc.stdout + proc.stderr
    assert proc.returncode == 0, out
    assert "1 passed" in out, out
    assert "WORKSPACE GUARD" not in out, out


def test_live_scanner_pid_downgrades_to_a_warning(tmp_path):
    body = _QUIET_PROBE + "scanner_liveness.scanner_pids = lambda: [424242]\n" + _LEAK
    proc, _ = _scratch_session(tmp_path, body)
    out = proc.stdout + proc.stderr
    assert proc.returncode == 0, out
    assert "WORKSPACE GUARD (warning only; live writers: scanner pid 424242)" in out, out
    assert "session marked FAILED" not in out
    for row in _LEAKED:
        assert row in out, (row, out)


def test_live_scanner_lock_downgrades_to_a_warning(tmp_path):
    proc, root = _scratch_session(tmp_path, _QUIET_PROBE + _LEAK + """
def test_lock_is_live():
    # this outer test process stands in for the scanner holding the lock
    (ROOT / "workspace/scanner.lock").write_text("%d")
""" % os.getpid())
    out = proc.stdout + proc.stderr
    assert proc.returncode == 0, out
    assert f"scanner lock {root / 'workspace' / 'scanner.lock'} held by live pid {os.getpid()}" in out, out
    assert "session marked FAILED" not in out


def test_workspace_guard_0_switches_it_off(tmp_path):
    proc, _ = _scratch_session(tmp_path, _QUIET_PROBE + _LEAK, WORKSPACE_GUARD="0")
    out = proc.stdout + proc.stderr
    assert proc.returncode == 0, out
    assert "1 passed" in out, out
    assert "WORKSPACE GUARD" not in out, out


def test_guard_sees_a_write_from_session_fixture_teardown(tmp_path):
    body = _QUIET_PROBE + """
import pytest
from pathlib import Path
ROOT = Path(__file__).resolve().parent

@pytest.fixture(scope="session")
def late_writer():
    yield
    (ROOT / "workspace/recipes/late-com.json").write_text("[]")

def test_uses_it(late_writer):
    pass
"""
    proc, _ = _scratch_session(tmp_path, body)
    out = proc.stdout + proc.stderr
    assert proc.returncode == 1, out
    assert "added    workspace/recipes/late-com.json" in out, out
