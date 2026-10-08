"""Root pytest configuration — ensures ``backend/`` is on sys.path for bare-import modules."""
import atexit
import itertools
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

# Dev/test may use plain SQLite for users.db; production forbids this (SEC-088).
os.environ.setdefault("ALLOW_UNENCRYPTED_USER_DB", "1")

# --- Hermetic databases ------------------------------------------------------------
#
# This runs at conftest import, i.e. before collection imports any test module.
# Nine test modules import ``backend.main`` at module top, and main.py loads .env
# and runs init_* on whatever databases the environment names, so per-test
# fixtures were too late: collection alone opened the developer's real users.db /
# dev_users.db and the local Postgres inventory, and test_compare_and_stats wrote
# rows into the real users.db (audit 2026-10-01, tests.md F1). Every database the
# code reads from the environment is pinned here to "" (Postgres URLs: off, tests
# are SQLite-only) or to a file in a per-session tmp dir, and loading .env is
# switched off for the whole session (``PROJECT_DOTENV_DISABLE``, honoured by
# ``backend.utils.project_env.load_project_dotenv``), so ``importlib.reload(main)``
# can no longer refill a blanked key or override a value a test set. These are
# assignments, not setdefault: a shell-exported URL must not leak in either.
# INVENTORY_DB_PATH is pinned just below to a session copy of the dev inventory.
_HERMETIC_DB_DIR = tempfile.mkdtemp(prefix="pytest-dbs-")
atexit.register(shutil.rmtree, _HERMETIC_DB_DIR, True)

HERMETIC_DB_ENV: dict = {
    "INVENTORY_DATABASE_URL": "",
    "DATABASE_URL": "",
    "PGVECTOR_URL": "",
    "USERS_DB_PATH": os.path.join(_HERMETIC_DB_DIR, "users.db"),
    "DEV_USERS_DB_PATH": os.path.join(_HERMETIC_DB_DIR, "dev_users.db"),
    "DEALER_PORTAL_DB_PATH": os.path.join(_HERMETIC_DB_DIR, "dealer_portal.db"),
    "INCOMPLETE_LISTINGS_DB_PATH": os.path.join(_HERMETIC_DB_DIR, "incomplete_listings.db"),
    "SCAN_LAB_INVENTORY_DB_PATH": os.path.join(_HERMETIC_DB_DIR, "scan_lab_inventory.db"),
}
os.environ.update(HERMETIC_DB_ENV)
os.environ["INVENTORY_SQLITE_TESTS"] = "1"
os.environ["PROJECT_DOTENV_DISABLE"] = "1"

# --- Hermetic default inventory ------------------------------------------------------
#
# The default SQLite inventory used to be the developer's own ``backend/inventory.db``
# (``_default_inventory_db_path()``): every test without its own DB fixture read AND
# wrote it, so results depended on what earlier runs had left there (tests.md F6).
# The session now gets a private copy: the dev file is copied once, read-only, into
# the session tmp dir when it exists (tests that need its rows, e.g. car 3608, still
# find them and skip when absent); otherwise the copy starts empty and
# ``_ensure_default_inventory_schema`` below builds the schema in it. The env var is
# set here, at conftest import, because collection-time ``import backend.main`` runs
# ``init_inventory_db()`` on ``base_repo.DB_PATH``, which reads INVENTORY_DB_PATH.
_REAL_DEFAULT_INVENTORY_DB = Path(__file__).resolve().parent / "backend" / "inventory.db"
TEST_INVENTORY_DB_PATH = os.path.join(_HERMETIC_DB_DIR, "inventory.db")


def _copy_default_inventory_db(src: Path, dst: str) -> None:
    """Snapshot *src* into *dst* through a read-only connection (WAL content included)."""
    import sqlite3

    if not src.is_file():
        return
    try:
        source = sqlite3.connect(f"file:{src}?mode=ro", uri=True)
    except sqlite3.Error:
        return
    try:
        target = sqlite3.connect(dst)
        try:
            source.backup(target)
        finally:
            target.close()
    except sqlite3.Error:
        # Unreadable dev file: an empty session copy, built by the schema fixture.
        if os.path.exists(dst):
            os.remove(dst)
    finally:
        source.close()


_copy_default_inventory_db(_REAL_DEFAULT_INVENTORY_DB, TEST_INVENTORY_DB_PATH)
os.environ["INVENTORY_DB_PATH"] = TEST_INVENTORY_DB_PATH

# --- Hermetic workspace/ (dealer logs, recipe caches) ----------------------------------
#
# Tests wrote the real workspace/: lines for lifecycle-dealer-com, bravo-norows-com,
# charlie-norecipe-com, broken-com and a fake avondaletoyota-com cap_hit landed in
# workspace/dealer_logs/_learning/errors_index.md (P1C.1). The dealer-log root is a
# constant computed at import time in four modules (pipeline/constants.py,
# scripts/platform_candidates.py, scripts/discovery_probe.py, scanner/recipe_validation.py),
# and a per-test patch only reaches the modules already imported when it runs. So
# DEALER_LOGS_ROOT is pinned here, before any backend import, the same way as the
# databases above: every one of those constants, and every subprocess a test starts,
# resolves to a session tmp dir. Assignment, not setdefault: a shell-exported
# DEALER_LOGS_ROOT pointing at a real tree must not leak in either. The recipe caches
# get a per-test dir from ``_isolate_recipe_dirs`` below, and ``pytest_sessionstart`` /
# ``pytest_sessionfinish`` fail the run when anything still wrote the real trees.
_HERMETIC_WORKSPACE_DIR = tempfile.mkdtemp(prefix="pytest-workspace-")
atexit.register(shutil.rmtree, _HERMETIC_WORKSPACE_DIR, True)
TEST_DEALER_LOGS_ROOT = os.path.join(_HERMETIC_WORKSPACE_DIR, "dealer_logs")
TEST_RECIPES_ROOT = Path(_HERMETIC_WORKSPACE_DIR) / "recipes"
os.makedirs(TEST_DEALER_LOGS_ROOT, exist_ok=True)
os.environ["DEALER_LOGS_ROOT"] = TEST_DEALER_LOGS_ROOT

PROD_TEST_USERS_DB_KEY = "pytest-users-db-encryption-key-32chars!"
PROD_TEST_DEV_USERS_DB_KEY = "pytest-dev-users-db-enc-key-32c!"


def apply_production_credential_encryption_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Wire SQLCipher keys for tests that import backend.main with FLASK_ENV=production."""
    # setenv("") not delenv: reload(main) re-runs load_project_dotenv(override=False),
    # which re-populates a deleted var from .env; an empty value survives and is falsy.
    monkeypatch.setenv("ALLOW_UNENCRYPTED_USER_DB", "")
    monkeypatch.setenv("USERS_DB_ENCRYPTION_KEY", PROD_TEST_USERS_DB_KEY)
    monkeypatch.setenv("DEV_USERS_DB_ENCRYPTION_KEY", PROD_TEST_DEV_USERS_DB_KEY)

# backend/ must be on sys.path so bare imports like ``from scraping.xxx`` and
# ``from intelligence.llm.xxx`` resolve to the correct packages.
_backend = str(Path(__file__).resolve().parent / "backend")
if _backend not in sys.path:
    sys.path.insert(0, _backend)


# --- Test-tier markers (see docs in pytest.ini; adapted from zumai's marker system) ---
#
# Modules listed here (bare basename, no package prefix) are auto-tagged with the
# corresponding marker at collection time. Prefix rules can be added to the tuples.
# Both sets start empty on purpose: no existing module is force-tagged — decorate
# individual tests with ``@pytest.mark.integration`` / ``@pytest.mark.regression``
# or add module basenames/prefixes here as tiers are classified.
#
# CI runs two jobs: ``-m "not integration"`` (offline) and ``-m integration``
# (pytest-integration in ci.yml). Tag integration tests ONLY with the
# stack_up self-skip pattern below, so they pass on a bare runner.
REGRESSION_MODULES: frozenset = frozenset()
REGRESSION_PREFIXES: tuple = ()

INTEGRATION_MODULES: frozenset = frozenset()
INTEGRATION_PREFIXES: tuple = ()


def pytest_collection_modifyitems(session, config, items):
    """Auto-tag tier markers by module-name convention (``pytest -m regression`` etc.)."""
    for item in items:
        mod = item.module.__name__.rsplit(".", 1)[-1]
        if mod in REGRESSION_MODULES or (REGRESSION_PREFIXES and mod.startswith(REGRESSION_PREFIXES)):
            item.add_marker(pytest.mark.regression)
        if mod in INTEGRATION_MODULES or (INTEGRATION_PREFIXES and mod.startswith(INTEGRATION_PREFIXES)):
            item.add_marker(pytest.mark.integration)


# --- Live-stack helpers for integration tests -------------------------------------
#
# Integration tests (marked ``@pytest.mark.integration``) should take the
# ``live_api_base`` and ``stack_up`` fixtures and self-skip when the stack is down:
#
#     @pytest.mark.integration
#     def test_live_health(live_api_base, stack_up):
#         if not stack_up:
#             pytest.skip("live stack not running")
#         ...
#
# The health probe runs at most once per session and costs ~2s when the stack is down.

def _live_base() -> str:
    return os.environ.get("DEALERSHIP_API_BASE", "http://localhost:5001").rstrip("/")


def _stack_running(base: str) -> bool:
    try:
        import requests

        r = requests.get(f"{base}/health", timeout=2.0)
        return r.status_code == 200
    except Exception:
        return False


@pytest.fixture(scope="session")
def live_api_base() -> str:
    return _live_base()


@pytest.fixture(scope="session")
def stack_up(live_api_base: str) -> bool:
    return _stack_running(live_api_base)


@pytest.fixture(scope="session", autouse=True)
def _ensure_default_inventory_schema() -> None:
    """
    Fresh clones ship no dev ``inventory.db``; tests that read the default
    sqlite path (filter options, listings perf/etag, public counts) need the
    schema to exist — empty tables are fine, a missing ``cars`` table is not.
    """
    mp = pytest.MonkeyPatch()
    # Tests run against sqlite (the per-test fixture clears the Postgres URL);
    # blank it here too so this init targets the same sqlite file, not prod.
    mp.setenv("INVENTORY_DATABASE_URL", "")
    mp.setenv("INVENTORY_SQLITE_TESTS", "1")
    try:
        import backend.db.inventory_db as inv_db

        # The scan-lab inventory now lives in the session tmp dir (HERMETIC_DB_ENV);
        # give it the same empty schema so dev scan-lab listings read zero cars.
        inv_db.DB_PATH = HERMETIC_DB_ENV["SCAN_LAB_INVENTORY_DB_PATH"]
        inv_db.init_inventory_db()

        inv_db.DB_PATH = TEST_INVENTORY_DB_PATH
        inv_db.init_inventory_db()
        # The dealer-portal sidecar now lives in the session tmp dir (HERMETIC_DB_ENV);
        # account deletion runs DELETE FROM dealer_vehicles, so the table must exist.
        from backend.db import dealer_portal_db

        dealer_portal_db.init_dealer_portal_db()
    finally:
        mp.undo()


@pytest.fixture(autouse=True)
def _isolate_listings_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Prevent ``FLASK_ENV=production`` and strict listings flags leaking across tests."""
    monkeypatch.delenv("FLASK_ENV", raising=False)
    monkeypatch.delenv("LISTINGS_INCLUDE_INCOMPLETE_CARS", raising=False)
    # Unset, not pinned: query_parser prefers the env var over inventory_db.DB_PATH,
    # so a pinned value would outrank a test's own DB_PATH patch. DB_PATH itself is
    # pointed at the session copy below, never at the developer's backend/inventory.db.
    monkeypatch.delenv("INVENTORY_DB_PATH", raising=False)
    # Tests are SQLite-only. The session fixture above assumed this fixture
    # blanked the Postgres URL; it never did, so every test that called
    # get_conn() without its own DB fixture ran against the .env database
    # (2026-09-28: chunk-m tests were reading production nhtsa_vpic_cache; on
    # 2026-07-04 a test wrote to prod the same way). setenv(""), not delenv:
    # the dotenv loader refills a missing key from .env, an empty one it keeps.
    monkeypatch.setenv("INVENTORY_DATABASE_URL", "")
    monkeypatch.setenv("INVENTORY_SQLITE_TESTS", "1")
    try:
        from backend.db import inventory_db as inv_db

        monkeypatch.setattr(inv_db, "DB_PATH", TEST_INVENTORY_DB_PATH, raising=False)
    except Exception:
        pass


@pytest.fixture(autouse=True)
def _clear_query_parser_caches() -> None:
    """Prevent inventory keyword cache leaking across tests with temp DB paths."""
    from backend.utils.query_parser import clear_query_parser_caches
    from backend.utils.ip_rate_limit import clear_rate_limit_state

    clear_query_parser_caches()
    clear_rate_limit_state()
    yield
    clear_query_parser_caches()
    clear_rate_limit_state()


# --- Recipe caches: one tmp dir per test --------------------------------------------
#
# ``backend.scanner.recipes.RECIPES_DIR`` and ``backend.scanner.vdp.vdp_recipes.
# VDP_RECIPES_DIR`` point at workspace/recipes; any test that reached save_recipes,
# mark_stale or the DB-adoption write in load_recipes without its own patch wrote the
# real cache. Every test now gets a fresh, not-yet-created dir (writers mkdir on
# demand). A test's own ``monkeypatch.setattr`` still wins: it runs after this autouse
# fixture. A value that differs, when the test starts, from the first value this fixture
# saw (the import-time default) was set by a broader-scoped fixture or by module code,
# and is left alone. A test that must read the real cache opts out with
# ``@pytest.mark.real_recipes_dir``; it must not write. Exemptions (grep of
# backend/tests, 2026-10-08): none, since no test reads real recipe files.
REAL_RECIPES_DIR_MARK = "real_recipes_dir"
_recipe_dir_seq = itertools.count()
_PRISTINE_RECIPE_DIRS: dict = {}


def _recipe_dir_targets() -> tuple:
    """(module, attribute, sub-path under the per-test dir) for every recipe cache dir."""
    from backend.scanner import recipes
    from backend.scanner.vdp import vdp_recipes

    return ((recipes, "RECIPES_DIR", ""), (vdp_recipes, "VDP_RECIPES_DIR", "vdp"))


def redirect_recipe_dirs(mp: pytest.MonkeyPatch) -> dict:
    """Point each recipe cache dir still at its import-time default at a fresh tmp dir.

    Returns ``{"module.ATTR": new_path}`` for the attributes it changed.
    """
    base = TEST_RECIPES_ROOT / f"t{next(_recipe_dir_seq)}"
    changed: dict = {}
    for mod, attr, sub in _recipe_dir_targets():
        key = f"{mod.__name__}.{attr}"
        current = getattr(mod, attr)
        pristine = _PRISTINE_RECIPE_DIRS.setdefault(key, current)
        if current != pristine:
            continue  # set elsewhere (a broader fixture, module code): that value wins
        target = base / sub if sub else base
        mp.setattr(mod, attr, target)
        changed[key] = target
    return changed


@pytest.fixture(autouse=True)
def _isolate_recipe_dirs(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> None:
    if request.node.get_closest_marker(REAL_RECIPES_DIR_MARK):
        return
    try:
        redirect_recipe_dirs(monkeypatch)
    except ImportError:
        pass  # recipes not importable here: nothing in this process writes through it


# --- Workspace guard: the session fails when a test wrote the real workspace/ ---------
#
# ``pytest_sessionstart`` snapshots names and mtimes under workspace/dealer_logs and
# workspace/recipes (recursively, so workspace/recipes/vdp too) of the checkout this
# conftest lives in; ``pytest_sessionfinish`` compares and fails the session, listing
# every added, removed or modified path. The check covers writes the in-process
# redirects above cannot see (a subprocess, a hard-coded path). A scanner or pipeline
# writes those trees legitimately, so a live one (``scanner_liveness.scanner_pids()``,
# a dealer_pipeline / discovery_probe / fleet_scan process, or a live scanner lock)
# at the start or end of the session downgrades the failure to a warning.
# ``WORKSPACE_GUARD=0`` switches the guard off.
WORKSPACE_GUARD_ENV = "WORKSPACE_GUARD"
WORKSPACE_GUARD_ROOT = Path(__file__).resolve().parent
_GUARDED_TREES = ("workspace/dealer_logs", "workspace/recipes")  # recipes/vdp is inside recipes
_PIPELINE_PROCESS_RE = re.compile(r"backend[./]scripts[./](?:dealer_pipeline|discovery_probe|fleet_scan)\b")
_WORKSPACE_GUARD: dict = {}


def _workspace_guard_disabled() -> bool:
    return (os.environ.get(WORKSPACE_GUARD_ENV) or "").strip().lower() in ("0", "false", "off", "no")


def workspace_snapshot(root: Path) -> dict:
    """``{relative path: (mtime_ns, size)}`` for every entry under the guarded trees.

    Directories are keyed with a trailing ``/`` and size 0.
    """
    snap: dict = {}
    for rel in _GUARDED_TREES:
        stack = [Path(root) / rel]
        while stack:
            d = stack.pop()
            try:
                entries = list(os.scandir(d))
            except OSError:
                continue
            for e in entries:
                try:
                    st = e.stat(follow_symlinks=False)
                    is_dir = e.is_dir(follow_symlinks=False)
                except OSError:
                    continue
                key = os.path.relpath(e.path, root)
                if is_dir:
                    snap[key + "/"] = (st.st_mtime_ns, 0)
                    stack.append(Path(e.path))
                else:
                    snap[key] = (st.st_mtime_ns, st.st_size)
    return snap


def workspace_touched(before: dict, after: dict) -> list:
    """``["added    <path>", "removed  <path>", "modified <path>"]``, sorted by path.

    A directory whose only change is its mtime is left out when an entry directly in it
    was added or removed (that entry is the news).
    """
    added = set(after) - set(before)
    removed = set(before) - set(after)
    modified = {k for k in set(before) & set(after) if before[k] != after[k]}
    parents = {os.path.dirname(p.rstrip("/")) + "/" for p in added | removed}
    modified = {k for k in modified if not (k.endswith("/") and k in parents)}
    rows = [(p, "added   ") for p in added] + [(p, "removed ") for p in removed] + [(p, "modified") for p in modified]
    return [f"{kind} {path}" for path, kind in sorted(rows)]


def _process_table() -> dict:
    """``{pid: command line}`` of every process (empty when ``ps`` is unavailable)."""
    try:
        out = subprocess.run(
            ["ps", "-Ao", "pid=,args="], capture_output=True, text=True, timeout=10
        ).stdout
    except (OSError, subprocess.SubprocessError):
        return {}
    table: dict = {}
    for line in out.splitlines():
        pid, _, cmd = line.strip().partition(" ")
        if pid.isdigit():
            table[int(pid)] = cmd.strip()
    return table


def _scanner_lock_paths(root: Path) -> list:
    ws = Path(root) / "workspace"
    paths = {ws / "scanner.lock"}
    raw = (os.environ.get("SCANNER_LOCK_PATH") or "").strip()
    if raw:
        p = Path(raw)
        paths.add(p if p.is_absolute() else Path(root) / p)
    paths.update(ws.glob("*.lock"))
    paths.update(ws.glob("pipeline/fleet_*/*.lock"))  # fleet_scan shard locks
    return sorted(paths)


def _live_lock_holder(lock: Path) -> int | None:
    try:
        m = re.search(r"\d+", lock.read_text(encoding="utf-8", errors="ignore"))
    except OSError:
        return None
    pid = int(m.group(0)) if m else 0
    if pid <= 0 or pid == os.getpid():
        return None
    try:
        os.kill(pid, 0)
    except PermissionError:
        return pid  # exists, owned by someone else
    except OSError:
        return None
    return pid


def live_workspace_writers(root: Path) -> list:
    """Why the workspace may legitimately change under this session (empty: no reason).

    Processes whose command line mentions pytest are not writers: ``pgrep -f
    scanner.py`` also matches ``pytest backend/tests/test_scanner.py``.
    """
    reasons: list = []
    procs = _process_table()
    own = os.getpid()
    try:
        from backend.scripts import scanner_liveness

        scanner_pids = scanner_liveness.scanner_pids()
    except Exception:  # noqa: BLE001 - the probe is best-effort
        scanner_pids = []
    for pid in scanner_pids:
        if pid != own and "pytest" not in procs.get(pid, ""):
            reasons.append(f"scanner pid {pid}")
    for pid, cmd in sorted(procs.items()):
        if pid != own and "pytest" not in cmd and _PIPELINE_PROCESS_RE.search(cmd):
            reasons.append(f"pipeline pid {pid} ({cmd[:100]})")
    for lock in _scanner_lock_paths(root):
        holder = _live_lock_holder(lock)
        if holder:
            reasons.append(f"scanner lock {lock} held by live pid {holder}")
    return reasons


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        f"{REAL_RECIPES_DIR_MARK}: keep the real recipe cache dirs for this test "
        "(conftest.py gives every other test a tmp dir); the test must only read them",
    )


def pytest_sessionstart(session: pytest.Session) -> None:
    if hasattr(session.config, "workerinput") or _workspace_guard_disabled():
        return  # xdist workers: the controller guards the session
    _WORKSPACE_GUARD["before"] = workspace_snapshot(WORKSPACE_GUARD_ROOT)
    _WORKSPACE_GUARD["live_at_start"] = live_workspace_writers(WORKSPACE_GUARD_ROOT)


@pytest.hookimpl(trylast=True)  # after session-scoped fixtures are torn down (runner hook)
def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    before = _WORKSPACE_GUARD.get("before")
    if before is None:
        return
    touched = workspace_touched(before, workspace_snapshot(WORKSPACE_GUARD_ROOT))
    if not touched:
        return
    live = list(_WORKSPACE_GUARD.get("live_at_start") or []) + live_workspace_writers(WORKSPACE_GUARD_ROOT)
    tr = session.config.pluginmanager.get_plugin("terminalreporter")
    if tr is not None:
        tr.ensure_newline()

    def emit(line: str, **markup: bool) -> None:
        if tr is not None:
            tr.write_line(line, **markup)
        else:
            sys.stderr.write(line + "\n")

    shown = touched[:60]
    if live:
        emit(f"WORKSPACE GUARD (warning only; live writers: {'; '.join(dict.fromkeys(live))}): "
             f"{len(touched)} path(s) changed under {WORKSPACE_GUARD_ROOT}/workspace during this session",
             yellow=True, bold=True)
    else:
        emit(f"WORKSPACE GUARD: this test session wrote {len(touched)} path(s) under the real "
             f"{WORKSPACE_GUARD_ROOT}/workspace; session marked FAILED", red=True, bold=True)
    for row in shown:
        emit(f"  {row}")
    if len(touched) > len(shown):
        emit(f"  ... and {len(touched) - len(shown)} more")
    if live:
        return
    emit("  Tests write dealer logs under DEALER_LOGS_ROOT (a session tmp dir, conftest.py) and "
         "recipe caches under the per-test RECIPES_DIR / VDP_RECIPES_DIR. If a scanner, pipeline or "
         f"person wrote these paths, rerun with {WORKSPACE_GUARD_ENV}=0.")
    if session.exitstatus in (pytest.ExitCode.OK, pytest.ExitCode.NO_TESTS_COLLECTED):
        session.exitstatus = pytest.ExitCode.TESTS_FAILED
