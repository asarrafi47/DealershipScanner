"""Root pytest configuration — ensures ``backend/`` is on sys.path for bare-import modules."""
import atexit
import os
import shutil
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
