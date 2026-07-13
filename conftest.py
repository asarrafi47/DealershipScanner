"""Root pytest configuration — ensures ``backend/`` is on sys.path for bare-import modules."""
import os
import sys
from pathlib import Path

import pytest

# Dev/test may use plain SQLite for users.db; production forbids this (SEC-088).
os.environ.setdefault("ALLOW_UNENCRYPTED_USER_DB", "1")

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

        inv_db.DB_PATH = inv_db._default_inventory_db_path()
        inv_db.init_inventory_db()
    finally:
        mp.undo()


@pytest.fixture(autouse=True)
def _isolate_listings_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Prevent ``FLASK_ENV=production`` and strict listings flags leaking across tests."""
    monkeypatch.delenv("FLASK_ENV", raising=False)
    monkeypatch.delenv("LISTINGS_INCLUDE_INCOMPLETE_CARS", raising=False)
    monkeypatch.delenv("INVENTORY_DB_PATH", raising=False)
    try:
        from backend.db import inventory_db as inv_db
        from backend.db.inventory_db import _default_inventory_db_path

        monkeypatch.setattr(inv_db, "DB_PATH", _default_inventory_db_path(), raising=False)
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
