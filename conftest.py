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
    monkeypatch.delenv("ALLOW_UNENCRYPTED_USER_DB", raising=False)
    monkeypatch.setenv("USERS_DB_ENCRYPTION_KEY", PROD_TEST_USERS_DB_KEY)
    monkeypatch.setenv("DEV_USERS_DB_ENCRYPTION_KEY", PROD_TEST_DEV_USERS_DB_KEY)

# backend/ must be on sys.path so bare imports like ``from scraping.xxx`` and
# ``from intelligence.llm.xxx`` resolve to the correct packages.
_backend = str(Path(__file__).resolve().parent / "backend")
if _backend not in sys.path:
    sys.path.insert(0, _backend)


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
