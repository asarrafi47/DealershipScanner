"""The test session must never point at the developer's real databases.

Audit 2026-10-01 (tests.md F1): tests reached the real users.db / dev_users.db
and the local Postgres inventory because (1) nine modules import backend.main at
collection time, before any fixture ran, (2) backend/tests/conftest.py deleted
INVENTORY_DATABASE_URL instead of blanking it, and (3) every
``importlib.reload(backend.main)`` re-ran ``load_project_dotenv()``, which
refilled the deleted key from .env. The root conftest now pins every database
env var at import and switches .env loading off for the session.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest

from backend.utils.project_env import DOTENV_DISABLE_ENV, dotenv_disabled, load_project_dotenv

REPO_ROOT = Path(__file__).resolve().parents[2]
_DB_KEYS = (
    "INVENTORY_DATABASE_URL",
    "DATABASE_URL",
    "USERS_DB_PATH",
    "DEV_USERS_DB_PATH",
    "DEALER_PORTAL_DB_PATH",
    "INCOMPLETE_LISTINGS_DB_PATH",
)


def _dotenv_db_values() -> dict[str, str]:
    env_path = REPO_ROOT / ".env"
    if not env_path.is_file():
        return {}
    from dotenv import dotenv_values

    return {k: v for k, v in dotenv_values(env_path).items() if k in _DB_KEYS and v}


def _real_path(raw: str) -> str:
    return os.path.realpath(raw if os.path.isabs(raw) else os.path.join(REPO_ROOT, raw))


def test_inventory_urls_are_blank_during_tests() -> None:
    assert os.environ.get("INVENTORY_DATABASE_URL") == ""
    assert os.environ.get("DATABASE_URL") == ""
    from backend.db import inventory_pg

    assert not inventory_pg.is_inventory_postgres()


def test_users_db_paths_live_in_a_tmp_dir() -> None:
    from backend.db.dev_users_sqlite import dev_users_db_path
    from backend.db.users_sqlite import users_db_path

    tmp_root = os.path.realpath(tempfile.gettempdir())
    for path in (users_db_path(), dev_users_db_path()):
        real = os.path.realpath(path)
        assert real.startswith(tmp_root), real
        assert not real.startswith(str(REPO_ROOT)), real


def test_no_db_env_matches_dotenv() -> None:
    dot = _dotenv_db_values()
    for key, value in dot.items():
        current = os.environ.get(key) or ""
        assert current != value, f"{key} carries the .env value during tests"
        if current and key.endswith("_PATH"):
            assert _real_path(current) != _real_path(value), key


def test_dotenv_loading_is_disabled_for_the_session() -> None:
    assert dotenv_disabled()


def test_load_project_dotenv_neither_refills_nor_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("INVENTORY_DATABASE_URL", raising=False)
    monkeypatch.setenv("USERS_DB_PATH", "/tmp/pytest-set-by-test.db")
    load_project_dotenv()
    load_project_dotenv(override=True)
    assert "INVENTORY_DATABASE_URL" not in os.environ
    assert os.environ["USERS_DB_PATH"] == "/tmp/pytest-set-by-test.db"


def test_reload_main_keeps_databases_isolated(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    import importlib

    monkeypatch.setenv("FLASK_ENV", "development")
    monkeypatch.setenv("USERS_DB_PATH", str(tmp_path / "users.db"))
    monkeypatch.setenv("DEV_USERS_DB_PATH", str(tmp_path / "dev_users.db"))
    monkeypatch.delenv("INVENTORY_DATABASE_URL", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    import backend.main as main

    importlib.reload(main)
    assert not os.environ.get("INVENTORY_DATABASE_URL")
    assert not os.environ.get("DATABASE_URL")
    assert os.environ["USERS_DB_PATH"] == str(tmp_path / "users.db")


@pytest.mark.parametrize("raw,expected", [("1", True), ("true", True), ("", False), ("0", False)])
def test_dotenv_disabled_flag(monkeypatch: pytest.MonkeyPatch, raw: str, expected: bool) -> None:
    monkeypatch.setenv(DOTENV_DISABLE_ENV, raw)
    assert dotenv_disabled() is expected
