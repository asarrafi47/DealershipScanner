"""Inventory backend must use Postgres (SEC-102)."""

from __future__ import annotations

import pytest


def test_inventory_backend_requires_postgres_url(monkeypatch: pytest.MonkeyPatch) -> None:
    from backend.db.inventory_pg import assert_inventory_backend_configured

    monkeypatch.delenv("INVENTORY_DATABASE_URL", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("INVENTORY_SQLITE_TESTS", raising=False)
    with pytest.raises(RuntimeError, match="INVENTORY_DATABASE_URL"):
        assert_inventory_backend_configured()


def test_inventory_backend_allows_postgres_url(monkeypatch: pytest.MonkeyPatch) -> None:
    from backend.db.inventory_pg import assert_inventory_backend_configured

    monkeypatch.setenv("INVENTORY_DATABASE_URL", "postgresql://u:p@localhost/db")
    assert_inventory_backend_configured()


def test_inventory_backend_allows_sqlite_tests_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    from backend.db.inventory_pg import assert_inventory_backend_configured

    monkeypatch.delenv("INVENTORY_DATABASE_URL", raising=False)
    monkeypatch.setenv("INVENTORY_SQLITE_TESTS", "1")
    assert_inventory_backend_configured()
