"""Shared pytest hooks for DealershipScanner backend tests."""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _inventory_sqlite_tests_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    """Default tests to SQLite inventory unless they set INVENTORY_DATABASE_URL."""
    monkeypatch.delenv("INVENTORY_DATABASE_URL", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("INVENTORY_SQLITE_TESTS", "1")
