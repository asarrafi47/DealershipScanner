"""Parallel vs serialized scanner upsert policy."""

from __future__ import annotations

import asyncio

from backend.scanner.inventory_write import (
    InventoryWriteCoordinator,
    default_max_dealer_concurrency,
    scanner_parallel_upsert_enabled,
)


def test_parallel_upsert_off_for_sqlite(monkeypatch):
    monkeypatch.delenv("SCANNER_PARALLEL_UPSERT", raising=False)
    monkeypatch.delenv("INVENTORY_DATABASE_URL", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    assert scanner_parallel_upsert_enabled() is False
    assert default_max_dealer_concurrency() == 3


def test_parallel_upsert_on_for_postgres(monkeypatch):
    monkeypatch.delenv("SCANNER_PARALLEL_UPSERT", raising=False)
    monkeypatch.setenv("INVENTORY_DATABASE_URL", "postgresql://u:p@localhost/db")
    assert scanner_parallel_upsert_enabled() is True
    assert default_max_dealer_concurrency() == 6


def test_parallel_upsert_explicit_override(monkeypatch):
    monkeypatch.setenv("SCANNER_PARALLEL_UPSERT", "0")
    monkeypatch.setenv("INVENTORY_DATABASE_URL", "postgresql://u:p@localhost/db")
    assert scanner_parallel_upsert_enabled() is False


def test_coordinator_uses_lock_on_sqlite(monkeypatch):
    monkeypatch.setenv("SCANNER_PARALLEL_UPSERT", "0")
    monkeypatch.delenv("INVENTORY_DATABASE_URL", raising=False)

    def mock_upsert(v):
        return len(v)

    monkeypatch.setattr("backend.scanner.database.upsert_vehicles", mock_upsert)
    coord = InventoryWriteCoordinator()
    assert coord.parallel is False

    async def run():
        return await coord.upsert_vehicles([{"vin": "1" * 17}])

    assert asyncio.run(run()) == 1


def test_coordinator_parallel_on_postgres(monkeypatch):
    monkeypatch.setenv("INVENTORY_DATABASE_URL", "postgresql://u:p@localhost/db")
    monkeypatch.delenv("SCANNER_PARALLEL_UPSERT", raising=False)

    def mock_upsert(v):
        return len(v)

    monkeypatch.setattr("backend.scanner.database.upsert_vehicles", mock_upsert)
    coord = InventoryWriteCoordinator()
    assert coord.parallel is True

    async def run():
        return await asyncio.gather(
            coord.upsert_vehicles([{"vin": "1" * 17}]),
            coord.upsert_vehicles([{"vin": "2" * 17}]),
        )

    assert sum(asyncio.run(run())) == 2
