"""
Scanner inventory write policy: serialized (SQLite) vs parallel (Postgres).
"""
from __future__ import annotations

import asyncio
import logging
import os

from backend.db.inventory_pg import is_inventory_postgres

logger = logging.getLogger(__name__)


def scanner_parallel_upsert_enabled() -> bool:
    """
    When true, dealer upserts are not serialized by a process-wide asyncio lock.

    Default: on for Postgres, off for SQLite. Override with SCANNER_PARALLEL_UPSERT=0|1.
    """
    raw = (os.environ.get("SCANNER_PARALLEL_UPSERT") or "").strip().lower()
    if raw in ("0", "false", "no", "off"):
        return False
    if raw in ("1", "true", "yes", "on"):
        return True
    return is_inventory_postgres()


def default_max_dealer_concurrency() -> int:
    """Higher default when Postgres parallel upsert is active."""
    raw = (os.environ.get("SCANNER_MAX_DEALER_CONCURRENCY") or "").strip()
    if raw:
        try:
            return max(1, int(raw))
        except ValueError:
            pass
    return 6 if scanner_parallel_upsert_enabled() else 3


class InventoryWriteCoordinator:
    """
    Wraps ``upsert_vehicles`` with optional process-wide serialization.

    SQLite: one upsert at a time (WAL still struggles with concurrent writers).
    Postgres: concurrent upserts across dealers (VIN-level ON CONFLICT).
    """

    def __init__(self) -> None:
        self._parallel = scanner_parallel_upsert_enabled()
        self._lock: asyncio.Lock | None = None if self._parallel else asyncio.Lock()
        if self._parallel:
            logger.info("Scanner: parallel inventory upsert enabled (Postgres)")
        else:
            logger.info("Scanner: serialized inventory upsert (SQLite single-writer)")

    @property
    def parallel(self) -> bool:
        return self._parallel

    async def upsert_vehicles(self, vehicles: list[dict]) -> int:
        if not vehicles:
            return 0
        from backend.scanner.database import upsert_vehicles as _upsert

        if self._lock is None:
            return await asyncio.to_thread(_upsert, vehicles)
        async with self._lock:
            return await asyncio.to_thread(_upsert, vehicles)
