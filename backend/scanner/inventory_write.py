"""
Scanner inventory write policy: serialized (SQLite) vs parallel (Postgres).
"""
from __future__ import annotations

import asyncio
import logging
import os

from backend.db import inventory_pg

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
    return inventory_pg.is_inventory_postgres()


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

    async def upsert_vehicles(self, vehicles: list[dict], stats: dict | None = None) -> int:
        """``stats``: optional dict ``upsert_vehicles`` fills with the VIN
        ownership-guard counts (``vin_owner_conflicts`` etc.)."""
        if not vehicles:
            return 0
        if self._lock is None:
            return await _upsert_with_retry(vehicles, stats)
        async with self._lock:
            return await _upsert_with_retry(vehicles, stats)


_UPSERT_RETRIES = 3


def _is_retryable_db_error(exc: BaseException) -> bool:
    """Transient Postgres write failures that a VIN-keyed, idempotent upsert can rerun.

    - 40P01 deadlock / 40001 serialization failure.
    - 55P03 lock_not_available: ``SET lock_timeout`` expired ("canceling statement
      due to lock timeout"). southcoasttoyota-com lost 609 rows to this on
      2026-09-29 while 8 fleet shards wrote concurrently.
    - 57014 query_canceled ONLY when the message says lock timeout (some drivers /
      poolers surface the lock-timeout cancel under 57014). A 57014 from
      ``statement_timeout`` or a user cancel is not retried.
    """
    code = getattr(exc, "sqlstate", None) or getattr(getattr(exc, "diag", None), "sqlstate", None)
    if code in ("40P01", "40001", "55P03"):
        return True
    if code == "57014":
        return "lock timeout" in str(exc).lower()
    name = type(exc).__name__
    return name in ("DeadlockDetected", "SerializationFailure", "LockNotAvailable")


async def _upsert_with_retry(vehicles: list[dict], stats: dict | None = None) -> int:
    """The upsert is VIN-keyed ON CONFLICT and therefore idempotent: a deadlock
    victim is rolled back by Postgres and can simply run again."""
    from backend.scanner.database import upsert_vehicles as _upsert

    for attempt in range(1, _UPSERT_RETRIES + 1):
        try:
            if stats is None:
                return await asyncio.to_thread(_upsert, vehicles)
            return await asyncio.to_thread(_upsert, vehicles, stats)
        except Exception as exc:  # noqa: BLE001
            if attempt >= _UPSERT_RETRIES or not _is_retryable_db_error(exc):
                raise
            logger.warning(
                "Inventory upsert hit %s (attempt %d/%d); retrying",
                type(exc).__name__, attempt, _UPSERT_RETRIES,
            )
            await asyncio.sleep(0.5 * attempt)
    return 0
