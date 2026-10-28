"""
Scanner inventory write policy: serialized (SQLite) vs parallel (Postgres).
"""
from __future__ import annotations

import asyncio
import logging
import os
import random

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
# Connection-class errors get one more attempt so the 2, 4, 8 s ladder is fully used.
_CONN_RETRIES = 4
_CONN_BACKOFF_BASE_S = 2.0
_CONN_BACKOFF_CAP_S = 8.0
_CONN_MESSAGE_MARKERS = (
    "connection refused", "connection reset", "connection closed", "connection lost",
    "server closed the connection", "connection timed out", "could not connect",
    "terminating connection", "consuming input failed", "the connection is lost",
)


def _sqlstate(exc: BaseException) -> str | None:
    return getattr(exc, "sqlstate", None) or getattr(getattr(exc, "diag", None), "sqlstate", None)


def _is_connection_error(exc: BaseException) -> bool:
    """Connection-class failures: SQLSTATE class 08, 53300 too_many_connections, 57P01
    admin_shutdown, or a psycopg OperationalError with no sqlstate whose message reads as a
    lost or refused connection. Under 8 shards 53300 does not clear in a second."""
    code = _sqlstate(exc)
    if code:
        return code.startswith("08") or code in ("53300", "57P01")
    if type(exc).__name__ != "OperationalError":
        return False
    msg = str(exc).lower()
    return any(m in msg for m in _CONN_MESSAGE_MARKERS)


def _is_retryable_db_error(exc: BaseException) -> bool:
    """Transient Postgres write failures that a VIN-keyed, idempotent upsert can rerun.

    - 40P01 deadlock / 40001 serialization failure.
    - 55P03 lock_not_available: ``SET lock_timeout`` expired ("canceling statement
      due to lock timeout"). southcoasttoyota-com lost 609 rows to this on
      2026-09-29 while 8 fleet shards wrote concurrently.
    - 57014 query_canceled ONLY when the message says lock timeout (some drivers /
      poolers surface the lock-timeout cancel under 57014). A 57014 from
      ``statement_timeout`` or a user cancel is not retried.
    - Connection-class errors (see ``_is_connection_error``).
    """
    code = _sqlstate(exc)
    if code in ("40P01", "40001", "55P03"):
        return True
    if code == "57014":
        return "lock timeout" in str(exc).lower()
    if _is_connection_error(exc):
        return True
    name = type(exc).__name__
    return name in ("DeadlockDetected", "SerializationFailure", "LockNotAvailable")


def _retry_delay(exc: BaseException, attempt: int) -> float:
    """Connection errors: 2, 4, 8 s (capped) plus jitter; deadlocks and lock timeouts keep
    ``0.5 * attempt + random(0, 0.5)``."""
    if _is_connection_error(exc):
        base = min(_CONN_BACKOFF_BASE_S * 2 ** (attempt - 1), _CONN_BACKOFF_CAP_S)
        return base + random.uniform(0, base / 4)
    return 0.5 * attempt + random.uniform(0, 0.5)


async def _upsert_with_retry(vehicles: list[dict], stats: dict | None = None) -> int:
    """The upsert is VIN-keyed ON CONFLICT and therefore idempotent: a deadlock
    victim is rolled back by Postgres and can simply run again.

    ``upsert_vehicles`` resets ``stats`` (timing included) on every attempt, so the retry
    counts live here and are written into ``stats['timing']['upsert_retries']`` /
    ``upsert_conn_retries`` after the last attempt, success or failure."""
    from backend.scanner.database import upsert_vehicles as _upsert

    retries = conn_retries = 0
    try:
        attempt = 0
        while True:
            attempt += 1
            try:
                if stats is None:
                    return await asyncio.to_thread(_upsert, vehicles)
                return await asyncio.to_thread(_upsert, vehicles, stats)
            except Exception as exc:  # noqa: BLE001
                is_conn = _is_connection_error(exc)
                limit = _CONN_RETRIES if is_conn else _UPSERT_RETRIES
                if attempt >= limit or not _is_retryable_db_error(exc):
                    raise
                retries += 1
                conn_retries += is_conn
                logger.warning(
                    "Inventory upsert hit %s (attempt %d/%d); retrying",
                    type(exc).__name__, attempt, limit,
                )
                await asyncio.sleep(_retry_delay(exc, attempt))
    finally:
        if stats is not None and retries:
            timing = stats.get("timing")
            if not isinstance(timing, dict):
                timing = stats["timing"] = {}
            timing["upsert_retries"] = retries
            timing["upsert_conn_retries"] = conn_retries
