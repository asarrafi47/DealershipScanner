"""Advisory scan leases (P6A.1): cross-host locks so two scanners never share a key.

``scan_lock`` is an ``fcntl.flock`` and only covers one host. These leases are
Postgres session advisory locks, so a second host (the MBP and Railway scanning
at once) is refused too. Two kinds of key share one lock space:

* ``fleet_lock(lane)``      one whole-fleet run per lane (``fleet_scan``, exit 3);
* ``try_lock_dealers(ids)`` one scanner per dealer (``P11A.3`` takes it around a dealer's
                            replay -> write -> reconcile).

Each is ``pg_try_advisory_lock(<SCAN_LEASE_CLASSID>, hashtext(key))`` on a dedicated
autocommit connection with ``connect_timeout``, TCP keepalives and
``application_name = scanner:<lane>:<hostname>``. The connection is never inside a
transaction (an idle-in-transaction session would also hold back vacuum) and its
``idle_session_timeout`` is set to 0, because a server that drops an idle session
silently drops every lock on it. The holder of a refused key is read from
``pg_locks`` joined with ``pg_stat_activity``. Locks end with the connection, so a
killed process frees them.

Advisory by design: with no Postgres DSN (offline tests) every call succeeds as a
no-op, and if the lock connection cannot be opened the call succeeds with a WARN
rather than stopping a scan on a lock-service hiccup.
"""
from __future__ import annotations

import logging
import os
import socket
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Iterable, Iterator

logger = logging.getLogger("scanner")

# First key of pg_try_advisory_lock(int, int): "SCAN" in ASCII. The second is hashtext(key).
SCAN_LEASE_CLASSID = 0x5343414E
CONNECT_TIMEOUT_S = 10

_HOLDER_SQL = (
    "SELECT a.application_name, l.pid, a.backend_start FROM pg_locks l "
    "LEFT JOIN pg_stat_activity a ON a.pid = l.pid "
    "WHERE l.locktype = 'advisory' AND l.classid = %s AND l.objsubid = 2 AND l.granted "
    "AND l.objid::bigint = (hashtext(%s)::bigint & 4294967295) AND l.pid <> pg_backend_pid() "
    "ORDER BY a.backend_start LIMIT 1"
)


def _key(kind: str, name: str) -> str:
    return f"{kind}:{name}"


def _describe(row: Any) -> str:
    app, pid, since = row
    return f"{app or 'unknown'} (backend pid {pid}, connected {since})"


class LeaseConnection:
    """One dedicated autocommit connection holding this process's session locks."""

    def __init__(self, lane: str, dsn: str | None = None) -> None:
        self.lane = lane
        self.conn: Any = None
        self.degraded: str | None = None  # why the locks are not real, when they are not
        if dsn is None:
            from backend.db.connect import inventory_dsn

            dsn = inventory_dsn(require=False)
        if not dsn or not str(dsn).startswith(("postgres://", "postgresql://")):
            self.degraded = "no postgres dsn"
            return
        try:
            import psycopg

            self.conn = psycopg.connect(
                dsn, autocommit=True, connect_timeout=CONNECT_TIMEOUT_S,
                keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=3,
                application_name=f"scanner:{lane}:{socket.gethostname()}"[:63],
            )
            self.conn.execute("SET idle_session_timeout = 0")
        except Exception as exc:  # noqa: BLE001 - advisory: never stop a scan for a lock hiccup
            self.conn = None
            self.degraded = f"lock connection failed: {str(exc)[:120]}"
            logger.warning("scan lease: %s; running without cross-host locks", self.degraded)

    def try_lock(self, key: str) -> tuple[bool, str | None]:
        """(acquired, holder description when refused). Re-entrant within this session."""
        if self.conn is None:
            return True, None
        got = self.conn.execute(
            "SELECT pg_try_advisory_lock(%s, hashtext(%s))", (SCAN_LEASE_CLASSID, key)).fetchone()[0]
        if got:
            return True, None
        row = self.conn.execute(_HOLDER_SQL, (SCAN_LEASE_CLASSID, key)).fetchone()
        return False, _describe(row) if row else "unknown holder"

    def unlock(self, key: str) -> None:
        if self.conn is not None:
            self.conn.execute("SELECT pg_advisory_unlock(%s, hashtext(%s))", (SCAN_LEASE_CLASSID, key))

    def close(self) -> None:
        conn, self.conn = self.conn, None
        if conn is not None:
            try:
                conn.close()  # the server drops every lock of the session
            except Exception:  # noqa: BLE001
                pass


# One lock connection per scanner process, reused across dealers (P11A.3).
_shared: dict[tuple[int, str], LeaseConnection] = {}
_shared_guard = threading.Lock()


def _shared_conn(lane: str) -> LeaseConnection:
    k = (os.getpid(), lane)  # a forked child must open its own session
    with _shared_guard:
        lc = _shared.get(k)
        if lc is None:
            lc = _shared[k] = LeaseConnection(lane)
        return lc


def try_lock_dealers(dealer_ids: Iterable[str], lane: str = "dealer") -> tuple[list[str], dict[str, str]]:
    """Take a lease per dealer: ``(acquired, held_elsewhere)``, the latter ``{dealer_id: holder}``.

    The caller scans the acquired ones and skips the rest; ``release_dealers`` (or the
    process ending) frees them."""
    lc = _shared_conn(lane)
    acquired: list[str] = []
    refused: dict[str, str] = {}
    for did in dealer_ids:
        ok, holder = lc.try_lock(_key("dealer", did))
        if ok:
            acquired.append(did)
        else:
            refused[did] = holder or "unknown holder"
    return acquired, refused


def release_dealers(dealer_ids: Iterable[str], lane: str = "dealer") -> None:
    lc = _shared_conn(lane)
    for did in dealer_ids:
        lc.unlock(_key("dealer", did))


@dataclass
class FleetLease:
    acquired: bool
    holder: str | None = None
    degraded: str | None = None


@contextmanager
def fleet_lock(lane: str) -> Iterator[FleetLease]:
    """Hold the whole-fleet lease of ``lane`` for the block. Never raises on contention:
    the caller reads ``.acquired`` / ``.holder`` and decides (fleet_scan exits 3)."""
    lc = LeaseConnection(lane)
    try:
        ok, holder = lc.try_lock(_key("fleet", lane))
        yield FleetLease(ok, holder, lc.degraded)
    finally:
        lc.close()
