"""Sliding-window rate limiting by string key (e.g. IP + route).

* In-process memory (default): not shared across gunicorn/uwsgi workers.
* Optional shared SQLite: set env ``RATE_LIMIT_SQLITE_PATH`` to a path on a filesystem
  all workers can read/write (WAL) so limits apply per deployment host.
"""

from __future__ import annotations

import os
import sqlite3
import threading
import time
from collections import defaultdict

_lock = threading.Lock()
_events: dict[str, list[float]] = defaultdict(list)

_sqlite_lock = threading.Lock()


def _sqlite_path_configured() -> str | None:
    raw = (os.environ.get("RATE_LIMIT_SQLITE_PATH") or "").strip()
    return raw or None


def _prepare_sqlite_path() -> str:
    p = _sqlite_path_configured() or ""
    if not p:
        raise RuntimeError("RATE_LIMIT_SQLITE_PATH is not set")
    d = os.path.dirname(p)
    if d:
        try:
            os.makedirs(d, exist_ok=True)
        except OSError:
            pass
    return p


def _sqlite_init_conn(conn: sqlite3.Connection) -> None:
    try:
        conn.execute("PRAGMA journal_mode=WAL")
    except sqlite3.OperationalError:
        pass
    try:
        conn.execute("PRAGMA busy_timeout=2000")
    except sqlite3.OperationalError:
        pass
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS _rate_hits (
            k TEXT NOT NULL,
            t REAL NOT NULL
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS _rate_hits_kt ON _rate_hits (k, t)")


def clear_rate_limit_state() -> None:
    """Reset in-process counters (pytest isolation)."""
    with _lock:
        _events.clear()


def allow_request(key: str, *, max_events: int, window_seconds: float) -> bool:
    """
    Record one event for ``key``. Return True if under limit, False if rate limited.
    When ``RATE_LIMIT_SQLITE_PATH`` is set, state is shared across processes on the same host.
    """
    if _sqlite_path_configured():
        return _allow_request_sqlite(key, max_events=max_events, window_seconds=window_seconds)
    return _allow_request_memory(key, max_events=max_events, window_seconds=window_seconds)


def _allow_request_memory(key: str, *, max_events: int, window_seconds: float) -> bool:
    now = time.monotonic()
    cutoff = now - window_seconds
    with _lock:
        buf = _events[key]
        while buf and buf[0] < cutoff:
            buf.pop(0)
        if len(buf) >= max_events:
            return False
        buf.append(now)
        return True


def _allow_request_sqlite(key: str, *, max_events: int, window_seconds: float) -> bool:
    t_wall = time.time()
    cutoff = t_wall - window_seconds
    try:
        p = _prepare_sqlite_path()
    except RuntimeError:
        return _allow_request_memory(key, max_events=max_events, window_seconds=window_seconds)
    with _sqlite_lock:
        try:
            conn = sqlite3.connect(p, check_same_thread=False, timeout=5.0)
        except sqlite3.Error:
            return _allow_request_memory(key, max_events=max_events, window_seconds=window_seconds)
        try:
            _sqlite_init_conn(conn)
            cur = conn.cursor()
            cur.execute("DELETE FROM _rate_hits WHERE t < ?", (cutoff,))
            cur.execute("SELECT COUNT(1) FROM _rate_hits WHERE k = ?", (key,))
            n = int(cur.fetchone()[0] or 0)
            if n >= max_events:
                conn.commit()
                return False
            cur.execute("INSERT INTO _rate_hits (k, t) VALUES (?, ?)", (key, t_wall))
            conn.commit()
        except sqlite3.Error:
            try:
                conn.rollback()
            except sqlite3.Error:
                pass
            return _allow_request_memory(key, max_events=max_events, window_seconds=window_seconds)
        finally:
            try:
                conn.close()
            except sqlite3.Error:
                pass
        return True
