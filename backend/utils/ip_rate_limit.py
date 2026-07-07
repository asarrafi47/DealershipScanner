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

# Periodic eviction of stale keys so rotating IPs cannot grow _events without bound.
_SWEEP_INTERVAL_SECONDS = 60.0
_max_window: float = 0.0
_last_sweep: float = 0.0

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
    global _max_window, _last_sweep
    with _lock:
        _events.clear()
        _max_window = 0.0
        _last_sweep = 0.0


def _sweep_stale_locked(now: float) -> None:
    """Drop keys whose most recent event is older than any live window. Call under _lock."""
    stale_before = now - _max_window
    for k in [k for k, buf in _events.items() if not buf or buf[-1] < stale_before]:
        del _events[k]


def allow_request(key: str, *, max_events: int, window_seconds: float) -> bool:
    """
    Record one event for ``key``. Return True if under limit, False if rate limited.
    When ``RATE_LIMIT_SQLITE_PATH`` is set, state is shared across processes on the same host.
    """
    if _sqlite_path_configured():
        return _allow_request_sqlite(key, max_events=max_events, window_seconds=window_seconds)
    return _allow_request_memory(key, max_events=max_events, window_seconds=window_seconds)


def _allow_request_memory(key: str, *, max_events: int, window_seconds: float) -> bool:
    global _max_window, _last_sweep
    now = time.monotonic()
    cutoff = now - window_seconds
    with _lock:
        if window_seconds > _max_window:
            _max_window = window_seconds
        if now - _last_sweep >= _SWEEP_INTERVAL_SECONDS:
            _sweep_stale_locked(now)
            _last_sweep = now
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
