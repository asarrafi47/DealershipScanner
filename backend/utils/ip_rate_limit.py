"""Sliding-window rate limiting by string key (e.g. IP + route).

In-process memory only (N37.11, D-PG5): state is not shared across gunicorn/uwsgi
workers, so the effective ceiling is the configured limit times the worker count.
"""

from __future__ import annotations

import threading
import time
from collections import defaultdict

_lock = threading.Lock()
_events: dict[str, list[float]] = defaultdict(list)

# Periodic eviction of stale keys so rotating IPs cannot grow _events without bound.
_SWEEP_INTERVAL_SECONDS = 60.0
_max_window: float = 0.0
_last_sweep: float = 0.0


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
    """Record one event for ``key``. Return True if under limit, False if rate limited."""
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


def refund_event(key: str) -> None:
    """Drop the most recent event for ``key`` (undo an ``allow_request`` reservation).
    Not tied to the caller's own slot: under concurrency it may free a parallel caller's
    slot instead (at most one extra guess per success, and only after a valid password)."""
    with _lock:
        buf = _events.get(key)
        if buf:
            buf.pop()
