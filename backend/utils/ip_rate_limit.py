"""Simple in-memory sliding-window rate limiting by string key (e.g. IP + route)."""

from __future__ import annotations

import threading
import time
from collections import defaultdict

_lock = threading.Lock()
_events: dict[str, list[float]] = defaultdict(list)


def allow_request(key: str, *, max_events: int, window_seconds: float) -> bool:
    """
    Record one event for ``key``. Return True if under limit, False if rate limited.
    Not suitable for multi-process deployments (use Redis, etc.).
    """
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
