"""Sliding-window rate limiting by string key (e.g. IP + route).

Two backends:
- in-process memory (N37.11, D-PG5), the default and the fallback: state is not shared across
  gunicorn workers, so the effective ceiling is the configured limit times the worker count;
- Redis (card R1), added when ``REDIS_URL`` is set: a sorted-set sliding window per key
  (ZREMRANGEBYSCORE, ZADD, ZCARD and EXPIRE in one MULTI pipeline), shared by every worker.

Redis may only make this stricter, never looser: on any Redis error the call falls back to the
in-process limiter, and with ``REDIS_URL`` unset the behaviour is exactly the memory one. Keys
are ``sc:rl:<sha256 of the caller's key>`` (an address never appears in Redis) and every key
expires one window after its last event.
"""

from __future__ import annotations

import hashlib
import secrets
import threading
import time
from collections import defaultdict

from backend.utils import redis_client

_lock = threading.Lock()
_events: dict[str, list[float]] = defaultdict(list)

# Periodic eviction of stale keys so rotating IPs cannot grow _events without bound.
_SWEEP_INTERVAL_SECONDS = 60.0
_max_window: float = 0.0
_last_sweep: float = 0.0

BACKEND_MEMORY = "memory"
BACKEND_REDIS = "redis"

_wall = time.time  # Redis scores are wall-clock seconds (shared across processes)


def clear_rate_limit_state() -> None:
    """Reset in-process counters (pytest isolation)."""
    global _max_window, _last_sweep
    with _lock:
        _events.clear()
        _max_window = 0.0
        _last_sweep = 0.0


def backend_name() -> str:
    """``redis`` when Redis is configured and not skipped by its breaker, else ``memory``."""
    return BACKEND_REDIS if redis_client.available() else BACKEND_MEMORY


def _sweep_stale_locked(now: float) -> None:
    """Drop keys whose most recent event is older than any live window. Call under _lock."""
    stale_before = now - _max_window
    for k in [k for k, buf in _events.items() if not buf or buf[-1] < stale_before]:
        del _events[k]


def allow_request(key: str, *, max_events: int, window_seconds: float) -> bool:
    """Record one event for ``key``. Return True if under limit, False if rate limited.

    The in-process window always runs first and alone decides when Redis is unset or failing, so
    the result is never looser than the memory limiter. With Redis configured, an event the memory
    window admits must also pass the shared Redis window (a Redis refusal undoes the memory event).
    """
    if not _allow_request_memory(key, max_events=max_events, window_seconds=window_seconds):
        return False
    client = redis_client.get_client()
    if client is None:
        return True
    try:
        allowed = _allow_request_redis(client, key, max_events=max_events, window_seconds=window_seconds)
    except Exception as exc:  # any Redis trouble: this call keeps the memory answer
        redis_client.record_failure(exc)
        return True
    redis_client.record_success()
    if not allowed:
        _refund_memory(key)
    return allowed


def _redis_key(key: str) -> str:
    return redis_client.key("rl", hashlib.sha256(key.encode("utf-8")).hexdigest()[:40])


def _allow_request_redis(client, key: str, *, max_events: int, window_seconds: float) -> bool:
    """Sorted-set sliding window: events in ``(now - window, now]`` are counted.

    The event is added first and removed again when it is over the limit, so two racing callers
    can both be refused but never both admitted past ``max_events`` (stricter, never looser).
    """
    now = _wall()
    rkey = _redis_key(key)
    member = "%.6f:%s" % (now, secrets.token_hex(4))
    ttl = max(1, int(window_seconds) + 1)
    pipe = client.pipeline(transaction=True)
    pipe.zremrangebyscore(rkey, "-inf", now - window_seconds)
    pipe.zadd(rkey, {member: now})
    pipe.zcard(rkey)
    pipe.expire(rkey, ttl)
    _removed, _added, count, _expired = pipe.execute()
    if int(count) > max_events:
        client.zrem(rkey, member)
        return False
    return True


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


def _refund_memory(key: str) -> None:
    with _lock:
        buf = _events.get(key)
        if buf:
            buf.pop()


def refund_event(key: str) -> None:
    """Drop the most recent event for ``key`` (undo an ``allow_request`` reservation).
    Not tied to the caller's own slot: under concurrency it may free a parallel caller's
    slot instead (at most one extra guess per success, and only after a valid password)."""
    _refund_memory(key)
    client = redis_client.get_client()
    if client is None:
        return
    try:
        client.zpopmax(_redis_key(key))
    except Exception as exc:
        redis_client.record_failure(exc)
    else:
        redis_client.record_success()
