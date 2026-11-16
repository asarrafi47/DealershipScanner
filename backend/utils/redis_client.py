"""One shared, optional Redis client (card R1, docs/CLOUD_RESTRUCTURE_PLAN.md section 4.1).

Redis may only make things faster or stricter. It is never the authority for a cap, a pool, a
lock or a spend sum (those stay in Postgres), and a flush or an eviction must never raise any
limit. With ``REDIS_URL`` unset nothing here connects and every caller behaves exactly as before.

Rules for every caller:
- keys start with ``sc:`` (use :func:`key`) and always carry a TTL;
- never a phone number, a code or a secret in a key or a value (hash first);
- every call is wrapped: on any error call :func:`record_failure` and fall back.

Short socket timeouts (150 ms) and a small circuit breaker (3 errors in a row: skip Redis for
30 seconds) mean a dead Redis never slows a request by more than one timeout per 30 seconds.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from typing import Any

from backend.config import Config

_log = logging.getLogger(__name__)

KEY_PREFIX = "sc:"
SOCKET_TIMEOUT_S = 0.15
CONNECT_TIMEOUT_S = 0.15
FAILURES_TO_OPEN = 3
OPEN_SECONDS = 30.0

_lock = threading.Lock()
_client: Any = None
_client_url: str = ""
_failures = 0
_open_until = 0.0
_clock: Callable[[], float] = time.monotonic
_factory: Callable[[str], Any] | None = None  # tests: build a fake client instead of redis-py


class RedisUnavailable(RuntimeError):
    """Redis is configured but skipped for now (the breaker is open). No URL in the message."""


def key(*parts: str) -> str:
    """``sc:part1:part2``. Callers hash anything personal before it gets here."""
    return KEY_PREFIX + ":".join(str(p) for p in parts)


def configured() -> bool:
    return bool(Config.redis_url())


def _build(url: str) -> Any:
    if _factory is not None:
        return _factory(url)
    import redis

    return redis.Redis.from_url(
        url,
        socket_timeout=SOCKET_TIMEOUT_S,
        socket_connect_timeout=CONNECT_TIMEOUT_S,
        retry_on_timeout=False,
        health_check_interval=0,
    )


def breaker_open() -> bool:
    with _lock:
        return _open_until > _clock()


def get_client() -> Any | None:
    """The shared client, or None when ``REDIS_URL`` is unset or the breaker is open.

    Created lazily on first use; rebuilt if the URL changes (tests switch it).
    """
    global _client, _client_url
    url = Config.redis_url()
    if not url:
        return None
    with _lock:
        if _open_until > _clock():
            return None
        if _client is None or url != _client_url:
            try:
                _client = _build(url)
                _client_url = url
            except Exception as exc:  # bad URL or redis-py missing: treat like a failure
                _client = None
                _note_failure_locked(exc)
                return None
        return _client


def available() -> bool:
    """True when Redis is configured and not skipped by the breaker (it may still fail a call)."""
    return configured() and not breaker_open()


def _note_failure_locked(exc: BaseException) -> None:
    global _failures, _open_until
    _failures += 1
    if _failures >= FAILURES_TO_OPEN:
        _open_until = _clock() + OPEN_SECONDS
        _failures = 0
        _log.warning("event=redis_breaker_open error=%s skip_s=%d", type(exc).__name__, int(OPEN_SECONDS))


def record_failure(exc: BaseException) -> None:
    """Count one error (class name only is ever logged); opens the breaker after a run of them."""
    with _lock:
        _note_failure_locked(exc)


def record_success() -> None:
    global _failures
    with _lock:
        _failures = 0


def ping() -> str:
    """``ok`` or raises (the readiness probe). An open breaker raises :class:`RedisUnavailable`."""
    client = get_client()
    if client is None:
        raise RedisUnavailable("redis skipped (breaker open)")
    try:
        client.ping()
    except Exception as exc:
        record_failure(exc)
        raise
    record_success()
    return "ok"


def reset_for_tests(
    *, factory: Callable[[str], Any] | None = None, clock: Callable[[], float] | None = None
) -> None:
    """Forget the client and the breaker; optionally install a fake client factory and clock."""
    global _client, _client_url, _failures, _open_until, _factory, _clock
    with _lock:
        _client = None
        _client_url = ""
        _failures = 0
        _open_until = 0.0
        _factory = factory
        _clock = clock or time.monotonic
