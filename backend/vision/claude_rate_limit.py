"""
Process-wide Anthropic API rate limiting for scanner/enrichment vision calls.

Haiku tier is often ~50 RPM; default cap is 42 RPM with jittered acquire.
Every Anthropic call goes through ``backend.llm.client.complete``, which takes one
slot here per attempt; ``anthropic_messages_create`` remains as a back-compat shim.
"""
from __future__ import annotations

import logging
import os
import random
import threading
import time
from typing import Any, Callable, TypeVar

logger = logging.getLogger(__name__)

_T = TypeVar("_T")

_lock = threading.Lock()
_tokens: float = 0.0
_last_refill: float = 0.0


def _rpm_cap() -> float:
    try:
        return max(5.0, min(120.0, float((os.environ.get("ANTHROPIC_VISION_RPM") or "42").strip())))
    except (TypeError, ValueError):
        return 42.0


def _max_retries() -> int:
    try:
        return max(1, min(8, int((os.environ.get("ANTHROPIC_VISION_MAX_RETRIES") or "5").strip())))
    except (TypeError, ValueError):
        return 5


def acquire_vision_slot(*, cost: float = 1.0) -> None:
    """Block until a vision API slot is available (token bucket)."""
    global _tokens, _last_refill
    rpm = _rpm_cap()
    rate = rpm / 60.0
    while True:
        with _lock:
            now = time.monotonic()
            if _last_refill <= 0:
                _last_refill = now
                _tokens = rpm
            elapsed = now - _last_refill
            if elapsed > 0:
                _tokens = min(rpm, _tokens + elapsed * rate)
                _last_refill = now
            if _tokens >= cost:
                _tokens -= cost
                return
            need = cost - _tokens
            wait_s = need / rate if rate > 0 else 1.0
        time.sleep(min(30.0, wait_s) + random.uniform(0.02, 0.12))


def anthropic_messages_create(client: Any, **kwargs: Any) -> Any:
    """
    Rate-limited ``client.messages.create`` (back-compat shim).

    New code calls ``backend.llm.client.complete``. This keeps the old surface
    but runs under the same single retry policy (429/5xx/overloaded/connection,
    jittered backoff, this module's limiter per attempt).
    """
    from backend.llm.client import call_with_retry

    value, _attempts = call_with_retry(lambda: client.messages.create(**kwargs))
    return value


def run_with_vision_limit(fn: Callable[[], _T]) -> _T:
    """Acquire one slot then run *fn* (for raw HTTP vision paths)."""
    acquire_vision_slot()
    return fn()
