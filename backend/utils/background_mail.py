"""Account mail is sent in the background, off the request thread.

``/forgot-password`` and ``/resend-verification`` hand the send to :func:`submit`, which passes
it to a small bounded pool and returns at once, so the page never waits on the mail provider.

* At most ``_MAX_PENDING`` sends wait or run at a time; a send past that is dropped and logged
  (the per-IP and per-account caps run first).
* ``AUTH_MAIL_INLINE=1`` runs the send on the caller's thread (the offline test tier sets it,
  so a test can assert on the mail right after the request). :func:`drain` waits for queued
  sends (tests of the background path).
* A send that raises is logged by class name and never reaches the caller.
"""

from __future__ import annotations

import logging
import os
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Callable

_log = logging.getLogger(__name__)

_MAX_WORKERS = 2
_MAX_PENDING = 64

_lock = threading.Lock()
_executor: ThreadPoolExecutor | None = None
_slots = threading.BoundedSemaphore(_MAX_PENDING)
_pending: set[Future] = set()


def _inline() -> bool:
    return (os.environ.get("AUTH_MAIL_INLINE") or "").strip().lower() in ("1", "true", "yes", "on")


def _run(fn: Callable[..., Any], surface: str, args: tuple, kwargs: dict) -> None:
    try:
        fn(*args, **kwargs)
    except Exception as exc:  # noqa: BLE001 - a mail failure never reaches the request
        _log.warning("event=background_mail_failed surface=%s err=%s", surface, type(exc).__name__)


def _pool() -> ThreadPoolExecutor:
    global _executor
    with _lock:
        if _executor is None:
            _executor = ThreadPoolExecutor(max_workers=_MAX_WORKERS, thread_name_prefix="auth-mail")
        return _executor


def submit(fn: Callable[..., Any], *args: Any, surface: str = "mail", **kwargs: Any) -> bool:
    """Run ``fn(*args, **kwargs)`` off this thread. False only when the pool was full (dropped)."""
    if _inline():
        _run(fn, surface, args, kwargs)
        return True
    if not _slots.acquire(blocking=False):
        _log.warning("event=background_mail_dropped surface=%s reason=queue_full", surface)
        return False
    try:
        fut = _pool().submit(_run, fn, surface, args, kwargs)
    except Exception as exc:  # noqa: BLE001 - interpreter shutdown
        _slots.release()
        _log.warning("event=background_mail_dropped surface=%s err=%s", surface, type(exc).__name__)
        return False
    with _lock:
        _pending.add(fut)

    def _done(f: Future) -> None:
        with _lock:
            _pending.discard(f)
        _slots.release()

    fut.add_done_callback(_done)
    return True


def drain(timeout: float = 10.0) -> bool:
    """Wait for every queued send (tests). True when none is left."""
    import concurrent.futures

    with _lock:
        waiting = list(_pending)
    if not waiting:
        return True
    _done, not_done = concurrent.futures.wait(waiting, timeout=timeout)
    return not not_done
