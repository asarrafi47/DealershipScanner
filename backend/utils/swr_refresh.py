"""Single-flight background refresh for in-process aggregates (web speed P6).

Five fleet aggregates used to be reloaded on the shopper's request when their TTL ran
out (rarity counts 600 s, price plausibility 300 s, deal-score bands 300 s, the public
count and featured cars, the dealer fallback URLs): the first car page after a quiet
spell paid 1.8 to 3.4 s, and four concurrent callers ran the same loader four times.

The contract every one of them now follows:

* cold (nothing loaded yet): load inline, once -- the cold fallback;
* loaded but past its TTL: return the OLD value at once and start ONE background
  reload; callers that arrive while it runs do not start another;
* a failing reload keeps the old value and is retried after ``RETRY_AFTER_S``, not on
  every request.

This module is only the "start one background run" piece. Each aggregate keeps its
value in its own module variables (tests and the Postgres harness reset those by
name), and calls :meth:`BackgroundRefresher.kick` when it finds the value stale.
"""

from __future__ import annotations

import logging
import threading
from typing import Callable

_log = logging.getLogger(__name__)

# After a failed background reload, wait this long before the next attempt.
RETRY_AFTER_S = 30.0


class BackgroundRefresher:
    """Runs at most one background job at a time for one cached value."""

    def __init__(self, name: str) -> None:
        self.name = name
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self.last_error: BaseException | None = None

    def kick(self, job: Callable[[], None]) -> bool:
        """Start *job* on a daemon thread unless one is already running.

        Returns True when a new run started. Never blocks and never raises.
        """
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return False
            t = threading.Thread(
                target=self._run, args=(job,), name=f"swr-{self.name}", daemon=True
            )
            self._thread = t
            try:
                t.start()
            except RuntimeError:  # interpreter shutting down
                self._thread = None
                return False
            return True

    def _run(self, job: Callable[[], None]) -> None:
        try:
            job()
            self.last_error = None
        except Exception as exc:  # a refresh must never take a thread's traceback to stderr
            self.last_error = exc
            _log.warning("background refresh of %s failed; keeping the old value", self.name, exc_info=True)

    def running(self) -> bool:
        t = self._thread
        return t is not None and t.is_alive()

    def join(self, timeout: float | None = None) -> bool:
        """Wait for the current run (tests, warm-up). True when nothing is running after."""
        t = self._thread
        if t is not None:
            t.join(timeout)
        return not self.running()
