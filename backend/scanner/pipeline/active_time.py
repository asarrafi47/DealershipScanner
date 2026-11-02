"""Pause-safe elapsed time for batch timeouts (N9.13).

The 2026-10-13 fleet lost 21.7 h to a Mac sleep; batch timeouts that counted that
time killed 8 batches (rc=-9) and left 21 dealers with a misleading error verdict.
``ActiveTimer`` is polled: each tick charges at most ``max_step`` seconds of the
time since the previous tick and books the rest as paused. That holds on any
clock - macOS ``time.monotonic`` already stops during sleep, a clock that keeps
running through suspend (or a SIGSTOP of this process) shows up as one huge gap.
"""
from __future__ import annotations

import time
from typing import Callable


class ActiveTimer:
    def __init__(self, *, max_step: float = 30.0, mono: Callable[[], float] = time.monotonic,
                 wall: Callable[[], float] = time.time) -> None:
        self._mono, self._wall, self.max_step = mono, wall, max_step
        self._last = mono()
        self._wall0 = wall()
        self.active = 0.0
        self.paused = 0.0

    def tick(self) -> float:
        """Charge the time since the last tick (capped) and return active seconds so far."""
        now = self._mono()
        gap = max(0.0, now - self._last)
        self._last = now
        step = min(gap, self.max_step)
        self.active += step
        self.paused += gap - step
        return self.active

    def stats(self) -> dict[str, float]:
        """Wall seconds next to active seconds; wall - active is the pause (sleep) time."""
        return {"wall_seconds": round(self._wall() - self._wall0, 1), "active_seconds": round(self.active, 1)}
