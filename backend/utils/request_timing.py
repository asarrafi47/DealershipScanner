"""Per-request duration accumulator behind the ``Server-Timing`` response header.

The web's request middleware (:mod:`backend.web.request_scope`) calls :func:`begin` when a
request enters and :func:`header_value` when its response starts. In between, cheap hooks
call :func:`add`: the inventory cursor (``db``), the Postgres connect (``db_connect``), the
users-DB open (``users_db``) and the Jinja render (``render``).

Outside a web request (the scanner, jobs, background threads, scripts) nothing was begun on
the thread, so :func:`add` is one ``getattr`` on a thread-local and returns. It never raises.

Only durations and counts ever leave this module: no SQL text, no parameters, no host names.
"""

from __future__ import annotations

import threading
import time

_tl = threading.local()

# Emitted in this order; anything else that was added follows alphabetically.
_ORDER = ("db", "db_connect", "users_db", "render")


def begin() -> None:
    """Start accumulating for the request on this thread (replaces any leftover state)."""
    _tl.acc = {}
    _tl.marks = []
    _tl.started = time.perf_counter()


def end() -> None:
    """Stop accumulating on this thread."""
    _tl.acc = None
    _tl.marks = None
    _tl.started = None


def active() -> bool:
    return getattr(_tl, "acc", None) is not None


def add(metric: str, dur_ms: float, count: int = 1) -> None:
    """Add ``dur_ms`` (and ``count`` events) to ``metric`` for the current request, if any."""
    acc = getattr(_tl, "acc", None)
    if acc is None:
        return
    try:
        slot = acc.get(metric)
        if slot is None:
            acc[metric] = [dur_ms, count]
        else:
            slot[0] += dur_ms
            slot[1] += count
    except Exception:  # noqa: BLE001 - observability must never break a request
        pass


def push_mark() -> None:
    """Remember now, for a later :func:`pop_mark` (signal pairs such as Jinja's render)."""
    marks = getattr(_tl, "marks", None)
    if marks is not None:
        marks.append(time.perf_counter())


def pop_mark(metric: str) -> None:
    """Add the time since the matching :func:`push_mark` to ``metric``."""
    marks = getattr(_tl, "marks", None)
    if marks:
        add(metric, (time.perf_counter() - marks.pop()) * 1000.0)


class timed:  # noqa: N801 - used as a lowercase context manager, like contextlib helpers
    """``with timed("db"): ...`` adds the block's wall time to ``metric`` (also on error)."""

    __slots__ = ("_metric", "_t0")

    def __init__(self, metric: str):
        self._metric = metric
        self._t0 = 0.0

    def __enter__(self) -> "timed":
        self._t0 = time.perf_counter()
        return self

    def __exit__(self, *exc) -> None:
        add(self._metric, (time.perf_counter() - self._t0) * 1000.0)


def snapshot() -> dict[str, tuple[float, int]]:
    """``{metric: (ms, count)}`` for the current request (empty when none is active)."""
    acc = getattr(_tl, "acc", None) or {}
    return {k: (v[0], v[1]) for k, v in acc.items()}


def _fmt(ms: float) -> str:
    return f"{ms:.1f}"


def header_value() -> str | None:
    """The ``Server-Timing`` value for the current request, or None when none is active.

    ``app`` is the wall time from :func:`begin` until now (the response is starting).
    Each metric carries its total duration and, as ``desc``, its event count.
    """
    acc = getattr(_tl, "acc", None)
    started = getattr(_tl, "started", None)
    if acc is None or started is None:
        return None
    try:
        parts = [f"app;dur={_fmt((time.perf_counter() - started) * 1000.0)}"]
        names = [n for n in _ORDER if n in acc] + sorted(n for n in acc if n not in _ORDER)
        for name in names:
            ms, count = acc[name]
            parts.append(f'{name};dur={_fmt(ms)};desc="{int(count)}x"')
        return ", ".join(parts)
    except Exception:  # noqa: BLE001
        return None
