"""Wall and thread-CPU laps for the upsert breakdown (P11A.7).

``stats['timing']`` records, per component, ``<name>_s`` (``perf_counter`` wall)
and ``<name>_cpu_s`` (``time.thread_time``). Wall time includes vPIC network
I/O and GIL waits, so it does not prove CPU cost; the pair does. Laps add into
the dict, so a component entered repeatedly (a per-car loop) accumulates.
"""
from __future__ import annotations

import time
from contextlib import contextmanager
from typing import Iterator

_ROUND = 4


def add_lap(timing: dict | None, name: str, wall: float, cpu: float) -> None:
    if timing is None:
        return
    timing[f"{name}_s"] = round(timing.get(f"{name}_s", 0.0) + wall, _ROUND)
    timing[f"{name}_cpu_s"] = round(timing.get(f"{name}_cpu_s", 0.0) + cpu, _ROUND)


@contextmanager
def lap(timing: dict | None, name: str) -> Iterator[None]:
    """Add the elapsed wall and thread-CPU seconds of the block to ``timing``."""
    if timing is None:
        yield
        return
    w0, c0 = time.perf_counter(), time.thread_time()
    try:
        yield
    finally:
        add_lap(timing, name, time.perf_counter() - w0, time.thread_time() - c0)
