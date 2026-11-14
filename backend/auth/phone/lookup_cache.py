"""A small in-process line-type cache for tests and development (see ``LookupCache``)."""

from __future__ import annotations

import time
from collections.abc import Callable

from backend.auth.phone.provider import LineInfo


class MemoryLookupCache:
    """Bounded, expiring, process-local. Not a production store: it forgets on restart."""

    def __init__(self, *, clock: Callable[[], float] = time.time, max_entries: int = 10000) -> None:
        self._clock = clock
        self._max = max_entries
        self._rows: dict[str, tuple[float, LineInfo]] = {}

    def get(self, phone_hash: str) -> LineInfo | None:
        row = self._rows.get(phone_hash)
        if row is None:
            return None
        if row[0] <= self._clock():
            self._rows.pop(phone_hash, None)
            return None
        return row[1]

    def put(self, phone_hash: str, info: LineInfo, ttl_s: int) -> None:
        if len(self._rows) >= self._max and phone_hash not in self._rows:
            self._rows.pop(next(iter(self._rows)))
        self._rows[phone_hash] = (self._clock() + ttl_s, info)
