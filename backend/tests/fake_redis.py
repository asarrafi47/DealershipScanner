"""A small in-memory Redis stand-in for the R2, R3 and R4 tests (no network, ever).

It speaks only the commands the app uses: sorted-set windows in a MULTI pipeline (R1, R2, R3),
plain strings with ``ex`` and ``nx`` (R2 cool-down, R4 cache), ``delete``, ``exists`` and
``flushall``. Every key's TTL is recorded so tests can assert that no key lives forever, and
``down = True`` makes every call raise, like a dead server.
"""

from __future__ import annotations

from typing import Any

import pytest


class FakeRedisError(Exception):
    pass


class FakeRedis:
    def __init__(self) -> None:
        self.z: dict[str, dict[str, float]] = {}
        self.s: dict[str, bytes] = {}
        self.ttl: dict[str, float] = {}
        self.expires_at: dict[str, float] = {}
        self.down = False
        self.calls = 0
        self.now = 1_700_000_000.0  # seconds; tests move it to expire string keys

    # ------------------------------------------------------------ plumbing

    def _hit(self) -> None:
        self.calls += 1
        if self.down:
            raise FakeRedisError("connection refused")

    def _expire_due(self) -> None:
        for k in [k for k, at in self.expires_at.items() if at <= self.now]:
            self.s.pop(k, None)
            self.z.pop(k, None)
            self.expires_at.pop(k, None)

    def keys_all(self) -> list[str]:
        return sorted(set(self.z) | set(self.s))

    def flushall(self) -> bool:
        self.z.clear()
        self.s.clear()
        self.ttl.clear()
        self.expires_at.clear()
        return True

    # ------------------------------------------------------------ commands

    def ping(self) -> bool:
        self._hit()
        return True

    def pipeline(self, transaction: bool = True) -> FakePipe:
        assert transaction is True
        return FakePipe(self)

    def zrem(self, key: str, member: str) -> int:
        self._hit()
        return 1 if self.z.get(key, {}).pop(member, None) is not None else 0

    def zpopmax(self, key: str) -> list:
        self._hit()
        s = self.z.get(key) or {}
        if not s:
            return []
        m = max(s, key=lambda k: s[k])
        return [(m, s.pop(m))]

    def get(self, key: str) -> bytes | None:
        self._hit()
        self._expire_due()
        return self.s.get(key)

    def set(self, key: str, value: Any, ex: int | float | None = None, nx: bool = False) -> bool | None:
        self._hit()
        self._expire_due()
        if nx and key in self.s:
            return None
        self.s[key] = value if isinstance(value, bytes) else str(value).encode("utf-8")
        if ex is not None:
            self.ttl[key] = ex
            self.expires_at[key] = self.now + float(ex)
        else:
            self.ttl.pop(key, None)
            self.expires_at.pop(key, None)
        return True

    def exists(self, key: str) -> int:
        self._hit()
        self._expire_due()
        return 1 if (key in self.s or key in self.z) else 0

    def delete(self, *keys: str) -> int:
        self._hit()
        n = 0
        for k in keys:
            n += int(self.s.pop(k, None) is not None) + int(self.z.pop(k, None) is not None)
            self.ttl.pop(k, None)
            self.expires_at.pop(k, None)
        return n


class FakePipe:
    def __init__(self, r: FakeRedis) -> None:
        self.r = r
        self.ops: list = []

    def zremrangebyscore(self, key, lo, hi):
        self.ops.append(("rem", key, hi))

    def zadd(self, key, mapping):
        self.ops.append(("add", key, mapping))

    def zcard(self, key):
        self.ops.append(("card", key))

    def expire(self, key, seconds):
        self.ops.append(("exp", key, seconds))

    def execute(self):
        self.r._hit()
        out = []
        for op in self.ops:
            s = self.r.z.setdefault(op[1], {})
            if op[0] == "rem":
                gone = [m for m, sc in s.items() if sc <= op[2]]
                for m in gone:
                    del s[m]
                out.append(len(gone))
            elif op[0] == "add":
                s.update(op[2])
                out.append(1)
            elif op[0] == "card":
                out.append(len(s))
            else:
                self.r.ttl[op[1]] = op[2]
                out.append(True)
        return out


def install(monkeypatch: pytest.MonkeyPatch) -> FakeRedis:
    """Point ``REDIS_URL`` at a fake host and make the shared client this fake. Undo with
    :func:`uninstall` (the fixtures below do)."""
    from backend.utils import ip_rate_limit as rl
    from backend.utils import redis_client as rc

    server = FakeRedis()
    breaker_clock = [1000.0]
    server.breaker_clock = breaker_clock
    monkeypatch.setenv("REDIS_URL", "redis://fake-host:6379/0")
    rc.reset_for_tests(factory=lambda url: server, clock=lambda: breaker_clock[0])
    rl.clear_rate_limit_state()
    monkeypatch.setattr(rl, "_wall", lambda: server.now)
    return server


def uninstall() -> None:
    from backend.utils import ip_rate_limit as rl
    from backend.utils import redis_client as rc

    rc.reset_for_tests()
    rl.clear_rate_limit_state()


@pytest.fixture
def fake_redis(monkeypatch):
    server = install(monkeypatch)
    yield server
    uninstall()


@pytest.fixture
def no_redis(monkeypatch):
    from backend.utils import ip_rate_limit as rl
    from backend.utils import redis_client as rc

    monkeypatch.delenv("REDIS_URL", raising=False)
    rc.reset_for_tests()
    rl.clear_rate_limit_state()
    yield
    rc.reset_for_tests()
    rl.clear_rate_limit_state()
