"""Card R1: the shared Redis client and the Redis sliding window of the IP rate limiter.

A fake Redis stands in for the server; no test touches the network. Acceptance bullets:
REDIS_URL unset gives identical results to today; Redis down falls back with no exception and no
slow request; the window math holds at its edges; keys are prefixed, hashed and expire;
/api/ready reports the rate-limit backend.
"""

from __future__ import annotations

import time

import pytest

from backend.utils import ip_rate_limit as rl
from backend.utils import redis_client as rc


class FakeRedisError(Exception):
    pass


class FakeRedis:
    """Sorted sets, TTLs and MULTI pipelines: just what the limiter uses."""

    def __init__(self) -> None:
        self.z: dict[str, dict[str, float]] = {}
        self.ttl: dict[str, int] = {}
        self.down = False
        self.calls = 0

    def _hit(self):
        self.calls += 1
        if self.down:
            raise FakeRedisError("connection refused")

    def ping(self):
        self._hit()
        return True

    def pipeline(self, transaction=True):
        assert transaction is True
        return FakePipe(self)

    def zrem(self, key, member):
        self._hit()
        return 1 if self.z.get(key, {}).pop(member, None) is not None else 0

    def zpopmax(self, key):
        self._hit()
        s = self.z.get(key) or {}
        if not s:
            return []
        m = max(s, key=lambda k: s[k])
        return [(m, s.pop(m))]

    def flushall(self):
        self.z.clear()
        self.ttl.clear()


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


@pytest.fixture
def fake(monkeypatch):
    server = FakeRedis()
    clock = [1000.0]
    monkeypatch.setenv("REDIS_URL", "redis://fake-host:6379/0")
    rc.reset_for_tests(factory=lambda url: server, clock=lambda: clock[0])
    rl.clear_rate_limit_state()
    wall = [1_700_000_000.0]
    monkeypatch.setattr(rl, "_wall", lambda: wall[0])
    server.wall = wall
    server.clock = clock
    yield server
    rc.reset_for_tests()
    rl.clear_rate_limit_state()


@pytest.fixture
def unset(monkeypatch):
    monkeypatch.delenv("REDIS_URL", raising=False)
    rc.reset_for_tests()
    rl.clear_rate_limit_state()
    yield
    rl.clear_rate_limit_state()


def test_unset_gives_identical_results_to_the_memory_limiter(unset):
    got = [rl.allow_request("k", max_events=3, window_seconds=60) for _ in range(5)]
    rl.clear_rate_limit_state()
    ref = [rl._allow_request_memory("k", max_events=3, window_seconds=60) for _ in range(5)]
    assert got == ref == [True, True, True, False, False]
    assert rc.get_client() is None and rl.backend_name() == "memory"


def test_unset_never_builds_a_client(unset, monkeypatch):
    built = []
    rc.reset_for_tests(factory=lambda url: built.append(url))
    rl.allow_request("k", max_events=1, window_seconds=1)
    rl.refund_event("k")
    assert built == []


def test_redis_window_is_shared_across_processes(fake):
    assert [rl.allow_request("ip:1", max_events=2, window_seconds=60) for _ in range(2)] == [True, True]
    rl.clear_rate_limit_state()  # a second worker (or a restart) has an empty memory window
    assert rl.allow_request("ip:1", max_events=2, window_seconds=60) is False
    assert rl.backend_name() == "redis"


def test_window_math_at_the_edges(fake):
    w = fake.wall
    assert rl.allow_request("e", max_events=2, window_seconds=10)
    w[0] += 5
    assert rl.allow_request("e", max_events=2, window_seconds=10)
    rl.clear_rate_limit_state()
    w[0] += 4.999  # the first event is 9.999 s old: still inside the window
    assert rl.allow_request("e", max_events=2, window_seconds=10) is False
    w[0] += 0.001  # exactly 10 s old: it has left the window
    assert rl.allow_request("e", max_events=2, window_seconds=10) is True


def test_a_refused_event_is_not_counted(fake):
    for _ in range(5):
        rl.allow_request("r", max_events=1, window_seconds=60)
        rl.clear_rate_limit_state()
    key = rl._redis_key("r")
    assert len(fake.z[key]) == 1


def test_keys_are_prefixed_hashed_and_expire(fake):
    rl.allow_request("login:203.0.113.7", max_events=5, window_seconds=60)
    (key,) = fake.z
    assert key.startswith("sc:rl:") and "203.0.113.7" not in key
    assert fake.ttl[key] == 61
    for member in fake.z[key]:
        assert "203.0.113.7" not in member


def test_redis_down_falls_back_without_exception_or_delay(fake):
    fake.down = True
    started = time.monotonic()
    got = [rl.allow_request("d", max_events=2, window_seconds=60) for _ in range(4)]
    assert got == [True, True, False, False]  # the memory window decided
    assert time.monotonic() - started < 0.5


def test_breaker_skips_redis_after_repeated_errors_then_retries(fake):
    fake.down = True
    for _ in range(rc.FAILURES_TO_OPEN):
        rl.allow_request("b", max_events=100, window_seconds=60)
    calls = fake.calls
    for _ in range(10):
        rl.allow_request("b", max_events=100, window_seconds=60)
    assert fake.calls == calls  # skipped while open
    assert rl.backend_name() == "memory"
    fake.down = False
    fake.clock[0] += rc.OPEN_SECONDS + 1
    assert rl.allow_request("b2", max_events=100, window_seconds=60)
    assert fake.calls == calls + 1 and rl.backend_name() == "redis"


def test_redis_can_only_make_it_stricter_never_looser(fake):
    fake.flushall()  # a flush or eviction
    got = [rl.allow_request("s", max_events=2, window_seconds=60) for _ in range(3)]
    assert got == [True, True, False]  # memory still refuses the third
    fake.flushall()
    assert rl.allow_request("s", max_events=2, window_seconds=60) is False


def test_refund_undoes_both_windows(fake):
    assert rl.allow_request("f", max_events=1, window_seconds=60)
    rl.refund_event("f")
    assert rl.allow_request("f", max_events=1, window_seconds=60)


def test_client_has_short_timeouts():
    assert rc.SOCKET_TIMEOUT_S <= 0.2 and rc.CONNECT_TIMEOUT_S <= 0.2
    assert rc.FAILURES_TO_OPEN <= 5 and rc.OPEN_SECONDS == 30


def test_real_client_is_built_lazily_with_the_short_timeouts(monkeypatch):
    monkeypatch.setenv("REDIS_URL", "redis://127.0.0.1:1/0")
    rc.reset_for_tests()
    client = rc.get_client()  # building connects to nothing
    kwargs = client.connection_pool.connection_kwargs
    assert kwargs["socket_timeout"] == rc.SOCKET_TIMEOUT_S
    assert kwargs["socket_connect_timeout"] == rc.CONNECT_TIMEOUT_S
    assert rc.get_client() is client
    rc.reset_for_tests()


def test_ready_reports_the_rate_limit_backend(fake, monkeypatch):
    from backend import main

    monkeypatch.delenv("PGVECTOR_URL", raising=False)
    with main.app.test_client() as c:
        data = c.get("/api/ready").get_json()
    assert data["checks"]["redis"] == "ok"
    assert data["rate_limit_backend"] == "redis"
    fake.down = True
    with main.app.test_client() as c:
        data = c.get("/api/ready").get_json()
    assert data["checks"]["redis"].startswith("error: ")
    assert data["ready"] is True


def test_ready_reports_memory_when_unset(unset, monkeypatch):
    from backend import main

    monkeypatch.delenv("PGVECTOR_URL", raising=False)
    with main.app.test_client() as c:
        data = c.get("/api/ready").get_json()
    assert data["rate_limit_backend"] == "memory"
    assert data["checks"]["redis"] == "skipped"


def test_no_redis_key_or_value_carries_a_phone_number(fake):
    rl.allow_request("phone:+12025550123", max_events=5, window_seconds=60)
    for key, members in fake.z.items():
        assert "2025550123" not in key
        assert all("2025550123" not in m for m in members)
