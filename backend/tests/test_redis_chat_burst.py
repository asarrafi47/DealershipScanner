"""Card R3: the chat burst guard on Redis (docs/auth_sms/TASKS.md, "Redis cards").

Acceptance, one or more named tests each:
- the per-minute burst guard of the four chat routes uses the R1 backend through the one
  function card C3 made (``chat_budget.web._allow``), so every worker shares one count
- Redis down: the in-process burst guard decides, with no exception and no slow request
- ``REDIS_URL`` unset behaves exactly as before; a Redis flush mid-test changes no limit
- the daily money buckets stay in Postgres through ``guard(...)``: Redis is never asked
- keys start with ``sc:``, hash the address and expire
A fake Redis stands in for the server; nothing touches the network.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from backend.chat_budget import web as chat_web
from backend.tests.fake_redis import fake_redis, no_redis  # noqa: F401  (fixtures)
from backend.tests.test_chat_limits_routes import clock, fakes, settings  # noqa: F401  (fixtures)
from backend.utils import ip_rate_limit as rl
from backend.utils import redis_client as rc

ROOT = Path(__file__).resolve().parents[2]
IP = "192.0.2.10"


@pytest.fixture
def two_per_car(monkeypatch):
    monkeypatch.setenv("RATE_LIMIT_CAR_CHAT_PER_MIN", "2")
    monkeypatch.setenv("RATE_LIMIT_CAR_CHAT_PER_IP_PER_MIN", "50")
    monkeypatch.setenv("RATE_LIMIT_CAR_CHAT_GLOBAL_PER_MIN", "0")


def _car(pair: str = "5") -> bool:
    return chat_web.burst_allowed(chat_web.BURST_CAR, IP, pair=pair)


def test_r3_the_burst_guard_is_shared_through_redis_across_workers(fake_redis, two_per_car):  # noqa: F811
    assert _car() and _car()
    rl.clear_rate_limit_state()  # another worker (or a restart): empty memory, same Redis
    assert not _car()
    assert _car("6")  # another car


def test_r3_every_chat_scope_reaches_redis_through_the_one_function(fake_redis, two_per_car):  # noqa: F811
    for scope in (chat_web.BURST_CAR, chat_web.BURST_COMPARE, chat_web.BURST_SIDEBAR, chat_web.BURST_NARRATE):
        before = fake_redis.calls
        assert chat_web.burst_allowed(scope, IP, pair="5")
        assert fake_redis.calls > before, scope


def test_r3_redis_down_falls_back_to_the_in_process_guard(fake_redis, two_per_car):  # noqa: F811
    fake_redis.down = True
    assert _car() and _car()
    assert not _car()  # the in-process window still holds the limit


def test_r3_redis_down_never_slows_or_fails_a_request(fake_redis, two_per_car):  # noqa: F811
    fake_redis.down = True
    for i in range(20):
        chat_web.burst_allowed(chat_web.BURST_SIDEBAR, "192.0.2.%d" % (i + 1))
    assert rc.breaker_open()
    assert fake_redis.calls <= rc.FAILURES_TO_OPEN  # skipped after the breaker opened


def test_r3_a_redis_flush_mid_test_raises_no_limit(fake_redis, two_per_car):  # noqa: F811
    assert _car() and _car()
    fake_redis.flushall()
    assert not _car()  # memory still counts; Redis can only add refusals


def test_r3_unset_redis_url_equals_the_in_process_guard(no_redis, two_per_car):  # noqa: F811
    built: list[str] = []
    rc.reset_for_tests(factory=lambda url: built.append(url))
    got = [_car() for _ in range(4)]
    rl.clear_rate_limit_state()
    ref = [rl._allow_request_memory("chat:%s:5" % IP, max_events=2, window_seconds=60.0) for _ in range(4)]
    assert got == ref == [True, True, False, False]
    assert built == []


def test_r3_burst_keys_hash_the_address_and_expire(fake_redis, two_per_car):  # noqa: F811
    _car()
    chat_web.burst_allowed(chat_web.BURST_NARRATE, IP)
    keys = fake_redis.keys_all()
    assert keys and all(k.startswith("sc:rl:") for k in keys)
    assert all(IP not in k for k in keys)
    assert all(fake_redis.ttl.get(k, 0) > 0 for k in keys)


def test_r3_daily_money_buckets_never_touch_redis(fake_redis):  # noqa: F811
    """guard(...) and its store keep the daily buckets in Postgres; they do not import Redis."""
    for path in sorted((ROOT / "backend" / "chat_budget").glob("*.py")):
        if path.name == "web.py":
            continue
        src = path.read_text(encoding="utf-8")
        assert "redis" not in src.lower(), path.name
        assert "ip_rate_limit" not in src, path.name
    web_src = (ROOT / "backend" / "chat_budget" / "web.py").read_text(encoding="utf-8")
    assert "redis_client" not in web_src  # the burst guard reaches Redis only through ip_rate_limit


# ---------------------------------------------------------------- the daily buckets (Postgres tier)


@pytest.mark.pg
@pytest.mark.usefixtures("session_users_exist")
@pytest.mark.parametrize("redis_state", ["flushed", "down"])
def test_r3_flush_or_outage_mid_test_changes_no_daily_chat_cap(
    pg_db, settings, clock, fakes, app_factory, fake_redis, redis_state  # noqa: F811
):
    from backend.tests.test_chat_limits_routes import _app, _client, car

    c = _client(_app(app_factory))
    for i in range(10):
        assert car(c, cid=1 + i % 3, text=f"q{i}").status_code == 200, i
    if redis_state == "flushed":
        fake_redis.flushall()
    else:
        fake_redis.down = True
    rl.clear_rate_limit_state()  # even a restart on top: the hour's count lives in Postgres
    rv = car(c, cid=4, text="q11")
    assert rv.status_code == 429
    assert rv.get_json()["error"] == "user_chat_limit_reached"
    assert fakes.count("car") == 10
