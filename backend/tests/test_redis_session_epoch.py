"""Card R4: a shared Redis cache of ``valid_after`` (docs/auth_sms/TASKS.md, "Redis cards").

Acceptance, one or more named tests each:
- lookup order Redis, in-process cache, Postgres; TTL 5 s for admin and dealer, 30 s for shoppers
- every write of ``valid_after`` deletes (and re-sets) the user's key, so a revoked cookie is
  refused at once, also by another process that still holds the old answer in memory
- a Postgres error still fails closed for admin and dealer and open for shoppers
- a Redis error is just a cache miss, and Redis down never slows or fails a request
- ``REDIS_URL`` unset behaves exactly as before card R4; a flush only means "ask Postgres"
- keys start with ``sc:``, carry a TTL and hold no phone number, code or secret
A fake Redis stands in for the server; nothing touches the network.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from backend.auth.phone import session_epoch
from backend.tests import fake_redis
from backend.tests.test_session_lifetime import (  # noqa: F401  (fixtures)
    HOUR,
    NOW,
    _me,
    _sign_in,
    _user,
    clock,
    epochs,
    main,
)
from backend.utils import redis_client as rc


@pytest.fixture
def shared(monkeypatch, epochs):  # noqa: F811
    server = fake_redis.install(monkeypatch)
    monkeypatch.setattr(session_epoch, "_wall", lambda: server.now)
    session_epoch.clear_cache()
    yield server
    session_epoch.clear_cache()
    fake_redis.uninstall()


@pytest.fixture
def unset(monkeypatch, epochs):  # noqa: F811
    monkeypatch.delenv("REDIS_URL", raising=False)
    rc.reset_for_tests()
    session_epoch.clear_cache()
    yield
    session_epoch.clear_cache()


def _at(ts: float) -> datetime:
    return datetime.fromtimestamp(ts, tz=timezone.utc)


def _other_process_revokes(epochs, server, uid: int, when: float) -> None:  # noqa: F811
    """What a revoke in ANOTHER process leaves behind: the row written, the shared key gone.
    This process's in-process cache is not told."""
    epochs.rows[uid] = _at(when)
    server.delete(rc.key("sva", str(uid)))


# ---------------------------------------------------------------- lookup order and TTLs


def test_r4_a_redis_hit_answers_without_postgres(shared, epochs):  # noqa: F811
    epochs.rows[7] = _at(NOW - 100)
    assert session_epoch.get_valid_after(7, strict=False) == NOW - 100
    assert epochs.reads == 1
    session_epoch.clear_cache()  # a second process (or a restart): empty memory, same Redis
    assert session_epoch.get_valid_after(7, strict=False) == NOW - 100
    assert epochs.reads == 1


def test_r4_ttl_is_5_seconds_for_admin_and_dealer_and_30_for_shoppers(shared, epochs):  # noqa: F811
    session_epoch.get_valid_after(1, strict=True)
    session_epoch.get_valid_after(2, strict=False)
    assert shared.ttl[rc.key("sva", "1")] == 5
    assert shared.ttl[rc.key("sva", "2")] == 30


def test_r4_a_strict_session_never_trusts_an_entry_older_than_5_seconds(shared, epochs):  # noqa: F811
    session_epoch.get_valid_after(3, strict=False)  # a shopper-length entry (30 s)
    assert epochs.reads == 1
    shared.now += 6
    session_epoch.clear_cache()
    session_epoch.get_valid_after(3, strict=True)
    assert epochs.reads == 2  # 6 s old: too old for an admin or dealer session


def test_r4_a_flush_only_means_ask_postgres(shared, epochs):  # noqa: F811
    epochs.rows[4] = _at(NOW - 50)
    assert session_epoch.get_valid_after(4, strict=False) == NOW - 50
    shared.flushall()
    assert session_epoch.get_valid_after(4, strict=False) == NOW - 50
    assert epochs.reads == 2


# ---------------------------------------------------------------- writes delete the key


def test_r4_every_write_replaces_the_shared_key_so_the_next_read_is_new(shared, epochs):  # noqa: F811
    assert session_epoch.get_valid_after(5, strict=False) is None
    assert shared.exists(rc.key("sva", "5"))
    stored = session_epoch.revoke_user_sessions(5, reason="test", now=_at(NOW))
    assert stored == NOW
    session_epoch.clear_cache()
    assert session_epoch.get_valid_after(5, strict=False) == NOW


def test_r4_revoke_in_another_process_is_seen_at_once_despite_this_process_cache(shared, epochs):  # noqa: F811
    assert session_epoch.get_valid_after(6, strict=False) is None  # this process caches "no row"
    _other_process_revokes(epochs, shared, 6, NOW - 10)
    assert session_epoch.get_valid_after(6, strict=False) == NOW - 10


def test_r4_revoked_cookie_is_refused_at_once_through_the_session_hook(main, clock, shared, epochs):  # noqa: F811
    uid = _user("r4_rev", "general_user")
    with main.app.test_client() as c:
        _sign_in(c, uid, role="general_user", auth_at=NOW - HOUR)
        assert _me(c).status_code == 200
        _other_process_revokes(epochs, shared, uid, NOW - 60)
        assert _me(c).status_code == 401  # no 30-second wait


def test_r4_a_read_racing_a_write_cannot_put_the_old_answer_back(shared, epochs, monkeypatch):  # noqa: F811
    from backend.db.repositories import auth_phone_repo

    def slow_read(user_id, **_k):
        # A revoke lands while this read is in flight: the read already saw "no row".
        epochs.rows[int(user_id)] = _at(NOW)
        session_epoch.revoke_user_sessions(int(user_id), reason="test", now=_at(NOW))
        return None

    monkeypatch.setattr(auth_phone_repo, "sessions_valid_after", slow_read)
    assert session_epoch.get_valid_after(8, strict=False) is None
    monkeypatch.setattr(auth_phone_repo, "sessions_valid_after", epochs.read)
    session_epoch.clear_cache()
    assert session_epoch.get_valid_after(8, strict=False) == NOW


def test_r4_a_write_while_redis_is_down_stops_this_process_trusting_redis_for_that_user(shared, epochs):  # noqa: F811
    session_epoch.get_valid_after(9, strict=False)  # Redis now holds "no row" for 30 s
    shared.down = True
    assert session_epoch.revoke_user_sessions(9, reason="test", now=_at(NOW)) == NOW
    shared.down = False
    rc.reset_for_tests(factory=lambda url: shared, clock=lambda: shared.breaker_clock[0])
    assert session_epoch.get_valid_after(9, strict=False) == NOW  # the stale key is skipped


# ---------------------------------------------------------------- failures


def test_r4_a_redis_error_is_just_a_cache_miss(shared, epochs):  # noqa: F811
    epochs.rows[10] = _at(NOW - 5)
    shared.down = True
    assert session_epoch.get_valid_after(10, strict=True) == NOW - 5
    assert session_epoch.get_valid_after(10, strict=True) == NOW - 5  # in-process cache now
    assert epochs.reads == 1


def test_r4_redis_down_never_slows_or_fails_a_request(main, clock, shared, epochs):  # noqa: F811
    shared.down = True
    uid = _user("r4_down", "general_user")
    with main.app.test_client() as c:
        _sign_in(c, uid, role="general_user", auth_at=NOW - HOUR)
        for _ in range(10):
            assert _me(c).status_code == 200
    # The breaker opened after 3 errors: later requests did not call Redis at all.
    assert shared.calls <= rc.FAILURES_TO_OPEN


def test_r4_postgres_error_still_fails_closed_for_admin_and_dealer(main, clock, shared, epochs):  # noqa: F811
    epochs.fail_reads = True
    for name, role in (("r4_fc_admin", "admin"), ("r4_fc_dealer", "dealership_owner")):
        uid = _user(name, role)
        with main.app.test_client() as c:
            _sign_in(c, uid, role=role, auth_at=NOW - HOUR)
            assert _me(c).status_code == 401, role


def test_r4_postgres_error_still_fails_open_for_shoppers(main, clock, shared, epochs):  # noqa: F811
    epochs.fail_reads = True
    uid = _user("r4_fo_shop", "general_user")
    with main.app.test_client() as c:
        _sign_in(c, uid, role="general_user", auth_at=NOW - HOUR)
        assert _me(c).status_code == 200


# ---------------------------------------------------------------- unset is today


def test_r4_unset_redis_url_behaves_exactly_as_before(main, clock, unset, epochs, monkeypatch):  # noqa: F811
    built: list[str] = []
    rc.reset_for_tests(factory=lambda url: built.append(url))
    uid = _user("r4_unset", "general_user")
    with main.app.test_client() as c:
        _sign_in(c, uid, role="general_user", auth_at=NOW - HOUR)
        for _ in range(3):
            assert _me(c).status_code == 200
        assert epochs.reads == 1
        clock.mono += 31
        _me(c)
        assert epochs.reads == 2
    assert built == []  # no client was ever built


# ---------------------------------------------------------------- keys


def test_r4_keys_are_prefixed_expire_and_hold_no_secret(shared, epochs):  # noqa: F811
    epochs.rows[11] = _at(NOW - 1)
    session_epoch.get_valid_after(11, strict=False)
    session_epoch.revoke_user_sessions(12, reason="test", now=_at(NOW))
    keys = shared.keys_all()
    assert keys and all(k.startswith("sc:") for k in keys)
    assert all(k in shared.ttl and shared.ttl[k] > 0 for k in keys)
    for k in keys:
        value = shared.s[k].decode("ascii")
        assert set(value) <= set("0123456789.|-"), value
