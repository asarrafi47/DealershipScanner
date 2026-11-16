"""Card R2: Redis burst speed bumps on phone start and check (docs/auth_sms/TASKS.md, "Redis cards").

Acceptance, one or more named tests each:
- per phone hash, per device hash and per /24 (/48) group burst limits on start and check, plus a
  resend cool-down, as speed bumps in front of the Postgres counts
- a Redis "no" gives the same neutral reply as a cap, byte-identical for known and unknown numbers
- a Redis "no" costs 0 cents, writes no send row and does NOT count against the Postgres
  per-phone, device or address limits; a check bump reserves no attempt
- Redis flushed mid-test changes no cap (Postgres keeps every cap, pool, lock and attempt)
- ``REDIS_URL`` unset behaves exactly as today; Redis down never slows or fails a request and
  leaves Postgres alone to decide
- keys start with ``sc:``, expire, and hold no phone number, code or secret
A fake Redis stands in for the server and the fake phone provider for Twilio; nothing touches
the network. Test numbers are in the fictional +1 202 555 01xx range.
"""

from __future__ import annotations

import logging

import pytest

from backend.auth.phone import burst
from backend.auth.phone import numbers
from backend.auth.phone import service as svc_mod
from backend.auth.phone.fake_provider import FAKE_CODE
from backend.tests import fake_redis as fr
from backend.tests.fake_redis import fake_redis, no_redis  # noqa: F401  (fixtures)
from backend.utils import redis_client as rc

PHONE_H = "a" * 64
DEV_H = "b" * 64
GROUP_H = "c" * 64


def _start(**kw) -> bool:
    args = dict(phone_hash=PHONE_H, device_hash=DEV_H, group_hash=GROUP_H)
    args.update(kw)
    return burst.start_allowed(**args)


# ---------------------------------------------------------------- the bumps alone (offline)


@pytest.mark.parametrize("subject", ["phone", "device", "group"])
def test_r2_start_bump_trips_at_its_number_for_each_subject(fake_redis, subject):  # noqa: F811
    limit = burst.START_LIMITS[subject]
    others = {"phone_hash": PHONE_H, "device_hash": DEV_H, "group_hash": GROUP_H}
    vary = {"phone": "phone_hash", "device": "device_hash", "group": "group_hash"}[subject]
    results = []
    for i in range(limit + 1):
        # Only the subject under test stays the same; the other two change every call.
        args = {k: ("%064x" % (i + 1 + 100 * n)) for n, k in enumerate(others)}
        args[vary] = others[vary]
        results.append(burst.start_allowed(**args))
    assert results == [True] * limit + [False]


@pytest.mark.parametrize("subject", ["phone", "device", "group"])
def test_r2_check_bump_trips_at_its_number_for_each_subject(fake_redis, subject):  # noqa: F811
    limit = burst.CHECK_LIMITS[subject]
    vary = {"phone": "phone_hash", "device": "device_hash", "group": "group_hash"}[subject]
    keys = ("phone_hash", "device_hash", "group_hash")
    results = []
    for i in range(limit + 1):
        args = {k: ("%064x" % (i + 1 + 100 * n)) for n, k in enumerate(keys)}
        args[vary] = "d" * 64
        results.append(burst.check_allowed(**args))
    assert results == [True] * limit + [False]


def test_r2_the_window_is_one_minute(fake_redis):  # noqa: F811
    for _ in range(burst.START_LIMITS["phone"]):
        assert _start(device_hash=None, group_hash=None)
    assert not _start(device_hash=None, group_hash=None)
    fake_redis.now += burst.WINDOW_S + 0.001
    assert _start(device_hash=None, group_hash=None)


def test_r2_unset_redis_url_never_builds_a_client_and_always_says_go_on(no_redis):  # noqa: F811
    built: list[str] = []
    rc.reset_for_tests(factory=lambda url: built.append(url))
    assert all(_start() for _ in range(50))
    assert all(burst.check_allowed(phone_hash=PHONE_H, device_hash=DEV_H, group_hash=GROUP_H) for _ in range(50))
    assert burst.cooldown_active("pub-1") is False
    burst.start_cooldown("pub-1", 60)
    assert built == [] and burst.configured() is False


def test_r2_redis_down_says_go_on_without_error_or_delay(fake_redis):  # noqa: F811
    fake_redis.down = True
    assert all(_start() for _ in range(20))
    burst.start_cooldown("pub-2", 60)
    assert burst.cooldown_active("pub-2") is False
    assert rc.breaker_open()
    assert fake_redis.calls <= rc.FAILURES_TO_OPEN  # the breaker skipped every later call


def test_r2_resend_cool_down_runs_for_its_seconds(fake_redis):  # noqa: F811
    assert burst.cooldown_active("pub-3") is False
    burst.start_cooldown("pub-3", 60)
    assert burst.cooldown_active("pub-3") is True
    fake_redis.now += 60
    assert burst.cooldown_active("pub-3") is False


def test_r2_keys_are_prefixed_hashed_expire_and_hold_no_phone_or_code(fake_redis):  # noqa: F811
    _start()
    burst.check_allowed(phone_hash=PHONE_H, device_hash=DEV_H, group_hash=GROUP_H)
    burst.start_cooldown("public-id-xyz", 60)
    keys = fake_redis.keys_all()
    assert keys and all(k.startswith("sc:") for k in keys)
    assert all(fake_redis.ttl.get(k, 0) > 0 for k in keys)
    blob = " ".join(keys) + " ".join(m for z in fake_redis.z.values() for m in z) + " ".join(
        v.decode() for v in fake_redis.s.values()
    )
    # (A code never reaches this module: check takes hashes only.)
    for secret in (PHONE_H, DEV_H, GROUP_H, "public-id-xyz", "2025550101"):
        assert secret not in blob


# ---------------------------------------------------------------- through the service (Postgres tier)

pg = pytest.mark.pg


@pytest.fixture
def svc_env(request, monkeypatch):
    request.getfixturevalue("pg_db")
    monkeypatch.setenv("PHONE_HASH_PEPPER", "t" * 40)
    monkeypatch.delenv("FLASK_ENV", raising=False)
    monkeypatch.delenv("SMS_ALERT_SEND_THRESHOLD", raising=False)


def _svc():
    from backend.tests import test_auth_phone_service as t

    return t


def _small_bumps(monkeypatch):
    monkeypatch.setattr(burst, "START_LIMITS", {"phone": 1, "device": 50, "group": 50})
    monkeypatch.setattr(burst, "CHECK_LIMITS", {"phone": 1, "device": 50, "group": 50})


@pg
def test_r2_a_bump_reply_is_byte_identical_to_the_cap_reply_for_known_and_unknown(svc_env, monkeypatch):
    t = _svc()
    s, prov, _ = t.make()
    # The Postgres cap reply, Redis unset (fill the day: every caller gets the busy sentence).
    t.fill(30)
    cap_reply = t.start(s, 1)
    assert cap_reply.reason == svc_mod.R_BUSY
    from backend.db.connect import connect

    with connect() as c:
        c.execute("DELETE FROM auth_phone_sends")
        c.commit()
    # Now the bump: one number with an account, one without, each over its per-phone bump.
    fr.install(monkeypatch)
    try:
        _small_bumps(monkeypatch)
        t.known(2, user_id=4242)
        assert t.start(s, 2).ok and t.start(s, 3).ok
        known_reply = t.start(s, 2, device="other-a")
        unknown_reply = t.start(s, 3, device="other-b")
    finally:
        fr.uninstall()
    assert known_reply == unknown_reply == cap_reply
    assert (known_reply.ok, known_reply.reason, known_reply.sentence) == (False, svc_mod.R_BUSY, svc_mod.BUSY)
    assert len(prov.outbox) == 2


@pg
def test_r2_a_bump_writes_no_row_costs_nothing_and_counts_against_no_postgres_limit(svc_env, monkeypatch):
    t = _svc()
    s, prov, _ = t.make()
    server = fr.install(monkeypatch)
    try:
        _small_bumps(monkeypatch)
        assert t.start(s, 5, ip="10.9.9.1").ok
        before = t.rows()
        for _ in range(5):
            assert t.start(s, 5, ip="10.9.9.1").reason == svc_mod.R_BUSY
        assert t.rows() == before  # no row, no cost, nothing for the Postgres counts
        server.flushall()
        # Postgres allows a number 2 sends in 15 minutes: the bumps above did not use the second.
        assert t.start(s, 5, ip="10.9.9.1").ok
        assert len(prov.outbox) == 2
    finally:
        fr.uninstall()


@pg
def test_r2_redis_flushed_mid_test_changes_no_cap(svc_env, monkeypatch):
    t = _svc()
    s, prov, _ = t.make()
    server = fr.install(monkeypatch)
    try:
        # Per-phone limit (2 in 15 minutes) is Postgres: a flush between sends does not reset it.
        assert t.start(s, 6).ok
        server.flushall()
        assert t.start(s, 6).ok
        server.flushall()
        third = t.start(s, 6)
        assert not third.ok and third.reason == svc_mod.R_LIMIT
        # The site cap is Postgres too.
        t.fill(30)
        server.flushall()
        assert t.start(s, 7).reason == svc_mod.R_BUSY
        assert len(prov.outbox) == 2
    finally:
        fr.uninstall()


@pg
def test_r2_unset_redis_url_equals_today(svc_env, monkeypatch, no_redis):  # noqa: F811
    t = _svc()
    built: list[str] = []
    rc.reset_for_tests(factory=lambda url: built.append(url))
    s, prov, clock = t.make()
    outs = [t.start(s, 8) for _ in range(4)]
    assert [o.reason for o in outs] == [svc_mod.R_SENT, svc_mod.R_SENT, svc_mod.R_LIMIT, svc_mod.R_LIMIT]
    first = outs[0]
    dev = numbers.hash_device("dev8")
    for _ in range(3):
        assert s.check(first.public_id, first.session_nonce, t.WRONG, device_hash=dev, ip="10.0.8.1").reason == svc_mod.R_WRONG
    assert built == []


@pg
def test_r2_redis_down_never_fails_a_start_or_check(svc_env, monkeypatch):
    t = _svc()
    s, prov, _ = t.make()
    server = fr.install(monkeypatch)
    server.down = True
    try:
        out = t.start(s, 9)
        assert out.ok
        dev = numbers.hash_device("dev9")
        ok = s.check(out.public_id, out.session_nonce, FAKE_CODE, device_hash=dev, ip="10.0.9.1")
        assert ok.approved
        assert server.calls <= rc.FAILURES_TO_OPEN
    finally:
        fr.uninstall()


@pg
def test_r2_a_check_bump_reserves_no_attempt_and_says_the_same_for_every_number(svc_env, monkeypatch):
    t = _svc()
    s, prov, _ = t.make()
    server = fr.install(monkeypatch)
    try:
        _small_bumps(monkeypatch)
        t.known(10, user_id=4343)
        a = t.start(s, 10)
        b = t.start(s, 11)
        replies = []
        for out, i in ((a, 10), (b, 11)):
            dev = numbers.hash_device("dev%d" % i)
            assert s.check(out.public_id, out.session_nonce, t.WRONG, device_hash=dev).reason == svc_mod.R_WRONG
            replies.append(s.check(out.public_id, out.session_nonce, t.WRONG, device_hash=dev))
        assert replies[0] == replies[1]
        assert (replies[0].reason, replies[0].sentence) == (svc_mod.R_BUSY, svc_mod.TRY_AGAIN)
        from backend.db.connect import connect

        with connect() as c:
            attempts = c.execute(
                "SELECT attempts FROM auth_phone_challenges WHERE public_id = %s", (a.public_id,)
            ).fetchone()[0]
        assert attempts == 1  # the bumped check reserved nothing
        server.flushall()
        dev = numbers.hash_device("dev10")
        assert s.check(a.public_id, a.session_nonce, FAKE_CODE, device_hash=dev).approved
    finally:
        fr.uninstall()


@pg
def test_r2_resend_cool_down_refuses_with_the_postgres_reply_until_it_ends(svc_env, monkeypatch):
    t = _svc()
    s, prov, clock = t.make()
    server = fr.install(monkeypatch)
    try:
        out = t.start(s, 12)
        dev = numbers.hash_device("dev12")
        clock.tick(seconds=61)  # the Postgres gate would let it through now
        early = s.resend(out.public_id, out.session_nonce, turnstile_token="pass", ip="10.0.12.1", device_hash=dev)
        assert (early.ok, early.reason, early.sentence) == (False, svc_mod.R_TOO_SOON, svc_mod.RESEND_TOO_SOON)
        assert len(prov.outbox) == 1
        server.now += 61
        later = s.resend(out.public_id, out.session_nonce, turnstile_token="pass", ip="10.0.12.1", device_hash=dev)
        assert later.ok and len(prov.outbox) == 2
    finally:
        fr.uninstall()


@pg
def test_r2_resend_bump_gives_the_cap_reply_and_no_row(svc_env, monkeypatch):
    t = _svc()
    s, prov, clock = t.make()
    server = fr.install(monkeypatch)
    try:
        _small_bumps(monkeypatch)
        out = t.start(s, 13)
        dev = numbers.hash_device("dev13")
        clock.tick(seconds=61)
        server.delete(burst._cool_key(out.public_id))  # the cool-down is over; the minute window is not
        before = t.rows()
        again = s.resend(out.public_id, out.session_nonce, turnstile_token="pass", ip="10.0.13.1", device_hash=dev)
        assert (again.ok, again.reason, again.sentence) == (False, svc_mod.R_BUSY, svc_mod.BUSY)
        assert t.rows() == before and len(prov.outbox) == 1
    finally:
        fr.uninstall()


@pg
def test_r2_bump_logs_hold_no_number_or_code(svc_env, monkeypatch, caplog):
    t = _svc()
    s, prov, _ = t.make()
    fr.install(monkeypatch)
    try:
        _small_bumps(monkeypatch)
        with caplog.at_level(logging.INFO, logger="auth_phone"):
            out = t.start(s, 14)
            t.start(s, 14)
            dev = numbers.hash_device("dev14")
            s.check(out.public_id, out.session_nonce, t.WRONG, device_hash=dev)
            s.check(out.public_id, out.session_nonce, FAKE_CODE, device_hash=dev)
    finally:
        fr.uninstall()
    text = caplog.text
    assert "blocked_burst" in text
    assert t.num(14) not in text and "2025550114" not in text and FAKE_CODE not in text
    assert numbers.hash_phone(t.num(14)) not in text  # only a 6-character prefix may appear


def test_r2_service_reads_redis_only_through_the_burst_module():
    src = svc_mod.__file__
    with open(src, encoding="utf-8") as f:
        text = f.read()
    assert "redis_client" not in text and "ip_rate_limit" not in text
    assert text.count("burst.start_allowed(") == 2  # start and resend
    assert text.count("burst.check_allowed(") == 1
