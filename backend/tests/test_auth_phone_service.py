"""Card S6: the verification service, end to end on the real repository (Postgres tier).

One named test (at least) per acceptance bullet of card S6 and its review lines. The provider is
the fake one, Turnstile is a fake verifier, the clock is fake; nothing touches the network.
Test numbers are in the fictional +1 202 555 01xx range.
"""

from __future__ import annotations

import logging
import re
import threading
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from backend.auth.phone import limits as lim
from backend.auth.phone import numbers
from backend.auth.phone import service as svc_mod
from backend.auth.phone.fake_provider import FAKE_CODE, FakePhoneProvider
from backend.auth.phone.service import AuthPhoneService, ServiceConfig
from backend.auth.phone.turnstile import TurnstileResult
from backend.db.repositories import auth_phone_repo as repo

pytestmark = pytest.mark.pg

UTC = timezone.utc
T0 = datetime(2026, 11, 16, 17, 0, tzinfo=UTC)
WRONG = "123456"


def num(i: int) -> str:
    return "+120255501%02d" % i


class OkTurnstile:
    """Accepts ``pass``; anything else (or nothing) fails."""

    def verify(self, token, ip):
        return TurnstileResult(token == "pass", "ok" if token == "pass" else "rejected")


class Clock:
    def __init__(self, at: datetime = T0) -> None:
        self.at = at

    def __call__(self) -> datetime:
        return self.at

    def tick(self, **kw) -> None:
        self.at = self.at + timedelta(**kw)


class FakeAccounts:
    def __init__(self) -> None:
        self.by_email: dict[str, int] = {}
        self.calls = 0

    def adopt_or_create(self, *, email: str, username: str) -> int:
        self.calls += 1
        if email not in self.by_email:
            self.by_email[email] = 1000 + len(self.by_email)
        return self.by_email[email]


@pytest.fixture
def env(pg_db, monkeypatch):
    monkeypatch.setenv("PHONE_HASH_PEPPER", "t" * 40)
    monkeypatch.delenv("FLASK_ENV", raising=False)
    monkeypatch.delenv("SMS_ALERT_SEND_THRESHOLD", raising=False)
    return pg_db


def make(clock=None, limits=None, paused=False, provider=None, **cfg):
    provider = provider or FakePhoneProvider(sleeper=lambda s: None)
    clock = clock or Clock()
    lims = limits or lim.Limits()
    service = AuthPhoneService(
        provider,
        OkTurnstile(),
        repo,
        clock,
        ServiceConfig(**cfg),
        limits_loader=lambda: lims,
        sends_paused=lambda: paused,
    )
    return service, provider, clock


def start(service, i=1, *, ip=None, device="dev", nonce=None, **kw):
    kw.setdefault("consent", True)
    kw.setdefault("turnstile_token", "pass")
    dev = None if device is None else numbers.hash_device(device + str(i))
    return service.start(num(i), ip=ip or "10.0.%d.1" % i, device_hash=dev, session_nonce=nonce, **kw)


def known(i: int, user_id: int, *, last_login: datetime | None = None) -> repo.Identity:
    e164 = num(i)
    ident = repo.insert_identity(
        phone_e164=e164, phone_hash=numbers.hash_phone(e164), country="US", now=T0 - timedelta(days=1),
        consent_at=T0 - timedelta(days=1), consent_text_version="1", user_id=user_id, state="active",
    )
    if last_login is not None:
        repo.touch_login(ident.id, now=last_login)
    return ident


def fill(n: int, *, pool: str = "known", outcome: str = "sent", cost: int = 6, at: datetime | None = None) -> None:
    for k in range(n):
        repo.record_send(
            repo.SendRow(
                phone_hash="%064x" % (k + 1), ip_hash="%064x" % (k + 5000), ip_prefix_hash="%064x" % (k + 9000),
                country="US", purpose="seller_verify" if pool == "seller" else "login", pool=pool,
                has_device_cookie=True, device_hash="%064x" % (k + 7000), est_cost_cents=cost, outcome=outcome,
            ),
            now=at or T0 - timedelta(minutes=30),
        )


def rows() -> list[tuple]:
    from backend.db.connect import connect

    with connect() as c:
        return c.execute(
            "SELECT outcome, pool, est_cost_cents, lookup_cost_cents, has_device_cookie FROM auth_phone_sends ORDER BY id"
        ).fetchall()


# ------------------------------------------------------------ codes


def test_right_code_approves_once(env):
    s, prov, clock = make()
    out = start(s, 1)
    assert out.ok and len(prov.outbox) == 1
    dev = numbers.hash_device("dev1")
    first = s.check(out.public_id, out.session_nonce, FAKE_CODE, device_hash=dev)
    assert first.approved and first.phone_e164 == num(1)
    again = s.check(out.public_id, out.session_nonce, FAKE_CODE, device_hash=dev)
    assert not again.approved and again.reason == svc_mod.R_NO_CHALLENGE


def test_five_wrong_tries_lock_and_a_resend_does_not_unlock(env):
    s, prov, clock = make()
    out = start(s, 2)
    dev = numbers.hash_device("dev2")
    reasons = [s.check(out.public_id, out.session_nonce, WRONG, device_hash=dev).reason for _ in range(5)]
    assert reasons[:4] == [svc_mod.R_WRONG] * 4 and reasons[4] == svc_mod.R_LOCKED
    clock.tick(seconds=61)
    assert s.resend(out.public_id, out.session_nonce, turnstile_token="pass", ip="10.0.2.1", device_hash=dev).reason in (
        svc_mod.R_NO_CHALLENGE, svc_mod.R_LIMIT
    )
    fresh = start(s, 2, nonce=out.session_nonce)  # a brand-new code from the same browser
    assert not fresh.ok and fresh.reason == svc_mod.R_LIMIT
    clock.tick(minutes=16)  # the lock lasts 15 minutes
    assert start(s, 2, nonce=out.session_nonce).ok


def test_a_stranger_cannot_lock_the_real_holder_out_from_another_browser(env):
    s, prov, clock = make()
    holder = start(s, 3, device="holder")
    stranger = start(s, 3, device="stranger", ip="10.9.9.9")
    sdev = numbers.hash_device("stranger3")
    for _ in range(5):
        s.check(stranger.public_id, stranger.session_nonce, WRONG, device_hash=sdev)
    ok = s.check(holder.public_id, holder.session_nonce, FAKE_CODE, device_hash=numbers.hash_device("holder3"))
    assert ok.approved


def test_a_code_from_another_session_fails(env):
    s, prov, clock = make()
    out = start(s, 4)
    other = s.check(out.public_id, "another-browser-nonce", FAKE_CODE, device_hash=None)
    assert not other.approved and other.reason == svc_mod.R_NO_CHALLENGE
    assert prov.check_calls == 0


def test_expiry_is_exact_at_five_minutes(env):
    s, prov, clock = make()
    a = start(s, 5)
    b = start(s, 6)
    clock.tick(seconds=299)
    assert s.check(a.public_id, a.session_nonce, FAKE_CODE, device_hash=None).approved
    clock.tick(seconds=1)  # exactly 300 seconds after b was sent
    late = s.check(b.public_id, b.session_nonce, FAKE_CODE, device_hash=None)
    assert not late.approved and late.reason == svc_mod.R_EXPIRED


def test_resend_before_60_seconds_refuses(env):
    s, prov, clock = make()
    out = start(s, 7)
    dev = numbers.hash_device("dev7")
    clock.tick(seconds=59)
    early = s.resend(out.public_id, out.session_nonce, turnstile_token="pass", ip="10.0.7.1", device_hash=dev)
    assert early.reason == svc_mod.R_TOO_SOON and len(prov.outbox) == 1
    clock.tick(seconds=1)
    late = s.resend(out.public_id, out.session_nonce, turnstile_token="pass", ip="10.0.7.1", device_hash=dev)
    assert late.ok and len(prov.outbox) == 2
    assert s.check(out.public_id, out.session_nonce, FAKE_CODE, device_hash=dev).approved


def test_concurrent_checks_never_exceed_five_provider_checks(env):
    s, prov, clock = make()
    out = start(s, 8)
    barrier = threading.Barrier(12)

    def go():
        barrier.wait()
        s.check(out.public_id, out.session_nonce, WRONG, device_hash=None)

    threads = [threading.Thread(target=go) for _ in range(12)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert prov.check_calls <= 5


def test_network_error_on_check_refunds_the_attempt(env):
    s, prov, clock = make()
    out = start(s, 9)
    prov.fail_next_check(times=6)
    for _ in range(6):
        assert s.check(out.public_id, out.session_nonce, WRONG, device_hash=None).reason == svc_mod.R_RETRY
    assert s.check(out.public_id, out.session_nonce, FAKE_CODE, device_hash=None).approved


# ------------------------------------------------------------ what never sends


def test_no_consent_means_no_text(env):
    s, prov, clock = make()
    out = start(s, 10, consent=False)
    assert out.reason == svc_mod.R_CONSENT and prov.outbox == [] and rows() == []


def test_no_turnstile_means_no_text_and_is_recorded(env):
    s, prov, clock = make()
    for token in (None, "", "forged"):
        out = start(s, 11, turnstile_token=token)
        assert out.reason == svc_mod.R_TURNSTILE
    assert prov.outbox == [] and prov.lookup_calls == []
    assert [r[0] for r in rows()] == ["blocked_turnstile"] * 3


def test_a_non_us_number_sends_nothing(env):
    s, prov, clock = make()
    out = s.start(
        "+44 20 7946 0958", consent=True, turnstile_token="pass", ip="10.0.12.1", device_hash=None
    )
    assert out.reason == svc_mod.R_COUNTRY and out.sentence == svc_mod.COULD_NOT_SEND
    assert prov.outbox == [] and prov.lookup_calls == []
    assert rows()[0][0] == "blocked_country"


@pytest.mark.parametrize("kind", ["voip", "landline"])
def test_voip_and_landline_send_nothing_and_the_lookup_is_still_counted(env, kind):
    s, prov, clock = make()
    (prov.mark_voip if kind == "voip" else prov.mark_landline)(num(13))
    out = start(s, 13)
    assert out.reason == svc_mod.R_LINE and out.sentence == svc_mod.COULD_NOT_SEND
    assert prov.outbox == []
    outcome, pool, est, lookup, _ = rows()[-1]
    assert (outcome, est, lookup) == ("blocked_voip", Decimal(0), Decimal(1))
    # the refusal is cached: the next try costs no second lookup
    clock.tick(minutes=20)
    start(s, 13)
    assert prov.lookup_calls == [num(13)]


def test_sends_paused_stops_sends_with_the_unavailable_sentence(env):
    s, prov, clock = make(paused=True)
    out = start(s, 14)
    assert out.sentence == svc_mod.UNAVAILABLE and prov.outbox == []
    assert rows()[0][0] == "blocked_cap"


def test_allow_list_limits_who_may_sign_in(env):
    s, prov, clock = make(allowed_numbers=frozenset({num(15)}))
    assert start(s, 15).ok
    assert start(s, 16).reason == svc_mod.R_NOT_ALLOWED
    assert len(prov.outbox) == 1


def test_allow_list_that_parsed_to_nothing_refuses_everyone(env):
    # AUTH_PHONE_ALLOWED_NUMBERS set but no entry parsed: closed, not open to every number.
    s, prov, clock = make(allowed_numbers=frozenset(), allow_list_active=True)
    assert start(s, 15).reason == svc_mod.R_NOT_ALLOWED
    assert start(s, 16).reason == svc_mod.R_NOT_ALLOWED
    assert prov.outbox == []


def test_service_config_from_env_keeps_a_malformed_allow_list_closed(monkeypatch):
    monkeypatch.setenv("AUTH_PHONE_ALLOWED_NUMBERS", "notanumber")
    cfg = svc_mod.ServiceConfig.from_env()
    assert cfg.allow_list_active is True and cfg.allowed_numbers == frozenset()
    monkeypatch.delenv("AUTH_PHONE_ALLOWED_NUMBERS")
    assert svc_mod.ServiceConfig.from_env().allow_list_active is False


# ------------------------------------------------------------ limits and caps


def test_phone_limit_trips_at_exactly_two_in_fifteen_minutes(env):
    s, prov, clock = make()
    assert start(s, 17, nonce="n1", ip="10.1.0.1").ok
    clock.tick(seconds=1)
    assert start(s, 17, nonce="n2", ip="10.2.0.1", device="b").ok
    clock.tick(seconds=1)
    third = start(s, 17, nonce="n3", ip="10.3.0.1", device="c")
    assert third.reason == svc_mod.R_LIMIT and len(prov.outbox) == 2
    assert rows()[-1][0] == "blocked_limit"


def test_one_address_group_cannot_pass_six_sends(env):
    s, prov, clock = make(limits=replace(lim.Limits(), ip_per_hour=100, ip_per_day=100, new_pool_prefix_per_day=100))
    got = [start(s, 20 + k, ip="10.50.60.%d" % (k + 1)).ok for k in range(7)]
    assert got == [True] * 6 + [False]


def test_one_address_group_uses_at_most_two_of_the_new_number_pool(env):
    s, prov, clock = make()
    out = [start(s, 30 + k, ip="10.70.80.%d" % (k + 1)) for k in range(3)]
    assert [o.ok for o in out] == [True, True, False]
    assert out[2].sentence == svc_mod.BUSY


def test_site_cap_blocks_and_records(env):
    s, prov, clock = make()
    fill(30)
    out = start(s, 40)
    assert out.reason == svc_mod.R_BUSY and prov.outbox == []
    assert rows()[-1][0] == "blocked_cap"


def test_spend_cap_blocks_and_records(env):
    s, prov, clock = make(limits=replace(lim.Limits(), daily_send_cap=1000))
    fill(1, cost=295)
    out = start(s, 41)  # 295 + 6 + 1 lookup > 300
    assert out.reason == svc_mod.R_BUSY and prov.outbox == []
    assert rows()[-1][0] == "blocked_cap"


def test_twenty_concurrent_starts_at_cap_minus_one_let_exactly_one_through(env):
    s, prov, clock = make()
    fill(26, pool="known")
    fill(3, pool="seller")
    for k in range(20):
        known(50 + k, 500 + k)
    barrier = threading.Barrier(20)
    results = []
    lock = threading.Lock()

    def go(k):
        barrier.wait()
        r = start(s, 50 + k)
        with lock:
            results.append(r)

    threads = [threading.Thread(target=go, args=(k,)) for k in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sum(1 for r in results if r.ok) == 1
    assert len(prov.outbox) == 1


def test_with_the_new_number_pool_used_up_a_known_number_still_gets_a_code(env):
    s, prov, clock = make()
    fill(15, pool="new")
    assert start(s, 71).reason == svc_mod.R_BUSY
    known(72, 72)
    assert start(s, 72).ok


def test_shopper_share_used_up_a_dealer_code_still_goes_out_and_the_reverse(env):
    s, prov, clock = make()
    fill(27, pool="known")
    assert start(s, 73).reason == svc_mod.R_BUSY
    assert start(s, 74, purpose="seller_verify", user_id=74).ok


def test_reserved_pool_used_up_shoppers_still_sign_in(env):
    s, prov, clock = make()
    fill(3, pool="seller")
    seller = start(s, 75, purpose="seller_verify", user_id=75)
    assert seller.reason == svc_mod.R_BUSY and seller.sentence == svc_mod.SELLER_BUSY
    assert start(s, 76).ok


def test_one_phone_or_device_uses_at_most_one_send_of_the_reserved_pool(env):
    s, prov, clock = make()
    first = start(s, 77, purpose="seller_verify", user_id=77)
    assert first.ok
    clock.tick(seconds=61)
    again = s.resend(first.public_id, first.session_nonce, turnstile_token="pass", ip="10.0.77.1",
                     device_hash=numbers.hash_device("dev77"))
    assert again.reason == svc_mod.R_BUSY  # resends count inside the pool


def test_caps_and_pools_answer_byte_identically_for_known_and_unknown_numbers(env):
    s, prov, clock = make()
    known(80, 80)
    fill(30)  # site cap reached
    a = start(s, 80)
    b = start(s, 81)
    assert (a.ok, a.reason, a.sentence, a.retry_after_s) == (b.ok, b.reason, b.sentence, b.retry_after_s)
    assert a.public_id is None and b.public_id is None


@pytest.mark.parametrize("setup", ["send_cap", "spend_cap", "shopper_pool"])
def test_known_and_unknown_get_the_same_busy_reply(env, setup):
    limits = lim.Limits()
    if setup == "send_cap":
        fill(30)
    elif setup == "spend_cap":
        limits = replace(limits, daily_send_cap=1000)
        fill(1, cost=299)
    else:
        fill(27)
    s, prov, clock = make(limits=limits)
    known(82, 82)
    a, b = start(s, 82), start(s, 83)
    assert a == replace(b, session_nonce=a.session_nonce)
    assert a.sentence == svc_mod.BUSY


def test_identical_reply_words_for_known_and_unknown_numbers(env):
    s, prov, clock = make()
    known(84, 84)
    a, b = start(s, 84), start(s, 85)
    assert a.ok and b.ok
    assert a.sentence.replace("84", "xx") == b.sentence.replace("85", "xx")
    assert (a.reason, a.retry_after_s) == (b.reason, b.retry_after_s)


# ------------------------------------------------------------ provider down


def test_unavailable_costs_nothing_and_does_not_count_against_the_person(env):
    s, prov, clock = make()
    prov.fail_next_start("unavailable", times=3)
    for _ in range(3):
        assert start(s, 86).sentence == svc_mod.UNAVAILABLE
    assert all(r[0] == "unavailable" and r[2] == 0 for r in rows())
    assert start(s, 86).ok  # 3 tries of the same number, still not at its limit


def test_breaker_skips_the_vendor_for_five_minutes_after_five_unavailable(env):
    s, prov, clock = make()
    prov.fail_next_start("unavailable", times=5)
    for k in range(5):
        start(s, 87 + k)
    calls = prov.start_calls
    assert start(s, 92).sentence == svc_mod.UNAVAILABLE
    assert prov.start_calls == calls  # skipped
    clock.tick(minutes=5, seconds=1)
    assert start(s, 93).ok



def test_a_start_that_may_have_sent_stays_pending_and_counts_against_the_number(env):
    # Review R1: a read timeout after the request went out may have produced a paid text. The row stays
    # pending at its full cost, so the number's limit (2 in 15 minutes) and the day's caps see it.
    s, prov, clock = make()
    prov.fail_next_start("unavailable", times=2, maybe_sent=True)
    assert start(s, 80, nonce="a", ip="10.80.0.1").sentence == svc_mod.UNAVAILABLE
    assert start(s, 80, nonce="b", ip="10.81.0.1", device="x").sentence == svc_mod.UNAVAILABLE
    assert [(r[0], r[2]) for r in rows()] == [("pending", 6), ("pending", 6)]
    third = start(s, 80, nonce="c", ip="10.82.0.1", device="y")
    assert third.reason == svc_mod.R_LIMIT and prov.outbox == []


def test_maybe_sent_failures_open_the_breaker_like_unavailable(env):
    s, prov, clock = make()
    prov.fail_next_start("network_error", times=5, maybe_sent=True)
    for k in range(5):
        start(s, 87 + k)
    calls = prov.start_calls
    assert start(s, 92).sentence == svc_mod.UNAVAILABLE
    assert prov.start_calls == calls  # skipped
    clock.tick(minutes=5, seconds=1)
    assert start(s, 93).ok

def test_lookup_is_skipped_for_known_numbers_and_when_down_known_proceed(env):
    s, prov, clock = make()
    known(94, 94)
    prov.fail_next_lookup()
    assert start(s, 94).ok
    assert prov.lookup_calls == []
    out = start(s, 95)  # unknown number, lookup down
    assert out.sentence == svc_mod.UNAVAILABLE and prov.outbox[-1].to == num(94)


def test_opted_out_number_gets_its_own_sentence(env):
    s, prov, clock = make()
    prov.fail_next_start("opted_out")
    assert start(s, 96).sentence == svc_mod.OPTED_OUT


# ------------------------------------------------------------ device cookie


def test_without_a_device_cookie_the_group_gets_two_sends_then_a_reload(env):
    s, prov, clock = make()
    got = [start(s, 97 + k, device=None, ip="10.99.1.%d" % (k + 1)) for k in range(3)]
    assert [g.ok for g in got] == [True, True, False]
    assert got[2].sentence == svc_mod.RELOAD
    assert [r[4] for r in rows()] == [False, False, False]


# ------------------------------------------------------------ accounts


def _approved(s, i, **kw):
    out = start(s, i, **kw)
    return s.check(out.public_id, out.session_nonce, FAKE_CODE, device_hash=None)


@pytest.mark.parametrize("purpose", ["link", "claim", "step_up", "change", "seller_verify"])
def test_only_a_login_code_signs_anyone_in(env, purpose):
    # Review R8: a code proven for another flow never creates, adopts or signs in to an account, and
    # never runs the dormant detach.
    s, prov, clock = make()
    known(62, 62)
    accounts = FakeAccounts()
    for i in (62, 63):
        approved = svc_mod.CheckOutcome(True, svc_mod.R_APPROVED, "", phone_e164=num(i), purpose=purpose)
        out = s.sign_in(approved, accounts)
        assert (out.ok, out.reason, out.user_id) == (False, "wrong_purpose", None)
    assert accounts.calls == 0
    assert repo.find_identity_by_phone(num(63)) is None


def test_challenge_and_identity_reprs_never_show_the_number(env):
    s, prov, clock = make()
    ident = known(64, 64)
    assert num(64) not in repr(ident) and num(64)[2:] not in repr(ident)
    out = start(s, 65)
    ch = repo.fetch_challenge(out.public_id, out.session_nonce)
    assert ch is not None and ch.phone_e164 == num(65)
    assert num(65) not in repr(ch) and num(65)[2:] not in repr(ch)

def test_new_number_signs_up_and_second_visit_signs_in_as_the_same_account(env):
    s, prov, clock = make()
    accounts = FakeAccounts()
    first = s.sign_in(_approved(s, 60), accounts)
    assert first.reason == "signed_up"
    assert repo.find_identity_by_phone(num(60)).state == "active"
    clock.tick(hours=1)
    second = s.sign_in(_approved(s, 60, nonce="other"), accounts)
    assert second.reason == "signed_in" and second.user_id == first.user_id


def test_a_stale_reserving_identity_is_finished_by_adopting_the_same_account(env):
    s, prov, clock = make()
    accounts = FakeAccounts()
    ident = repo.insert_identity(
        phone_e164=num(61), phone_hash=numbers.hash_phone(num(61)), country="US", now=T0 - timedelta(minutes=5),
        consent_at=T0, consent_text_version="1",
    )
    email, _ = svc_mod.placeholder_for(ident)
    accounts.by_email[email] = 4242  # step 2 already ran before the crash
    out = s.sign_in(_approved(s, 61), accounts)
    assert (out.ok, out.user_id) == (True, 4242)


def test_a_fresh_reserving_identity_means_the_other_tab_is_still_working(env):
    s, prov, clock = make()
    repo.insert_identity(
        phone_e164=num(62), phone_hash=numbers.hash_phone(num(62)), country="US", now=T0,
        consent_at=T0, consent_text_version="1",
    )
    out = s.sign_in(_approved(s, 62), FakeAccounts())
    assert out == svc_mod.SignInOutcome(False, "retry")


def test_a_dormant_number_opens_a_fresh_account_not_the_old_one(env):
    s, prov, clock = make()
    known(63, 6300, last_login=T0 - timedelta(days=181))
    accounts = FakeAccounts()
    out = s.sign_in(_approved(s, 63), accounts)
    assert out.reason == "dormant_fresh" and out.user_id != 6300
    ident = repo.find_identity_by_phone(num(63))
    assert (ident.user_id, ident.dormant_user_id, ident.state) == (out.user_id, 6300, "active")


def test_a_number_used_recently_is_not_dormant(env):
    s, prov, clock = make()
    known(64, 6400, last_login=T0 - timedelta(days=179))
    assert s.sign_in(_approved(s, 64), FakeAccounts()).user_id == 6400


# ------------------------------------------------------------ logs


def test_captured_logs_hold_no_number_or_code(env, caplog):
    s, prov, clock = make()
    errors: list[str] = []
    with caplog.at_level(logging.DEBUG):
        out = start(s, 65)
        s.check(out.public_id, out.session_nonce, WRONG, device_hash=None)
        ok = s.check(out.public_id, out.session_nonce, FAKE_CODE, device_hash=None)
        s.sign_in(ok, FakeAccounts())
        prov.mark_voip(num(66))
        start(s, 66)
        start(s, 67, turnstile_token="bad")
        fill(30)
        start(s, 68)
        try:
            repo.insert_identity(phone_e164=num(65), phone_hash="x", country="US", now=T0, consent_at=T0,
                                 consent_text_version="1")
        except Exception as exc:  # noqa: BLE001
            errors.append(repr(exc) + str(exc))
    text = caplog.text + "".join(errors) + repr(out) + repr(ok)
    assert "event=start" in caplog.text and "event=blocked_cap" in caplog.text
    for i in (65, 66, 67, 68):
        assert num(i) not in text and num(i)[2:] not in text
    assert FAKE_CODE not in caplog.text and WRONG not in caplog.text
    assert not re.search(r"\d{7,}", caplog.text)
    assert not re.search(r"[0-9a-f]{64}", text)
    for m in re.finditer(r"\bh=(\S+)", caplog.text):
        assert len(m.group(1)) <= 6


# ------------------------------------------------------------ review fixes


def test_new_pool_refusals_cost_the_source_so_probing_for_accounts_is_not_free(env):
    s, prov, clock = make()
    fill(15, pool="new")
    known(10, 110)
    got = [start(s, 11 + k, ip="10.120.0.9", device="same").sentence for k in range(3)]
    assert got == [svc_mod.BUSY] * 3
    assert [r[0] for r in rows()[-3:]] == ["blocked_limit"] * 3
    # the address has spent its hour: even a known number gets the neutral refusal now
    assert start(s, 10, ip="10.120.0.9", device="same").reason == svc_mod.R_LIMIT


def test_a_lookup_outage_never_trips_the_breaker_for_known_numbers(env):
    s, prov, clock = make()
    for k in range(6):
        prov.fail_next_lookup()
        assert start(s, 20 + k).sentence == svc_mod.UNAVAILABLE
    known(27, 127)
    assert start(s, 27).ok
    assert prov.outbox[-1].to == num(27)


def test_one_account_cannot_use_two_sends_of_the_reserved_pool(env):
    s, prov, clock = make()
    assert start(s, 30, purpose="seller_verify", user_id=9).ok
    second = start(s, 31, purpose="seller_verify", user_id=9, ip="10.131.0.1", device="other")
    assert second.sentence == svc_mod.SELLER_BUSY and len(prov.outbox) == 1
    assert start(s, 32, purpose="seller_verify", user_id=10).ok


def test_a_refused_resend_does_not_stretch_the_old_code_past_five_minutes(env):
    s, prov, clock = make()
    out = start(s, 33)
    clock.tick(seconds=61)
    fill(30)  # site cap reached: the resend is refused
    refused = s.resend(out.public_id, out.session_nonce, turnstile_token="pass", ip="10.0.33.1",
                       device_hash=numbers.hash_device("dev33"))
    assert refused.sentence == svc_mod.BUSY
    clock.at = T0 + timedelta(seconds=300)
    late = s.check(out.public_id, out.session_nonce, FAKE_CODE, device_hash=None)
    assert late.reason == svc_mod.R_EXPIRED


def test_phone_wide_lock_after_fifteen_wrong_codes_from_several_browsers(env):
    s, prov, clock = make(limits=replace(lim.Limits(), phone_per_15min=100, phone_per_day=100, ip_per_hour=100,
                                         ip_per_day=100, prefix_per_day=100, new_pool_prefix_per_day=100))
    wrong = 0
    for b in range(4):  # 4 browsers, 4 wrong codes each: no browser reaches its own 5
        out = start(s, 40, device="b%d" % b, nonce="n%d" % b)
        dev = numbers.hash_device("b%d40" % b)
        for _ in range(4):
            wrong += 1
            r = s.check(out.public_id, out.session_nonce, WRONG, device_hash=dev)
            if wrong < lim.PHONE_WIDE_MAX_WRONG:
                assert r.reason == svc_mod.R_WRONG, wrong
            if wrong == lim.PHONE_WIDE_MAX_WRONG:
                break
        if wrong == lim.PHONE_WIDE_MAX_WRONG:
            break
    fresh = start(s, 40, device="new", nonce="new")
    assert fresh.reason == svc_mod.R_LIMIT
    clock.tick(minutes=lim.PHONE_WIDE_LOCK_MINUTES + 1)
    assert start(s, 40, device="new", nonce="new").ok


def test_half_cap_and_prepaid_alerts_log_once(env, caplog):
    topup = T0 - timedelta(days=1)
    s, prov, clock = make(limits=replace(lim.Limits(), topup_at=topup, prepaid_cents=1000, daily_spend_cap_cents=100000))
    fill(14, cost=35, at=T0 - timedelta(hours=1))  # 490 cents since the top-up
    with caplog.at_level(logging.INFO, logger="auth_phone"):
        start(s, 50)  # the 15th send of the day, spend 497
        start(s, 51)  # spend 504: past 50 percent of 1000
        start(s, 52)
    assert caplog.text.count("event=cap_50pct") == 1
    assert caplog.text.count("event=prepaid_50pct") == 1
    assert "event=prepaid_75pct" not in caplog.text


@pytest.mark.parametrize("state,expect", [("active", "signed_in"), ("reserving", "retry")])
def test_the_tab_that_loses_the_identity_race_signs_in_as_the_winner(env, monkeypatch, state, expect):
    s, prov, clock = make()
    ok = _approved(s, 60)
    ident = repo.insert_identity(
        phone_e164=num(60), phone_hash=numbers.hash_phone(num(60)), country="US", now=T0,
        consent_at=T0, consent_text_version="1",
    )
    if state == "active":
        repo.activate_identity(ident.id, 1600, now=T0)
    real_find = repo.find_identity_by_phone
    calls = []

    def first_miss(e164, **kw):
        calls.append(e164)
        return None if len(calls) == 1 else real_find(e164, **kw)

    monkeypatch.setattr(repo, "find_identity_by_phone", first_miss)
    out = s.sign_in(ok, FakeAccounts())
    assert out.reason == expect
    if state == "active":
        assert (out.ok, out.user_id) == (True, 1600)
