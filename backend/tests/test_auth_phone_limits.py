"""Card S6: send limits, pools and caps (offline; the decision is pure over a window of rows).

Acceptance bullets covered here: limits trip at exactly their numbers; the site cap and the spend
cap block; settings rows raise the cap within the cache time, a row above the ceiling is clamped
and logged, and with no row and no variable the cap is 30; the day boundary is midnight Eastern;
``unavailable`` and cap outcomes do not count against a person. Hashes are made-up hex.
"""

from __future__ import annotations

import logging
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from backend import settings_store as ss
from backend.auth.phone import limits as lim

UTC = timezone.utc
NOW = datetime(2026, 11, 16, 17, 0, tzinfo=UTC)  # 12:00 in New York
L = lim.Limits()
PH = "a1" * 32
IP = "c3" * 32
PFX = "d4" * 32
DEV = "e5" * 32


class FakeWindow:
    """The same counting rules as ``auth_phone_repo.SendWindow``, over a list of dicts."""

    def __init__(self) -> None:
        self.rows: list[dict] = []

    def add(self, n: int = 1, *, at: datetime = NOW - timedelta(minutes=1), outcome: str = "sent", **kw) -> None:
        base = dict(
            phone_hash="f0" * 32, ip_hash="f1" * 32, ip_prefix_hash="f2" * 32, device_hash="f3" * 32,
            pool="known", has_device_cookie=True, cost=Decimal(6),
        )
        base.update(kw)
        for _ in range(n):
            self.rows.append(dict(base, at=at, outcome=outcome))

    def _match(self, r, since, until, filters, outcomes=None, exclude=None):
        if r["at"] < since or (until is not None and r["at"] >= until):
            return False
        if outcomes and r["outcome"] not in outcomes:
            return False
        if exclude and r["outcome"] in exclude:
            return False
        return all(r[k] == v for k, v in filters.items())

    def count(self, *, since, until=None, outcomes=None, exclude_outcomes=None, **filters):
        return sum(1 for r in self.rows if self._match(r, since, until, filters, outcomes, exclude_outcomes))

    def cost_cents(self, *, since, until=None, **filters):
        return sum((r["cost"] for r in self.rows if self._match(r, since, until, filters)), Decimal(0))


def ask(**kw) -> lim.SendAsk:
    base = dict(phone_hash=PH, ip_hash=IP, ip_prefix_hash=PFX, device_hash=DEV, pool="known", cost_cents=Decimal(6))
    base.update(kw)
    return lim.SendAsk(**base)


def decide(w, a=None, limits=L, now=NOW):
    return lim.decide(w, a or ask(), limits, now)


# ------------------------------------------------------------ per-subject limits, at exactly their numbers


def test_phone_limit_two_in_15_minutes_trips_at_exactly_two():
    w = FakeWindow()
    w.add(1, phone_hash=PH, at=NOW - timedelta(minutes=10))
    assert decide(w) is None
    w.add(1, phone_hash=PH, at=NOW - timedelta(minutes=5))
    assert decide(w) == lim.BLOCKED_LIMIT


def test_phone_limit_three_a_day_trips_at_exactly_three():
    w = FakeWindow()
    w.add(2, phone_hash=PH, at=NOW - timedelta(hours=5))
    assert decide(w) is None
    w.add(1, phone_hash=PH, at=NOW - timedelta(hours=3))
    assert decide(w) == lim.BLOCKED_LIMIT
    # rolling: a send 24 hours and one second ago no longer counts
    w2 = FakeWindow()
    w2.add(3, phone_hash=PH, at=NOW - timedelta(hours=24, seconds=1))
    assert decide(w2) is None


def test_ip_limits_three_an_hour_and_five_a_day():
    w = FakeWindow()
    w.add(2, ip_hash=IP, at=NOW - timedelta(minutes=30))
    assert decide(w) is None
    w.add(1, ip_hash=IP, at=NOW - timedelta(minutes=20))
    assert decide(w) == lim.BLOCKED_LIMIT
    w = FakeWindow()
    w.add(4, ip_hash=IP, at=NOW - timedelta(hours=5))
    assert decide(w) is None
    w.add(1, ip_hash=IP, at=NOW - timedelta(hours=4))
    assert decide(w) == lim.BLOCKED_LIMIT


def test_device_limit_three_a_day():
    w = FakeWindow()
    w.add(2, device_hash=DEV, at=NOW - timedelta(hours=3))
    assert decide(w) is None
    w.add(1, device_hash=DEV, at=NOW - timedelta(hours=2))
    assert decide(w) == lim.BLOCKED_LIMIT


def test_address_group_limit_six_a_day():
    w = FakeWindow()
    w.add(5, ip_prefix_hash=PFX, at=NOW - timedelta(hours=3))
    assert decide(w) is None
    w.add(1, ip_prefix_hash=PFX, at=NOW - timedelta(hours=2))
    assert decide(w) == lim.BLOCKED_LIMIT


def test_no_cookie_bucket_two_a_day_per_group():
    w = FakeWindow()
    w.add(1, ip_prefix_hash=PFX, has_device_cookie=False, device_hash=None)
    assert decide(w, ask(device_hash=None)) is None
    w.add(1, ip_prefix_hash=PFX, has_device_cookie=False, device_hash=None)
    assert decide(w, ask(device_hash=None)) == lim.BLOCKED_LIMIT
    assert decide(w, ask()) is None  # a browser with its device cookie is not in that bucket


def test_unavailable_and_cap_outcomes_do_not_count_against_a_person():
    w = FakeWindow()
    for outcome in ("unavailable", "blocked_cap"):
        w.add(10, outcome=outcome, phone_hash=PH, ip_hash=IP, device_hash=DEV, ip_prefix_hash=PFX, cost=Decimal(0))
    assert decide(w) is None


def test_turnstile_and_limit_refusals_never_use_up_someone_elses_number():
    w = FakeWindow()
    for outcome in ("blocked_turnstile", "blocked_limit", "blocked_country"):
        w.add(5, outcome=outcome, phone_hash=PH, cost=Decimal(0))
    assert decide(w) is None


# ------------------------------------------------------------ pools and site caps


def test_site_send_cap_blocks_at_thirty():
    w = FakeWindow()
    w.add(27, pool="known")
    w.add(2, pool="seller")
    assert decide(w, ask(pool="seller")) is None
    w.add(1, pool="seller")
    assert decide(w, ask(pool="seller")) == lim.BLOCKED_CAP


def test_shopper_share_is_twenty_seven_and_reserved_pool_is_three():
    assert (L.shopper_sends, L.reserved_sends, L.new_pool_sends, L.resend_pool_sends) == (27, 3, 15, 6)
    w = FakeWindow()
    w.add(26, pool="known")
    assert decide(w) is None
    w.add(1, pool="known")
    assert decide(w) == lim.BLOCKED_CAP
    assert decide(w, ask(pool="seller")) is None  # the reserved pool is still open


def test_reserved_pool_is_never_used_by_shoppers_and_caps_at_three():
    w = FakeWindow()
    w.add(3, pool="seller")
    assert decide(w, ask(pool="seller")) == lim.BLOCKED_CAP
    assert decide(w) is None  # shoppers unaffected


def test_reserved_pool_one_send_per_phone_and_per_device():
    w = FakeWindow()
    w.add(1, pool="seller", phone_hash=PH)
    assert decide(w, ask(pool="seller")) == lim.BLOCKED_CAP
    w = FakeWindow()
    w.add(1, pool="seller", device_hash=DEV)
    assert decide(w, ask(pool="seller")) == lim.BLOCKED_CAP


def test_new_number_pool_fifteen_and_two_per_address_group():
    w = FakeWindow()
    w.add(14, pool="new")
    assert decide(w, ask(pool="new")) is None
    w.add(1, pool="new")
    assert decide(w, ask(pool="new")) == lim.BLOCKED_CAP
    assert decide(w, ask(pool="known")) is None  # a known number still gets a code
    w = FakeWindow()
    w.add(2, pool="new", ip_prefix_hash=PFX, ip_hash="99" * 32, device_hash="98" * 32)
    assert decide(w, ask(pool="new")) == lim.BLOCKED_CAP


def test_resend_pool_six():
    w = FakeWindow()
    w.add(5, pool="resend")
    assert decide(w, ask(pool="resend")) is None
    w.add(1, pool="resend")
    assert decide(w, ask(pool="resend")) == lim.BLOCKED_CAP


def test_spend_cap_three_hundred_cents_blocks():
    w = FakeWindow()
    w.add(1, pool="known", cost=Decimal(294))
    assert decide(w) is None  # 294 + 6 = 300, not over
    w.add(1, pool="known", cost=Decimal("0.5"), outcome="blocked_voip")
    assert decide(w) == lim.BLOCKED_CAP


def test_prepaid_guard_stops_at_ninety_percent_since_topup():
    topup = NOW - timedelta(days=3)
    limits = replace(L, topup_at=topup, daily_spend_cap_cents=100000)
    w = FakeWindow()
    w.add(1, at=topup + timedelta(hours=1), cost=Decimal(1794))
    assert decide(w, limits=limits) is None  # 1794 + 6 = 1800 = 90 percent of 2000
    w.add(1, at=topup + timedelta(hours=2), cost=Decimal(1))
    assert decide(w, limits=limits) == lim.BLOCKED_CAP
    w2 = FakeWindow()
    w2.add(1, at=topup - timedelta(hours=1), cost=Decimal(1900))  # before the top-up: not counted
    assert decide(w2, limits=limits) is None


def test_cap_day_starts_at_midnight_eastern_including_clock_changes():
    assert lim.day_start(NOW, "America/New_York") == datetime(2026, 11, 16, 5, 0, tzinfo=UTC)
    summer = datetime(2026, 7, 1, 3, 59, tzinfo=UTC)  # 23:59 on June 30 in New York (EDT)
    assert lim.day_start(summer, "America/New_York") == datetime(2026, 6, 30, 4, 0, tzinfo=UTC)
    fall_back = datetime(2026, 11, 1, 12, 0, tzinfo=UTC)  # the day clocks go back
    assert lim.day_start(fall_back, "America/New_York") == datetime(2026, 11, 1, 4, 0, tzinfo=UTC)
    # a send from before midnight Eastern does not use today's cap
    w = FakeWindow()
    w.add(30, at=datetime(2026, 11, 16, 4, 59, tzinfo=UTC), phone_hash="77" * 32, ip_hash="76" * 32,
          ip_prefix_hash="75" * 32, device_hash="74" * 32)
    assert decide(w, limits=replace(L, prefix_per_day=1000, ip_per_day=1000)) is None


# ------------------------------------------------------------ settings rows (card S6 review (b))


@pytest.fixture
def clean_settings(monkeypatch):
    for name in (
        "SMS_DAILY_SEND_CAP", "SMS_DAILY_SEND_HARD_MAX", "SMS_DAILY_SPEND_CAP_CENTS", "SMS_DAILY_SPEND_HARD_MAX_CENTS",
        "SMS_NEW_NUMBER_SHARE_PCT", "SMS_DEALER_SIGNUP_SHARE_PCT", "SMS_PREPAID_CENTS", "SMS_TOPUP_AT",
        "CHAT_RESET_TZ",
    ):
        monkeypatch.delenv(name, raising=False)
    ss.set_loader(lambda: {})
    yield
    ss.set_loader(None)
    ss.set_clock(None)


def test_with_no_row_and_no_variable_the_cap_is_thirty(clean_settings):
    got = lim.load_limits()
    assert (got.daily_send_cap, got.daily_spend_cap_cents) == (30, 300)
    assert got == lim.Limits()


def test_a_row_raises_the_cap_within_the_cache_time_without_restart(clean_settings):
    t = [1000.0]
    ss.set_clock(lambda: t[0])
    rows = {}
    ss.set_loader(lambda: dict(rows))
    ss.set_clock(lambda: t[0])
    assert lim.load_limits().daily_send_cap == 30
    rows["sms.daily_send_cap"] = "60"
    t[0] += ss.CACHE_SECONDS + 1
    assert lim.load_limits().daily_send_cap == 60


def test_a_row_above_the_ceiling_is_clamped_and_logged(clean_settings, caplog):
    ss.set_loader(lambda: {"sms.daily_send_cap": "5000", "sms.daily_spend_cap_cents": "90000"})
    with caplog.at_level(logging.WARNING):
        got = lim.load_limits()
    assert (got.daily_send_cap, got.daily_spend_cap_cents) == (100, 600)
    assert "SMS_DAILY_SEND_HARD_MAX" in caplog.text
    assert ss.resolve("sms.daily_send_cap") == (100, "ceiling")


def test_the_ceiling_itself_is_environment_only(clean_settings, monkeypatch):
    monkeypatch.setenv("SMS_DAILY_SEND_HARD_MAX", "200")
    ss.set_loader(lambda: {"sms.daily_send_cap": "150", "SMS_DAILY_SEND_HARD_MAX": "9999"})
    assert lim.load_limits().daily_send_cap == 150


def test_per_subject_limits_are_settings_rows(clean_settings):
    ss.set_loader(lambda: {"sms.phone_per_day": "1", "sms.prefix_per_day": "9"})
    got = lim.load_limits()
    assert (got.phone_per_day, got.prefix_per_day) == (1, 9)
