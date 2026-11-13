"""C2: the budget gate. Database tests are in the Postgres tier; a fake clock is used throughout."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any

import pytest

from backend import settings_store as ss
from backend.chat_budget import ChatDenied, guard
from backend.chat_budget import messages as msg
from backend.db.repositories import chat_usage_repo as repo
from backend.llm import client as llm

UTC = timezone.utc
NY = "America/New_York"
NOW = datetime(2026, 11, 13, 19, 35, tzinfo=UTC)  # 2:35 PM Eastern
TOP = datetime(2026, 11, 13, 20, 0, tzinfo=UTC)
MIDNIGHT = datetime(2026, 11, 14, 5, 0, tzinfo=UTC)

pg = pytest.mark.pg


@pytest.fixture(autouse=True)
def settings(monkeypatch):
    for spec in ss.SPECS.values():
        monkeypatch.delenv(spec.env, raising=False)
    rows: dict[str, str] = {}
    ss.set_loader(lambda: dict(rows))
    yield rows
    ss.set_loader(None)


def put(rows, **kw):
    for k, v in kw.items():
        rows["chat." + k] = str(v)


def _enter(**kw):
    args = dict(kind="car", user_id=1, phone_hash="ph", device_hash="dh", ip_hash="ih", listing_id=None, weight=1, now=NOW)
    args.update(kw)
    kind = args.pop("kind")
    return guard(kind, args.pop("user_id"), args.pop("phone_hash"), args.pop("device_hash"), args.pop("ip_hash"),
                 args.pop("listing_id"), args.pop("weight"), **args)


def _ask(**kw):
    with _enter(**kw):
        pass


def _denial(**kw):
    with pytest.raises(ChatDenied) as e:
        _ask(**kw)
    return e.value.denial


def _day_row(pg_db, kind, key):
    rows = repo.read_day_totals(now=NOW, tz_name=NY, kind=kind, key=key)
    return rows[0] if rows else None


def _api_result(text="ok", stop="end_turn", tin=100, tout=20):
    return SimpleNamespace(content=[SimpleNamespace(type="text", text=text)], stop_reason=stop,
                           usage={"input_tokens": tin, "output_tokens": tout})


class _FakeSDK:
    def __init__(self, outs):
        self.outs = list(outs)
        self.messages = SimpleNamespace(create=lambda **kw: self.outs.pop(0))


@pytest.fixture
def model(monkeypatch):
    def install(outs):
        sdk = _FakeSDK(outs)
        monkeypatch.setattr(llm, "_sdk_client", lambda key: sdk)

    monkeypatch.setattr("backend.vision.claude_rate_limit.acquire_vision_slot", lambda: None)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    return install


def _call():
    return llm.complete("hi", model_role="chat", max_tokens=10)


# ---------------------------------------------------------------- offline


def test_chat_paused_setting_stops_everyone(settings):
    put(settings, enabled="off")
    d = _denial()
    assert (d.error, d.message, d.resets_at) == ("chat_paused", msg.PAUSED_TEXT, None)


def test_zero_budget_means_chat_off(settings):
    put(settings, budget_usd_per_day_cents=0)
    assert _denial().error == "chat_paused"


def test_fails_closed_when_postgres_is_unreachable(monkeypatch):
    def down(*a, **k):
        raise OSError("connection refused")

    monkeypatch.setattr(repo, "reserve", down)
    d = _denial()
    assert (d.error, d.status) == ("chat_unavailable", 503)
    assert d.message == msg.UNAVAILABLE_TEXT


def test_denial_json_shape():
    from backend.chat_budget.gate import Denial

    j = Denial("budget", "chat_budget_reached", "x", 429, MIDNIGHT).to_json()
    assert j == {"error": "chat_budget_reached", "message": "x", "resets_at": "2026-11-14T05:00:00Z"}


def test_hook_inactive_outside_a_guard(model):
    model([_api_result()])
    assert _call().ok  # no sink installed: nothing recorded, nothing raised


class _FakeRepo:
    """Stands in for the database: records the reserve and the true-up."""

    def __init__(self, monkeypatch):
        self.trued = []
        res = repo.Reservation(weight=1, est_tokens_in=1000, est_tokens_out=500, est_cost_micro=7)
        monkeypatch.setattr(repo, "reserve", lambda charges, **kw: repo.ReserveResult(ok=True, violations=[], reservation=res))
        monkeypatch.setattr(repo, "true_up", lambda r, **kw: self.trued.append(kw))


def test_no_reported_usage_keeps_the_estimate_charged(monkeypatch):
    fake = _FakeRepo(monkeypatch)
    with _enter():
        pass  # a model call from another thread or a stream would look exactly like this
    assert fake.trued == []


def test_explicit_no_model_call_replaces_the_estimate_with_zero(monkeypatch):
    fake = _FakeRepo(monkeypatch)
    with _enter() as g:
        g.no_model_call()
    assert [(k["tokens_in"], k["tokens_out"], k["cost_micro"], k["messages_back"]) for k in fake.trued] == [(0, 0, 0, False)]


def test_exception_with_no_usage_still_gives_the_message_back(monkeypatch):
    fake = _FakeRepo(monkeypatch)
    with pytest.raises(RuntimeError):
        with _enter():
            raise RuntimeError("boom")
    assert [k["messages_back"] for k in fake.trued] == [True]


def test_missing_pepper_fails_closed_with_unavailable_and_logs_once(monkeypatch, caplog):
    from backend.chat_budget import gate

    monkeypatch.setattr(gate, "_PEPPER_LOGGED", False)
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("RAILWAY_ENVIRONMENT", "production")
    monkeypatch.delenv("PHONE_HASH_PEPPER", raising=False)
    from backend.utils import device_id

    monkeypatch.setattr(device_id, "is_production_env", lambda: True)
    reserved = []
    monkeypatch.setattr(repo, "reserve", lambda *a, **k: reserved.append(1))
    caplog.set_level("ERROR")
    for _ in range(3):
        d = _denial()
        assert (d.error, d.status) == ("chat_unavailable", 503)
    assert reserved == []  # never opened the gate, never touched the database
    assert sum("PHONE_HASH_PEPPER" in r.getMessage() for r in caplog.records) == 1
    assert "dev-only" not in caplog.text


# ---------------------------------------------------------------- Postgres tier


@pg
def test_account_hour_limit_denies_at_exactly_the_limit(pg_db, settings):
    put(settings, user_per_hour=2)
    _ask(); _ask()
    d = _denial()
    assert d.error == "user_chat_limit_reached" and d.kind == "user_hour"
    assert d.message == msg.hour_limit_text(2, TOP, NOW, NY)
    assert d.resets_at == TOP


@pg
def test_account_day_limit_denies_at_exactly_the_limit(pg_db, settings):
    put(settings, user_per_hour=0, user_per_day=3)
    for _ in range(3):
        _ask()
    d = _denial()
    assert d.kind == "user_day" and d.message == msg.day_limit_text(3, MIDNIGHT, NOW, NY)
    assert d.resets_at == MIDNIGHT


@pg
def test_listing_limit_gives_the_car_sentence(pg_db, settings):
    put(settings, user_per_hour=0, listing_per_day=2)
    _ask(listing_id=55); _ask(listing_id=55)
    d = _denial(listing_id=55)
    assert d.error == "listing_chat_limit_reached"
    assert d.message == msg.listing_limit_text(2, MIDNIGHT, NY) and d.resets_at == MIDNIGHT
    _ask(listing_id=56)  # another car still works


@pg
@pytest.mark.parametrize("kind,setting,hashes", [
    ("phone", "phone_per_day", {"phone_hash": "p1", "device_hash": None, "ip_hash": None}),
    ("device", "device_per_day", {"device_hash": "d1", "phone_hash": None, "ip_hash": None}),
    ("ip", "ip_per_day", {"ip_hash": "i1", "phone_hash": None, "device_hash": None}),
])
def test_phone_device_and_ip_limits_use_the_day_sentence_and_hide_the_counter(pg_db, settings, kind, setting, hashes):
    put(settings, user_per_hour=0, user_per_day=40, **{setting: 2})
    # different accounts, same hidden subject
    _ask(user_id=1, **hashes); _ask(user_id=2, **hashes)
    d = _denial(user_id=3, **hashes)
    assert d.kind == kind and d.error == "user_chat_limit_reached"
    assert d.message == msg.day_limit_text(40, MIDNIGHT, NOW, NY)
    for word in ("phone", "device", "address", "account"):
        assert word not in d.message.lower().replace("assistant", "")
    assert d.resets_at == MIDNIGHT


@pg
def test_strictest_wins_when_several_limits_apply(pg_db, settings):
    put(settings, user_per_hour=1, user_per_day=1, device_per_day=1)
    _ask()
    d = _denial()
    # hour, day and device are all hit; the longest wait (next midnight) wins, account before device
    assert d.kind == "user_day" and d.resets_at == MIDNIGHT


@pg
def test_hour_limit_alone_resets_at_the_next_hour_and_a_new_hour_works(pg_db, settings):
    put(settings, user_per_hour=1)
    _ask()
    assert _denial().resets_at == TOP
    _ask(now=TOP)  # next hour


@pg
def test_admin_skips_per_account_limits_but_not_the_site_budget(pg_db, settings, model):
    put(settings, user_per_hour=1, user_per_day=1, listing_per_day=1, device_per_day=1, ip_per_day=1, budget_tokens_per_day=1000,
        est_tokens_in_per_message=400, est_tokens_out_per_message=0)
    model([_api_result(tin=400, tout=0), _api_result(tin=400, tout=0)])
    for _ in range(2):  # far past every per-account number
        with _enter(is_admin=True, listing_id=9):
            _call()
    d = _denial(is_admin=True, listing_id=9)  # 800 used + 400 estimated passes the 1000 token budget
    assert d.error == "chat_budget_reached"


@pg
def test_site_budget_stops_everyone_with_the_budget_sentence(pg_db, settings, model):
    put(settings, budget_tokens_per_day=1000, est_tokens_in_per_message=600, est_tokens_out_per_message=0)
    model([_api_result(tin=600, tout=0), _api_result(tin=1, tout=0)])
    with _enter(user_id=1):
        _call()
    for uid in (1, 2, 3):
        d = _denial(user_id=uid)
        assert d.error == "chat_budget_reached"
        assert d.message == msg.budget_text(MIDNIGHT, NY) and d.resets_at == MIDNIGHT
    with _enter(user_id=4, now=MIDNIGHT):  # next day works
        _call()


@pg
def test_cost_budget_uses_the_price_settings(pg_db, settings):
    put(settings, budget_usd_per_day_cents=1, price_in_micro_per_mtok=10_000_000, price_out_micro_per_mtok=0,
        est_tokens_in_per_message=600_000, est_tokens_out_per_message=0)  # 600k tokens at $10/M = $6 > 1 cent
    assert _denial().error == "chat_budget_reached"


@pg
def test_token_cap_works_without_prices(pg_db, settings):
    put(settings, budget_tokens_per_day=100, est_tokens_in_per_message=60, est_tokens_out_per_message=60)
    assert _denial().error == "chat_budget_reached"


@pg
def test_denied_attempt_changes_no_counter_but_is_counted_as_denied(pg_db, settings):
    put(settings, user_per_hour=1)
    _ask()
    _denial()
    row = _day_row(pg_db, "user", "1")
    assert row["messages"] == 1 and row["denied"] == 0  # the hour row carries the denial
    hr = repo.read_hour_totals(now=NOW, tz_name=NY, kind="user", key="1")[0]
    assert hr["messages"] == 1 and hr["denied"] == 1
    assert _day_row(pg_db, "global", "all")["messages"] == 1


@pg
def test_real_tokens_replace_the_estimate(pg_db, settings, model):
    put(settings, est_tokens_in_per_message=1000, est_tokens_out_per_message=500, price_in_micro_per_mtok=3_000_000,
        price_out_micro_per_mtok=15_000_000)
    model([_api_result(tin=120, tout=30)])
    with _enter():
        _call()
    row = _day_row(pg_db, "user", "1")
    assert (row["messages"], row["tokens_in"], row["tokens_out"]) == (1, 120, 30)
    assert row["cost_micro"] == -(-(120 * 3_000_000 + 30 * 15_000_000) // 1_000_000)
    assert _day_row(pg_db, "global", "all")["tokens_in"] == 120


@pg
def test_two_model_calls_in_one_request_record_both(pg_db, settings, model):
    model([_api_result(tin=100, tout=10), _api_result(tin=200, tout=40)])
    with _enter():
        _call(); _call()
    row = _day_row(pg_db, "user", "1")
    assert (row["messages"], row["tokens_in"], row["tokens_out"]) == (1, 300, 50)


@pg
def test_an_exception_gives_the_message_back(pg_db, settings):
    put(settings, user_per_hour=1)
    with pytest.raises(RuntimeError):
        with _enter():
            raise RuntimeError("model blew up")
    assert _day_row(pg_db, "user", "1")["messages"] == 0
    _ask()  # the hourly allowance is still there


@pg
def test_a_refused_answer_gives_the_message_back_but_keeps_real_tokens(pg_db, settings, model):
    model([_api_result(text="", stop="refusal", tin=80, tout=0)])
    with _enter():
        _call()
    row = _day_row(pg_db, "user", "1")
    assert row["messages"] == 0 and row["tokens_in"] == 80


@pg
def test_caller_can_mark_a_request_failed(pg_db, settings):
    with _enter() as g:
        g.fail()
    assert _day_row(pg_db, "user", "1")["messages"] == 0


@pg
def test_new_accounts_get_half_the_account_numbers(pg_db, settings):
    put(settings, user_per_hour=0, user_per_day=4)
    _ask(new_account=True); _ask(new_account=True)
    d = _denial(new_account=True)
    assert d.kind == "user_day" and "2 questions" in d.message


@pg
def test_web_research_weighs_three_messages(pg_db, settings):
    from backend.chat_budget import web_research_weight

    put(settings, user_per_hour=0, user_per_day=4)
    assert web_research_weight() == 3
    _ask(weight=3)
    assert _denial(weight=3).kind == "user_day"
    _ask(weight=1)


@pg
def test_signed_out_caller_is_counted_by_device_and_address_only(pg_db, settings):
    put(settings, device_per_day=1)
    _ask(user_id=None, phone_hash=None)
    assert _denial(user_id=None, phone_hash=None).kind == "device"


@pg
def test_no_message_text_or_hash_in_logs(pg_db, settings, caplog):
    import logging

    put(settings, user_per_hour=1)
    with caplog.at_level(logging.DEBUG):
        _ask(phone_hash="secret-phone-hash")
        _denial(phone_hash="secret-phone-hash")
    assert "secret-phone-hash" not in caplog.text
