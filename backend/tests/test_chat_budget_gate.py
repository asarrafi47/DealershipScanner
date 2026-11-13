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


ESTABLISHED = 10 * 24 * 3600.0  # seconds: an account over 7 days old keeps the full site budget in these tests


def _enter(**kw):
    args = dict(kind="car", user_id=1, phone_hash="ph", device_hash="dh", ip_hash="ih", listing_id=None, weight=1, now=NOW,
                account_age_s=ESTABLISHED)
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
    assert llm._usage_sink.get() is None  # no guard is open: no sink is installed
    assert _call().ok  # nothing raised
    assert llm._usage_sink.get() is None


class _FakeRepo:
    """Stands in for the database: records the reserve and the true-up."""

    def __init__(self, monkeypatch, *, give_backs=True):
        self.trued = []
        self.reserved = []
        self.claims = []
        res = repo.Reservation(weight=1, est_tokens_in=1000, est_tokens_out=500, est_cost_micro=7)

        def reserve(charges, **kw):
            self.reserved.append((charges, kw))
            return repo.ReserveResult(ok=True, violations=[], reservation=res)

        def claim(user_id, **kw):
            self.claims.append((user_id, kw))
            return give_backs

        monkeypatch.setattr(repo, "reserve", reserve)
        monkeypatch.setattr(repo, "true_up", lambda r, **kw: self.trued.append(kw))
        monkeypatch.setattr(repo, "claim_give_back", claim)


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


def test_no_settings_rows_and_no_environment_still_reserve_a_non_zero_cost(monkeypatch):
    fake = _FakeRepo(monkeypatch)
    _ask()
    charges, kw = fake.reserved[0]
    assert kw["est_cost_micro"] > 0 and kw["est_tokens_in"] > 0
    glob = [c for c in charges if c.kind == "global" and c.key == repo.GLOBAL_KEY][0]
    assert glob.cost_limit_micro == 500 * 10_000 and glob.tokens_limit == 2_000_000  # the 500 cent cap is live


def test_a_row_asking_for_more_than_the_hard_ceiling_is_clamped_in_the_charge(monkeypatch, settings):
    fake = _FakeRepo(monkeypatch)
    put(settings, budget_usd_per_day_cents=999_999, budget_tokens_per_day=999_999_999)
    _ask()
    glob = [c for c in fake.reserved[0][0] if c.key == repo.GLOBAL_KEY][0]
    assert glob.cost_limit_micro == 2000 * 10_000 and glob.tokens_limit == 8_000_000


def test_an_answer_cut_short_by_the_token_limit_stays_counted_and_keeps_real_tokens(monkeypatch, model):
    fake = _FakeRepo(monkeypatch)
    model([_api_result(text="A half answer that was cut", stop="max_tokens", tin=90, tout=500)])
    with _enter():
        _call()
    assert len(fake.trued) == 1 and fake.trued[0]["messages_back"] is False
    assert (fake.trued[0]["tokens_in"], fake.trued[0]["tokens_out"]) == (90, 500)  # tokens are never given back
    assert fake.claims == []  # nothing to give back, so no give-back slot was used


def test_an_empty_text_refusal_gives_the_message_back_through_a_give_back_slot(monkeypatch, model):
    fake = _FakeRepo(monkeypatch)
    model([_api_result(text="", stop="refusal", tin=80, tout=0)])
    with _enter():
        _call()
    assert fake.trued[0]["messages_back"] is True and fake.trued[0]["tokens_in"] == 80
    assert len(fake.claims) == 1 and fake.claims[0][0] == "1" and fake.claims[0][1]["cap"] == 3


def test_a_spent_give_back_cap_keeps_the_message_counted(monkeypatch, model):
    fake = _FakeRepo(monkeypatch, give_backs=False)
    model([_api_result(text="", stop="end_turn", tin=80, tout=0)])
    with _enter():
        _call()
    assert fake.trued[0]["messages_back"] is False and fake.trued[0]["tokens_in"] == 80


def test_a_signed_out_caller_gets_no_give_back(monkeypatch):
    fake = _FakeRepo(monkeypatch)
    with pytest.raises(RuntimeError):
        with _enter(user_id=None):
            raise RuntimeError("boom")
    assert fake.trued[0]["messages_back"] is False and fake.claims == []


def test_a_database_error_while_taking_the_give_back_slot_keeps_the_message_counted(monkeypatch):
    fake = _FakeRepo(monkeypatch)
    monkeypatch.setattr(repo, "claim_give_back", lambda *a, **k: (_ for _ in ()).throw(OSError("down")))
    with pytest.raises(RuntimeError):
        with _enter():
            raise RuntimeError("boom")
    assert fake.trued[0]["messages_back"] is False


def test_web_research_gives_back_only_if_no_call_produced_text(monkeypatch, model):
    fake = _FakeRepo(monkeypatch)
    model([_api_result(text="found it", tin=50, tout=20), _api_result(text="", stop="refusal", tin=30, tout=0)])
    with _enter(weight=3):
        _call(); _call()  # call one answered, call two came back empty
    assert fake.trued[0]["messages_back"] is False
    model([_api_result(text="", stop="refusal"), _api_result(text="  ", stop="refusal")])
    with _enter(weight=3):
        _call(); _call()
    assert fake.trued[1]["messages_back"] is True


def test_a_failed_request_after_text_was_produced_is_not_given_back(monkeypatch, model):
    fake = _FakeRepo(monkeypatch)
    model([_api_result(text="the answer", tin=10, tout=5)])
    with _enter() as g:
        _call()
        g.fail()  # even a route that says "failed" cannot give back a message that produced text
    assert fake.trued[0]["messages_back"] is False


def test_closing_a_stream_after_the_first_text_never_gives_the_message_back(monkeypatch):
    fake = _FakeRepo(monkeypatch)

    def stream(g):
        with g:
            g.note_text()  # the first chunk went to the browser
            yield "chunk"
            yield "more"

    g = _enter()
    gen = stream(g)
    assert next(gen) == "chunk"
    gen.close()  # the browser left: GeneratorExit inside the with block
    assert fake.trued[0]["messages_back"] is False and fake.claims == []


def test_closing_a_stream_before_any_text_gives_the_message_back(monkeypatch):
    fake = _FakeRepo(monkeypatch)

    def stream(g):
        with g:
            yield "nothing sent yet"

    gen = stream(_enter())
    next(gen)
    gen.close()
    assert fake.trued[0]["messages_back"] is True


def test_account_tiers_follow_the_age(monkeypatch):
    day = 24 * 3600
    tiers = {a: _enter(account_age_s=a).tier() for a in (0, 23 * 3600, day, 6 * day, 7 * day, 40 * day)}
    assert tiers == {0: "new", 23 * 3600: "new", day: "mid", 6 * day: "mid", 7 * day: "established", 40 * day: "established"}
    assert _enter(account_age_s=None).tier() == "mid"
    assert _enter(user_id=None).tier() == "new"
    assert _enter(new_account=True).tier() == "new"
    assert _enter(is_admin=True, account_age_s=0).tier() == "admin"


def test_new_and_mid_accounts_carry_sub_budget_rows_established_and_admin_do_not(monkeypatch):
    fake = _FakeRepo(monkeypatch)

    def sub_rows(**kw):
        fake.reserved.clear()
        _ask(**kw)
        return {c.key: c for c in fake.reserved[0][0] if c.kind == "global"}

    total = 500 * 10_000
    new = sub_rows(account_age_s=3600)
    assert set(new) == {"all", "open", "new"}
    assert new["new"].cost_limit_micro == total * 25 // 100 and new["new"].tokens_limit == 2_000_000 * 25 // 100
    assert new["open"].cost_limit_micro == total * 50 // 100
    mid = sub_rows(account_age_s=3 * 24 * 3600)
    assert set(mid) == {"all", "open"}
    assert set(sub_rows(account_age_s=ESTABLISHED)) == {"all"}
    assert set(sub_rows(is_admin=True, account_age_s=0)) == {"all"}  # admins: the site budget only


def test_a_zero_reserve_setting_means_no_open_row(monkeypatch, settings):
    fake = _FakeRepo(monkeypatch)
    put(settings, established_reserve_pct=0)
    _ask(account_age_s=3 * 24 * 3600)
    assert {c.key for c in fake.reserved[0][0] if c.kind == "global"} == {"all"}


def test_missing_pepper_fails_closed_with_unavailable_and_logs_once(monkeypatch, caplog):
    from backend.chat_budget import gate

    monkeypatch.setattr(gate, "_PEPPER_LOGGED", False)
    monkeypatch.delenv("PHONE_HASH_PEPPER", raising=False)
    monkeypatch.delenv("ALLOW_DEV_PHONE_PEPPER", raising=False)  # a missing pepper is never a default, in any environment
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


# ---------------------------------------------------------------- wave 1 review follow-ups, Postgres tier


@pg
def test_a_truncated_answer_keeps_the_message_counted_and_records_real_tokens(pg_db, settings, model):
    model([_api_result(text="half an answer", stop="max_tokens", tin=90, tout=500)])
    with _enter():
        _call()
    row = _day_row(pg_db, "user", "1")
    assert (row["messages"], row["tokens_in"], row["tokens_out"]) == (1, 90, 500)


@pg
def test_an_empty_text_refusal_gives_the_message_back_and_the_fourth_give_back_in_an_hour_is_not_given(pg_db, settings, model):
    put(settings, user_per_hour=0, user_per_day=0)
    model([_api_result(text="", stop="refusal", tin=10, tout=0) for _ in range(4)])
    for n in range(1, 5):
        with _enter():
            _call()
        row = _day_row(pg_db, "user", "1")
        assert row["messages"] == (0 if n <= 3 else 1), n  # the 4th stays counted
        assert row["tokens_in"] == 10 * n  # tokens are never given back
    rows = repo.read_hour_totals(now=NOW, tz_name=NY, kind="user", key="giveback:1")
    assert rows[0]["messages"] == 3  # the cap row stops at the cap


@pg
def test_give_backs_are_counted_per_account_and_per_clock_hour(pg_db, settings, model):
    model([_api_result(text="", stop="refusal") for _ in range(5)])
    for uid in (1, 1, 1, 2):
        with _enter(user_id=uid):
            _call()
    assert _day_row(pg_db, "user", "1")["messages"] == 0 and _day_row(pg_db, "user", "2")["messages"] == 0
    with _enter(user_id=1, now=TOP):  # a new clock hour: the account has its three again
        _call()
    assert repo.read_day_totals(now=TOP, tz_name=NY, kind="user", key="1")[0]["messages"] == 0


@pg
def test_claim_give_back_is_atomic_at_the_cap(pg_db):
    got = [repo.claim_give_back("7", now=NOW, cap=3) for _ in range(5)]
    assert got == [True, True, True, False, False]
    assert repo.claim_give_back("7", now=NOW, cap=0) is False
    assert repo.claim_give_back("8", now=NOW, cap=1) is True


@pg
def test_web_research_with_text_from_one_call_keeps_all_three_messages(pg_db, settings, model):
    model([_api_result(text="found", tin=5, tout=5), _api_result(text="", stop="refusal", tin=5, tout=0)])
    with _enter(weight=3):
        _call(); _call()
    assert _day_row(pg_db, "user", "1")["messages"] == 3


@pg
def test_with_the_new_account_sub_budget_used_up_an_established_account_still_gets_an_answer(pg_db, settings, model):
    put(settings, budget_tokens_per_day=1000, est_tokens_in_per_message=100, est_tokens_out_per_message=0,
        user_per_hour=0, user_per_day=0, new_account_budget_pct=25)  # a quarter of 1000 is 250 tokens
    model([_api_result(tin=100, tout=0) for _ in range(5)])
    with _enter(user_id=1, account_age_s=3600):
        _call()
    with _enter(user_id=2, account_age_s=3600):
        _call()
    d = _denial(user_id=3, account_age_s=3600)  # 200 used, the next estimate (100) passes 250
    assert d.error == "chat_budget_reached" and d.message == msg.budget_text(MIDNIGHT, NY)
    with _enter(user_id=9, account_age_s=ESTABLISHED):  # established: the site budget still has room
        _call()
    assert _day_row(pg_db, "global", "all")["tokens_in"] == 300


@pg
def test_accounts_under_a_week_old_cannot_use_the_reserve_kept_for_established_ones(pg_db, settings, model):
    put(settings, budget_tokens_per_day=1000, est_tokens_in_per_message=250, est_tokens_out_per_message=0,
        user_per_hour=0, user_per_day=0, established_reserve_pct=50)
    model([_api_result(tin=250, tout=0) for _ in range(4)])
    for uid in (1, 2):
        with _enter(user_id=uid, account_age_s=3 * 24 * 3600):
            _call()  # 500 of 1000: exactly the open half
    assert _denial(user_id=3, account_age_s=3 * 24 * 3600).error == "chat_budget_reached"
    for uid in (4, 5):
        with _enter(user_id=uid, account_age_s=ESTABLISHED):
            _call()  # the reserve is theirs
    assert _day_row(pg_db, "global", "all")["tokens_in"] == 1000


@pg
def test_admins_skip_the_tiers_but_the_site_budget_still_stops_them(pg_db, settings, model):
    put(settings, budget_tokens_per_day=1000, est_tokens_in_per_message=300, est_tokens_out_per_message=0,
        user_per_hour=0, user_per_day=0)
    model([_api_result(tin=300, tout=0) for _ in range(4)])
    for _ in range(3):
        with _enter(is_admin=True, account_age_s=0):  # a brand new admin account: 900 tokens, over a new account's 250
            _call()
    assert _denial(is_admin=True, account_age_s=0).error == "chat_budget_reached"


@pg
def test_with_no_settings_and_no_environment_the_default_dollar_cap_can_trip(pg_db, settings):
    n = 0
    while True:
        try:
            _ask(user_id=1000 + n, phone_hash=None, device_hash=None, ip_hash=None)
        except ChatDenied as denied:
            assert denied.denial.error == "chat_budget_reached"
            break
        n += 1
        assert n < 1000, "the default budget never tripped: the dollar cap is not live"
    row = _day_row(pg_db, "global", "all")
    assert 0 < row["cost_micro"] <= 500 * 10_000 and n == 500 * 10_000 // ss.get_int("chat.est_cost_micro_per_message")


@pg
def test_the_hard_ceiling_beats_a_settings_row_in_a_real_reserve(pg_db, settings):
    put(settings, budget_usd_per_day_cents=999_999, est_cost_micro_per_message=1_000_000, user_per_hour=0, user_per_day=0)
    for n in range(20):  # 20 dollars: the 2000 cent ceiling
        _ask(user_id=500 + n, phone_hash=None, device_hash=None, ip_hash=None)
    assert _denial(user_id=999, phone_hash=None, device_hash=None, ip_hash=None).error == "chat_budget_reached"
