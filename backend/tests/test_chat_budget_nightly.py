"""C6: nightly chat totals, retention and alert levels (CHAT_LIMITS.md section 9).

Offline tests use a fake summary and a fake sender; the Postgres tier writes real rows. A fake
clock is used throughout. Test numbers are in the fictional range +1 202 555 01xx.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone

import pytest

from backend import settings_store as ss
from backend.chat_budget import alerts
from backend.db.repositories import chat_usage_repo as repo
from backend.db.repositories.chat_usage_repo import Charge
from backend.scripts import chat_budget_nightly as nightly

UTC = timezone.utc
NY = "America/New_York"
# 3:30 AM Eastern on 15 November: "yesterday" is 14 November in New York.
NOW = datetime(2026, 11, 15, 8, 30, tzinfo=UTC)
Y_START = datetime(2026, 11, 14, 5, 0, tzinfo=UTC)  # midnight Eastern, 14 November
T_START = datetime(2026, 11, 15, 5, 0, tzinfo=UTC)
TEST_PHONE = "+12025550123"
TEST_USER = "pat_tester_name"
DIGITS7 = re.compile(r"\d{7,}")
HEX7 = re.compile(r"\b[0-9a-f]{7,}\b")

pg = pytest.mark.pg


@pytest.fixture(autouse=True)
def settings(monkeypatch):
    for spec in ss.SPECS.values():
        monkeypatch.delenv(spec.env, raising=False)
    monkeypatch.delenv("OWNER_ALERT_EMAIL", raising=False)
    rows: dict[str, str] = {}
    ss.set_loader(lambda: dict(rows))
    alerts.reset()
    yield rows
    ss.set_loader(None)
    alerts.set_clock(None)


def _summary(messages=0, tin=0, tout=0, cost=0, denied=None):
    return {"messages": messages, "tokens_in": tin, "tokens_out": tout, "cost_micro": cost, "denied": denied or {}}


class _Fake:
    def __init__(self, monkeypatch, summary, *, fail=False):
        self.calls = []
        self.deleted = []
        self.mails = []

        def day_summary(**kw):
            self.calls.append(kw)
            if fail:
                raise OSError("connection refused")
            return summary

        def delete_older_than(**kw):
            self.deleted.append(kw)
            return 12

        monkeypatch.setattr(repo, "day_summary", day_summary)
        monkeypatch.setattr(repo, "delete_older_than", delete_older_than)

    def send(self, subject, text):
        self.mails.append((subject, text))


# ---------------------------------------------------------------- exit codes (offline)


def test_exit_code_zero_on_a_day_under_every_alert_level(monkeypatch, capsys):
    fake = _Fake(monkeypatch, _summary(messages=30, tin=40_000, tout=8_000, cost=1_000_000))  # $1 of $5: 20%
    assert nightly.run(now=NOW, send_email=fake.send) == nightly.EXIT_OK
    out = capsys.readouterr().out
    assert "result: under every alert level" in out
    assert fake.mails == []


def test_exit_code_two_on_a_day_that_passed_an_alert_level(monkeypatch, capsys):
    fake = _Fake(monkeypatch, _summary(messages=200, tin=500_000, tout=50_000, cost=4_100_000))  # $4.10 of $5: 82%
    assert nightly.run(now=NOW, send_email=fake.send) == nightly.EXIT_ALERT
    assert "passed the 80% alert level" in capsys.readouterr().out


def test_exit_code_two_when_only_the_token_share_passes_a_level(monkeypatch):
    _Fake(monkeypatch, _summary(tin=1_000_000, tout=100_000, cost=10))  # 55% of 2,000,000 tokens
    assert nightly.run(now=NOW) == nightly.EXIT_ALERT


def test_exit_code_three_when_the_budget_was_reached(monkeypatch, capsys):
    _Fake(monkeypatch, _summary(messages=400, tin=900_000, tout=90_000, cost=5_000_000))  # exactly $5
    assert nightly.run(now=NOW) == nightly.EXIT_BUDGET
    assert "result: the budget was reached" in capsys.readouterr().out


def test_exit_code_three_when_the_site_budget_refused_a_message(monkeypatch):
    # The estimate reserved before each answer can refuse a message while the real total stays under 100%.
    _Fake(monkeypatch, _summary(cost=4_900_000, denied={"global": 3}))
    assert nightly.run(now=NOW) == nightly.EXIT_BUDGET


def test_alert_levels_come_from_the_setting(monkeypatch, settings):
    settings["chat.alert_pcts"] = "10,90"
    _Fake(monkeypatch, _summary(cost=1_000_000))  # 20%
    assert nightly.run(now=NOW) == nightly.EXIT_ALERT


def test_exit_code_one_and_nothing_deleted_when_the_database_is_down(monkeypatch, capsys):
    fake = _Fake(monkeypatch, None, fail=True)
    assert nightly.run(now=NOW, send_email=fake.send) == nightly.EXIT_ERROR
    assert fake.deleted == [] and fake.mails == []
    assert "could not read the totals (OSError)" in capsys.readouterr().out


def test_yesterday_is_the_new_york_day_before_today(monkeypatch):
    fake = _Fake(monkeypatch, _summary())
    nightly.run(now=NOW)
    assert fake.calls == [{"day_start_utc": Y_START, "next_day_utc": T_START}]


def test_yesterday_across_the_fall_back_day_is_25_hours(monkeypatch):
    fake = _Fake(monkeypatch, _summary())
    nightly.run(now=datetime(2026, 11, 2, 8, 30, tzinfo=UTC))  # 3:30 AM EST, the day after the change
    kw = fake.calls[0]
    assert kw["day_start_utc"] == datetime(2026, 11, 1, 4, 0, tzinfo=UTC)  # midnight EDT
    assert kw["next_day_utc"] - kw["day_start_utc"] == timedelta(hours=25)


def test_retention_runs_with_35_days_and_can_be_skipped(monkeypatch):
    fake = _Fake(monkeypatch, _summary())
    nightly.run(now=NOW)
    assert fake.deleted == [{"now": NOW, "days": 35}]
    fake.deleted.clear()
    nightly.run(now=NOW, purge=False)
    assert fake.deleted == []


def test_main_no_purge_flag(monkeypatch):
    fake = _Fake(monkeypatch, _summary())
    assert nightly.main(["--no-purge"]) == nightly.EXIT_OK
    assert fake.deleted == []


# ---------------------------------------------------------------- owner email (offline, fake sender)


def test_alert_email_sent_once_on_an_alert_day_when_the_address_is_set(monkeypatch):
    monkeypatch.setenv("OWNER_ALERT_EMAIL", "owner@example.invalid")
    fake = _Fake(monkeypatch, _summary(cost=5_000_000))
    nightly.run(now=NOW, send_email=fake.send)
    assert len(fake.mails) == 1
    subject, text = fake.mails[0]
    assert subject == "Chat budget 2026-11-14: the budget was reached"
    assert "dollars $5.00 of $5.00 (100% of the budget)" in text


def test_no_email_on_a_quiet_day_or_without_the_address(monkeypatch):
    fake = _Fake(monkeypatch, _summary(cost=5_000_000))
    nightly.run(now=NOW, send_email=fake.send)  # no OWNER_ALERT_EMAIL
    assert fake.mails == []
    monkeypatch.setenv("OWNER_ALERT_EMAIL", "owner@example.invalid")
    fake2 = _Fake(monkeypatch, _summary(cost=100))
    nightly.run(now=NOW, send_email=fake2.send)
    assert fake2.mails == []


def test_a_failed_email_keeps_the_alert_exit_code(monkeypatch, capsys):
    monkeypatch.setenv("OWNER_ALERT_EMAIL", "owner@example.invalid")
    _Fake(monkeypatch, _summary(cost=4_500_000))

    def broken(subject, text):
        raise RuntimeError("smtp down owner@example.invalid")

    assert nightly.run(now=NOW, send_email=broken) == nightly.EXIT_ALERT
    out = capsys.readouterr().out
    assert "alert email failed (RuntimeError)" in out and "example.invalid" not in out


# ---------------------------------------------------------------- what the lines may carry


def _assert_clean(text: str) -> None:
    assert not DIGITS7.search(text), text
    assert not HEX7.search(text), text
    for bad in (TEST_PHONE, "2025550123", TEST_USER, "what is the towing capacity"):
        assert bad not in text


def test_nightly_lines_carry_no_long_digit_runs_hashes_or_names(monkeypatch, capsys):
    monkeypatch.setenv("OWNER_ALERT_EMAIL", "owner@example.invalid")
    fake = _Fake(monkeypatch, _summary(messages=1_234_567, tin=9_876_543, tout=1_234_567, cost=12_345_678,
                                       denied={"user": 1_000_001, "phone": 2, "device": 3, "ip": 4, "listing": 5,
                                               "global": 6}))
    assert nightly.run(now=NOW, send_email=fake.send) == nightly.EXIT_BUDGET
    out = capsys.readouterr().out
    _assert_clean(out)
    _assert_clean("\n".join(m[0] + m[1] for m in fake.mails))
    assert "messages 1,234,567" in out and "tokens 11,111,110" in out
    assert "denied account 1,000,001, address 4, budget 6, car 5, device 3, phone 2" in out


# ---------------------------------------------------------------- web process: alert lines (offline)


def _lines(caplog):
    return [r.getMessage() for r in caplog.records if r.getMessage().startswith("chat_budget: day")]


def test_gate_alert_line_once_per_level_per_day(caplog):
    caplog.set_level(logging.INFO, logger="backend.chat_budget")
    for cost in (1_000_000, 2_600_000, 2_700_000, 4_100_000, 4_200_000):  # 20%, 52%, 54%, 82%, 84%
        alerts.note_day_total((10, 1_000, cost), now=NOW, tz=NY)
    lines = _lines(caplog)
    assert len(lines) == 2
    assert lines[0].startswith("chat_budget: day 2026-11-15 passed 50% of the budget")
    assert lines[1].startswith("chat_budget: day 2026-11-15 passed 80% of the budget")


def test_gate_alert_lines_start_again_on_a_new_day(caplog):
    caplog.set_level(logging.INFO, logger="backend.chat_budget")
    alerts.note_day_total((10, 1_000, 2_600_000), now=NOW, tz=NY)
    alerts.note_day_total((10, 1_000, 2_600_000), now=NOW + timedelta(days=1), tz=NY)
    assert [ln.split(" passed")[0] for ln in _lines(caplog)] == ["chat_budget: day 2026-11-15",
                                                                   "chat_budget: day 2026-11-16"]


def test_gate_budget_reached_line_once_per_day(caplog):
    caplog.set_level(logging.INFO, logger="backend.chat_budget")
    alerts.note_budget_reached(now=NOW, tz=NY)
    alerts.note_budget_reached(now=NOW, tz=NY)
    assert _lines(caplog) == ["chat_budget: day 2026-11-15 reached the budget; chat is stopped until midnight"]


def test_gate_alert_lines_carry_no_long_digit_runs(caplog):
    caplog.set_level(logging.INFO, logger="backend.chat_budget")
    alerts.note_day_total((1_234_567, 98_765_432, 99_000_000), now=NOW, tz=NY)
    text = "\n".join(_lines(caplog))
    assert "passed 80%" in text
    _assert_clean(text)


def test_gate_writes_the_line_when_a_message_passes_a_level(monkeypatch, caplog):
    from backend.chat_budget import guard

    caplog.set_level(logging.INFO, logger="backend.chat_budget")
    res = repo.Reservation(weight=1, est_tokens_in=1, est_tokens_out=1, est_cost_micro=1,
                           global_day=(120, 50_000, 2_550_000))  # 51%
    monkeypatch.setattr(repo, "reserve", lambda charges, **kw: repo.ReserveResult(ok=True, reservation=res))
    monkeypatch.setattr(repo, "true_up", lambda r, **kw: None)
    monkeypatch.setattr(repo, "delete_older_than", lambda **kw: 0)
    for _ in range(3):
        with guard("car", 1, None, None, None, None, 1, now=NOW, account_age_s=10 * 86400) as g:
            g.no_model_call()
    lines = _lines(caplog)
    assert len(lines) == 1 and lines[0].startswith("chat_budget: day 2026-11-15 passed 50% of the budget")


def test_gate_writes_the_budget_line_when_the_site_budget_refuses(monkeypatch, caplog):
    from backend.chat_budget import ChatDenied, guard

    caplog.set_level(logging.INFO, logger="backend.chat_budget")
    v = repo.Violation("global", repo.GLOBAL_KEY, "day", "cost", 5_000_000, 5_010_000)
    monkeypatch.setattr(repo, "reserve", lambda charges, **kw: repo.ReserveResult(ok=False, violations=[v]))
    monkeypatch.setattr(repo, "add_denied", lambda *a, **k: None)
    for _ in range(2):
        with pytest.raises(ChatDenied):
            with guard("car", 1, None, None, None, None, 1, now=NOW, account_age_s=10 * 86400):
                pass
    assert len([ln for ln in _lines(caplog) if "reached the budget" in ln]) == 1


def test_gate_no_budget_line_for_an_account_limit(monkeypatch, caplog):
    from backend.chat_budget import ChatDenied, guard

    caplog.set_level(logging.INFO, logger="backend.chat_budget")
    v = repo.Violation("user", "1", "hour", "messages", 10, 10)
    monkeypatch.setattr(repo, "reserve", lambda charges, **kw: repo.ReserveResult(ok=False, violations=[v]))
    monkeypatch.setattr(repo, "add_denied", lambda *a, **k: None)
    with pytest.raises(ChatDenied):
        with guard("car", 1, None, None, None, None, 1, now=NOW, account_age_s=10 * 86400):
            pass
    assert _lines(caplog) == []


def test_an_alert_check_error_never_breaks_a_message(monkeypatch):
    monkeypatch.setattr(ss, "get_int", lambda key: 1 // 0)
    alerts.note_day_total((1, 2, 3), now=NOW, tz=NY)  # must not raise


# ---------------------------------------------------------------- web process: hourly purge fallback (offline)


def test_web_purge_runs_at_most_once_an_hour(monkeypatch):
    t = [1000.0]
    alerts.set_clock(lambda: t[0])
    calls = []
    monkeypatch.setattr(repo, "delete_older_than", lambda **kw: calls.append(kw) or 0)
    alerts.maybe_purge(now=NOW)
    t[0] += 3599
    alerts.maybe_purge(now=NOW)
    assert len(calls) == 1
    t[0] += 2
    alerts.maybe_purge(now=NOW)
    assert len(calls) == 2


def test_web_purge_error_never_raises(monkeypatch):
    def down(**kw):
        raise OSError("connection refused")

    monkeypatch.setattr(repo, "delete_older_than", down)
    alerts.maybe_purge(now=NOW)


# ---------------------------------------------------------------- Postgres tier


def _put(pg_db, period, start, kind, key, messages=0, tin=0, tout=0, cost=0, denied=0):
    with pg_db.connect() as conn:
        conn.execute(
            "INSERT INTO chat_usage_buckets (period, bucket_start, subject_kind, subject_key, messages, tokens_in, "
            "tokens_out, cost_micro, denied) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (period, start, kind, key, messages, tin, tout, cost, denied),
        )
        conn.commit()


def _count(pg_db):
    with pg_db.connect() as conn:
        return conn.execute("SELECT count(*) FROM chat_usage_buckets").fetchone()[0]


@pg
def test_pg_day_summary_reads_yesterday_and_its_denials_only(pg_db):
    _put(pg_db, "day", Y_START, "global", "all", messages=50, tin=60_000, tout=9_000, cost=700_000, denied=1)
    _put(pg_db, "day", Y_START, "user", "7", messages=12, denied=2)
    _put(pg_db, "hour", Y_START + timedelta(hours=3), "user", "7", denied=4)
    _put(pg_db, "hour", Y_START + timedelta(hours=23), "device", "abcdef12", denied=1)
    _put(pg_db, "hour", T_START, "user", "7", denied=9)  # today: not counted
    _put(pg_db, "day", T_START, "global", "all", messages=99, denied=9)  # today: not counted
    _put(pg_db, "hour", Y_START - timedelta(hours=1), "ip", "x", denied=9)  # the day before: not counted
    s = repo.day_summary(day_start_utc=Y_START, next_day_utc=T_START)
    assert (s["messages"], s["tokens_in"], s["tokens_out"], s["cost_micro"]) == (50, 60_000, 9_000, 700_000)
    assert s["denied"] == {"global": 1, "user": 6, "device": 1}


@pg
def test_pg_empty_day_is_zero_and_exit_ok(pg_db):
    assert repo.day_summary(day_start_utc=Y_START, next_day_utc=T_START) == {
        "messages": 0, "tokens_in": 0, "tokens_out": 0, "cost_micro": 0, "denied": {}}
    assert nightly.run(now=NOW) == nightly.EXIT_OK


@pg
def test_pg_exit_codes_for_under_alert_and_budget_days(pg_db):
    _put(pg_db, "day", Y_START, "global", "all", messages=10, tin=1_000, tout=100, cost=500_000)  # 10%
    assert nightly.run(now=NOW, purge=False) == nightly.EXIT_OK
    with pg_db.connect() as conn:
        conn.execute("UPDATE chat_usage_buckets SET cost_micro = 4_000_000")  # 80%
        conn.commit()
    assert nightly.run(now=NOW, purge=False) == nightly.EXIT_ALERT
    with pg_db.connect() as conn:
        conn.execute("UPDATE chat_usage_buckets SET cost_micro = 5_200_000")  # over
        conn.commit()
    assert nightly.run(now=NOW, purge=False) == nightly.EXIT_BUDGET


@pg
def test_pg_deletion_respects_the_age(pg_db):
    cutoff = NOW - timedelta(days=35)
    _put(pg_db, "day", cutoff - timedelta(hours=1), "global", "all", messages=1)  # older than 35 days: removed
    _put(pg_db, "hour", cutoff - timedelta(days=3), "user", "7", messages=1)  # removed
    _put(pg_db, "day", cutoff + timedelta(hours=1), "global", "all", messages=1)  # 34 days and change: kept
    _put(pg_db, "day", Y_START, "global", "all", messages=1)  # yesterday: kept and reported
    nightly.run(now=NOW)
    with pg_db.connect() as conn:
        left = sorted(r[0] for r in conn.execute("SELECT bucket_start FROM chat_usage_buckets").fetchall())
    assert left == [cutoff + timedelta(hours=1), Y_START]


@pg
def test_pg_reserve_reports_the_site_day_row_for_the_alert_line(pg_db):
    charges = [Charge("user", "7", day_limit=40), Charge("global", repo.GLOBAL_KEY, tokens_limit=10**6)]
    r1 = repo.reserve(charges, now=NOW, tz_name=NY, est_tokens_in=100, est_tokens_out=20, est_cost_micro=5)
    r2 = repo.reserve(charges, now=NOW, tz_name=NY, est_tokens_in=100, est_tokens_out=20, est_cost_micro=5)
    assert r1.reservation.global_day == (1, 120, 5)
    assert r2.reservation.global_day == (2, 240, 10)
    assert _count(pg_db) == 4
