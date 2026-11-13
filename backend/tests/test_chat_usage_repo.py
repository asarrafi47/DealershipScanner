"""C1: chat usage counters (Postgres tier) and bucket time maths (offline)."""

from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone

import pytest

from backend.db.repositories import chat_usage_repo as repo
from backend.db.repositories.chat_usage_repo import Charge

NY = "America/New_York"
UTC = timezone.utc
T0 = datetime(2026, 11, 13, 15, 25, 10, tzinfo=UTC)  # 10:25 in New York (EST)


# ------------------------------------------------------------ offline: time buckets


def test_hour_bucket_starts_on_the_clock_hour_in_new_york():
    assert repo.hour_start(T0, NY) == datetime(2026, 11, 13, 15, 0, tzinfo=UTC)
    assert repo.next_hour(T0, NY) == datetime(2026, 11, 13, 16, 0, tzinfo=UTC)


def test_day_bucket_starts_at_new_york_midnight_est():
    # EST is UTC-5 on 2026-11-13
    assert repo.day_start(T0, NY) == datetime(2026, 11, 13, 5, 0, tzinfo=UTC)
    assert repo.next_day(T0, NY) == datetime(2026, 11, 14, 5, 0, tzinfo=UTC)


def test_day_bucket_starts_at_new_york_midnight_edt():
    now = datetime(2026, 7, 4, 3, 30, tzinfo=UTC)  # 23:30 on July 3 in New York (EDT, UTC-4)
    assert repo.day_start(now, NY) == datetime(2026, 7, 3, 4, 0, tzinfo=UTC)
    assert repo.next_day(now, NY) == datetime(2026, 7, 4, 4, 0, tzinfo=UTC)


def test_spring_forward_day_is_23_hours_and_still_starts_at_midnight():
    now = datetime(2026, 3, 8, 12, 0, tzinfo=UTC)  # clocks go forward at 2 AM on 2026-03-08
    start = repo.day_start(now, NY)
    assert start == datetime(2026, 3, 8, 5, 0, tzinfo=UTC)  # midnight EST
    assert repo.next_day(now, NY) == datetime(2026, 3, 9, 4, 0, tzinfo=UTC)  # midnight EDT
    assert repo.next_day(now, NY) - start == timedelta(hours=23)


def test_fall_back_day_is_25_hours_and_still_starts_at_midnight():
    now = datetime(2026, 11, 1, 12, 0, tzinfo=UTC)  # clocks go back at 2 AM on 2026-11-01
    start = repo.day_start(now, NY)
    assert start == datetime(2026, 11, 1, 4, 0, tzinfo=UTC)  # midnight EDT
    assert repo.next_day(now, NY) == datetime(2026, 11, 2, 5, 0, tzinfo=UTC)  # midnight EST
    assert repo.next_day(now, NY) - start == timedelta(hours=25)


def test_the_hour_after_a_clock_change_starts_on_the_hour():
    now = datetime(2026, 3, 8, 7, 10, tzinfo=UTC)  # 3:10 EDT, just after the jump
    assert repo.hour_start(now, NY) == datetime(2026, 3, 8, 7, 0, tzinfo=UTC)


def test_unknown_time_zone_falls_back_to_new_york():
    assert repo.day_start(T0, "Not/AZone") == repo.day_start(T0, NY)


# ------------------------------------------------------------ Postgres tier

pg = pytest.mark.pg


def _row(pg_db, period, kind, key, start):
    with pg_db.connect() as conn:
        r = conn.execute(
            "SELECT messages, tokens_in, tokens_out, cost_micro, denied FROM chat_usage_buckets "
            "WHERE period=%s AND subject_kind=%s AND subject_key=%s AND bucket_start=%s",
            (period, kind, key, start),
        ).fetchone()
    return r


@pg
def test_migration_applies_twice_cleanly(pg_db):
    from pathlib import Path

    sql = (Path(__file__).resolve().parents[2] / "migrations" / "V030__chat_usage.sql").read_text()
    with pg_db.connect() as conn:
        conn.execute(sql)
        conn.execute(sql)
        conn.commit()
        n = conn.execute("SELECT count(*) FROM information_schema.tables WHERE table_name IN ('chat_usage_buckets','app_settings')").fetchone()[0]
    assert n == 2


@pg
def test_reserve_counts_hour_and_day_rows_for_every_subject(pg_db):
    charges = [Charge("user", "7", hour_limit=10, day_limit=40), Charge("global", "all")]
    res = repo.reserve(charges, now=T0, tz_name=NY, est_tokens_in=100, est_tokens_out=20, est_cost_micro=5)
    assert res.ok
    h, d = repo.hour_start(T0, NY), repo.day_start(T0, NY)
    assert _row(pg_db, "hour", "user", "7", h) == (1, 100, 20, 5, 0)
    assert _row(pg_db, "day", "user", "7", d) == (1, 100, 20, 5, 0)
    assert _row(pg_db, "day", "global", "all", d) == (1, 100, 20, 5, 0)


@pg
def test_reserve_denies_at_exactly_the_limit_and_changes_no_counter(pg_db):
    user = Charge("user", "7", hour_limit=2, day_limit=40)
    glob = Charge("global", "all")
    assert repo.reserve([user, glob], now=T0, tz_name=NY).ok
    assert repo.reserve([user, glob], now=T0, tz_name=NY).ok
    third = repo.reserve([user, glob], now=T0, tz_name=NY, est_tokens_in=50)
    assert not third.ok
    assert [(v.kind, v.period, v.limit) for v in third.violations] == [("user", "hour", 2)]
    h, d = repo.hour_start(T0, NY), repo.day_start(T0, NY)
    assert _row(pg_db, "hour", "user", "7", h)[0] == 2
    assert _row(pg_db, "day", "user", "7", d)[0] == 2
    assert _row(pg_db, "day", "global", "all", d)[0] == 2  # the global row did not move either
    assert _row(pg_db, "day", "global", "all", d)[1] == 0


@pg
def test_reserve_reports_every_limit_it_would_pass(pg_db):
    a = Charge("user", "1", hour_limit=1)
    b = Charge("device", "dd", day_limit=1)
    assert repo.reserve([a, b], now=T0, tz_name=NY).ok
    res = repo.reserve([a, b], now=T0, tz_name=NY)
    assert {(v.kind, v.period) for v in res.violations} == {("user", "hour"), ("device", "day")}


@pg
def test_weight_counts_as_several_messages(pg_db):
    c = Charge("user", "1", day_limit=5)
    assert repo.reserve([c], now=T0, tz_name=NY, weight=3).ok
    assert not repo.reserve([c], now=T0, tz_name=NY, weight=3).ok
    assert repo.reserve([c], now=T0, tz_name=NY, weight=2).ok


@pg
def test_token_and_cost_limits_on_the_global_row(pg_db):
    g = Charge("global", "all", tokens_limit=1000, cost_limit_micro=500)
    assert repo.reserve([g], now=T0, tz_name=NY, est_tokens_in=600, est_cost_micro=100).ok
    res = repo.reserve([g], now=T0, tz_name=NY, est_tokens_in=600, est_cost_micro=100)
    assert [v.what for v in res.violations] == ["tokens"]
    res = repo.reserve([g], now=T0, tz_name=NY, est_tokens_in=10, est_cost_micro=450)
    assert [v.what for v in res.violations] == ["cost"]


@pg
def test_two_concurrent_reserves_at_the_limit_let_exactly_one_through(pg_db):
    c = Charge("user", "9", day_limit=1)
    results: list[bool] = []
    barrier = threading.Barrier(2)

    def go():
        barrier.wait()
        results.append(repo.reserve([c, Charge("global", "all")], now=T0, tz_name=NY).ok)

    threads = [threading.Thread(target=go) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sorted(results) == [False, True]
    assert _row(pg_db, "day", "user", "9", repo.day_start(T0, NY))[0] == 1
    assert _row(pg_db, "day", "global", "all", repo.day_start(T0, NY))[0] == 1


@pg
def test_many_concurrent_reserves_never_pass_the_limit(pg_db):
    c = Charge("user", "5", day_limit=3)
    results: list[bool] = []
    lock = threading.Lock()

    def go():
        ok = repo.reserve([c], now=T0, tz_name=NY).ok
        with lock:
            results.append(ok)

    threads = [threading.Thread(target=go) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert results.count(True) == 3
    assert _row(pg_db, "day", "user", "5", repo.day_start(T0, NY))[0] == 3


@pg
def test_true_up_replaces_the_estimate_with_real_numbers(pg_db):
    res = repo.reserve([Charge("user", "1"), Charge("global", "all")], now=T0, tz_name=NY,
                       est_tokens_in=1000, est_tokens_out=200, est_cost_micro=300).reservation
    repo.true_up(res, tokens_in=1800, tokens_out=90, cost_micro=250)
    d = repo.day_start(T0, NY)
    assert _row(pg_db, "day", "user", "1", d) == (1, 1800, 90, 250, 0)
    assert _row(pg_db, "hour", "user", "1", repo.hour_start(T0, NY)) == (1, 1800, 90, 250, 0)
    assert _row(pg_db, "day", "global", "all", d) == (1, 1800, 90, 250, 0)


@pg
def test_give_back_removes_the_message_and_the_estimate(pg_db):
    res = repo.reserve([Charge("user", "1")], now=T0, tz_name=NY, weight=3,
                       est_tokens_in=100, est_tokens_out=10, est_cost_micro=7).reservation
    repo.give_back(res)
    assert _row(pg_db, "day", "user", "1", repo.day_start(T0, NY)) == (0, 0, 0, 0, 0)
    assert repo.reserve([Charge("user", "1", day_limit=1)], now=T0, tz_name=NY).ok


@pg
def test_add_denied_counts_on_its_own(pg_db):
    repo.add_denied("device", "dd", "day", now=T0, tz_name=NY)
    repo.add_denied("device", "dd", "day", now=T0, tz_name=NY)
    assert _row(pg_db, "day", "device", "dd", repo.day_start(T0, NY)) == (0, 0, 0, 0, 2)


@pg
def test_hour_and_day_buckets_roll_over_at_the_right_instant(pg_db):
    c = Charge("user", "1", hour_limit=1, day_limit=2)
    # 9:59:59 PM EST on Nov 13 = 02:59:59 UTC Nov 14
    late = datetime(2026, 11, 14, 2, 59, 59, tzinfo=UTC)
    assert repo.reserve([c], now=late, tz_name=NY).ok
    assert not repo.reserve([c], now=late, tz_name=NY).ok  # same hour
    assert repo.reserve([c], now=late + timedelta(seconds=1), tz_name=NY).ok  # new hour, same day
    # 11:59:59 PM EST then midnight: a new day
    eve = datetime(2026, 11, 14, 4, 59, 59, tzinfo=UTC)
    assert not repo.reserve([c], now=eve, tz_name=NY).ok  # day has 2 already (limit 2)
    midnight = datetime(2026, 11, 14, 5, 0, 0, tzinfo=UTC)
    assert repo.reserve([c], now=midnight, tz_name=NY).ok


@pg
def test_read_day_and_hour_totals(pg_db):
    repo.reserve([Charge("user", "1"), Charge("user", "2")], now=T0, tz_name=NY, est_tokens_in=10)
    repo.reserve([Charge("user", "1")], now=T0, tz_name=NY, est_tokens_in=10)
    day = repo.read_day_totals(now=T0, tz_name=NY, kind="user")
    assert [(r["subject_key"], r["messages"], r["tokens_in"]) for r in day] == [("1", 2, 20), ("2", 1, 10)]
    hour = repo.read_hour_totals(now=T0, tz_name=NY, kind="user", key="2")
    assert len(hour) == 1 and hour[0]["messages"] == 1
    assert repo.read_day_totals(now=T0 + timedelta(days=1), tz_name=NY) == []


@pg
def test_cleanup_removes_only_rows_older_than_35_days(pg_db):
    old = T0 - timedelta(days=40)
    recent = T0 - timedelta(days=34)
    repo.reserve([Charge("user", "1")], now=old, tz_name=NY)
    repo.reserve([Charge("user", "2")], now=recent, tz_name=NY)
    repo.reserve([Charge("user", "3")], now=T0, tz_name=NY)
    removed = repo.delete_older_than(now=T0)
    assert removed == 2  # the hour and day rows of the old user only
    with pg_db.connect() as conn:
        keys = sorted(r[0] for r in conn.execute("SELECT DISTINCT subject_key FROM chat_usage_buckets").fetchall())
    assert keys == ["2", "3"]


@pg
def test_app_settings_roundtrip_and_reader_precedence(pg_db, monkeypatch):
    from backend import settings_store as ss

    for spec in ss.SPECS.values():
        monkeypatch.delenv(spec.env, raising=False)
    ss.set_loader(None)
    try:
        assert ss.resolve("chat.user_per_hour") == (10, "default")
        repo.set_app_setting("chat.user_per_hour", "4", updated_by=1)
        ss.reset_cache()
        assert ss.resolve("chat.user_per_hour") == (4, "row")
        repo.set_app_setting("chat.user_per_hour", "oops")
        ss.reset_cache()
        assert ss.resolve("chat.user_per_hour") == (10, "default")
    finally:
        ss.reset_cache()


@pg
def test_no_message_text_column_exists(pg_db):
    with pg_db.connect() as conn:
        cols = {r[0] for r in conn.execute("SELECT column_name FROM information_schema.columns WHERE table_name='chat_usage_buckets'").fetchall()}
    assert cols == {"period", "bucket_start", "subject_kind", "subject_key", "messages", "tokens_in", "tokens_out", "cost_micro", "denied"}
