"""C1: settings reader precedence (row > environment > default), validation, 30 s cache."""

from __future__ import annotations

import logging

import pytest

from backend import settings_store as ss


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    for spec in ss.SPECS.values():
        monkeypatch.delenv(spec.env, raising=False)
    ss.set_loader(lambda: {})
    ss.set_clock(None)
    yield
    ss.set_loader(None)
    ss.set_clock(None)


def test_default_when_nothing_set():
    assert ss.get_int("chat.user_per_hour") == 10
    assert ss.get_int("chat.user_per_day") == 40
    assert ss.get_int("chat.budget_usd_per_day_cents") == 500
    assert ss.resolve("chat.user_per_hour")[1] == "default"


def test_environment_beats_default(monkeypatch):
    monkeypatch.setenv("CHAT_USER_PER_HOUR", "7")
    assert ss.resolve("chat.user_per_hour") == (7, "env")


def test_existing_env_names_are_kept(monkeypatch):
    monkeypatch.setenv("CAR_CHAT_MAX_PER_USER_DAILY", "25")
    monkeypatch.setenv("CAR_CHAT_MAX_PER_LISTING_DAILY", "4")
    assert ss.get_int("chat.user_per_day") == 25
    assert ss.get_int("chat.listing_per_day") == 4


def test_row_beats_environment_and_default(monkeypatch):
    monkeypatch.setenv("CHAT_USER_PER_HOUR", "7")
    ss.set_loader(lambda: {"chat.user_per_hour": "3"})
    assert ss.resolve("chat.user_per_hour") == (3, "row")


@pytest.mark.parametrize("bad", ["abc", "-5", "1.5", "", "99999999999999"])
def test_bad_row_falls_back_to_environment(monkeypatch, bad):
    monkeypatch.setenv("CHAT_USER_PER_HOUR", "7")
    ss.set_loader(lambda: {"chat.user_per_hour": bad})
    assert ss.resolve("chat.user_per_hour") == (7, "env")


@pytest.mark.parametrize("bad", ["abc", "-1", "1e3", "99999999999999"])
def test_bad_environment_falls_back_to_default(monkeypatch, bad):
    monkeypatch.setenv("CHAT_DEVICE_PER_DAY", bad)
    assert ss.resolve("chat.device_per_day") == (60, "default")


def test_zero_is_allowed_it_means_no_limit():
    ss.set_loader(lambda: {"chat.ip_per_day": "0"})
    assert ss.get_int("chat.ip_per_day") == 0


def test_bad_value_logs_one_line_without_the_value(caplog):
    ss.set_loader(lambda: {"chat.user_per_hour": "secret-looking-nonsense"})
    with caplog.at_level(logging.WARNING, logger="backend.settings_store"):
        ss.get_int("chat.user_per_hour")
        ss.get_int("chat.user_per_hour")
    lines = [r.getMessage() for r in caplog.records if "chat.user_per_hour" in r.getMessage()]
    assert len(lines) == 1
    assert "secret-looking-nonsense" not in lines[0]


def test_bool_tz_and_pcts_validation(monkeypatch):
    ss.set_loader(lambda: {"chat.enabled": "off", "chat.reset_tz": "Nowhere/Land", "chat.alert_pcts": "90, 40"})
    assert ss.get_bool("chat.enabled") is False
    assert ss.get_str("chat.reset_tz") == "America/New_York"
    assert ss.get("chat.alert_pcts") == (40, 90)
    ss.set_loader(lambda: {"chat.enabled": "maybe", "chat.alert_pcts": "50,x"})
    assert ss.get_bool("chat.enabled") is True
    assert ss.get("chat.alert_pcts") == (50, 80)


def test_values_cached_for_30_seconds_then_reread():
    t = [1000.0]
    ss.set_clock(lambda: t[0])
    calls = []
    store = {"chat.user_per_hour": "5"}

    def loader():
        calls.append(1)
        return dict(store)

    ss.set_loader(loader)
    ss.set_clock(lambda: t[0])
    assert ss.get_int("chat.user_per_hour") == 5
    store["chat.user_per_hour"] = "6"
    t[0] += 29
    assert ss.get_int("chat.user_per_hour") == 5 and len(calls) == 1
    t[0] += 2
    assert ss.get_int("chat.user_per_hour") == 6 and len(calls) == 2


def test_unreadable_database_uses_environment_and_defaults(monkeypatch):
    def boom():
        raise RuntimeError("relation app_settings does not exist")

    ss.set_loader(boom)
    monkeypatch.setenv("CHAT_IP_PER_DAY", "11")
    assert ss.resolve("chat.ip_per_day") == (11, "env")
    assert ss.resolve("chat.user_per_hour") == (10, "default")


def test_empty_settings_table_gives_the_documented_defaults():
    got = {k: v["value"] for k, v in ss.all_settings().items()}
    assert got["chat.enabled"] is True
    assert got["chat.user_per_hour"] == 10 and got["chat.user_per_day"] == 40
    assert got["chat.phone_per_day"] == 40 and got["chat.device_per_day"] == 60
    assert got["chat.ip_per_day"] == 100 and got["chat.new_account_factor_pct"] == 50
    assert got["chat.budget_usd_per_day_cents"] == 500 and got["chat.budget_tokens_per_day"] == 2_000_000
    assert got["chat.reset_tz"] == "America/New_York" and got["chat.alert_pcts"] == (50, 80)
