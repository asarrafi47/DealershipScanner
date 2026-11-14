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


# ---------------------------------------------------------------- wave 1 review follow-ups


@pytest.mark.parametrize("bad", ["\u00b2", "\u0663", "\u0661\u0662", "1\u0663", "\uff11", "\u2460"])
def test_a_value_with_a_non_ascii_digit_is_ignored_not_a_crash(monkeypatch, bad):
    monkeypatch.setenv("CHAT_USER_PER_HOUR", "7")
    ss.set_loader(lambda: {"chat.user_per_hour": bad, "chat.alert_pcts": "5" + bad})
    assert ss.resolve("chat.user_per_hour") == (7, "env")  # no ValueError out of int()
    assert ss.get("chat.alert_pcts") == (50, 80)
    monkeypatch.setenv("CHAT_IP_PER_DAY", bad)
    ss.set_loader(lambda: {})
    assert ss.resolve("chat.ip_per_day") == (100, "default")


def test_a_failed_settings_read_is_not_cached_so_the_next_call_asks_again():
    t = [1000.0]
    calls = []
    state = {"down": True}

    def loader():
        calls.append(1)
        if state["down"]:
            raise OSError("connection refused")
        return {"chat.user_per_hour": "2"}

    ss.set_loader(loader)
    ss.set_clock(lambda: t[0])
    assert ss.get_int("chat.user_per_hour") == 10  # database down: the default, for now
    state["down"] = False
    t[0] += 1  # one second later, far inside the 30 second window
    assert ss.get_int("chat.user_per_hour") == 2  # the blip did not stick for 30 seconds
    assert len(calls) == 2


def test_a_successful_empty_read_is_still_cached_for_30_seconds():
    t = [1000.0]
    calls = []
    ss.set_loader(lambda: calls.append(1) or {})
    ss.set_clock(lambda: t[0])
    ss.get_int("chat.user_per_hour")
    t[0] += 5
    ss.get_int("chat.user_per_hour")
    assert len(calls) == 1


def test_prices_and_the_cost_estimate_ship_non_zero():
    assert ss.get_int("chat.price_in_micro_per_mtok") > 0
    assert ss.get_int("chat.price_out_micro_per_mtok") > 0
    assert ss.get_int("chat.est_cost_micro_per_message") > 0


@pytest.mark.parametrize("key", ["chat.price_in_micro_per_mtok", "chat.price_out_micro_per_mtok",
                                 "chat.est_cost_micro_per_message"])
def test_a_zero_price_row_cannot_switch_the_dollar_cap_off(key):
    ss.set_loader(lambda: {key: "0"})
    assert ss.get_int(key) > 0 and ss.resolve(key)[1] == "default"


def test_budget_rows_are_clamped_to_the_env_only_hard_ceilings(monkeypatch, caplog):
    ss.set_loader(lambda: {"chat.budget_usd_per_day_cents": "5000", "chat.budget_tokens_per_day": "9000000"})
    with caplog.at_level(logging.WARNING, logger="backend.settings_store"):
        assert ss.resolve("chat.budget_usd_per_day_cents") == (2000, "ceiling")
        assert ss.resolve("chat.budget_tokens_per_day") == (8_000_000, "ceiling")
        ss.get_int("chat.budget_usd_per_day_cents")
    lines = [r.getMessage() for r in caplog.records if "ceiling" in r.getMessage()]
    assert len(lines) == 2 and "CHAT_BUDGET_HARD_MAX_CENTS" in " ".join(lines)  # logged once per key
    assert "5000" not in " ".join(lines)
    monkeypatch.setenv("CHAT_BUDGET_HARD_MAX_CENTS", "3000")
    assert ss.get_int("chat.budget_usd_per_day_cents") == 3000  # the ceiling comes from the environment only


def test_a_row_cannot_raise_its_own_ceiling():
    ss.set_loader(lambda: {"chat.budget_usd_per_day_cents": "99999", "CHAT_BUDGET_HARD_MAX_CENTS": "99999",
                           "chat.budget_hard_max_cents": "99999"})
    assert ss.get_int("chat.budget_usd_per_day_cents") == 2000


def test_a_bad_ceiling_variable_falls_back_to_the_default_ceiling(monkeypatch):
    monkeypatch.setenv("CHAT_BUDGET_HARD_MAX_CENTS", "\u0663\u0660\u0660\u0660")
    ss.set_loader(lambda: {"chat.budget_usd_per_day_cents": "5000"})
    assert ss.get_int("chat.budget_usd_per_day_cents") == 2000


def test_values_under_the_ceiling_are_untouched():
    ss.set_loader(lambda: {"chat.budget_usd_per_day_cents": "1500"})
    assert ss.resolve("chat.budget_usd_per_day_cents") == (1500, "row")


def test_sub_budget_and_give_back_settings_have_the_documented_defaults():
    assert ss.get_int("chat.new_account_budget_pct") == 25
    assert ss.get_int("chat.established_reserve_pct") == 50
    assert ss.get_int("chat.giveback_per_hour") == 3
