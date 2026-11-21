"""C5: the admin chat usage page and settings form (docs/auth_sms/TASKS.md, CHAT_LIMITS.md section 8).

The form tests use a fake settings table (a dict behind ``chat_usage_repo.set_app_setting`` and the
settings loader), so they run offline; the one ``pg`` test runs the real queries on Postgres. No
SQLite construct, no network, no phone number and no message text anywhere.
"""
from __future__ import annotations

import logging
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from backend import settings_store
from backend.dealer.admin import chat_usage_hub as hub
from backend.utils.csrf import _SESSION_KEY as CSRF_KEY

CSRF = "csrf-test-token"
URL = "/admin/chat-usage"
ROOT = Path(__file__).resolve().parents[2]
BOX_WORDS = ("card", "box", "pill", "badge", "chip", "panel", "tag", "banner", "callout", "alert")

FAKE_USAGE = {
    "error": None, "status": "on", "messages": 12, "tokens_in": 30000, "tokens_out": 6000,
    "dollars": 0.42, "dollars_pct": 8, "tokens_pct": 2, "budget_dollars": 5.0, "budget_tokens": 2_000_000,
    "next_reset": "2026-11-22 00:00 EST",
    "days": [{"day": "Fri 2026-11-20", "messages": 40, "tokens": 90000, "dollars": 1.5, "refusals": 3}],
    "top": [{"id": "7", "username": "alice", "messages": 9}],
    "denied": [{"label": "Account", "count": 2}, {"label": "Site budget", "count": 0}],
}


@pytest.fixture()
def rows():
    """A fake app_settings table behind the repository and the settings loader."""
    return {}


@pytest.fixture()
def settings(rows, monkeypatch):
    from backend.db.repositories import chat_usage_repo as repo

    written: list[tuple[str, str, int | None]] = []

    def fake_set(key, value, updated_by=None, *, conn=None):
        rows[key] = value
        written.append((key, value, updated_by))

    monkeypatch.setattr(repo, "set_app_setting", fake_set)
    for env in ("CHAT_BUDGET_HARD_MAX_CENTS", "CHAT_BUDGET_HARD_MAX_TOKENS", "CHAT_ENABLED",
                "AUTH_PHONE_SENDS_PAUSED"):
        monkeypatch.delenv(env, raising=False)
    settings_store.set_loader(lambda: dict(rows))
    yield written
    settings_store.set_loader(None)


@pytest.fixture()
def app(app_factory, settings, monkeypatch):
    monkeypatch.setattr(hub, "build_snapshot", lambda now=None: dict(FAKE_USAGE))
    return app_factory().app


def _user(name: str, role: str) -> int:
    from backend.db.users_db import save_user

    return int(save_user(name, f"{name}@example.com", "a-long-test-password-1", role=role))


def _client(app, role: str = "admin", name: str = "boss"):
    from backend.auth import session as auth_session

    uid = _user(name, role)
    c = app.test_client()
    with c.session_transaction() as sess:
        sess["user_id"] = uid
        sess[auth_session.AUTH_AT] = time.time()  # the sign-in time a real sign-in writes (card S10)
        sess[CSRF_KEY] = CSRF
    return c, uid


def _post(c, data: dict):
    """Post with the session's current CSRF token, as the page's hidden field carries it (a GET may re-mint it)."""
    with c.session_transaction() as sess:
        token = sess.get(CSRF_KEY) or CSRF
    return c.post(URL, data={"csrf_token": token, **data}, follow_redirects=False)


def _flashes(c) -> list[str]:
    with c.session_transaction() as sess:
        return [m for _cat, m in sess.get("_flashes", [])]


# --------------------------------------------------------------------------- access


def test_non_admin_gets_the_normal_redirect(app):
    c, _ = _client(app, role="general_user", name="shopper")
    for r in (c.get(URL), _post(c, {"action": "pause_chat"})):
        assert r.status_code in (302, 303)
        assert "/chat-usage" not in (r.headers.get("Location") or "")  # sent away, not shown the page
    # nothing was changed by the refused POST
    assert settings_store.get_bool("chat.enabled") is True
    anon = app.test_client().get(URL)
    assert anon.status_code in (302, 303)


def test_a_demoted_admin_loses_the_page_at_once(app):
    """The role is re-read from the database on every request, not trusted from the session."""
    from backend.db.users_db import set_user_is_active

    c, uid = _client(app, name="soonsuspended")
    assert c.get(URL).status_code == 200
    assert set_user_is_active(uid, active=False)
    assert c.get(URL).status_code in (302, 303)


def test_a_post_without_the_csrf_token_is_refused(app):
    c, _ = _client(app)
    r = c.post(URL, data={"action": "pause_chat"}, follow_redirects=False)
    assert r.status_code == 403
    assert settings_store.get_bool("chat.enabled") is True


# --------------------------------------------------------------------------- the page


def test_page_shows_today_the_days_the_top_accounts_and_the_settings(app):
    c, _ = _client(app)
    page = c.get(URL).get_data(as_text=True)
    for text in ("Today so far", "Last 14 days", "Busiest accounts today", "Refusals today, by kind",
                 "alice", "Fri 2026-11-20", "$0.42 of $5.00", "chat.user_per_day", "Pause chat now",
                 "Pause text messages", "settings row", "default"):
        assert text in page, text
    assert "2026-11-22 00:00 EST" in page


def test_page_without_usage_figures_still_shows_the_settings(app, monkeypatch):
    monkeypatch.setattr(hub, "build_snapshot", lambda now=None: {"error": hub.UNAVAILABLE})
    c, _ = _client(app)
    page = c.get(URL).get_data(as_text=True)
    assert hub.UNAVAILABLE in page and "chat.user_per_day" in page and "Save settings" in page


def test_build_snapshot_never_raises_without_a_database(monkeypatch):
    monkeypatch.setenv("INVENTORY_DATABASE_URL", "")
    assert hub.build_snapshot() == {"error": hub.UNAVAILABLE}


def test_template_draws_no_cards_boxes_or_pills():
    src = (ROOT / "frontend" / "templates" / "admin" / "chat_usage.html").read_text(encoding="utf-8")
    classes = [c for attr in re.findall(r'class="([^"]*)"', src) for c in attr.split()]
    assert classes, "expected some class names"
    assert not [c for c in classes if any(w in c for w in BOX_WORDS)], classes
    assert "<table" in src and "style=" not in src


def test_page_holds_no_phone_number_or_message_text(app):
    c, _ = _client(app)
    page = c.get(URL).get_data(as_text=True)
    assert not re.search(r"\+?\d{1,2}[\s().-]*\d{3}[\s().-]*\d{3}[\s.-]*\d{4}", re.sub(r"\d{5,}", "", page))
    assert set(FAKE_USAGE["top"][0]) == {"id", "username", "messages"}  # the shape the hub hands the template


# --------------------------------------------------------------------------- editing


def test_an_edit_changes_the_live_limit(app, settings):
    c, uid = _client(app)
    assert settings_store.get_int("chat.user_per_day") == 40
    r = _post(c, {"action": "save", "s__chat.user_per_day": "25"})
    assert r.status_code in (302, 303)
    assert settings_store.get_int("chat.user_per_day") == 25  # live at once, not after the cache time
    assert settings_store.resolve("chat.user_per_day") == (25, "row")
    assert settings.count(("chat.user_per_day", "25", uid)) == 1  # written with the admin's id
    assert any("Saved chat.user_per_day" in m for m in _flashes(c))


def test_a_bad_value_is_refused_with_a_sentence_and_nothing_is_written(app, settings):
    c, _ = _client(app)
    _post(c, {"action": "save", "s__chat.user_per_hour": "5", "s__chat.user_per_day": "lots"})
    assert settings == []  # the good value was not applied either
    msgs = _flashes(c)
    assert any(m.startswith("chat.user_per_day: that is not a valid value.") for m in msgs)
    assert "Nothing was changed." in msgs
    assert settings_store.get_int("chat.user_per_hour") == 10


def test_zero_for_a_per_subject_limit_needs_the_unlimited_tick(app, settings):
    c, uid = _client(app)
    _post(c, {"action": "save", "s__chat.phone_per_day": "0"})
    assert settings == []
    assert any('Tick "unlimited"' in m for m in _flashes(c))
    _post(c, {"action": "save", "s__chat.phone_per_day": "40", "unlimited__chat.phone_per_day": "1"})
    assert settings == [("chat.phone_per_day", "0", uid)]
    assert settings_store.get_int("chat.phone_per_day") == 0


def test_a_budget_above_the_environment_ceiling_is_refused(app, settings, monkeypatch):
    monkeypatch.setenv("CHAT_BUDGET_HARD_MAX_CENTS", "1000")
    c, _ = _client(app)
    _post(c, {"action": "save", "s__chat.budget_usd_per_day_cents": "1001"})
    assert settings == []
    assert any("above 1000" in m for m in _flashes(c))
    _post(c, {"action": "save", "s__chat.budget_usd_per_day_cents": "1000"})
    assert settings_store.get_int("chat.budget_usd_per_day_cents") == 1000


def test_other_kinds_validate_through_the_same_parser(app, settings):
    c, _ = _client(app)
    _post(c, {"action": "save", "s__chat.reset_tz": "Not/AZone", "s__chat.alert_pcts": "50, 80, 95"})
    assert [m for m in _flashes(c) if m.startswith("chat.reset_tz")]
    assert settings == []
    _post(c, {"action": "save", "s__chat.reset_tz": "America/Chicago", "s__chat.alert_pcts": "50, 80, 95"})
    assert settings_store.get_str("chat.reset_tz") == "America/Chicago"
    assert settings_store.get("chat.alert_pcts") == (50, 80, 95)


def test_unchanged_values_write_nothing(app, settings):
    c, _ = _client(app)
    page = c.get(URL).get_data(as_text=True)
    form = dict(re.findall(r'name="(s__[^"]+)" value="([^"]*)"', page))
    assert "s__chat.user_per_day" in form
    r = _post(c, {"action": "save", **form})
    assert r.status_code in (302, 303), (r.status_code, r.get_data(as_text=True)[:200])
    assert settings == []
    assert "No change to save." in _flashes(c)


def test_every_change_is_logged_with_the_admin_id_and_the_old_and_new_value(app, settings, caplog):
    c, uid = _client(app)
    with caplog.at_level(logging.INFO, logger="chat_usage_admin"):
        _post(c, {"action": "save", "s__chat.user_per_day": "25"})
    line = [r.getMessage() for r in caplog.records if r.name == "chat_usage_admin"]
    assert line == [f"event=setting_changed admin={uid} key=chat.user_per_day old=40 new=25"]


# --------------------------------------------------------------------------- the switches


def test_pause_chat_stops_chat_and_shows_the_paused_sentence(app, settings):
    from backend.chat_budget import messages as msg
    from backend.chat_budget.gate import ChatDenied, guard

    c, uid = _client(app)
    _post(c, {"action": "pause_chat"})
    assert settings == [("chat.enabled", "false", uid)]
    with pytest.raises(ChatDenied) as denied:
        with guard("car", 1, None, None, None, 1):
            pass
    assert denied.value.denial.message == msg.PAUSED_TEXT
    page = c.get(URL).get_data(as_text=True)
    assert "Chat is <strong>paused</strong>" in page and "Turn chat back on" in page
    _post(c, {"action": "resume_chat"})
    assert settings_store.get_bool("chat.enabled") is True


def test_the_text_message_switch_flips_auth_phone_sends_paused(app, settings):
    c, uid = _client(app)
    assert settings_store.get_bool("auth_phone.sends_paused") is False
    _post(c, {"action": "pause_texts"})
    assert settings_store.get_bool("auth_phone.sends_paused") is True
    assert "Turn text messages back on" in c.get(URL).get_data(as_text=True)
    _post(c, {"action": "resume_texts"})
    assert settings_store.get_bool("auth_phone.sends_paused") is False
    assert [s[0] for s in settings] == ["auth_phone.sends_paused"] * 2 and {s[2] for s in settings} == {uid}


def test_a_database_failure_while_saving_says_nothing_was_changed(app, monkeypatch):
    from backend.db.repositories import chat_usage_repo as repo

    def boom(*a, **k):
        raise RuntimeError("database down")

    monkeypatch.setattr(repo, "set_app_setting", boom)
    c, _ = _client(app)
    r = _post(c, {"action": "pause_chat"})
    assert r.status_code in (302, 303)
    assert "The change could not be saved. Nothing was changed." in _flashes(c)


def test_the_page_is_linked_from_the_admin_sidebar():
    nav = (ROOT / "frontend" / "templates" / "_nav_sidebar.html").read_text(encoding="utf-8")
    assert "admin_chat_usage_hub" in nav and "Chat usage" in nav


# --------------------------------------------------------------------------- the real queries (Postgres)


@pytest.mark.pg
def test_snapshot_reads_the_real_tables_on_postgres(pg_db, app_factory, monkeypatch):
    from backend.db.repositories import chat_usage_repo as repo
    from backend.db.users_db import save_user

    app_factory()  # starts the users database the usernames come from; the Postgres URL is left alone
    for env in ("CHAT_ENABLED", "CHAT_BUDGET_HARD_MAX_CENTS", "CHAT_BUDGET_HARD_MAX_TOKENS"):
        monkeypatch.delenv(env, raising=False)
    settings_store.set_loader(lambda: {})
    try:
        now = datetime.now(timezone.utc)
        tz = settings_store.get_str("chat.reset_tz")
        today = repo.day_start(now, tz)
        yesterday = repo.day_start(today - timedelta(seconds=1), tz)
        ids = [int(save_user(f"u{n:02d}", f"u{n:02d}@example.com", "a-long-test-password-1", role="general_user"))
               for n in range(25)]
        with pg_db.connect() as conn:
            ins = ("INSERT INTO public.chat_usage_buckets (period, bucket_start, subject_kind, subject_key, "
                   "messages, tokens_in, tokens_out, cost_micro, denied) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)")
            conn.execute(ins, ("day", today, "global", "all", 100, 200_000, 50_000, 1_000_000, 4))
            conn.execute(ins, ("day", yesterday, "global", "all", 7, 1_000, 500, 20_000, 0))
            for n, uid in enumerate(ids):
                conn.execute(ins, ("day", today, "user", str(uid), n + 1, 0, 0, 0, 1 if n == 24 else 0))
            conn.commit()
        snap = hub.build_snapshot(now)
        assert snap["error"] is None and snap["status"] == "on"
        assert (snap["messages"], snap["tokens_in"], snap["tokens_out"]) == (100, 200_000, 50_000)
        assert snap["dollars"] == 1.0 and snap["dollars_pct"] == 20  # $1.00 of the $5.00 default budget
        assert snap["tokens_pct"] == 12  # 250,000 of 2,000,000 tokens
        assert len(snap["top"]) == 20 and snap["top"][0]["messages"] == 25  # busiest first, capped at 20
        assert set(snap["top"][0]) == {"id", "username", "messages"} and snap["top"][0]["username"] == "u24"
        assert len(snap["days"]) == 14 and snap["days"][0]["messages"] == 7
        assert {d["label"]: d["count"] for d in snap["denied"]}["Account"] == 1
        # a budget spent turns the status
        settings_store.set_loader(lambda: {"chat.budget_usd_per_day_cents": "100"})
        assert hub.build_snapshot(now)["status"].startswith("stopped by the budget")
        settings_store.set_loader(lambda: {"chat.enabled": "false"})
        assert hub.build_snapshot(now)["status"] == "paused by an admin"
    finally:
        settings_store.set_loader(None)
