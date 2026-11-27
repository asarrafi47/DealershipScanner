"""Review R4: a phone session stamps last_login_at at most once a day, best effort, never with the flag off."""

from __future__ import annotations

import time

import pytest
from flask import Flask, session

from backend.auth import session as auth_session
from backend.auth.phone import accounts


@pytest.fixture
def ctx():
    app = Flask("touch_probe")
    app.secret_key = "t" * 32
    with app.test_request_context("/"):
        yield


def _calls(monkeypatch, *, fail: bool = False) -> list[int]:
    calls: list[int] = []

    def fake(uid, *, now=None):
        calls.append(uid)
        if fail:
            raise RuntimeError("database down")
        return True

    monkeypatch.setattr(accounts, "touch_phone_last_login", fake)
    return calls


def test_no_database_call_while_the_flag_is_off(ctx, monkeypatch):
    monkeypatch.delenv("AUTH_PHONE_LOGIN_ENABLED", raising=False)
    calls = _calls(monkeypatch)
    session[auth_session.AUTH_METHOD] = "phone"
    auth_session._touch_phone_login(7, time.time())
    assert calls == [] and auth_session.PHONE_TOUCH_DAY not in session


def test_no_call_for_a_password_session(ctx, monkeypatch):
    monkeypatch.setenv("AUTH_PHONE_LOGIN_ENABLED", "1")
    calls = _calls(monkeypatch)
    session[auth_session.AUTH_METHOD] = "password"
    auth_session._touch_phone_login(7, time.time())
    assert calls == []


def test_once_a_day_for_a_phone_session(ctx, monkeypatch):
    monkeypatch.setenv("AUTH_PHONE_LOGIN_ENABLED", "1")
    calls = _calls(monkeypatch)
    session[auth_session.AUTH_METHOD] = "phone"
    now = time.time()
    auth_session._touch_phone_login(7, now)
    auth_session._touch_phone_login(7, now + 60)
    assert calls == [7]
    auth_session._touch_phone_login(7, now + 86400)
    assert calls == [7, 7]


def test_a_database_error_never_ends_the_session(ctx, monkeypatch, caplog):
    monkeypatch.setenv("AUTH_PHONE_LOGIN_ENABLED", "1")
    calls = _calls(monkeypatch, fail=True)
    session[auth_session.AUTH_METHOD] = "phone"
    session["user_id"] = 7
    with caplog.at_level("WARNING"):
        auth_session._touch_phone_login(7, time.time())
    assert calls == [7] and session["user_id"] == 7
    assert "event=phone_login_touch_failed err=RuntimeError" in caplog.text
