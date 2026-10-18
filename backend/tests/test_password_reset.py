"""Tests for password reset (B2 scaffold)."""

from __future__ import annotations

import re
import time
from urllib.parse import parse_qs, urlparse

import pytest

from backend.auth.password_reset import (
    complete_password_reset,
    hash_reset_token,
    password_reset_enabled,
    request_password_reset,
)
from backend.db.users_db import (
    authenticate_app_user,
    get_user_id_by_password_reset_token_hash,
    init_users_db,
    save_user,
    set_user_password_reset_token,
)


@pytest.fixture()
def reset_env(monkeypatch, tmp_path):
    db_path = tmp_path / "users.db"
    monkeypatch.setenv("USERS_DB_PATH", str(db_path))
    monkeypatch.setenv("PASSWORD_RESET_ENABLED", "1")
    monkeypatch.setenv("PASSWORD_RESET_PEPPER", "test-reset-pepper")
    monkeypatch.setenv("MFA_DELIVERY_MODE", "log")
    # In-process rate-limit state (cleared per test by the root conftest), never a shared file.
    monkeypatch.setenv("RATE_LIMIT_SQLITE_PATH", "")
    init_users_db()
    yield db_path


def test_password_reset_disabled_by_default(monkeypatch) -> None:
    monkeypatch.delenv("PASSWORD_RESET_ENABLED", raising=False)
    assert password_reset_enabled() is False


def test_request_and_complete_reset(reset_env) -> None:
    uid = save_user("resetuser", "reset@example.com", "oldpassword1", role="general_user")
    assert request_password_reset(login_input="reset@example.com") is True

    token = "reset-token-abc"
    set_user_password_reset_token(uid, hash_reset_token(token), int(time.time()) + 3600)
    ok, msg = complete_password_reset(token=token, new_password="newpassword9")
    assert ok is True
    assert authenticate_app_user("reset@example.com", "newpassword9")
    assert not authenticate_app_user("reset@example.com", "oldpassword1")


def test_complete_rejects_expired_token(reset_env) -> None:
    uid = save_user("expired", "exp@example.com", "oldpassword1", role="general_user")
    token = "expired-token"
    set_user_password_reset_token(uid, hash_reset_token(token), int(time.time()) - 10)
    ok, _msg = complete_password_reset(token=token, new_password="newpassword9")
    assert ok is False


# --- P3.6: per-account reset cooldown --------------------------------------------------


@pytest.fixture()
def sent_emails(monkeypatch):
    """Capture reset emails instead of sending them."""
    sent: list[dict] = []

    def _fake_send(*, to_email, subject, text, log_surface="app"):
        sent.append({"to": to_email, "text": text})

    monkeypatch.setattr("backend.auth.password_reset.send_transactional_email", _fake_send)
    return sent


def _token_from_email(msg: dict) -> str:
    url = next(line for line in msg["text"].splitlines() if "reset-password?" in line)
    return parse_qs(urlparse(url.strip()).query)["token"][0]


def test_fourth_reset_request_within_hour_sends_no_email(reset_env, sent_emails) -> None:
    uid = save_user("cooldown", "cool@example.com", "oldpassword1", role="general_user")

    for _ in range(3):
        assert request_password_reset(login_input="cool@example.com") is True
    assert len(sent_emails) == 3
    third_token = _token_from_email(sent_emails[-1])

    assert request_password_reset(login_input="cool@example.com") is True
    assert len(sent_emails) == 3
    # The throttled request did not rotate the token: the last emailed link still works.
    assert get_user_id_by_password_reset_token_hash(hash_reset_token(third_token)) == uid


def test_reset_cooldown_counts_per_account_across_login_forms(reset_env, sent_emails) -> None:
    save_user("bymail", "bymail@example.com", "oldpassword1", role="general_user")
    for login in ("bymail@example.com", "bymail", "BYMAIL@example.com", "bymail"):
        assert request_password_reset(login_input=login) is True
    assert len(sent_emails) == 3


def test_reset_cooldown_is_per_account(reset_env, sent_emails) -> None:
    save_user("first", "first@example.com", "oldpassword1", role="general_user")
    save_user("second", "second@example.com", "oldpassword1", role="general_user")
    for _ in range(4):
        request_password_reset(login_input="first@example.com")
    assert request_password_reset(login_input="second@example.com") is True
    assert [m["to"] for m in sent_emails].count("first@example.com") == 3
    assert [m["to"] for m in sent_emails].count("second@example.com") == 1


def test_reset_cooldown_window_expires(reset_env, sent_emails, monkeypatch) -> None:
    import backend.utils.ip_rate_limit as rl

    save_user("later", "later@example.com", "oldpassword1", role="general_user")
    clock = [1000.0]
    monkeypatch.setattr(rl.time, "monotonic", lambda: clock[0])
    for _ in range(4):
        request_password_reset(login_input="later@example.com")
    assert len(sent_emails) == 3
    clock[0] += 3601.0
    request_password_reset(login_input="later@example.com")
    assert len(sent_emails) == 4


def _post_forgot(client, login: str):
    from flask import session

    client.get("/forgot-password")
    return client.post(
        "/forgot-password",
        data={"csrf_token": session.get("_csrf_token"), "login": login},
        follow_redirects=False,
    )


def _page_text(resp) -> str:
    # Per-request CSP nonces are the only expected difference between two renders.
    return re.sub(r'nonce="[^"]*"', 'nonce=""', resp.get_data(as_text=True))


def test_forgot_password_route_fourth_request_same_page_no_email(app_factory, sent_emails) -> None:
    app = app_factory(
        PASSWORD_RESET_ENABLED="1",
        PASSWORD_RESET_PEPPER="test-reset-pepper",
        RATE_LIMIT_SQLITE_PATH="",
        ALLOW_DEFAULT_APP_USER="0",
        BILLING_STRIPE_ENABLED="0",
    ).app
    save_user("routeuser", "route@example.com", "oldpassword1", role="general_user")

    client = app.test_client()
    with client:
        responses = [_post_forgot(client, "route@example.com") for _ in range(4)]
        unknown = _post_forgot(client, "nobody@example.com")

    assert [r.status_code for r in responses] == [200, 200, 200, 200]
    assert len(sent_emails) == 3
    assert _page_text(responses[3]) == _page_text(responses[2])
    # Same page as an unknown account: the cooldown does not reveal that the account exists.
    assert unknown.status_code == 200
    assert _page_text(unknown) == _page_text(responses[3])
