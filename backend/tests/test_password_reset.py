"""Tests for password reset (B2 scaffold)."""

from __future__ import annotations

import time

import pytest

from backend.auth.password_reset import (
    complete_password_reset,
    hash_reset_token,
    password_reset_enabled,
    request_password_reset,
)
from backend.db.users_db import (
    authenticate_app_user,
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
