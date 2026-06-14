"""Tests for email verification (B1 scaffold)."""

from __future__ import annotations

import os

import pytest

from backend.auth.email_verification import (
    email_verification_enabled,
    hash_verify_token,
    issue_and_send_verification_email,
    user_needs_email_verification,
    verify_email_token,
)
from backend.db.users_db import (
    get_user_email_verification_state,
    init_users_db,
    mark_user_email_verified,
    save_user,
    set_user_email_verify_token,
)


@pytest.fixture()
def email_verify_env(monkeypatch, tmp_path):
    db_path = tmp_path / "users.db"
    monkeypatch.setenv("USERS_DB_PATH", str(db_path))
    monkeypatch.setenv("EMAIL_VERIFICATION_ENABLED", "1")
    monkeypatch.setenv("EMAIL_VERIFY_PEPPER", "test-pepper")
    monkeypatch.setenv("MFA_DELIVERY_MODE", "log")
    init_users_db()
    yield db_path


def test_email_verification_disabled_by_default(monkeypatch) -> None:
    monkeypatch.delenv("EMAIL_VERIFICATION_ENABLED", raising=False)
    assert email_verification_enabled() is False


def test_issue_and_verify_round_trip(email_verify_env, monkeypatch) -> None:
    uid = save_user("verifyuser", "verify@example.com", "password123", role="general_user")
    assert issue_and_send_verification_email(user_id=uid, to_email="verify@example.com") is True
    state = get_user_email_verification_state(uid)
    assert state
    assert state.get("email_verify_token_hash")
    assert not state.get("email_verified_at")
    assert user_needs_email_verification(uid) is True

    token = "known-test-token"
    set_user_email_verify_token(uid, hash_verify_token(token))
    ok, msg = verify_email_token(token)
    assert ok is True
    assert "verified" in msg.lower()
    assert user_needs_email_verification(uid) is False


def test_verify_rejects_bad_token(email_verify_env) -> None:
    ok, msg = verify_email_token("not-a-real-token")
    assert ok is False
    assert msg


def test_oauth_user_marked_verified(email_verify_env) -> None:
    uid = save_user("oauthuser", "oauth@example.com", "password123", role="general_user")
    mark_user_email_verified(uid)
    assert user_needs_email_verification(uid) is False


def test_issue_skipped_when_disabled(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "users.db"
    monkeypatch.setenv("USERS_DB_PATH", str(db_path))
    monkeypatch.delenv("EMAIL_VERIFICATION_ENABLED", raising=False)
    init_users_db()
    uid = save_user("offuser", "off@example.com", "password123", role="general_user")
    assert issue_and_send_verification_email(user_id=uid, to_email="off@example.com") is False
