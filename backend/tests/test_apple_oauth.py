"""Tests for Apple OAuth scaffold (configuration gates only)."""

from __future__ import annotations

import importlib

import pytest

from backend.auth import apple_oauth


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch, tmp_path):
    monkeypatch.setenv("FLASK_ENV", "development")
    monkeypatch.setenv("USERS_DB_PATH", str(tmp_path / "users_test.db"))
    monkeypatch.setenv("DEV_USERS_DB_PATH", str(tmp_path / "dev_users_test.db"))
    monkeypatch.delenv("USERS_DB_ENCRYPTION_KEY", raising=False)
    monkeypatch.delenv("DEV_USERS_DB_ENCRYPTION_KEY", raising=False)
    import backend.main as main

    importlib.reload(main)
    main.app.config["TESTING"] = True
    return main.app.test_client()


def test_apple_not_configured_by_default(monkeypatch) -> None:
    for key in (
        "APPLE_OAUTH_CLIENT_ID",
        "APPLE_OAUTH_TEAM_ID",
        "APPLE_OAUTH_KEY_ID",
        "APPLE_OAUTH_PRIVATE_KEY",
        "APPLE_OAUTH_SHOW_BUTTON",
    ):
        monkeypatch.delenv(key, raising=False)
    assert apple_oauth.apple_oauth_configured() is False
    assert apple_oauth.apple_signin_visible() is False


def test_apple_visible_when_fully_configured(monkeypatch) -> None:
    monkeypatch.setenv("APPLE_OAUTH_CLIENT_ID", "com.example.web")
    monkeypatch.setenv("APPLE_OAUTH_TEAM_ID", "TEAM123")
    monkeypatch.setenv("APPLE_OAUTH_KEY_ID", "KEY123")
    monkeypatch.setenv("APPLE_OAUTH_PRIVATE_KEY", "-----BEGIN PRIVATE KEY-----\nMIGT\n-----END PRIVATE KEY-----")
    monkeypatch.setenv("APPLE_OAUTH_SHOW_BUTTON", "1")
    assert apple_oauth.apple_oauth_configured() is True
    assert apple_oauth.apple_signin_visible() is True


def test_apple_start_redirects_to_login_when_unconfigured(client, monkeypatch) -> None:
    monkeypatch.delenv("APPLE_OAUTH_CLIENT_ID", raising=False)
    rv = client.get("/auth/apple", follow_redirects=False)
    assert rv.status_code == 302
    assert "/login" in (rv.headers.get("Location") or "")
