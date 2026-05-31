"""POST /api/auth/register uses the same users.db as the website."""

from __future__ import annotations

import importlib

import pytest


def _fresh_app(monkeypatch, tmp_path):
    monkeypatch.setenv("FLASK_ENV", "development")
    monkeypatch.setenv("MFA_DELIVERY_MODE", "log")
    monkeypatch.setenv("USERS_DB_PATH", str(tmp_path / "users_test.db"))
    monkeypatch.setenv("DEV_USERS_DB_PATH", str(tmp_path / "dev_users_test.db"))
    monkeypatch.delenv("USERS_DB_ENCRYPTION_KEY", raising=False)
    import backend.main as main

    importlib.reload(main)
    return main.app


def _csrf(client) -> str:
    rv = client.get("/api/auth/csrf")
    return rv.get_json()["csrf_token"]


def test_api_register_creates_user_visible_on_web_login(monkeypatch, tmp_path) -> None:
    app = _fresh_app(monkeypatch, tmp_path)
    with app.test_client() as client:
        token = _csrf(client)
        rv = client.post(
            "/api/auth/register",
            json={
                "username": "ios_user",
                "email": "ios@example.com",
                "password": "long-enough-password",
                "plan": "free",
            },
            headers={"X-CSRF-Token": token},
        )
        assert rv.status_code == 200
        body = rv.get_json()
        assert body["ok"] is True
        assert body["user"]["username"] == "ios_user"

        me = client.get("/api/auth/me")
        assert me.status_code == 200

        client.post("/api/auth/logout", headers={"X-CSRF-Token": _csrf(client)})

        token = _csrf(client)
        login_rv = client.post(
            "/api/auth/login",
            json={"login": "ios@example.com", "password": "long-enough-password"},
            headers={"X-CSRF-Token": token},
        )
        assert login_rv.status_code == 200
        assert login_rv.get_json()["user"]["email"] == "ios@example.com"


def test_api_register_duplicate_returns_409(monkeypatch, tmp_path) -> None:
    app = _fresh_app(monkeypatch, tmp_path)
    payload = {
        "username": "dup_user",
        "email": "dup@example.com",
        "password": "long-enough-password",
    }
    with app.test_client() as client:
        token = _csrf(client)
        hdr = {"X-CSRF-Token": token}
        assert client.post("/api/auth/register", json=payload, headers=hdr).status_code == 200
        token = _csrf(client)
        rv = client.post("/api/auth/register", json=payload, headers={"X-CSRF-Token": token})
    assert rv.status_code == 409
    assert rv.get_json().get("error") == "duplicate_user"


def test_api_register_requires_csrf(monkeypatch, tmp_path) -> None:
    app = _fresh_app(monkeypatch, tmp_path)
    with app.test_client() as client:
        rv = client.post(
            "/api/auth/register",
            json={
                "username": "no_csrf",
                "email": "no_csrf@example.com",
                "password": "long-enough-password",
            },
        )
    assert rv.status_code == 403
