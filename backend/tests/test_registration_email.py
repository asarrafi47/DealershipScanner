"""Registration email normalization and duplicate detection."""

from __future__ import annotations

import importlib

import pytest


def _fresh_app(monkeypatch: pytest.MonkeyPatch, tmp_path):
    monkeypatch.setenv("FLASK_ENV", "development")
    monkeypatch.setenv("MFA_DELIVERY_MODE", "log")
    monkeypatch.setenv("USERS_DB_PATH", str(tmp_path / "users_test.db"))
    monkeypatch.setenv("DEV_USERS_DB_PATH", str(tmp_path / "dev_users_test.db"))
    monkeypatch.setenv("BILLING_STRIPE_ENABLED", "0")
    monkeypatch.setenv("ALLOW_DEFAULT_APP_USER", "0")
    monkeypatch.delenv("USERS_DB_ENCRYPTION_KEY", raising=False)
    import backend.main as main

    importlib.reload(main)
    return main.app


def _register(client, *, username: str, email: str, password: str = "long-enough-password"):
    client.get("/register")
    from flask import session

    csrf = session.get("_csrf_token")
    return client.post(
        "/register",
        data={
            "csrf_token": csrf,
            "username": username,
            "email": email,
            "password": password,
            "plan": "free",
        },
        follow_redirects=False,
    )


def test_register_normalizes_email_and_rejects_case_duplicate(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    app = _fresh_app(monkeypatch, tmp_path)
    client = app.test_client()
    with client:
        r1 = _register(client, username="user1", email="Test@Example.com")
        assert r1.status_code in (302, 303)

        from backend.db.users_db import get_user_by_login

        u = get_user_by_login("test@example.com")
        assert u is not None
        assert u["email"] == "test@example.com"

        r2 = _register(client, username="user2", email="test@example.com")
        assert r2.status_code == 200
        assert b"already registered" in r2.data


def test_register_rejects_env_admin_email(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setenv("APP_ADMIN_EMAILS", "admin@example.com")
    app = _fresh_app(monkeypatch, tmp_path)
    client = app.test_client()
    with client:
        r = _register(client, username="admin1", email="admin@example.com")
        assert r.status_code == 200
        assert b"reserved" in r.data.lower() or b"administrator" in r.data.lower()


def test_login_promotes_env_admin_after_manual_create(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setenv("APP_ADMIN_EMAILS", "admin@example.com")
    app = _fresh_app(monkeypatch, tmp_path)
    from backend.db.users_db import save_user, sync_env_admin_user_row
    from backend.utils.roles import ROLE_GENERAL

    uid = save_user("admin1", "admin@example.com", "long-enough-password", role=ROLE_GENERAL)
    sync_env_admin_user_row(uid)

    client = app.test_client()
    with client:
        client.get("/login")
        from flask import session

        csrf = session.get("_csrf_token")
        r = client.post(
            "/login",
            data={
                "csrf_token": csrf,
                "login": "admin@example.com",
                "password": "long-enough-password",
            },
            follow_redirects=False,
        )
        assert r.status_code in (302, 303)
        assert session.get("user_role") == "admin"
