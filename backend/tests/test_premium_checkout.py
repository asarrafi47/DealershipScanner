"""Consumer premium checkout verification (Stripe session, no grant without payment)."""

from __future__ import annotations

import importlib
from unittest.mock import patch

import pytest


def _fresh_app(monkeypatch: pytest.MonkeyPatch, tmp_path, *, admin_email: str = "prem@example.com"):
    monkeypatch.setenv("FLASK_ENV", "development")
    monkeypatch.setenv("MFA_DELIVERY_MODE", "log")
    monkeypatch.setenv("USERS_DB_PATH", str(tmp_path / "users_test.db"))
    monkeypatch.setenv("DEV_USERS_DB_PATH", str(tmp_path / "dev_users_test.db"))
    monkeypatch.setenv("BILLING_STRIPE_ENABLED", "1")
    monkeypatch.setenv("APP_ADMIN_EMAILS", admin_email)
    monkeypatch.delenv("USERS_DB_ENCRYPTION_KEY", raising=False)
    import backend.main as main

    importlib.reload(main)
    return main.app


def _login_admin(client) -> None:
    client.get("/register")
    from flask import session

    csrf = session.get("_csrf_token")
    client.post(
        "/register",
        data={
            "csrf_token": csrf,
            "username": "premuser",
            "email": "prem@example.com",
            "password": "long-enough-password",
        },
        follow_redirects=True,
    )


def test_premium_success_without_session_does_not_grant(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    app = _fresh_app(monkeypatch, tmp_path)
    client = app.test_client()
    with client:
        _login_admin(client)
        rv = client.get("/billing/premium/success")
        assert rv.status_code == 200
        assert b"Payment not confirmed" in rv.data
        from backend.db.users_db import get_user_by_login

        u = get_user_by_login("prem@example.com")
        assert u is not None
        assert not bool(u.get("is_premium"))


def test_premium_success_verifies_stripe_before_grant(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    app = _fresh_app(monkeypatch, tmp_path)
    client = app.test_client()
    with client:
        _login_admin(client)
        from backend.db.users_db import get_user_by_login

        u = get_user_by_login("prem@example.com")
        uid = int(u["id"])

        monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_test_premium_checkout")
        with patch(
            "backend.billing.routes.verify_premium_checkout_session",
            return_value=True,
        ):
            rv = client.get("/billing/premium/success?session_id=cs_test_abc")
        assert rv.status_code == 200
        assert b"Payment Successful" in rv.data
        u2 = get_user_by_login("prem@example.com")
        assert u2.get("is_premium") is True
