"""Consumer premium checkout verification (Stripe subscription session)."""

from __future__ import annotations

from unittest.mock import patch

import pytest


_APP_ENV = {"BILLING_STRIPE_ENABLED": "1", "APP_ADMIN_EMAILS": "prem@example.com"}


def _login_admin(client) -> None:
    from backend.db.users_db import save_user, sync_env_admin_user_row
    from backend.utils.roles import ROLE_GENERAL

    uid = save_user("premuser", "prem@example.com", "long-enough-password", role=ROLE_GENERAL)
    sync_env_admin_user_row(uid)
    client.get("/login")
    from flask import session

    csrf = session.get("_csrf_token")
    client.post(
        "/login",
        data={
            "csrf_token": csrf,
            "login": "prem@example.com",
            "password": "long-enough-password",
        },
        follow_redirects=True,
    )


def test_premium_success_without_session_does_not_grant(monkeypatch: pytest.MonkeyPatch, tmp_path, app_factory) -> None:
    app = app_factory(**_APP_ENV).app
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


def test_premium_success_verifies_stripe_before_grant(monkeypatch: pytest.MonkeyPatch, tmp_path, app_factory) -> None:
    app = app_factory(**_APP_ENV).app
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
