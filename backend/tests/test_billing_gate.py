from __future__ import annotations

import importlib


def _fresh_app(monkeypatch, tmp_path, *, production: bool = False):
    if production:
        monkeypatch.setenv("FLASK_ENV", "production")
        monkeypatch.setenv("SECRET_KEY", "pytest-secret-key-do-not-use-in-deployment")
        monkeypatch.setenv("ADMIN_PASSWORD", "pytest-admin-bootstrap-do-not-use-in-deployment")
    else:
        monkeypatch.setenv("FLASK_ENV", "development")
    monkeypatch.setenv("MFA_DELIVERY_MODE", "log")
    monkeypatch.setenv("USERS_DB_PATH", str(tmp_path / "users_test.db"))
    monkeypatch.setenv("DEV_USERS_DB_PATH", str(tmp_path / "dev_users_test.db"))
    monkeypatch.delenv("USERS_DB_ENCRYPTION_KEY", raising=False)
    # Import after env wiring so backend.main initializes with our temp DB path
    import backend.main as main

    importlib.reload(main)
    return main.app


def test_env_admin_email_login_bypasses_billing(monkeypatch, tmp_path):
    monkeypatch.setenv("BILLING_STRIPE_ENABLED", "1")
    monkeypatch.setenv("APP_ADMIN_EMAILS", "admin@example.com")
    app = _fresh_app(monkeypatch, tmp_path)
    from backend.db.users_db import save_user, sync_env_admin_user_row
    from backend.utils.roles import ROLE_GENERAL

    uid = save_user("admin1", "admin@example.com", "long-enough-password", role=ROLE_GENERAL)
    sync_env_admin_user_row(uid)
    client = app.test_client()
    with client:
        r0 = client.get("/login")
        assert r0.status_code == 200
        from flask import session

        csrf = session.get("_csrf_token")
        assert csrf
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
        assert r.headers["Location"].endswith("/home")

        assert int(session.get("user_id") or 0) > 0
        assert session.get("user_role") == "admin"
        assert session.get("mfa_ok") is True
        assert not session.get("mfa_pending_user_id")


def test_register_rejects_env_admin_email(monkeypatch, tmp_path):
    monkeypatch.setenv("BILLING_STRIPE_ENABLED", "1")
    monkeypatch.setenv("APP_ADMIN_EMAILS", "admin@example.com")
    app = _fresh_app(monkeypatch, tmp_path)
    client = app.test_client()
    with client:
        r0 = client.get("/register")
        assert r0.status_code == 200
        from flask import session

        csrf = session.get("_csrf_token")
        r = client.post(
            "/register",
            data={
                "csrf_token": csrf,
                "username": "admin1",
                "email": "admin@example.com",
                "password": "long-enough-password",
            },
            follow_redirects=False,
        )
        assert r.status_code == 200
        assert b"reserved" in r.data.lower() or b"administrator" in r.data.lower()


def test_register_non_admin_requires_billing(monkeypatch, tmp_path):
    monkeypatch.setenv("BILLING_STRIPE_ENABLED", "1")
    monkeypatch.setenv("APP_ADMIN_EMAILS", "")
    app = _fresh_app(monkeypatch, tmp_path)
    client = app.test_client()
    with client:
        r0 = client.get("/dealer/register")
        assert r0.status_code == 200
        from flask import session

        csrf = session.get("_csrf_token")
        assert csrf
        r = client.post(
            "/dealer/register",
            data={
                "csrf_token": csrf,
                "username": "u1",
                "email": "u1@example.com",
                "password": "long-enough-password",
                "org_name": "Test Org",
            },
            follow_redirects=False,
        )
        assert r.status_code in (302, 303)
        assert "/billing/required" in (r.headers.get("Location") or "")

        from flask import session

        assert int(session.get("user_id") or 0) > 0
        assert session.get("mfa_ok") is True
        assert not session.get("mfa_pending_user_id")


def test_env_admin_username_login_bypasses_billing(monkeypatch, tmp_path):
    monkeypatch.setenv("BILLING_STRIPE_ENABLED", "1")
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_test_fake")
    monkeypatch.setenv("STRIPE_WEBHOOK_SECRET", "whsec_fake")
    monkeypatch.setenv("STRIPE_PRICE_ID", "price_fake")
    monkeypatch.setenv("APP_ADMIN_EMAILS", "")
    monkeypatch.setenv("APP_ADMIN_USERNAMES", "power_ops")
    app = _fresh_app(monkeypatch, tmp_path, production=True)
    from backend.db.users_db import save_user, sync_env_admin_user_row
    from backend.utils.roles import ROLE_GENERAL

    uid = save_user("power_ops", "ops@example.com", "long-enough-password", role=ROLE_GENERAL)
    sync_env_admin_user_row(uid)
    client = app.test_client()
    with client:
        r0 = client.get("/login")
        assert r0.status_code == 200
        from flask import session

        csrf = session.get("_csrf_token")
        r = client.post(
            "/login",
            data={
                "csrf_token": csrf,
                "login": "power_ops",
                "password": "long-enough-password",
            },
            follow_redirects=False,
        )
        assert r.status_code in (302, 303)
        assert r.headers["Location"].endswith("/home")

        assert int(session.get("user_id") or 0) > 0
        assert session.get("user_role") == "admin"
        assert session.get("mfa_ok") is True
        assert not session.get("mfa_pending_user_id")


def test_register_rejects_env_admin_username(monkeypatch, tmp_path):
    monkeypatch.setenv("BILLING_STRIPE_ENABLED", "1")
    monkeypatch.setenv("APP_ADMIN_USERNAMES", "power_ops")
    app = _fresh_app(monkeypatch, tmp_path, production=True)
    client = app.test_client()
    with client:
        r0 = client.get("/register")
        from flask import session

        csrf = session.get("_csrf_token")
        r = client.post(
            "/register",
            data={
                "csrf_token": csrf,
                "username": "power_ops",
                "email": "ops@example.com",
                "password": "long-enough-password",
            },
            follow_redirects=False,
        )
        assert r.status_code == 200
        assert b"reserved" in r.data.lower() or b"administrator" in r.data.lower()


def test_stripe_webhook_rejects_invalid_signature(monkeypatch, tmp_path):
    monkeypatch.setenv("BILLING_STRIPE_ENABLED", "1")
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_test_fake")
    monkeypatch.setenv("STRIPE_WEBHOOK_SECRET", "whsec_test_secret")
    monkeypatch.setenv("STRIPE_PRICE_ID", "price_fake")
    app = _fresh_app(monkeypatch, tmp_path)
    client = app.test_client()
    rv = client.post(
        "/billing/webhook",
        data=b'{"id":"evt_test","type":"ping"}',
        headers={"Content-Type": "application/json", "Stripe-Signature": "t=0,v1=deadbeef"},
    )
    assert rv.status_code == 400
    body = rv.get_json()
    assert body is not None
    assert body.get("error") == "invalid_signature"


def test_stripe_webhook_disabled_returns_404(monkeypatch, tmp_path):
    monkeypatch.setenv("BILLING_STRIPE_ENABLED", "0")
    app = _fresh_app(monkeypatch, tmp_path)
    client = app.test_client()
    rv = client.post("/billing/webhook", data=b"{}")
    assert rv.status_code == 404
