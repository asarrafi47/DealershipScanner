from __future__ import annotations

import importlib


def _fresh_app(monkeypatch, tmp_path):
    monkeypatch.setenv("FLASK_ENV", "development")
    monkeypatch.setenv("MFA_DELIVERY_MODE", "log")
    monkeypatch.setenv("USERS_DB_PATH", str(tmp_path / "users_test.db"))
    monkeypatch.delenv("USERS_DB_ENCRYPTION_KEY", raising=False)
    # Import after env wiring so backend.main initializes with our temp DB path
    import backend.main as main

    importlib.reload(main)
    return main.app


def test_register_admin_bypasses_billing(monkeypatch, tmp_path):
    monkeypatch.setenv("BILLING_STRIPE_ENABLED", "1")
    monkeypatch.setenv("APP_ADMIN_EMAILS", "admin@example.com")
    app = _fresh_app(monkeypatch, tmp_path)
    client = app.test_client()
    with client:
        # GET to establish session + csrf
        r0 = client.get("/register")
        assert r0.status_code == 200
        from flask import session

        csrf = session.get("_csrf_token")
        assert csrf
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
        assert r.status_code in (302, 303)
        assert r.headers["Location"].endswith("/listings")

        from flask import session

        assert int(session.get("user_id") or 0) > 0
        assert session.get("mfa_ok") is True
        assert not session.get("mfa_pending_user_id")


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
        assert r.headers["Location"].endswith("/mfa/choose")

        from flask import session

        pending = int(session.get("mfa_pending_user_id") or 0)
        assert pending > 0
        client.get("/mfa/choose")
        rc = client.post(
            "/mfa/choose",
            data={"csrf_token": session.get("_csrf_token"), "channel": "email"},
            follow_redirects=False,
        )
        assert rc.status_code in (302, 303)
        assert rc.headers["Location"].endswith("/mfa/verify")
        code = session.get("mfa_test_last_code")
        assert code
        client.get("/mfa/verify")
        r2 = client.post(
            "/mfa/verify",
            data={"csrf_token": session.get("_csrf_token"), "code": code},
            follow_redirects=False,
        )
        assert r2.status_code in (302, 303)
        assert "/billing/required" in r2.headers["Location"]

