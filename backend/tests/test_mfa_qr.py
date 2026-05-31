"""Legacy QR MFA routes redirect after 2FA removal."""

from __future__ import annotations

import importlib


def _app(monkeypatch, tmp_path):
    monkeypatch.setenv("FLASK_ENV", "development")
    monkeypatch.setenv("USERS_DB_PATH", str(tmp_path / "users_mfa_qr.db"))
    monkeypatch.setenv("DEV_USERS_DB_PATH", str(tmp_path / "dev_users_mfa_qr.db"))
    for k in (
        "ADMIN_PASSWORD",
        "ADMIN_USERNAME",
        "ADMIN_EMAIL",
        "USERS_DB_ENCRYPTION_KEY",
        "DEV_USERS_DB_ENCRYPTION_KEY",
    ):
        monkeypatch.delenv(k, raising=False)
    import backend.main as main

    importlib.reload(main)
    return main.app


def test_legacy_mfa_qr_routes_redirect(monkeypatch, tmp_path):
    app = _app(monkeypatch, tmp_path)
    client = app.test_client()
    with client:
        client.get("/register")
        from flask import session

        client.post(
            "/register",
            data={
                "csrf_token": session.get("_csrf_token"),
                "username": "qr1",
                "email": "qr1@example.com",
                "password": "long-enough-password",
            },
            follow_redirects=False,
        )
        rv = client.get("/mfa/qr-wait", follow_redirects=False)
        assert rv.status_code in (302, 303)
        assert (rv.headers.get("Location") or "").endswith("/home")

        rv2 = client.get("/mfa/qr-confirm/fake-token", follow_redirects=False)
        assert rv2.status_code in (302, 303)
        assert (rv2.headers.get("Location") or "").endswith("/home")
