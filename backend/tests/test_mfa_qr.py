"""Legacy QR MFA routes redirect after 2FA removal."""

from __future__ import annotations


def test_legacy_mfa_qr_routes_redirect(monkeypatch, tmp_path, app_factory):
    app = app_factory(ADMIN_PASSWORD=None, ADMIN_USERNAME=None, ADMIN_EMAIL=None).app
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
