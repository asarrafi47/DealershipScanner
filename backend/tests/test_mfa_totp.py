"""App auth after 2FA removal: password-only login/register and legacy /mfa redirects."""

from __future__ import annotations


_APP_ENV = {"ADMIN_PASSWORD": None, "ADMIN_USERNAME": None, "ADMIN_EMAIL": None}


def test_app_register_login_password_only(monkeypatch, tmp_path, app_factory):
    app = app_factory(**_APP_ENV).app
    client = app.test_client()
    with client:
        client.get("/register")
        from flask import session

        csrf = session.get("_csrf_token")
        assert csrf
        r = client.post(
            "/register",
            data={
                "csrf_token": csrf,
                "username": "u1",
                "email": "u1@example.com",
                "password": "long-enough-password",
            },
            follow_redirects=False,
        )
        assert r.status_code in (302, 303)
        assert r.headers["Location"].endswith("/listings")
        assert int(session.get("user_id") or 0) > 0
        assert session.get("mfa_ok") is True
        assert not session.get("mfa_pending_user_id")

        client.get("/listings")
        r3 = client.post(
            "/logout",
            data={"csrf_token": session.get("_csrf_token")},
            follow_redirects=False,
        )
        assert r3.status_code in (302, 303)

        client.get("/login")
        r4 = client.post(
            "/login",
            data={
                "csrf_token": session.get("_csrf_token"),
                "login": "u1@example.com",
                "password": "long-enough-password",
            },
            follow_redirects=False,
        )
        assert r4.status_code in (302, 303)
        assert r4.headers["Location"].endswith("/home")
        assert session.get("mfa_ok") is True


def test_legacy_mfa_urls_redirect(monkeypatch, tmp_path, app_factory):
    app = app_factory(**_APP_ENV).app
    client = app.test_client()
    with client:
        for path in ("/mfa/choose", "/mfa/setup", "/mfa/verify", "/mfa/qr-wait"):
            rv = client.get(path, follow_redirects=False)
            assert rv.status_code in (302, 303)
            assert (rv.headers.get("Location") or "").endswith("/login")

        client.get("/register")
        from flask import session

        client.post(
            "/register",
            data={
                "csrf_token": session.get("_csrf_token"),
                "username": "u2",
                "email": "u2@example.com",
                "password": "long-enough-password",
            },
            follow_redirects=False,
        )
        rv2 = client.get("/mfa/verify", follow_redirects=False)
        assert rv2.status_code in (302, 303)
        assert (rv2.headers.get("Location") or "").endswith("/home")


def test_dev_login_password_only_no_mfa(monkeypatch, tmp_path, app_factory):
    monkeypatch.setenv("ALLOW_DEV_PUBLIC_REGISTER", "1")
    app = app_factory(**_APP_ENV).app
    client = app.test_client()
    with client:
        client.get("/dev/register")
        from flask import session

        csrf = session.get("_csrf_token")
        assert csrf
        r0 = client.post(
            "/dev/register",
            data={
                "csrf_token": csrf,
                "username": "op1",
                "email": "op1@example.com",
                "password": "long-enough-password",
            },
            follow_redirects=False,
        )
        assert r0.status_code in (302, 303)

        client.get("/dev/login")
        csrf = session.get("_csrf_token")
        assert csrf
        r = client.post(
            "/dev/login",
            data={"csrf_token": csrf, "login": "op1@example.com", "password": "long-enough-password"},
            follow_redirects=False,
        )
        assert r.status_code in (302, 303)
        loc = (r.headers.get("Location") or "")
        assert "/dev" in loc
        assert "mfa" not in loc.lower()
        r2 = client.get("/dev/", follow_redirects=True)
        assert r2.status_code == 200

        r_legacy = client.get("/dev/mfa/verify", follow_redirects=False)
        assert r_legacy.status_code in (302, 303)
        assert (r_legacy.headers.get("Location") or "").find("/dev") != -1
