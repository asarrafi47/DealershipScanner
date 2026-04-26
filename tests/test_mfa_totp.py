from __future__ import annotations

import importlib
import re

import pyotp

def _fresh_app(monkeypatch, tmp_path):
    monkeypatch.setenv("FLASK_ENV", "development")
    monkeypatch.setenv("MFA_DELIVERY_MODE", "log")
    monkeypatch.setenv("USERS_DB_PATH", str(tmp_path / "users_test.db"))
    monkeypatch.setenv("DEV_USERS_DB_PATH", str(tmp_path / "dev_users_test.db"))
    monkeypatch.delenv("ADMIN_PASSWORD", raising=False)
    monkeypatch.delenv("ADMIN_USERNAME", raising=False)
    monkeypatch.delenv("ADMIN_EMAIL", raising=False)
    monkeypatch.delenv("USERS_DB_ENCRYPTION_KEY", raising=False)
    monkeypatch.delenv("DEV_USERS_DB_ENCRYPTION_KEY", raising=False)
    import backend.main as main

    importlib.reload(main)
    return main.app


def test_app_login_requires_mfa_setup_then_verify(monkeypatch, tmp_path):
    app = _fresh_app(monkeypatch, tmp_path)
    client = app.test_client()
    with client:
        # seed a user
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
        assert r.headers["Location"].endswith("/mfa/choose")

        uid = int(session.get("mfa_pending_user_id") or 0)
        assert uid > 0
        assert (session.get("mfa_pending_method") or "") == "choose"
        client.get("/mfa/choose")
        r_choose = client.post(
            "/mfa/choose",
            data={"csrf_token": session.get("_csrf_token"), "channel": "email"},
            follow_redirects=False,
        )
        assert r_choose.status_code in (302, 303)
        assert r_choose.headers["Location"].endswith("/mfa/verify")
        code = session.get("mfa_test_last_code")
        assert code
        client.get("/mfa/verify")
        r2 = client.post("/mfa/verify", data={"csrf_token": session.get("_csrf_token"), "code": code}, follow_redirects=False)
        assert r2.status_code in (302, 303)

        # logout clears MFA state
        r3 = client.get("/logout", follow_redirects=False)
        assert r3.status_code in (302, 303)

        # login now goes to verify (not setup)
        client.get("/login")
        csrf2 = session.get("_csrf_token")
        r4 = client.post(
            "/login",
            data={"csrf_token": csrf2, "login": "u1@example.com", "password": "long-enough-password"},
            follow_redirects=False,
        )
        assert r4.status_code in (302, 303)
        assert r4.headers["Location"].endswith("/mfa/choose")

        client.get("/mfa/choose")
        r_ch = client.post(
            "/mfa/choose",
            data={"csrf_token": session.get("_csrf_token"), "channel": "email"},
            follow_redirects=False,
        )
        assert r_ch.status_code in (302, 303)
        assert r_ch.headers["Location"].endswith("/mfa/verify")
        # request a new email code then verify
        code2 = session.get("mfa_test_last_code")
        assert code2
        client.get("/mfa/verify")
        r6 = client.post("/mfa/verify", data={"csrf_token": session.get("_csrf_token"), "code": code2}, follow_redirects=False)
        assert r6.status_code in (302, 303)


def test_dev_login_password_only_no_mfa(monkeypatch, tmp_path):
    app = _fresh_app(monkeypatch, tmp_path)
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


def _b32_from_mfa_page(html: str) -> str:
    m = re.search(
        r"<code[^>]*>([A-Z2-7]+=*?)</code>",
        html,
    )
    assert m, "expected base32 secret in page"
    return m.group(1).strip().upper()


def test_app_enroll_totp_and_login_uses_authenticator(monkeypatch, tmp_path):
    app = _fresh_app(monkeypatch, tmp_path)
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
                "username": "totp1",
                "email": "totp1@example.com",
                "password": "long-enough-password",
            },
            follow_redirects=False,
        )
        assert r.status_code in (302, 303)

        rg = client.get("/mfa/setup", follow_redirects=False)
        assert rg.status_code == 200
        sec = _b32_from_mfa_page(rg.get_data(as_text=True))
        assert session.get("mfa_pending_user_id")

        r_c = client.post(
            "/mfa/setup",
            data={
                "csrf_token": session.get("_csrf_token"),
                "mfa_action": "confirm_totp",
                "code": pyotp.TOTP(sec).now(),
            },
            follow_redirects=False,
        )
        assert r_c.status_code in (302, 303)
        assert r_c.headers["Location"].endswith("/listings")

        client.get("/logout", follow_redirects=False)
        client.get("/login")
        r_login = client.post(
            "/login",
            data={
                "csrf_token": session.get("_csrf_token"),
                "login": "totp1@example.com",
                "password": "long-enough-password",
            },
            follow_redirects=False,
        )
        assert r_login.status_code in (302, 303)
        assert r_login.headers["Location"].endswith("/mfa/verify")
        assert session.get("mfa_test_last_code") is None
        assert (session.get("mfa_pending_method") or "") == "totp"

        client.get("/mfa/verify")
        r_v = client.post(
            "/mfa/verify",
            data={
                "csrf_token": session.get("_csrf_token"),
                "code": pyotp.TOTP(sec).now(),
            },
            follow_redirects=False,
        )
        assert r_v.status_code in (302, 303)
        assert r_v.headers["Location"].endswith("/listings")


