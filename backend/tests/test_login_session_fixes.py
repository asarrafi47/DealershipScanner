"""Login fixes from the 2026-09-30 auth audit.

- The web login is a 14-day permanent session, not a browser-close cookie.
- /register strips the password like /login; an account whose stored password kept its
  outer spaces (registered before the fix) still logs in.
- TRUST_PROXY_HEADERS=1 wraps the app in ProxyFix so redirects keep https.
- A relative USERS_DB_PATH resolves against the repo root, not the cwd.
"""
from __future__ import annotations

import importlib
import os

_TOK = "t" * 32


def _fresh_app(monkeypatch, tmp_path, **env):
    monkeypatch.setenv("FLASK_ENV", "development")
    monkeypatch.setenv("MFA_DELIVERY_MODE", "log")
    monkeypatch.setenv("USERS_DB_PATH", str(tmp_path / "users_test.db"))
    monkeypatch.setenv("DEV_USERS_DB_PATH", str(tmp_path / "dev_users_test.db"))
    monkeypatch.setenv("USERS_DB_ENCRYPTION_KEY", "")
    monkeypatch.setenv("TRUST_PROXY_HEADERS", "")
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    import backend.main as main

    importlib.reload(main)
    return main


def _post_form(client, url, data):
    from backend.utils.csrf import _SESSION_KEY

    with client.session_transaction() as sess:
        sess[_SESSION_KEY] = _TOK
    return client.post(url, data={"csrf_token": _TOK, **data})


def test_login_session_is_permanent(monkeypatch, tmp_path):
    main = _fresh_app(monkeypatch, tmp_path)
    with main.app.test_client() as c:
        rv = _post_form(c, "/register", {
            "username": "perm_user", "email": "perm@example.com",
            "password": "long-enough-password", "plan": "free",
        })
        assert rv.status_code == 302
        _post_form(c, "/logout", {})
        rv = _post_form(c, "/login", {"login": "perm_user", "password": "long-enough-password"})
        assert rv.status_code == 302
        cookie = rv.headers.get("Set-Cookie") or ""
        assert "Expires=" in cookie, cookie
        with c.session_transaction() as sess:
            assert sess.permanent and sess.get("username") == "perm_user"


def test_register_strips_password_like_login(monkeypatch, tmp_path):
    main = _fresh_app(monkeypatch, tmp_path)
    with main.app.test_client() as c:
        _post_form(c, "/register", {
            "username": "space_user", "email": "space@example.com",
            "password": "  long-enough-password  ", "plan": "free",
        })
        _post_form(c, "/logout", {})
        rv = _post_form(c, "/login", {"login": "space_user", "password": "long-enough-password"})
        assert rv.status_code == 302


def test_pre_fix_account_with_spaced_password_still_logs_in(monkeypatch, tmp_path):
    main = _fresh_app(monkeypatch, tmp_path)
    from backend.db.users_db import save_user

    save_user("legacy_user", "legacy@example.com", " spaced-password-123 ", role="general", org_id=None)
    with main.app.test_client() as c:
        rv = _post_form(c, "/login", {"login": "legacy_user", "password": " spaced-password-123 "})
        assert rv.status_code == 302
        _post_form(c, "/logout", {})
        rv = _post_form(c, "/login", {"login": "legacy_user", "password": "wrong-password-123"})
        assert rv.status_code == 200 and b"Invalid" in rv.data


def test_proxy_fix_keeps_https_behind_trusted_proxy(monkeypatch, tmp_path):
    main = _fresh_app(monkeypatch, tmp_path, TRUST_PROXY_HEADERS="1")
    from werkzeug.middleware.proxy_fix import ProxyFix

    assert isinstance(main.app.wsgi_app, ProxyFix)
    from flask import request

    main.app.add_url_rule("/_scheme_probe", "_scheme_probe", lambda: request.scheme)
    with main.app.test_client() as c:
        assert c.get("/_scheme_probe", headers={"X-Forwarded-Proto": "https"}).data == b"https"
    main2 = _fresh_app(monkeypatch, tmp_path)
    assert not isinstance(main2.app.wsgi_app, ProxyFix)


def test_relative_users_db_path_resolves_against_repo_root(monkeypatch, tmp_path):
    from backend.db import dev_users_sqlite, users_sqlite

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("USERS_DB_PATH", "backend/users.db")
    monkeypatch.setenv("DEV_USERS_DB_PATH", "dev_users.db")
    assert users_sqlite.users_db_path() == os.path.join(users_sqlite._REPO_ROOT, "backend/users.db")
    assert dev_users_sqlite.dev_users_db_path() == os.path.join(dev_users_sqlite._REPO_ROOT, "dev_users.db")
    monkeypatch.setenv("USERS_DB_PATH", "/abs/users.db")
    assert users_sqlite.users_db_path() == "/abs/users.db"


def test_password_attempts_shared_by_every_login_path():
    from backend.db.users_db import submitted_password_attempts

    assert submitted_password_attempts("  pw-123  ") == ["pw-123", "  pw-123  "]
    assert submitted_password_attempts("pw-123") == ["pw-123"]
    assert submitted_password_attempts("   ") == []
    assert submitted_password_attempts(None) == []


def test_api_login_accepts_pre_fix_spaced_password(monkeypatch, tmp_path):
    main = _fresh_app(monkeypatch, tmp_path)
    from backend.db.users_db import save_user

    save_user("legacy_api", "legacy_api@example.com", " spaced-password-123 ", role="general", org_id=None)
    with main.app.test_client() as c:
        tok = c.get("/api/auth/csrf").get_json()["csrf_token"]
        rv = c.post("/api/auth/login", json={"login": "legacy_api", "password": " spaced-password-123 "},
                    headers={"X-CSRF-Token": tok})
        assert rv.status_code == 200
