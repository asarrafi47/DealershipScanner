"""JSON auth endpoints for mobile / native clients."""

from __future__ import annotations

import importlib


def _fresh_app(monkeypatch, tmp_path):
    monkeypatch.setenv("FLASK_ENV", "development")
    monkeypatch.setenv("MFA_DELIVERY_MODE", "log")
    monkeypatch.setenv("USERS_DB_PATH", str(tmp_path / "users_test.db"))
    monkeypatch.setenv("DEV_USERS_DB_PATH", str(tmp_path / "dev_users_test.db"))
    monkeypatch.delenv("USERS_DB_ENCRYPTION_KEY", raising=False)
    import backend.main as main

    importlib.reload(main)
    return main.app


def _csrf(client) -> str:
    rv = client.get("/api/auth/csrf")
    assert rv.status_code == 200
    data = rv.get_json()
    assert data.get("ok") is True
    token = data.get("csrf_token")
    assert isinstance(token, str) and len(token) >= 32
    return token


def test_auth_csrf_returns_token(monkeypatch, tmp_path) -> None:
    app = _fresh_app(monkeypatch, tmp_path)
    with app.test_client() as client:
        token = _csrf(client)
        assert token


def test_auth_me_anonymous(monkeypatch, tmp_path) -> None:
    app = _fresh_app(monkeypatch, tmp_path)
    with app.test_client() as client:
        rv = client.get("/api/auth/me")
    assert rv.status_code == 401
    assert rv.get_json().get("error") == "not_logged_in"


def test_auth_login_logout_flow(monkeypatch, tmp_path) -> None:
    app = _fresh_app(monkeypatch, tmp_path)
    from backend.db.users_db import save_user
    from backend.utils.roles import ROLE_GENERAL

    save_user("mobile1", "mobile1@example.com", "long-enough-password", role=ROLE_GENERAL)
    with app.test_client() as client:
        token = _csrf(client)
        bad = client.post(
            "/api/auth/login",
            json={"login": "mobile1@example.com", "password": "wrong-password-here"},
            headers={"X-CSRF-Token": token},
        )
        assert bad.status_code == 401
        assert bad.get_json().get("error") == "invalid_credentials"

        token = _csrf(client)
        rv = client.post(
            "/api/auth/login",
            json={"login": "mobile1@example.com", "password": "long-enough-password"},
            headers={"X-CSRF-Token": token},
        )
        assert rv.status_code == 200
        body = rv.get_json()
        assert body.get("ok") is True
        assert body["user"]["email"] == "mobile1@example.com"

        me = client.get("/api/auth/me")
        assert me.status_code == 200
        assert me.get_json()["user"]["username"] == "mobile1"

        logout_token = _csrf(client)
        out = client.post("/api/auth/logout", headers={"X-CSRF-Token": logout_token})
        assert out.status_code == 200
        assert out.get_json().get("ok") is True

        me2 = client.get("/api/auth/me")
        assert me2.status_code == 401


def test_auth_login_requires_csrf(monkeypatch, tmp_path) -> None:
    app = _fresh_app(monkeypatch, tmp_path)
    from backend.db.users_db import save_user
    from backend.utils.roles import ROLE_GENERAL

    save_user("mobile2", "mobile2@example.com", "long-enough-password", role=ROLE_GENERAL)
    with app.test_client() as client:
        client.get("/api/auth/csrf")
        rv = client.post(
            "/api/auth/login",
            json={"login": "mobile2@example.com", "password": "long-enough-password"},
        )
    assert rv.status_code == 403


def test_auth_logout_requires_csrf(monkeypatch, tmp_path) -> None:
    app = _fresh_app(monkeypatch, tmp_path)
    with app.test_client() as client:
        rv = client.post("/api/auth/logout")
    assert rv.status_code == 403
