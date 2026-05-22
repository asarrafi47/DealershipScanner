"""MFA audit log is inactive after app 2FA removal (utilities remain for legacy tooling)."""

from __future__ import annotations

import importlib
import json


def _fresh_app(monkeypatch, tmp_path):
    monkeypatch.setenv("FLASK_ENV", "development")
    monkeypatch.setenv("USERS_DB_PATH", str(tmp_path / "users_test.db"))
    monkeypatch.delenv("USERS_DB_ENCRYPTION_KEY", raising=False)
    import backend.main as main

    importlib.reload(main)
    return main.app


def test_register_does_not_write_mfa_action_log(monkeypatch, tmp_path):
    log_path = tmp_path / "mfa.jsonl"
    monkeypatch.setenv("MFA_ACTION_LOG_PATH", str(log_path))
    app = _fresh_app(monkeypatch, tmp_path)
    client = app.test_client()
    with client:
        client.get("/register")
        from flask import session

        r = client.post(
            "/register",
            data={
                "csrf_token": session.get("_csrf_token"),
                "username": "a1",
                "email": "a1@example.com",
                "password": "long-enough-password",
            },
            follow_redirects=False,
        )
        assert r.status_code in (302, 303)
    assert not log_path.exists() or not log_path.read_text(encoding="utf-8").strip()
    if log_path.exists():
        for ln in log_path.read_text(encoding="utf-8").splitlines():
            if ln.strip():
                row = json.loads(ln)
                assert "mfa_start" not in (row.get("event") or "")
