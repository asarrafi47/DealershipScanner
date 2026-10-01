"""MFA audit log is inactive after app 2FA removal (utilities remain for legacy tooling)."""

from __future__ import annotations

import json


def test_register_does_not_write_mfa_action_log(monkeypatch, tmp_path, app_factory):
    log_path = tmp_path / "mfa.jsonl"
    monkeypatch.setenv("MFA_ACTION_LOG_PATH", str(log_path))
    app = app_factory().app
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
