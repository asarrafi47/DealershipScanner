"""Optional /dev IP allowlist (production hardening)."""

from __future__ import annotations

import importlib


def _fresh_app(monkeypatch, tmp_path):
    monkeypatch.setenv("FLASK_ENV", "development")
    monkeypatch.setenv("MFA_DELIVERY_MODE", "log")
    monkeypatch.setenv("USERS_DB_PATH", str(tmp_path / "users_test.db"))
    monkeypatch.delenv("USERS_DB_ENCRYPTION_KEY", raising=False)
    import backend.main as main

    importlib.reload(main)
    return main.app


def test_dev_allowlist_blocks_non_matching_ip(monkeypatch, tmp_path):
    monkeypatch.setenv("DEV_IP_ALLOWLIST", "10.0.0.1")
    app = _fresh_app(monkeypatch, tmp_path)
    client = app.test_client()
    rv = client.get("/dev/login", environ_overrides={"REMOTE_ADDR": "10.0.0.2"})
    assert rv.status_code == 403


def test_dev_allowlist_allows_matching_ip(monkeypatch, tmp_path):
    monkeypatch.setenv("DEV_IP_ALLOWLIST", "10.0.0.2")
    app = _fresh_app(monkeypatch, tmp_path)
    client = app.test_client()
    rv = client.get("/dev/login", environ_overrides={"REMOTE_ADDR": "10.0.0.2"})
    assert rv.status_code == 200


def test_dev_allowlist_cidr(monkeypatch, tmp_path):
    monkeypatch.setenv("DEV_IP_ALLOWLIST", "192.168.50.0/24")
    app = _fresh_app(monkeypatch, tmp_path)
    client = app.test_client()
    rv = client.get("/dev/login", environ_overrides={"REMOTE_ADDR": "192.168.50.99"})
    assert rv.status_code == 200


def test_dev_allowlist_json_for_api(monkeypatch, tmp_path):
    monkeypatch.setenv("DEV_IP_ALLOWLIST", "10.1.1.1")
    app = _fresh_app(monkeypatch, tmp_path)
    client = app.test_client()
    rv = client.get("/dev/api/status", environ_overrides={"REMOTE_ADDR": "10.2.2.2"})
    assert rv.status_code == 403
    body = rv.get_json()
    assert body is not None
    assert body.get("reason") == "dev_ip_allowlist"
