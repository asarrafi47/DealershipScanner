"""Optional /dev IP allowlist (production hardening)."""

from __future__ import annotations


def test_dev_allowlist_blocks_non_matching_ip(monkeypatch, tmp_path, app_factory):
    monkeypatch.setenv("DEV_IP_ALLOWLIST", "10.0.0.1")
    app = app_factory().app
    client = app.test_client()
    rv = client.get("/dev/login", environ_overrides={"REMOTE_ADDR": "10.0.0.2"})
    assert rv.status_code == 403


def test_dev_allowlist_allows_matching_ip(monkeypatch, tmp_path, app_factory):
    monkeypatch.setenv("DEV_IP_ALLOWLIST", "10.0.0.2")
    app = app_factory().app
    client = app.test_client()
    rv = client.get("/dev/login", environ_overrides={"REMOTE_ADDR": "10.0.0.2"})
    assert rv.status_code == 200


def test_dev_allowlist_cidr(monkeypatch, tmp_path, app_factory):
    monkeypatch.setenv("DEV_IP_ALLOWLIST", "192.168.50.0/24")
    app = app_factory().app
    client = app.test_client()
    rv = client.get("/dev/login", environ_overrides={"REMOTE_ADDR": "192.168.50.99"})
    assert rv.status_code == 200


def test_dev_allowlist_json_for_api(monkeypatch, tmp_path, app_factory):
    monkeypatch.setenv("DEV_IP_ALLOWLIST", "10.1.1.1")
    app = app_factory().app
    client = app.test_client()
    rv = client.get("/dev/api/status", environ_overrides={"REMOTE_ADDR": "10.2.2.2"})
    assert rv.status_code == 403
    body = rv.get_json()
    assert body is not None
    assert body.get("reason") == "dev_ip_allowlist"
