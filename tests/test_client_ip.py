"""Tests for trusted-proxy client IP behavior."""

from __future__ import annotations

from backend.utils import client_ip as client_ip_mod


def test_client_ip_ignores_forwarded_by_default(monkeypatch):
    monkeypatch.delenv("TRUST_PROXY_HEADERS", raising=False)

    class R:
        headers = {"X-Forwarded-For": "203.0.113.7"}
        remote_addr = "10.0.0.1"

    assert client_ip_mod.client_ip(R()) == "10.0.0.1"


def test_client_ip_uses_forwarded_when_trusted(monkeypatch):
    monkeypatch.setenv("TRUST_PROXY_HEADERS", "1")

    class R:
        headers = {"X-Forwarded-For": "203.0.113.7, 10.0.0.2"}
        remote_addr = "10.0.0.1"

    assert client_ip_mod.client_ip(R()) == "203.0.113.7"


def test_client_ip_trusted_empty_forwarded_falls_back(monkeypatch):
    monkeypatch.setenv("TRUST_PROXY_HEADERS", "1")

    class R:
        headers = {}
        remote_addr = "10.0.0.1"

    assert client_ip_mod.client_ip(R()) == "10.0.0.1"
