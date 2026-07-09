"""Tests for trusted-proxy client IP behavior."""

from __future__ import annotations

import pytest

from backend.utils import client_ip as client_ip_mod


class _Req:
    def __init__(self, forwarded: str | None = None, remote_addr: str | None = "10.0.0.1"):
        self.headers = {} if forwarded is None else {"X-Forwarded-For": forwarded}
        self.remote_addr = remote_addr


@pytest.fixture
def trusted(monkeypatch):
    monkeypatch.setenv("TRUST_PROXY_HEADERS", "1")
    monkeypatch.delenv("TRUSTED_PROXY_HOPS", raising=False)


def test_client_ip_ignores_forwarded_by_default(monkeypatch):
    monkeypatch.delenv("TRUST_PROXY_HEADERS", raising=False)
    assert client_ip_mod.client_ip(_Req("203.0.113.7")) == "10.0.0.1"


def test_client_ip_uses_forwarded_when_trusted(trusted):
    assert client_ip_mod.client_ip(_Req("203.0.113.7")) == "203.0.113.7"


def test_client_ip_trusted_empty_forwarded_falls_back(trusted):
    assert client_ip_mod.client_ip(_Req()) == "10.0.0.1"


def test_client_ip_ignores_client_injected_left_hop(trusted):
    """The proxy appends the peer it saw, so a forged leading entry must never win."""
    assert client_ip_mod.client_ip(_Req("6.6.6.6, 203.0.113.7")) == "203.0.113.7"


def test_client_ip_ignores_long_forged_chain(trusted):
    req = _Req("1.1.1.1, 2.2.2.2, 3.3.3.3, 203.0.113.7")
    assert client_ip_mod.client_ip(req) == "203.0.113.7"


def test_client_ip_honors_two_trusted_hops(monkeypatch):
    """Cloudflare in front of Railway: the client sits two entries from the right."""
    monkeypatch.setenv("TRUST_PROXY_HEADERS", "1")
    monkeypatch.setenv("TRUSTED_PROXY_HOPS", "2")
    assert client_ip_mod.client_ip(_Req("203.0.113.7, 172.16.0.5")) == "203.0.113.7"


def test_client_ip_short_chain_falls_back_to_socket_peer(monkeypatch):
    """Fewer entries than trusted hops means a hop misbehaved — never trust the remainder."""
    monkeypatch.setenv("TRUST_PROXY_HEADERS", "1")
    monkeypatch.setenv("TRUSTED_PROXY_HOPS", "2")
    assert client_ip_mod.client_ip(_Req("6.6.6.6")) == "10.0.0.1"


def test_client_ip_rejects_garbage_entry(trusted):
    assert client_ip_mod.client_ip(_Req("not-an-ip")) == "10.0.0.1"


def test_client_ip_strips_ipv4_port(trusted):
    assert client_ip_mod.client_ip(_Req("203.0.113.7:44321")) == "203.0.113.7"


def test_client_ip_handles_bracketed_ipv6_with_port(trusted):
    assert client_ip_mod.client_ip(_Req("[2001:db8::1]:443")) == "2001:db8::1"


def test_client_ip_handles_bare_ipv6(trusted):
    assert client_ip_mod.client_ip(_Req("2001:db8::1")) == "2001:db8::1"


def test_client_ip_unknown_when_no_socket_peer(monkeypatch):
    monkeypatch.delenv("TRUST_PROXY_HEADERS", raising=False)
    assert client_ip_mod.client_ip(_Req(remote_addr=None)) == "unknown"


def test_trusted_proxy_hops_defaults_and_clamps(monkeypatch):
    monkeypatch.delenv("TRUSTED_PROXY_HOPS", raising=False)
    assert client_ip_mod.trusted_proxy_hops() == 1
    monkeypatch.setenv("TRUSTED_PROXY_HOPS", "0")
    assert client_ip_mod.trusted_proxy_hops() == 1
    monkeypatch.setenv("TRUSTED_PROXY_HOPS", "junk")
    assert client_ip_mod.trusted_proxy_hops() == 1
    monkeypatch.setenv("TRUSTED_PROXY_HOPS", "3")
    assert client_ip_mod.trusted_proxy_hops() == 3
