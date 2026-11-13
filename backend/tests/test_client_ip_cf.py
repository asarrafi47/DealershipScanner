"""D1: phone_client_ip trusts CF-Connecting-IP only for requests that came through Cloudflare."""

from __future__ import annotations

import pytest

from backend.utils.client_ip import client_ip, phone_client_ip

CF_EDGE = "172.67.1.1"  # inside Cloudflare's 172.64.0.0/13
CLIENT = "198.51.100.23"
ATTACKER = "203.0.113.9"
FORGED = "192.0.2.77"


class _Req:
    def __init__(self, headers=None, remote_addr="10.0.0.1"):
        self.headers = headers or {}
        self.remote_addr = remote_addr


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("TRUST_PROXY_HEADERS", "1")
    monkeypatch.setenv("TRUSTED_PROXY_HOPS", "2")
    monkeypatch.setenv("TRUSTED_PROXY_CIDRS", "cloudflare")


def test_header_with_cloudflare_peer_is_used():
    r = _Req({"CF-Connecting-IP": CLIENT, "X-Forwarded-For": f"{CLIENT}, {CF_EDGE}"})
    assert phone_client_ip(r) == CLIENT


def test_header_from_non_cloudflare_peer_is_ignored():
    # direct hit on the origin: the edge appended the attacker's own address
    r = _Req({"CF-Connecting-IP": FORGED, "X-Forwarded-For": ATTACKER})
    assert phone_client_ip(r) == ATTACKER


def test_header_ignored_without_cidr_setting(monkeypatch):
    monkeypatch.setenv("TRUSTED_PROXY_CIDRS", "")
    r = _Req({"CF-Connecting-IP": FORGED, "X-Forwarded-For": f"{CLIENT}, {CF_EDGE}"})
    assert phone_client_ip(r) != FORGED


def test_forged_x_forwarded_for_does_not_change_result():
    base = _Req({"CF-Connecting-IP": CLIENT, "X-Forwarded-For": f"{CLIENT}, {CF_EDGE}"})
    forged = _Req({"CF-Connecting-IP": CLIENT, "X-Forwarded-For": f"{FORGED}, {CLIENT}, {CF_EDGE}"})
    assert phone_client_ip(forged) == phone_client_ip(base) == CLIENT
    # and with no header, a forged leftmost entry cannot be picked either
    no_hdr = _Req({"X-Forwarded-For": f"{FORGED}, {ATTACKER}"})
    assert phone_client_ip(no_hdr) == client_ip(no_hdr) == ATTACKER


def test_invalid_header_falls_back():
    r = _Req({"CF-Connecting-IP": "not-an-ip", "X-Forwarded-For": f"{CLIENT}, {CF_EDGE}"})
    assert phone_client_ip(r) == client_ip(r)


def test_ipv6_counted_by_slash_64():
    a = _Req({"CF-Connecting-IP": "2001:db8:1:2:aaaa::1", "X-Forwarded-For": f"x, {CF_EDGE}"})
    b = _Req({"CF-Connecting-IP": "2001:db8:1:2:bbbb::9", "X-Forwarded-For": f"x, {CF_EDGE}"})
    c = _Req({"CF-Connecting-IP": "2001:db8:1:3::1", "X-Forwarded-For": f"x, {CF_EDGE}"})
    assert phone_client_ip(a) == phone_client_ip(b) == "2001:db8:1:2::/64"
    assert phone_client_ip(c) != phone_client_ip(a)
