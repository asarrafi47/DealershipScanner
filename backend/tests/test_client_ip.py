"""Tests for trusted-proxy client IP behavior."""

from __future__ import annotations

import ipaddress
import logging

import pytest

from backend.utils import client_ip as client_ip_mod

# A Cloudflare edge address (172.64.0.0/13) and two outside ones (documentation ranges).
CF_EDGE = "172.67.1.1"
CLIENT = "198.51.100.23"
ATTACKER = "203.0.113.9"


class _Req:
    def __init__(self, forwarded: str | None = None, remote_addr: str | None = "10.0.0.1"):
        self.headers = {} if forwarded is None else {"X-Forwarded-For": forwarded}
        self.remote_addr = remote_addr


@pytest.fixture(autouse=True)
def _no_cidrs_by_default(monkeypatch):
    """Every test starts in the hop-count mode unless it sets TRUSTED_PROXY_CIDRS itself.

    setenv("") rather than delenv: a dotenv reload elsewhere cannot resurrect it.
    """
    monkeypatch.setenv("TRUSTED_PROXY_CIDRS", "")


@pytest.fixture
def trusted(monkeypatch):
    monkeypatch.setenv("TRUST_PROXY_HEADERS", "1")
    monkeypatch.delenv("TRUSTED_PROXY_HOPS", raising=False)


@pytest.fixture
def cloudflare(monkeypatch):
    """Prod after N3.13: Cloudflare -> Railway edge -> app, hops=2, CIDRs=cloudflare."""
    monkeypatch.setenv("TRUST_PROXY_HEADERS", "1")
    monkeypatch.setenv("TRUSTED_PROXY_HOPS", "2")
    monkeypatch.setenv("TRUSTED_PROXY_CIDRS", "cloudflare")


# --- Hop-count mode (TRUSTED_PROXY_CIDRS unset): behaviour unchanged ----------------------


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


def test_unset_cidrs_keep_the_hop_rule_direct_origin_spoof(monkeypatch):
    """The hole P3.2 closes, pinned so the unset path stays exactly as it was.

    A request sent straight to the Railway origin has only the edge's entry to the right of
    its own forged one, so hops=2 selects the forgery.
    """
    monkeypatch.setenv("TRUST_PROXY_HEADERS", "1")
    monkeypatch.setenv("TRUSTED_PROXY_HOPS", "2")
    assert client_ip_mod.trusted_proxy_cidrs() is None
    assert client_ip_mod.client_ip(_Req(f"6.6.6.6, {ATTACKER}")) == "6.6.6.6"


@pytest.mark.parametrize("unset_value", ["", "   ", " , "])
def test_blank_cidrs_count_as_unset(monkeypatch, unset_value):
    monkeypatch.setenv("TRUSTED_PROXY_CIDRS", unset_value)
    if unset_value.strip():
        # " , " is set but holds no entry: CIDR mode with nothing trusted (fails closed).
        assert client_ip_mod.trusted_proxy_cidrs() == ()
    else:
        assert client_ip_mod.trusted_proxy_cidrs() is None


# --- CIDR mode (TRUSTED_PROXY_CIDRS set) --------------------------------------------------


def test_through_cloudflare_resolves_to_client(cloudflare):
    """Spec: through Cloudflare, 'client, 172.67.1.1' resolves to the client."""
    assert client_ip_mod.client_ip(_Req(f"{CLIENT}, {CF_EDGE}")) == CLIENT


def test_direct_origin_spoof_resolves_to_real_peer(cloudflare):
    """Spec: 'spoof, 203.0.113.9' with hops=2 and CIDRs=cloudflare resolves to 203.0.113.9."""
    assert client_ip_mod.client_ip(_Req(f"spoof, {ATTACKER}")) == ATTACKER
    assert client_ip_mod.client_ip(_Req(f"6.6.6.6, {ATTACKER}")) == ATTACKER


def test_client_forged_entry_before_cloudflare_is_ignored(cloudflare):
    """Cloudflare appends the real peer after anything the client sent."""
    req = _Req(f"6.6.6.6, {CF_EDGE}, {CLIENT}, {CF_EDGE}")
    assert client_ip_mod.client_ip(req) == CLIENT
    assert client_ip_mod.client_ip(_Req(f"6.6.6.6, {CLIENT}, {CF_EDGE}")) == CLIENT


def test_cidr_mode_ignores_hop_count(monkeypatch):
    """With CIDRs set, hops no longer decide: hops=1 would have returned the Cloudflare edge."""
    monkeypatch.setenv("TRUST_PROXY_HEADERS", "1")
    monkeypatch.setenv("TRUSTED_PROXY_HOPS", "1")
    monkeypatch.setenv("TRUSTED_PROXY_CIDRS", "cloudflare")
    assert client_ip_mod.client_ip(_Req(f"{CLIENT}, {CF_EDGE}")) == CLIENT


def test_cloudflare_ipv6_edge_is_skipped(cloudflare):
    assert client_ip_mod.client_ip(_Req("2001:db8::5, 2606:4700::1")) == "2001:db8::5"
    assert client_ip_mod.client_ip(_Req(f"{CLIENT}, [2a06:98c0::7]:443")) == CLIENT


def test_ipv4_mapped_cloudflare_edge_is_skipped(cloudflare):
    assert client_ip_mod.client_ip(_Req(f"{CLIENT}, ::ffff:{CF_EDGE}")) == CLIENT


def test_cidr_mode_strips_client_port(cloudflare):
    assert client_ip_mod.client_ip(_Req(f"{CLIENT}:5555, {CF_EDGE}")) == CLIENT


def test_cidr_mode_without_header_falls_back(cloudflare):
    assert client_ip_mod.client_ip(_Req()) == "10.0.0.1"


def test_cidr_mode_malformed_client_entry_falls_back(cloudflare):
    """Garbage where the client should be means a hop misbehaved: use the socket peer."""
    assert client_ip_mod.client_ip(_Req(f"not-an-ip, {CF_EDGE}")) == "10.0.0.1"


def test_cidr_mode_all_trusted_returns_leftmost(cloudflare):
    """Every hop a Cloudflare address: the leftmost one is the best-known client."""
    assert client_ip_mod.client_ip(_Req(f"172.67.9.9, {CF_EDGE}")) == "172.67.9.9"


def test_cidrs_without_trust_switch_never_read_the_header(monkeypatch):
    """TRUSTED_PROXY_CIDRS does not turn proxy trust on by itself."""
    monkeypatch.delenv("TRUST_PROXY_HEADERS", raising=False)
    monkeypatch.setenv("TRUSTED_PROXY_CIDRS", "cloudflare")
    assert client_ip_mod.client_ip(_Req(f"{CLIENT}, {CF_EDGE}")) == "10.0.0.1"


def test_custom_cidrs_and_bare_ips(monkeypatch):
    monkeypatch.setenv("TRUST_PROXY_HEADERS", "1")
    monkeypatch.setenv("TRUSTED_PROXY_CIDRS", "10.20.0.0/16, 192.0.2.10")
    req = _Req(f"6.6.6.6, {CLIENT}, 192.0.2.10, 10.20.3.4")
    assert client_ip_mod.client_ip(req) == CLIENT
    # Cloudflare is not trusted unless asked for: its edge is the client here.
    assert client_ip_mod.client_ip(_Req(f"{CLIENT}, {CF_EDGE}")) == CF_EDGE


def test_cloudflare_token_mixes_with_cidrs_case_insensitively(monkeypatch):
    monkeypatch.setenv("TRUST_PROXY_HEADERS", "1")
    monkeypatch.setenv("TRUSTED_PROXY_CIDRS", " CloudFlare , 10.20.0.0/16 ")
    req = _Req(f"{CLIENT}, {CF_EDGE}, 10.20.3.4")
    assert client_ip_mod.client_ip(req) == CLIENT


def test_invalid_cidr_tokens_are_dropped_with_a_warning(monkeypatch, caplog):
    monkeypatch.setenv("TRUST_PROXY_HEADERS", "1")
    monkeypatch.setenv("TRUSTED_PROXY_CIDRS", "cloudflare, not-a-cidr-p32-test")
    client_ip_mod._parse_trusted_cidrs.cache_clear()
    with caplog.at_level(logging.WARNING, logger=client_ip_mod.__name__):
        assert client_ip_mod.client_ip(_Req(f"{CLIENT}, {CF_EDGE}")) == CLIENT
        client_ip_mod.client_ip(_Req(f"{CLIENT}, {CF_EDGE}"))
    invalid_logs = [r for r in caplog.records if "not-a-cidr-p32-test" in r.getMessage()]
    assert len(invalid_logs) == 1


def test_all_invalid_cidrs_fail_closed(monkeypatch):
    """A set but unusable value trusts nothing past the socket peer, never the hop rule."""
    monkeypatch.setenv("TRUST_PROXY_HEADERS", "1")
    monkeypatch.setenv("TRUSTED_PROXY_HOPS", "2")
    monkeypatch.setenv("TRUSTED_PROXY_CIDRS", "junk")
    assert client_ip_mod.trusted_proxy_cidrs() == ()
    assert client_ip_mod.client_ip(_Req(f"6.6.6.6, {ATTACKER}")) == ATTACKER
    assert client_ip_mod.client_ip(_Req(f"{CLIENT}, {CF_EDGE}")) == CF_EDGE


def test_cloudflare_constant_is_dated_and_parses():
    assert client_ip_mod.CLOUDFLARE_PROXY_CIDRS_AS_OF == "2026-10-18"
    networks = [ipaddress.ip_network(c) for c in client_ip_mod.CLOUDFLARE_PROXY_CIDRS]
    assert {n.version for n in networks} == {4, 6}
    assert len(networks) == len(set(networks)) == 22
    assert ipaddress.ip_address(CF_EDGE) in ipaddress.ip_network("172.64.0.0/13")
    for addr in (CLIENT, ATTACKER, "10.0.0.1", "127.0.0.1", "2001:db8::5"):
        assert not client_ip_mod._in_networks(addr, tuple(networks))


# --- One-time warning: hops >= 2 without CIDRs --------------------------------------------


def _no_cidr_warnings(caplog) -> list[logging.LogRecord]:
    return [r for r in caplog.records if "without TRUSTED_PROXY_CIDRS" in r.getMessage()]


def test_hops_two_without_cidrs_logs_once(monkeypatch, caplog):
    monkeypatch.setattr(client_ip_mod, "_NO_CIDRS_LOGGED", False)
    monkeypatch.setenv("TRUST_PROXY_HEADERS", "1")
    monkeypatch.setenv("TRUSTED_PROXY_HOPS", "2")
    with caplog.at_level(logging.WARNING, logger=client_ip_mod.__name__):
        client_ip_mod.client_ip(_Req(f"{CLIENT}, {CF_EDGE}"))
        client_ip_mod.client_ip(_Req(f"{CLIENT}, {CF_EDGE}"))
        client_ip_mod.client_ip(_Req())
    records = _no_cidr_warnings(caplog)
    assert len(records) == 1
    assert "TRUSTED_PROXY_HOPS=2" in records[0].getMessage()
    # Addresses never reach the log.
    assert CLIENT not in records[0].getMessage()


@pytest.mark.parametrize(
    "hops, cidrs",
    [("1", ""), ("", ""), ("2", "cloudflare"), ("3", "10.0.0.0/8")],
)
def test_no_cidr_warning_when_not_applicable(monkeypatch, caplog, hops, cidrs):
    monkeypatch.setattr(client_ip_mod, "_NO_CIDRS_LOGGED", False)
    monkeypatch.setenv("TRUST_PROXY_HEADERS", "1")
    monkeypatch.setenv("TRUSTED_PROXY_HOPS", hops)
    monkeypatch.setenv("TRUSTED_PROXY_CIDRS", cidrs)
    with caplog.at_level(logging.WARNING, logger=client_ip_mod.__name__):
        client_ip_mod.client_ip(_Req(f"{CLIENT}, {CF_EDGE}"))
    assert _no_cidr_warnings(caplog) == []


def test_no_cidr_warning_when_proxy_headers_untrusted(monkeypatch, caplog):
    """Hops mean nothing while X-Forwarded-For is ignored, so no warning."""
    monkeypatch.setattr(client_ip_mod, "_NO_CIDRS_LOGGED", False)
    monkeypatch.delenv("TRUST_PROXY_HEADERS", raising=False)
    monkeypatch.setenv("TRUSTED_PROXY_HOPS", "2")
    with caplog.at_level(logging.WARNING, logger=client_ip_mod.__name__):
        client_ip_mod.client_ip(_Req(f"{CLIENT}, {CF_EDGE}"))
    assert _no_cidr_warnings(caplog) == []


# --- DEV_IP_ALLOWLIST (dev/routes.py) goes through client_ip() ----------------------------


@pytest.fixture
def dev_routes():
    from backend.dev import routes

    return routes


def test_dev_allowlist_spoof_refused_with_cloudflare_cidrs(cloudflare, monkeypatch, dev_routes):
    """Spec: a DEV allowlist spoof is refused once CIDRs are configured."""
    monkeypatch.setenv("DEV_IP_ALLOWLIST", CLIENT)
    # Direct to origin, claiming to be the allowlisted admin.
    assert dev_routes._dev_client_ip_allowed(_Req(f"{CLIENT}, {ATTACKER}")) is False
    # The real admin through Cloudflare is still let in.
    assert dev_routes._dev_client_ip_allowed(_Req(f"{CLIENT}, {CF_EDGE}")) is True
    # Forging the admin's address through Cloudflare does not help either.
    assert dev_routes._dev_client_ip_allowed(_Req(f"{CLIENT}, {ATTACKER}, {CF_EDGE}")) is False


def test_dev_allowlist_spoof_accepted_without_cidrs(monkeypatch, dev_routes):
    """Why DEV_IP_ALLOWLIST stays unset until the CIDRs are live (D-SEC5): hops alone leak."""
    monkeypatch.setenv("TRUST_PROXY_HEADERS", "1")
    monkeypatch.setenv("TRUSTED_PROXY_HOPS", "2")
    monkeypatch.setenv("DEV_IP_ALLOWLIST", CLIENT)
    assert dev_routes._dev_client_ip_allowed(_Req(f"{CLIENT}, {ATTACKER}")) is True
