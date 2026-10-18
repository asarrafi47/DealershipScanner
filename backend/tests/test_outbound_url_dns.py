"""DNS-aware SSRF guards for dev scanner URLs (SEC-089, P3.3)."""

from __future__ import annotations

import socket
from collections.abc import Callable
from typing import Any
from unittest.mock import patch

import pytest

from backend.utils.outbound_url import destination_host_blocked_after_dns, validate_dev_scanner_url


def _answer(*ips: str) -> list[tuple[Any, ...]]:
    out: list[tuple[Any, ...]] = []
    for ip in ips:
        if ":" in ip:
            out.append((socket.AF_INET6, socket.SOCK_STREAM, 6, "", (ip, 0, 0, 0)))
        else:
            out.append((socket.AF_INET, socket.SOCK_STREAM, 6, "", (ip, 0)))
    return out


def _resolver(table: dict[str, list[tuple[Any, ...]]]) -> Callable[..., list[tuple[Any, ...]]]:
    def fake_getaddrinfo(host: str, *args: Any, **kwargs: Any) -> list[tuple[Any, ...]]:
        if host in table:
            return table[host]
        raise socket.gaierror(socket.EAI_NONAME, f"unexpected host {host!r}")

    return fake_getaddrinfo


def test_dev_scanner_url_blocks_hostname_resolving_to_private_ip() -> None:
    def fake_getaddrinfo(host, *args, **kwargs):
        if host == "metadata.example-dealer.com":
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("169.254.169.254", 0))]
        raise socket.gaierror("unexpected host")

    with patch("backend.utils.outbound_url.socket.getaddrinfo", side_effect=fake_getaddrinfo):
        assert validate_dev_scanner_url("https://metadata.example-dealer.com/inventory") == "url_host_blocked"


@pytest.mark.parametrize(
    "resolved",
    [
        "100.64.0.1",
        "100.100.100.200",
        "100.127.255.254",
        "::ffff:100.64.0.1",
        "2002:6440:1::1",
        "198.18.0.1",
        "192.0.0.8",
        "fd7a:115c:a1e0::1",
        "fec0::1",
    ],
)
def test_name_resolving_to_non_global_address_is_blocked(monkeypatch: pytest.MonkeyPatch, resolved: str) -> None:
    monkeypatch.setattr(socket, "getaddrinfo", _resolver({"tailnet-node.example-dealer.com": _answer(resolved)}))
    assert destination_host_blocked_after_dns("tailnet-node.example-dealer.com") is True
    assert validate_dev_scanner_url("https://tailnet-node.example-dealer.com/inventory") == "url_host_blocked"


def test_name_with_one_cgnat_answer_among_public_ones_is_blocked(monkeypatch: pytest.MonkeyPatch) -> None:
    answers = _answer("8.8.8.8", "2606:4700:4700::1111", "100.64.0.1")
    monkeypatch.setattr(socket, "getaddrinfo", _resolver({"mixed.example-dealer.com": answers}))
    assert destination_host_blocked_after_dns("mixed.example-dealer.com") is True


@pytest.mark.parametrize("resolved", ["8.8.8.8", "2606:4700:4700::1111", "100.63.255.255", "100.128.0.1"])
def test_name_resolving_to_global_address_is_allowed(monkeypatch: pytest.MonkeyPatch, resolved: str) -> None:
    monkeypatch.setattr(socket, "getaddrinfo", _resolver({"www.example-dealer.com": _answer(resolved)}))
    assert destination_host_blocked_after_dns("www.example-dealer.com") is False
    assert validate_dev_scanner_url("https://www.example-dealer.com/inventory") is None


def test_fake_dns_table_entry_in_cgnat_is_blocked(fake_dns) -> None:
    fake_dns.table["tailnet.example-dealer.com"] = "100.101.102.103"
    assert validate_dev_scanner_url("https://tailnet.example-dealer.com/") == "url_host_blocked"
    assert "tailnet.example-dealer.com" in fake_dns.lookups


def test_answer_without_ip_address_is_blocked(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(socket, "getaddrinfo", _resolver({"empty.example-dealer.com": []}))
    assert destination_host_blocked_after_dns("empty.example-dealer.com") is True


def test_answer_with_unparseable_address_is_blocked(monkeypatch: pytest.MonkeyPatch) -> None:
    answers = [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("not-an-ip", 0))]
    monkeypatch.setattr(socket, "getaddrinfo", _resolver({"odd.example-dealer.com": answers}))
    assert destination_host_blocked_after_dns("odd.example-dealer.com") is True


def test_unresolvable_name_is_blocked(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(socket, "getaddrinfo", _resolver({}))
    assert destination_host_blocked_after_dns("gone.example-dealer.com") is True


def test_literal_non_global_host_is_blocked_before_dns(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    def recording(host: str, *args: Any, **kwargs: Any) -> list[tuple[Any, ...]]:
        calls.append(host)
        return _answer("8.8.8.8")

    monkeypatch.setattr(socket, "getaddrinfo", recording)
    assert destination_host_blocked_after_dns("100.64.0.1") is True
    assert destination_host_blocked_after_dns("[::ffff:100.64.0.1]") is True
    assert calls == []


def test_private_use_and_encoded_hosts_are_blocked_before_dns(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    def recording(host: str, *args: Any, **kwargs: Any) -> list[tuple[Any, ...]]:
        calls.append(host)
        return _answer("8.8.8.8")

    monkeypatch.setattr(socket, "getaddrinfo", recording)
    for host in ("postgres.railway.internal", "nas.home.arpa", "127.0.0.%31", "100.64.0.%2531", "localhos%74"):
        assert destination_host_blocked_after_dns(host) is True
    assert calls == []


def test_resolver_gets_the_decoded_name(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    def recording(host: str, *args: Any, **kwargs: Any) -> list[tuple[Any, ...]]:
        calls.append(host)
        return _answer("100.64.0.1")

    monkeypatch.setattr(socket, "getaddrinfo", recording)
    assert destination_host_blocked_after_dns("tailnet%2Dnode.example-dealer.com") is True
    assert calls == ["tailnet-node.example-dealer.com"]
