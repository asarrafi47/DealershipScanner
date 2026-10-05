"""Outbound URL guards (dev scanner SSRF mitigation)."""

from __future__ import annotations

import pytest

from backend.utils.outbound_url import validate_dev_scanner_url

# Public names resolve from the conftest fake_dns table, never the network.
pytestmark = pytest.mark.usefixtures("fake_dns")


def test_dev_scanner_url_accepts_public_https() -> None:
    assert validate_dev_scanner_url("https://www.example-dealer.com/inventory") is None


def test_dev_scanner_url_blocks_names_resolving_to_private(fake_dns) -> None:
    fake_dns.table["intranet.example-dealer.com"] = "10.1.2.3"
    fake_dns.table["gone.example-dealer.com"] = None
    assert validate_dev_scanner_url("https://intranet.example-dealer.com/") == "url_host_blocked"
    # Unresolvable names are allowed through (documented in destination_host_blocked_after_dns).
    assert validate_dev_scanner_url("https://gone.example-dealer.com/") is None
    assert "intranet.example-dealer.com" in fake_dns.lookups


def test_dev_scanner_url_blocks_private_hosts() -> None:
    assert validate_dev_scanner_url("http://127.0.0.1/inventory") == "url_host_blocked"
    assert validate_dev_scanner_url("http://192.168.0.5/") == "url_host_blocked"
    assert validate_dev_scanner_url("http://localhost/") == "url_host_blocked"


def test_dev_scanner_url_blocks_non_http_scheme() -> None:
    assert validate_dev_scanner_url("file:///etc/passwd") == "url_scheme"
    assert validate_dev_scanner_url("javascript:alert(1)") == "url_scheme"


def test_dev_scanner_url_blocks_embedded_credentials() -> None:
    assert validate_dev_scanner_url("https://user:pass@example.com/") == "url_credentials"
