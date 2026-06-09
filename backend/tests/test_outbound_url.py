"""Outbound URL guards (dev scanner SSRF mitigation)."""

from __future__ import annotations

from backend.utils.outbound_url import validate_dev_scanner_url


def test_dev_scanner_url_accepts_public_https() -> None:
    assert validate_dev_scanner_url("https://www.example-dealer.com/inventory") is None


def test_dev_scanner_url_blocks_private_hosts() -> None:
    assert validate_dev_scanner_url("http://127.0.0.1/inventory") == "url_host_blocked"
    assert validate_dev_scanner_url("http://192.168.0.5/") == "url_host_blocked"
    assert validate_dev_scanner_url("http://localhost/") == "url_host_blocked"


def test_dev_scanner_url_blocks_non_http_scheme() -> None:
    assert validate_dev_scanner_url("file:///etc/passwd") == "url_scheme"
    assert validate_dev_scanner_url("javascript:alert(1)") == "url_scheme"


def test_dev_scanner_url_blocks_embedded_credentials() -> None:
    assert validate_dev_scanner_url("https://user:pass@example.com/") == "url_credentials"
