"""DNS-aware SSRF guards for dev scanner URLs (SEC-089)."""

from __future__ import annotations

import socket
from unittest.mock import patch

from backend.utils.outbound_url import validate_dev_scanner_url


def test_dev_scanner_url_blocks_hostname_resolving_to_private_ip() -> None:
    def fake_getaddrinfo(host, *args, **kwargs):
        if host == "metadata.example.internal":
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("169.254.169.254", 0))]
        raise socket.gaierror("unexpected host")

    with patch("backend.utils.outbound_url.socket.getaddrinfo", side_effect=fake_getaddrinfo):
        assert validate_dev_scanner_url("https://metadata.example.internal/inventory") == "url_host_blocked"
