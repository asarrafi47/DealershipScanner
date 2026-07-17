"""Tests for the optional proxy plumbing in ``backend.scanner.http_fetch``.

Guarantees:
  * strict no-op when no proxy env var is set (default direct connection);
  * ``SCANNER_HTTP_PROXY`` is honored and wins over the standard proxy vars;
  * fallback to ``HTTPS_PROXY`` / ``HTTP_PROXY`` when the scanner var is unset;
  * the urllib opener actually installs a ProxyHandler only when configured.
"""

from __future__ import annotations

import urllib.request

import pytest

from backend.scanner import http_fetch

_PROXY_VARS = ("SCANNER_HTTP_PROXY", "HTTPS_PROXY", "HTTP_PROXY")


@pytest.fixture(autouse=True)
def _clear_proxy_env(monkeypatch):
    for name in _PROXY_VARS:
        monkeypatch.delenv(name, raising=False)


def _proxies_of(opener: urllib.request.OpenerDirector) -> dict:
    for h in opener.handlers:
        if isinstance(h, urllib.request.ProxyHandler):
            return h.proxies
    return {}


# --- no-op when unset -------------------------------------------------------

def test_proxy_url_none_when_unset():
    assert http_fetch.proxy_url() is None


def test_requests_proxies_none_when_unset():
    # None keeps requests' default behavior — byte-for-byte the old path.
    assert http_fetch.requests_proxies() is None


def test_opener_has_no_proxy_when_unset():
    # Default opener carries an empty ProxyHandler (same as urlopen's default),
    # i.e. no explicit http/https proxy is injected.
    assert _proxies_of(http_fetch.proxied_opener()).get("https") is None


def test_blank_value_treated_as_unset(monkeypatch):
    monkeypatch.setenv("SCANNER_HTTP_PROXY", "   ")
    assert http_fetch.proxy_url() is None
    assert http_fetch.requests_proxies() is None


# --- honored when set -------------------------------------------------------

def test_scanner_var_is_used(monkeypatch):
    monkeypatch.setenv("SCANNER_HTTP_PROXY", "http://user:pass@proxy.example:8000")
    assert http_fetch.proxy_url() == "http://user:pass@proxy.example:8000"
    assert http_fetch.requests_proxies() == {
        "http": "http://user:pass@proxy.example:8000",
        "https": "http://user:pass@proxy.example:8000",
    }


def test_opener_installs_proxy_when_set(monkeypatch):
    monkeypatch.setenv("SCANNER_HTTP_PROXY", "http://127.0.0.1:9")
    proxies = _proxies_of(http_fetch.proxied_opener())
    assert proxies.get("http") == "http://127.0.0.1:9"
    assert proxies.get("https") == "http://127.0.0.1:9"


def test_scanner_var_wins_over_standard(monkeypatch):
    monkeypatch.setenv("SCANNER_HTTP_PROXY", "http://scanner.example:1")
    monkeypatch.setenv("HTTPS_PROXY", "http://standard.example:2")
    assert http_fetch.proxy_url() == "http://scanner.example:1"


def test_falls_back_to_standard_vars(monkeypatch):
    monkeypatch.setenv("HTTPS_PROXY", "http://standard.example:2")
    assert http_fetch.proxy_url() == "http://standard.example:2"
    # requests_proxies only reflects the scanner-specific var (requests already
    # honors HTTPS_PROXY on its own), so it stays None here.
    assert http_fetch.requests_proxies() is None


def test_open_url_uses_bogus_proxy_and_fails_fast(monkeypatch):
    """A bogus proxy must be attempted (and fail) rather than connecting direct."""
    monkeypatch.setenv("SCANNER_HTTP_PROXY", "http://127.0.0.1:9")
    req = urllib.request.Request("http://example.com/", headers={"Accept": "*/*"})
    with pytest.raises(urllib.error.URLError):
        http_fetch.open_url(req, timeout=3)
