"""P16.4: the scanner SSRF guard at net/client.send, http_fetch.open_url, the
shopperexpress session, recipe validation and registry ingestion. The guard's
resolver (``outbound_url._blocked_uncached``) is stubbed here only."""
from __future__ import annotations

import asyncio
import urllib.error

import pytest

from backend.scanner import http_fetch, recipe_validation
from backend.scanner.net import client, ssrf
from backend.utils import outbound_url

PUBLIC = "dealer.example.com"


@pytest.fixture(autouse=True)
def _guard(monkeypatch):
    for k in ("SCANNER_ALLOW_PRIVATE_HOSTS", "SCANNER_HTTP_PROXY", "HTTPS_PROXY", "HTTP_PROXY"):
        monkeypatch.delenv(k, raising=False)
    outbound_url.clear_destination_cache()
    calls: list[str] = []

    def fake(host):
        calls.append(host)
        return host.startswith("rebind.")  # a public-looking name resolving privately

    monkeypatch.setattr(outbound_url, "_blocked_uncached", fake)
    yield calls
    outbound_url.clear_destination_cache()


class _Lib:
    def __init__(self):
        self.urls = []

    def get(self, url, **kw):
        self.urls.append(url)
        return "ok"


@pytest.mark.parametrize("url", [
    "http://100.64.0.1/x", "http://127.0.0.1/", "http://169.254.169.254/latest",
    "http://localhost:8080/", "http://svc.railway.internal/", "http://[::1]/", "http://rebind.evil.test/",
    "file:///etc/passwd",
])
def test_send_refuses_non_global(url):
    lib = _Lib()
    with pytest.raises(ssrf.BlockedDestination):
        client.send(lib, "GET", url, via_get=True)
    assert lib.urls == []


def test_send_allows_public_and_caches(_guard):
    lib = _Lib()
    for _ in range(3):
        assert client.send(lib, "GET", f"https://{PUBLIC}/a", via_get=True) == "ok"
    assert _guard == [PUBLIC]  # one lookup, then cached


def test_cache_expires(monkeypatch, _guard):
    t = [1000.0]
    monkeypatch.setattr(outbound_url.time, "monotonic", lambda: t[0])
    outbound_url.destination_host_blocked_cached(PUBLIC)
    t[0] += outbound_url.RESOLVER_CACHE_TTL_SEC + 1
    outbound_url.destination_host_blocked_cached(PUBLIC)
    assert _guard == [PUBLIC, PUBLIC]


def test_escape_hatch_and_proxy_exempt(monkeypatch):
    lib = _Lib()
    monkeypatch.setenv("SCANNER_ALLOW_PRIVATE_HOSTS", "1")
    assert client.send(lib, "GET", "http://100.64.0.1/", via_get=True) == "ok"
    monkeypatch.delenv("SCANNER_ALLOW_PRIVATE_HOSTS")
    monkeypatch.setenv("SCANNER_HTTP_PROXY", "http://proxy.example:8000")
    assert client.send(lib, "GET", "http://100.64.0.1/", via_get=True) == "ok"


def test_ambient_proxy_does_not_exempt(monkeypatch):
    monkeypatch.setenv("HTTPS_PROXY", "http://proxy.example:8000")
    with pytest.raises(ssrf.BlockedDestination):
        client.send(_Lib(), "GET", "http://100.64.0.1/", via_get=True)


def test_open_url_refuses_and_is_urlerror():
    import urllib.request

    with pytest.raises(urllib.error.URLError):
        http_fetch.open_url("http://100.64.0.1/")
    with pytest.raises(ConnectionError):
        http_fetch.open_url(urllib.request.Request("http://10.0.0.5/"))


def test_shopperexpress_list_fetch_refused(monkeypatch):
    from backend.scanner.scrapers import shopperexpress

    def boom(*a, **k):
        raise AssertionError("session.get reached")

    monkeypatch.setattr(shopperexpress.aiohttp.ClientSession, "get", boom)
    out = asyncio.run(shopperexpress.fetch_shopperexpress_inventory("http://100.64.0.1", "d", "D"))
    assert out == []


def test_acheck_url():
    with pytest.raises(ssrf.BlockedDestination):
        asyncio.run(ssrf.acheck_url("http://192.168.1.89/"))
    asyncio.run(ssrf.acheck_url(f"https://{PUBLIC}/"))


def test_recipe_validation_refuses_private_endpoint():
    from backend.scanner.recipes import EndpointRecipe

    r = EndpointRecipe(dealer_id="d", url="http://100.64.0.1/inventory.json", method="GET", content_type="application/json", post_template=None)
    fetched = []
    chk, vins, *_ = recipe_validation._check_recipe(
        r, fetch=lambda *a: fetched.append(a) or (200, {}), base_url=f"https://{PUBLIC}",
        dealer_id="d", dealer_name="D", place=None, pages=1)
    assert fetched == [] and not vins
    assert chk.pages[0]["status"] == 0 and "refused" in chk.pages[0]["error"]


def test_registry_ingestion_skips_private():
    from backend.scanner import manifest

    assert manifest._registry_url_blocked("http://100.64.0.1/")
    assert manifest._registry_url_blocked("localhost:5000")
    assert not manifest._registry_url_blocked(f"https://www.{PUBLIC}")


def test_dealer_eprocess_results_api_guarded():
    from backend.scanner.scrapers import dealer_eprocess

    assert dealer_eprocess._ssrf is ssrf
