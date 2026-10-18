"""Listing image URL sanitization for mobile + web grid JSON."""

from __future__ import annotations

import pytest

from backend.utils.safe_listing_url import normalize_listing_image_url, normalize_safe_http_url

# P3.3: the same non-global rule as the server-side guard (outbound_url.ip_is_non_global).
NON_GLOBAL_HOSTS = [
    "100.64.0.1",
    "100.127.255.254",
    "100.100.100.200",
    "[::ffff:100.64.0.1]",
    "198.18.0.1",
    "192.0.0.8",
    "[2002:6440:1::1]",  # 6to4 carrying 100.64.0.1
    "[fd7a:115c:a1e0::1]",  # Tailscale ULA
    "[::ffff:192.168.1.10]",
    "[fe80::1]",
    "169.254.169.254",
    "0.0.0.0",
    "224.0.0.1",
    "2130706433",  # legacy spelling of 127.0.0.1; browsers fetch it
    "0x64.0x40.0.1",  # legacy spelling of 100.64.0.1
    "printer.local",
    "[fec0::1]",  # deprecated site-local
    "postgres.railway.internal",
    "nas.home.arpa",
    # spellings a browser percent-decodes or folds into a non-global address
    "100.64.0.%31",
    "127.0.0.%32",  # 127.0.0.2: outside the dev allowance (exactly 127.0.0.1)
    "%31%30%30.64.0.1",
    "\uff11\uff10\uff10.\uff16\uff14.\uff10.\uff11",  # fullwidth digits: 100.64.0.1
    "100\u300264\u30020\u30021",
    "100\uff0e64\uff0e0\uff0e1",
    "[%3a%3affff%3a100.64.0.1]",
    "100.64.0.%2531",  # a browser refuses the host; never passed on as a name
]

GLOBAL_HOSTS = [
    "8.8.8.8",
    "[2606:4700:4700::1111]",
    "100.63.255.255",
    "100.128.0.0",
    "images.dealer.com",
    "b\u00fccher.example",
]

# Encoded spellings of localhost: refused in production. Outside production they
# mean the dev box, which the localhost allowance already lets through.
ENCODED_LOCALHOST_HOSTS = [
    "localhos%74",
    "\uff4c\uff4f\uff43\uff41\uff4c\uff48\uff4f\uff53\uff54",
    "local\u00adhost",
    "%31%32%37.0.0.1",
    "127.0.0.%31",
]


@pytest.fixture
def non_production(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLASK_ENV", "development")


@pytest.fixture
def production(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLASK_ENV", "production")


def test_blocks_javascript_and_data_urls() -> None:
    assert normalize_listing_image_url("javascript:alert(1)") is None
    assert normalize_listing_image_url("data:image/png;base64,abc") is None


def test_allows_car_images_path() -> None:
    assert normalize_listing_image_url("/car-images/dealer/vin/1.jpg") == "/car-images/dealer/vin/1.jpg"


def test_allows_https_dealer_cdn() -> None:
    u = normalize_listing_image_url("https://images.dealer.com/photo.jpg")
    assert u == "https://images.dealer.com/photo.jpg"


def test_blocks_private_ip_image_url() -> None:
    assert normalize_listing_image_url("https://192.168.1.10/photo.jpg") is None


def test_blocks_localhost_in_production(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLASK_ENV", "production")
    assert normalize_listing_image_url("https://127.0.0.1/photo.jpg") is None
    assert normalize_listing_image_url("https://localhost/photo.jpg") is None


def test_normalize_safe_http_url_blocks_javascript() -> None:
    assert normalize_safe_http_url("javascript:alert(1)") is None
    assert normalize_safe_http_url("https://dealer.example.com") == "https://dealer.example.com"


def test_blocks_credentials_in_url() -> None:
    assert normalize_listing_image_url("https://user:pass@example.com/x.jpg") is None


@pytest.mark.parametrize("env", ["non_production", "production"])
@pytest.mark.parametrize("host", NON_GLOBAL_HOSTS)
def test_blocks_non_global_hosts(request: pytest.FixtureRequest, env: str, host: str) -> None:
    request.getfixturevalue(env)
    assert normalize_listing_image_url(f"https://{host}/photo.jpg") is None
    assert normalize_safe_http_url(f"https://{host}/") is None
    assert normalize_safe_http_url(f"http://{host}/") is None


@pytest.mark.parametrize("env", ["non_production", "production"])
@pytest.mark.parametrize("host", GLOBAL_HOSTS)
def test_allows_global_hosts(request: pytest.FixtureRequest, env: str, host: str) -> None:
    request.getfixturevalue(env)
    assert normalize_listing_image_url(f"https://{host}/photo.jpg") == f"https://{host}/photo.jpg"
    assert normalize_safe_http_url(f"https://{host}/") == f"https://{host}/"


@pytest.mark.parametrize("host", ["localhost", "127.0.0.1", "[::1]", "localhost."])
def test_keeps_localhost_allowance_outside_production(non_production: None, host: str) -> None:
    assert normalize_listing_image_url(f"http://{host}:5000/static/x.jpg") == f"http://{host}:5000/static/x.jpg"


@pytest.mark.parametrize("host", ["localhost", "127.0.0.1", "[::1]", "localhost.", "127.0.0.2", "api.localhost"])
def test_localhost_blocked_in_production(production: None, host: str) -> None:
    assert normalize_listing_image_url(f"https://{host}/x.jpg") is None
    assert normalize_safe_http_url(f"https://{host}/") is None


@pytest.mark.parametrize("url", ["https://[::1/x.jpg", "https://[zz]/x.jpg", "https://a]b/x.jpg"])
def test_malformed_host_returns_none(url: str) -> None:
    assert normalize_listing_image_url(url) is None
    assert normalize_safe_http_url(url) is None


@pytest.mark.parametrize("env", ["non_production", "production"])
@pytest.mark.parametrize(
    "url",
    ["https://100.64.0.%31/x.jpg", "https://\uff11\uff10\uff10.\uff16\uff14.\uff10.\uff11/x.jpg"],
)
def test_encoded_cgnat_url_returns_none(request: pytest.FixtureRequest, env: str, url: str) -> None:
    # Reviewer repro: these came back unchanged with FLASK_ENV=production.
    request.getfixturevalue(env)
    assert normalize_listing_image_url(url) is None
    assert normalize_safe_http_url(url) is None


@pytest.mark.parametrize("host", ENCODED_LOCALHOST_HOSTS)
def test_encoded_localhost_blocked_in_production(production: None, host: str) -> None:
    assert normalize_listing_image_url(f"https://{host}/x.jpg") is None
    assert normalize_safe_http_url(f"https://{host}/") is None
