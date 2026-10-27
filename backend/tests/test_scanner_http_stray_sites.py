"""P12A.3: the stray scanner HTTP sites send dealer-site fetches through the scanner proxy
and the shared net/client helpers (fakes only, no network)."""
from __future__ import annotations

import asyncio
import io
import json
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from backend.scanner.net import client as nc

PROXY = "http://proxy.test:8080"
REPO = Path(__file__).resolve().parents[2]


@pytest.fixture(autouse=True)
def _proxy(monkeypatch):
    for name in ("HTTPS_PROXY", "HTTP_PROXY"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("SCANNER_HTTP_PROXY", PROXY)
    nc.HOST_PROFILES.clear()
    yield
    nc.HOST_PROFILES.clear()


class FakeCffi:
    """curl_cffi.requests stand-in: records every call, answers from a status queue."""

    def __init__(self, statuses=(200,), body="{}"):
        self.calls: list[dict] = []
        self.statuses = list(statuses)
        self.body = body

    def _answer(self, url, kwargs):
        self.calls.append({"url": url, **kwargs})
        status = self.statuses.pop(0) if len(self.statuses) > 1 else self.statuses[0]
        return SimpleNamespace(status_code=status, text=self.body, json=lambda: json.loads(self.body))

    def get(self, url, **kwargs):
        return self._answer(url, kwargs)

    def request(self, method, url, **kwargs):
        return self._answer(url, kwargs)


def test_pipeline_status_probe_uses_proxy_and_status_probe_timeout(monkeypatch):
    from backend.scanner import recipe_synth
    from backend.scanner.pipeline import recipes as pr

    fake = FakeCffi(statuses=(403,))
    monkeypatch.setattr(recipe_synth, "fetch_dealer_html", lambda url: None)
    monkeypatch.setattr(nc, "import_curl_cffi", lambda: fake)
    monkeypatch.setattr("backend.scanner.recipes.load_recipes", lambda did: [])
    info = pr.ensure_recipe({"dealer_id": "d1", "url": "https://dealer.example.com"})
    assert info["synth"] == "homepage_unreachable_403"
    (call,) = fake.calls
    assert call["proxies"] == {"http": PROXY, "https": PROXY}
    assert call["impersonate"] == "chrome"
    assert call["timeout"] == nc.STATUS_PROBE.timeout


def test_wp_vehicles_fetch_rotates_proxies_paces_and_remembers(monkeypatch):
    from backend.scanner.synth import http as synth_http
    from backend.scanner.synth.platforms import wp_vehicles

    body = json.dumps({"vehicles": [{"title": "x"}] * 6})
    fake = FakeCffi(statuses=(403, 200), body=body)
    paced: list[int] = []
    monkeypatch.setattr(nc, "import_curl_cffi", lambda: fake)
    monkeypatch.setattr(synth_http, "_pace", lambda: paced.append(1))
    items = wp_vehicles._fetch_wp_vehicles("https://dealer.example.com")
    assert len(items) == 6
    assert [c["impersonate"] for c in fake.calls] == ["chrome", "chrome124"]
    assert all(c["proxies"] == {"http": PROXY, "https": PROXY} for c in fake.calls)
    assert len(paced) == 2
    assert nc.HOST_PROFILES.get("https://dealer.example.com") == "chrome124"
    # the next call starts from the remembered winner
    fake.calls.clear()
    wp_vehicles._fetch_wp_vehicles("https://dealer.example.com")
    assert fake.calls[0]["impersonate"] == "chrome124"


def test_wp_vehicles_fetch_none_when_every_profile_blocked(monkeypatch):
    from backend.scanner.synth import http as synth_http
    from backend.scanner.synth.platforms import wp_vehicles

    fake = FakeCffi(statuses=(403,))
    monkeypatch.setattr(nc, "import_curl_cffi", lambda: fake)
    monkeypatch.setattr(synth_http, "_pace", lambda: None)
    assert wp_vehicles._fetch_wp_vehicles("https://dealer.example.com") is None
    assert len(fake.calls) == len(nc.IMPERSONATE_PROFILES)


def _fake_requests_get(monkeypatch, gap_fill, text, record):
    def get(url, **kwargs):
        record.append({"url": url, **kwargs})
        return SimpleNamespace(status_code=200, text=text, raise_for_status=lambda: None)

    monkeypatch.setattr(gap_fill.requests, "get", get)


def test_gap_fill_requests_fetch_passes_proxy(monkeypatch):
    from backend.scanner.post_scan import gap_fill

    calls: list[dict] = []
    _fake_requests_get(monkeypatch, gap_fill, "<html>" + "ok " * 400 + "</html>", calls)
    assert gap_fill._requests_fetch_html("https://dealer.example.com/vdp/1")
    assert calls[0]["proxies"] == {"http": PROXY, "https": PROXY}


def test_gap_fill_requests_fetch_rejects_challenge_page(monkeypatch):
    from backend.scanner.post_scan import gap_fill

    calls: list[dict] = []
    page = "<html><title>Just a moment...</title>" + "x" * 3000 + "</html>"
    _fake_requests_get(monkeypatch, gap_fill, page, calls)
    assert gap_fill._requests_fetch_html("https://dealer.example.com/vdp/1") is None


def test_gap_fill_ddg_search_stays_off_the_proxy():
    """D-OD13 (a): the DuckDuckGo spec search is not a dealer-site fetch."""
    from backend.scanner.post_scan import gap_fill

    src = Path(gap_fill.__file__).read_text()
    block = src[src.index("DDG_HTML_URL,\n"):]
    block = block[: block.index("except requests.RequestException")]
    assert "proxies" not in block


def test_autowall_http_sessions_carry_the_proxy(monkeypatch):
    from backend.scanner.scrapers import autowall

    sessions: list = []

    class Session:
        def __init__(self):
            self.headers = {}
            self.proxies = None
            self.cookies = SimpleNamespace(set=lambda *a, **k: None)
            sessions.append(self)

        def get(self, url, **kw):
            return SimpleNamespace(status_code=404, text="")

    monkeypatch.setattr(autowall, "_requests", SimpleNamespace(Session=Session))
    asyncio.run(autowall.fetch_autowall_inventory_http("https://dealer.example.com", "d1", "Dealer"))
    assert sessions and all(s.proxies == {"http": PROXY, "https": PROXY} for s in sessions)


def test_autowall_sessions_without_proxy_get_empty_mapping(monkeypatch):
    from backend.scanner.scrapers import autowall

    monkeypatch.delenv("SCANNER_HTTP_PROXY")
    sessions: list = []

    class Session:
        def __init__(self):
            self.headers = {}
            self.proxies = None
            sessions.append(self)

        def get(self, url, **kw):
            return SimpleNamespace(status_code=404, text="")

    monkeypatch.setattr(autowall, "_requests", SimpleNamespace(Session=Session))
    asyncio.run(autowall.fetch_autowall_inventory_http("https://dealer.example.com", "d1", "Dealer"))
    assert sessions and all(s.proxies == {} for s in sessions)


def test_dealer_eprocess_results_api_opens_through_http_fetch(monkeypatch):
    from backend.scanner import http_fetch
    from backend.scanner.scrapers import dealer_eprocess

    opened: list = []

    class Resp(io.BytesIO):
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    def open_url(req, timeout=None):
        opened.append((req.full_url, timeout))
        return Resp(json.dumps({"results": []}).encode())

    monkeypatch.setattr(http_fetch, "open_url", open_url)
    asyncio.run(dealer_eprocess._fetch_results_api("https://dealer.example.com", "d1", "Dealer", "https://dealer.example.com"))
    assert len(opened) == 2 and all("/resrc/inventory/results/" in u for u, _ in opened)
    assert all(t == 20 for _, t in opened)


def test_no_direct_curl_cffi_import_outside_net_client_in_stray_sites():
    """P12A.3 acceptance: 'from curl_cffi' under backend/scanner matches only net/client.py."""
    out = subprocess.run(["git", "grep", "-l", "from curl_cffi", "--", "backend/scanner"],
                         cwd=REPO, capture_output=True, text=True).stdout.split()
    assert [p for p in out if p != "backend/scanner/net/client.py"] == []
