"""Tests for HTTP-first recipe synthesis (fingerprint + synthesize + fetch guards).

Network-free: HTML is supplied as fixtures and reference templates are injected
by monkeypatching ``load_recipes``, so nothing here touches a live dealer.
"""
from __future__ import annotations

import json

import pytest

from backend.scanner import recipe_synth
from backend.scanner.recipes import (
    PAGINATION_CARSCOMMERCE,
    PAGINATION_DEALER_COM,
    EndpointRecipe,
)

# ── Fixtures: minimal but realistic platform HTML ─────────────────────────────

_DEALER_COM_HTML = """
<html><head><script>
  var ddc = window.DDC || {};
  window.DDC.pageData = {"siteId":"showcasehonda","pageId":"showcasehonda_X"};
</script></head>
<body><div data-widget-name="ws-inv-listing" data-widget-id="inv1"></div>
<div class="ddc-content"></div></body></html>
"""

_CARSCOMMERCE_HTML = """
<html><head><script>
  var account = "77777";
  var config = {"url":"https://websites-search.api.carscommerce.inc","ccid":"77777",
                "enabled":"1","apiKey":"ABCDEF0123456789ABCD"};
</script></head><body>carscommerce powered</body></html>
"""

_TEAM_VELOCITY_HTML = """
<html><head><script>
  var inventoryApiBaseUrl = 'https://websites.api.teamvelocityportal.com/';
  var accountId = '28846';
</script></head><body></body></html>
"""

_UNKNOWN_HTML = "<html><body><h1>Welcome to Our Dealership</h1><p>Cars for sale.</p></body></html>"


def _dealer_com_ref() -> EndpointRecipe:
    body = {
        "siteId": "camelbacktoyotavtg",
        "pageId": "camelbacktoyotavtg_SITEBUILDER_INVENTORY",
        "widgetName": "ws-inv-data",
        "inventoryParameters": {"start": ["0"]},
        "preferences": {"pageSize": "500", "listing.config.id": "auto-certified-used"},
        "includePricing": True,
    }
    return EndpointRecipe(
        dealer_id="camelbacktoyota-com",
        url="https://www.camelbacktoyota.com/api/widget/ws-inv-data/getInventory",
        method="POST",
        content_type="application/json; charset=utf-8",
        post_template=json.dumps(body),
        pagination=PAGINATION_DEALER_COM,
    )


def _carscommerce_ref() -> EndpointRecipe:
    return EndpointRecipe(
        dealer_id="courtesychev-com",
        url="https://websites-search.api.carscommerce.inc/api/v1/listings/23658/search",
        method="POST",
        content_type="application/json; charset=utf-8",
        post_template=json.dumps({"page": 1, "perPage": 20, "requestedFields": ["vin"]}),
        auth_headers={"x-api-key": "SHAREDKEYshared0000"},
        pagination=PAGINATION_CARSCOMMERCE,
    )


@pytest.fixture
def _inject_refs(monkeypatch):
    refs = {"camelbacktoyota-com": [_dealer_com_ref()], "courtesychev-com": [_carscommerce_ref()]}
    monkeypatch.setattr(recipe_synth, "load_recipes", lambda did: list(refs.get(did, [])))


# ── fingerprint_platform ──────────────────────────────────────────────────────


def test_fingerprint_dealer_com():
    assert recipe_synth.fingerprint_platform(_DEALER_COM_HTML, "https://www.showcasehonda.com") == "dealer_dot_com"


def test_fingerprint_carscommerce():
    assert recipe_synth.fingerprint_platform(_CARSCOMMERCE_HTML, "https://www.courtesychev.com") == "carscommerce"


def test_fingerprint_team_velocity():
    assert recipe_synth.fingerprint_platform(_TEAM_VELOCITY_HTML, "https://www.righthonda.com") == "team_velocity"


def test_fingerprint_unknown_returns_none():
    assert recipe_synth.fingerprint_platform(_UNKNOWN_HTML, "https://example.com") is None


def test_carscommerce_not_misread_as_dealer_com():
    # CarsCommerce pages can carry a stray siteId/ddc; must not classify as Dealer.com.
    assert recipe_synth.fingerprint_platform(_CARSCOMMERCE_HTML, "x") == "carscommerce"


# ── synthesize_recipe: Dealer.com ─────────────────────────────────────────────


def test_synthesize_dealer_com(_inject_refs):
    r = recipe_synth.synthesize_recipe(
        "showcasehonda-com", "https://www.showcasehonda.com", _DEALER_COM_HTML, "dealer_dot_com"
    )
    assert r is not None
    assert r.url == "https://www.showcasehonda.com/api/widget/ws-inv-data/getInventory"
    assert r.method == "POST"
    assert r.pagination == PAGINATION_DEALER_COM
    assert r.provider_hint == "dealer_dot_com"
    body = json.loads(r.post_template)
    # siteId swapped from the reference to the target dealer's siteId, everywhere.
    assert body["siteId"] == "showcasehonda"
    assert body["pageId"].startswith("showcasehonda_")
    assert "camelbacktoyotavtg" not in r.post_template


def test_synthesize_dealer_com_needs_siteid(_inject_refs):
    html = '<div data-widget-name="ws-inv-data"></div><div class="ddc"></div>'  # markers but no siteId
    assert recipe_synth.synthesize_recipe("x-com", "https://x.com", html, "dealer_dot_com") is None


# ── synthesize_recipe: CarsCommerce ───────────────────────────────────────────


def test_synthesize_carscommerce_extracts_ccid_and_key(_inject_refs):
    r = recipe_synth.synthesize_recipe(
        "somedealer-com", "https://www.somedealer.com", _CARSCOMMERCE_HTML, "carscommerce"
    )
    assert r is not None
    assert r.url == "https://websites-search.api.carscommerce.inc/api/v1/listings/77777/search"
    assert r.pagination == PAGINATION_CARSCOMMERCE
    # Prefer the key shipped in the dealer's own HTML.
    assert r.auth_headers["x-api-key"] == "ABCDEF0123456789ABCD"


def test_synthesize_carscommerce_falls_back_to_shared_key(_inject_refs):
    html = _CARSCOMMERCE_HTML.replace('"apiKey":"ABCDEF0123456789ABCD"', '"other":"1"')
    r = recipe_synth.synthesize_recipe("d-com", "https://d.com", html, "carscommerce")
    assert r is not None
    assert r.auth_headers["x-api-key"] == "SHAREDKEYshared0000"  # reference recipe's shared key


# ── synthesize_recipe: unsynthesizable platforms ──────────────────────────────


def test_team_velocity_recognized_but_not_synthesizable():
    assert recipe_synth.is_synthesizable("team_velocity") is False
    assert recipe_synth.synthesize_recipe("rh-com", "https://x.com", _TEAM_VELOCITY_HTML, "team_velocity") is None


def test_unknown_platform_not_synthesizable():
    assert recipe_synth.is_synthesizable(None) is False
    assert recipe_synth.synthesize_recipe("x-com", "https://x.com", _UNKNOWN_HTML) is None


# ── fetch_dealer_html guards (no real network) ────────────────────────────────


class _FakeResp:
    def __init__(self, body: bytes):
        self._body = body

    def read(self) -> bytes:
        return self._body

    class headers:  # noqa: N801 - mimic urllib response.headers
        @staticmethod
        def get_content_charset():
            return "utf-8"


def test_fetch_rejects_thin_body(monkeypatch):
    monkeypatch.setattr(recipe_synth, "open_url", lambda *a, **k: _FakeResp(b"<html>tiny</html>"))
    assert recipe_synth.fetch_dealer_html("https://x.com") is None


def test_fetch_rejects_challenge_page(monkeypatch):
    body = (b"<html><body>Just a moment... Checking your browser before accessing. "
            + b"cf-challenge " * 200 + b"</body></html>")
    monkeypatch.setattr(recipe_synth, "open_url", lambda *a, **k: _FakeResp(body))
    assert recipe_synth.fetch_dealer_html("https://x.com") is None


def test_fetch_accepts_real_html(monkeypatch):
    body = b"<html><body>" + b"real dealership content " * 200 + b"</body></html>"
    monkeypatch.setattr(recipe_synth, "open_url", lambda *a, **k: _FakeResp(body))
    html = recipe_synth.fetch_dealer_html("https://x.com")
    assert html and "real dealership content" in html


def test_fetch_rejects_non_http_url():
    assert recipe_synth.fetch_dealer_html("ftp://x.com") is None
