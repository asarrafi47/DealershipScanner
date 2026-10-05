"""Tests for HTTP-first recipe synthesis (fingerprint + synthesize + fetch guards).

Network-free: HTML is supplied as fixtures and reference templates are injected
by monkeypatching ``load_recipes``, so nothing here touches a live dealer.
"""
from __future__ import annotations

import json

import pytest

from backend.scanner import recipe_synth
# Patches go on the synth module that LOOKS THE NAME UP (recipe_synth is a facade).
from backend.scanner.synth import common as synth_common
from backend.scanner.synth import http as synth_http
from backend.scanner.synth import registry as synth_registry
from backend.scanner.synth import validate as synth_validate
from backend.scanner.synth.platforms import (
    chapman,
    dealer_eprocess,
    dealeron_cosmos,
    jazel,
    nabthat,
    overfuel,
    team_velocity,
    typesense,
)
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

_COSMOS_HOME_HTML = """
<html><head><script type="application/json">{"dealerId":25003,"sincrowebId":"toyd-25003"}</script>
</head><body>
<navigation-bar-search dealer-id='25003'></navigation-bar-search>
<img src="/static/dealer-25003/logo.png">
<script data-website-provider="dealeron" data-website-id="do-25003"></script>
</body></html>
"""

# A Used SRP page carrying the itemlist page config (the pagecfg source).
_COSMOS_SRP_HTML = """
<html><body><script type="application/json">
{"dealerId":"25003","pageId":2483381,"pageType":"itemlist","items":["VIN1"]}
</script>
<script src="/resources/vhcliaa/components/navbarSearch/navbarSearchBundle.min.js"></script>
</body></html>
"""

_TEAM_VELOCITY_HTML = """
<html><head><script>
  var inventoryApiBaseUrl = 'https://websites.api.teamvelocityportal.com/';
  var accountId = '28846';
</script></head><body></body></html>
"""

_TYPESENSE_HTML = """
<html><head><script>
  var __tsHost = "hjnrb3s21408ezpfp.a1.typesense.net";
  var __tsApiKey = "eQUa8iq30l8Tu908Drz9WKqar6tCJGd4";
  function boot(){ let currentIndex = "vehicles-HON208436"; return currentIndex; }
</script></head><body>typesense multi_search</body></html>
"""

_SISTER_TV_HTML = """
<html><body><script>
  var esUrl = "https://es-data-v2.sister.tv/vehicles/inventory/_search";
  var library_id = "202405220922";
</script></body></html>
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
        post_template=json.dumps({
            "page": 1, "perPage": 20,
            "facetFilters": {"type_slug": ["Used", "Certified Used"]},
            "requestedFields": ["vin"],
        }),
        auth_headers={"x-api-key": "SHAREDKEYshared0000"},
        pagination=PAGINATION_CARSCOMMERCE,
    )


def _typesense_ref() -> EndpointRecipe:
    from backend.scanner.recipes import PAGINATION_TYPESENSE

    body = {"searches": [{"collection": "vehicles-TOY04247", "q": "*",
                          "filter_by": "condition:Used", "page": 1, "per_page": 24}]}
    return EndpointRecipe(
        dealer_id="toyotaoforange-com",
        url="https://hjnrb3s21408ezpfp.a1.typesense.net/multi_search?x-typesense-api-key=REFKEY0000000000",
        method="POST",
        content_type="application/json; charset=utf-8",
        post_template=json.dumps(body),
        pagination=PAGINATION_TYPESENSE,
    )


@pytest.fixture
def _inject_refs(monkeypatch):
    refs = {
        "camelbacktoyota-com": [_dealer_com_ref()],
        "courtesychev-com": [_carscommerce_ref()],
        "toyotaoforange-com": [_typesense_ref()],
    }
    monkeypatch.setattr(synth_common, "load_recipes", lambda did: list(refs.get(did, [])))


@pytest.fixture
def _offline_carscommerce_scope(monkeypatch):
    """Store-scope replays fail as they would offline (no live carscommerce API)."""
    from backend.scanner.synth.platforms import carscommerce_scope

    monkeypatch.setattr(carscommerce_scope, "_replay_request", lambda *a, **k: (None, None))


# ── fingerprint_platform ──────────────────────────────────────────────────────


def test_fingerprint_dealer_com():
    assert recipe_synth.fingerprint_platform(_DEALER_COM_HTML, "https://www.showcasehonda.com") == "dealer_dot_com"


def test_fingerprint_carscommerce():
    assert recipe_synth.fingerprint_platform(_CARSCOMMERCE_HTML, "https://www.courtesychev.com") == "carscommerce"


def test_fingerprint_dealer_on_cosmos():
    assert recipe_synth.fingerprint_platform(_COSMOS_HOME_HTML, "https://www.bellroadtoyota.com") == "dealer_on_cosmos"


def test_cosmos_not_misread_as_dealer_com():
    # cosmos pages can carry stray ddc/widget tokens; must classify as cosmos.
    assert recipe_synth.fingerprint_platform(_COSMOS_HOME_HTML, "x") == "dealer_on_cosmos"


def test_fingerprint_team_velocity():
    assert recipe_synth.fingerprint_platform(_TEAM_VELOCITY_HTML, "https://www.righthonda.com") == "team_velocity"


def test_fingerprint_typesense():
    assert recipe_synth.fingerprint_platform(_TYPESENSE_HTML, "https://www.freewayhonda.com") == "typesense"


def test_fingerprint_sister_tv():
    assert recipe_synth.fingerprint_platform(_SISTER_TV_HTML, "https://x.com") == "sister_tv"


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


def test_synthesize_carscommerce_extracts_ccid_and_key(_inject_refs, _offline_carscommerce_scope):
    r = recipe_synth.synthesize_recipe(
        "somedealer-com", "https://www.somedealer.com", _CARSCOMMERCE_HTML, "carscommerce"
    )
    assert r is not None
    assert r.url == "https://websites-search.api.carscommerce.inc/api/v1/listings/77777/search"
    assert r.pagination == PAGINATION_CARSCOMMERCE
    # Prefer the key shipped in the dealer's own HTML.
    assert r.auth_headers["x-api-key"] == "ABCDEF0123456789ABCD"
    # Full-lot body: the Used/CPO facet restriction is dropped (new included) and
    # perPage is raised so the bounded page walk reaches large accounts.
    body = json.loads(r.post_template)
    assert "facetFilters" not in body
    assert body["perPage"] == 100


def test_synthesize_carscommerce_falls_back_to_shared_key(_inject_refs, _offline_carscommerce_scope):
    html = _CARSCOMMERCE_HTML.replace('"apiKey":"ABCDEF0123456789ABCD"', '"other":"1"')
    r = recipe_synth.synthesize_recipe("d-com", "https://d.com", html, "carscommerce")
    assert r is not None
    assert r.auth_headers["x-api-key"] == "SHAREDKEYshared0000"  # reference recipe's shared key


# ── synthesize_recipe: DealerOn cosmos ────────────────────────────────────────


def test_synthesize_cosmos_from_srp_in_html(monkeypatch):
    # itemlist blob already present in the passed html → no extra fetch needed.
    # The synth still probes the used/new SRP paths; keep that off the network.
    monkeypatch.setattr(dealeron_cosmos, "fetch_dealer_html", lambda url, **k: None)
    r = recipe_synth.synthesize_recipe(
        "bellroadtoyota-com", "https://www.bellroadtoyota.com", _COSMOS_SRP_HTML, "dealer_on_cosmos"
    )
    assert r is not None
    assert r.url == (
        "https://www.bellroadtoyota.com/api/vhcliaa/vehicle-pages/cosmos/srp/vehicles/25003/2483381"
    )
    assert r.method == "GET"
    assert r.provider_hint == "dealer_on_cosmos"
    assert r.post_template is None


def test_synthesize_cosmos_fetches_srp_for_pagecfg(monkeypatch):
    # Homepage has the account but not the pagecfg; synth must fetch an SRP page.
    monkeypatch.setattr(dealeron_cosmos, "fetch_dealer_html", lambda url, **k: _COSMOS_SRP_HTML)
    r = recipe_synth.synthesize_recipe(
        "bellroadtoyota-com", "https://www.bellroadtoyota.com", _COSMOS_HOME_HTML, "dealer_on_cosmos"
    )
    assert r is not None
    assert r.url.endswith("/cosmos/srp/vehicles/25003/2483381")


def test_synthesize_cosmos_needs_pagecfg(monkeypatch):
    # No itemlist config anywhere → cannot synthesize.
    monkeypatch.setattr(dealeron_cosmos, "fetch_dealer_html", lambda url, **k: None)
    assert recipe_synth.synthesize_recipe(
        "x-com", "https://x.com", _COSMOS_HOME_HTML, "dealer_on_cosmos"
    ) is None


# ── synthesize_recipe: Typesense ──────────────────────────────────────────────


def test_synthesize_typesense(_inject_refs):
    r = recipe_synth.synthesize_recipe(
        "freewayhonda-com", "https://www.freewayhonda.com", _TYPESENSE_HTML, "typesense"
    )
    assert r is not None
    assert r.method == "POST"
    assert r.provider_hint == "typesense"
    assert r.pagination == "typesense_page"
    # host + shared key taken from the page; collection swapped to this dealer's.
    assert r.url == (
        "https://hjnrb3s21408ezpfp.a1.typesense.net/multi_search"
        "?x-typesense-api-key=eQUa8iq30l8Tu908Drz9WKqar6tCJGd4"
    )
    body = json.loads(r.post_template)
    search = body["searches"][0]
    assert search["collection"] == "vehicles-HON208436"
    assert "vehicles-TOY04247" not in r.post_template
    # Full-lot body: the condition:Used filter is dropped (new included) and
    # per_page is raised to the Typesense max so the page walk reaches large lots.
    assert "filter_by" not in search
    assert search["per_page"] == 250


def test_synthesize_typesense_needs_collection(_inject_refs, monkeypatch):
    # No collection in the homepage and the SRP-config fallback finds nothing.
    monkeypatch.setattr(typesense, "fetch_dealer_html", lambda url, **k: None)
    html = '<script>var __tsHost="x.a1.typesense.net";var __tsApiKey="k00000000000000000";</script>'
    assert recipe_synth.synthesize_recipe("x-com", "https://x.com", html, "typesense") is None


# ── synthesize_recipe: Team Velocity (same-origin JSON feed) ───────────────────


def test_synthesize_team_velocity_feed(monkeypatch):
    # synth probes page 1 of each feed to confirm existence + read totalVehicles.
    totals = {"used": 196, "new": 377}

    def fake_feed(url):
        kind = "new" if "-new.json" in url else "used"
        return {"totalVehicles": totals[kind], "totalPages": 4, "vehicles": [{"vin": f"V-{kind}"}]}

    monkeypatch.setattr(team_velocity, "_cosmos_get_json", fake_feed)
    # Full lot => TWO recipes: used + new (CPO ⊆ used, no combined feed exists).
    recipes = recipe_synth.synthesize_recipes(
        "righthonda-com", "https://www.righthonda.com", _TEAM_VELOCITY_HTML, "team_velocity"
    )
    assert len(recipes) == 2
    by_url = {r.url: r for r in recipes}
    assert set(by_url) == {
        "https://www.righthonda.com/inventory-used.json",
        "https://www.righthonda.com/inventory-new.json",
    }
    for r in recipes:
        assert r.method == "GET"
        # Dedicated TV parser owns these rows (feed field mapping + VDP completion).
        assert r.provider_hint == "team_velocity"
        assert r.pagination == "page_query"  # replay/delta walk every page
    assert by_url["https://www.righthonda.com/inventory-used.json"].total_count == 196
    assert by_url["https://www.righthonda.com/inventory-new.json"].total_count == 377
    # singular wrapper still returns the primary (used) recipe.
    r0 = recipe_synth.synthesize_recipe(
        "righthonda-com", "https://www.righthonda.com", _TEAM_VELOCITY_HTML, "team_velocity"
    )
    assert r0.url == "https://www.righthonda.com/inventory-used.json"


def test_synthesize_team_velocity_missing_feed_returns_none(monkeypatch):
    monkeypatch.setattr(team_velocity, "_cosmos_get_json", lambda url: None)
    assert recipe_synth.synthesize_recipe(
        "x-com", "https://x.com", _TEAM_VELOCITY_HTML, "team_velocity"
    ) is None


# ── synthesize_recipe: unsynthesizable platforms ──────────────────────────────


def test_sister_tv_recognized_but_not_synthesizable():
    # sister.tv is detected but has no HTML-extractable library_id on live
    # dealers (they migrated to CarsCommerce), so it stays unsynthesizable.
    assert recipe_synth.is_synthesizable("sister_tv") is False
    assert recipe_synth.synthesize_recipe("x-com", "https://x.com", _SISTER_TV_HTML, "sister_tv") is None


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


# A thin body or a challenge page no longer ends the attempt -- it escalates to a
# TLS-impersonating retry, because that is exactly the shape a Cloudflare fingerprint
# rejection takes. These two pin the urllib verdict itself, with the escalation stubbed
# out; test_fetch_escalates_* below pin the escalation.
def test_fetch_rejects_thin_body(monkeypatch):
    monkeypatch.setattr(synth_http, "open_url", lambda *a, **k: _FakeResp(b"<html>tiny</html>"))
    monkeypatch.setattr(synth_http, "_fetch_impersonated", lambda *a, **k: None)
    assert recipe_synth.fetch_dealer_html("https://x.com") is None


def test_fetch_rejects_challenge_page(monkeypatch):
    body = (b"<html><body>Just a moment... Checking your browser before accessing. "
            + b"cf-challenge " * 200 + b"</body></html>")
    monkeypatch.setattr(synth_http, "open_url", lambda *a, **k: _FakeResp(body))
    monkeypatch.setattr(synth_http, "_fetch_impersonated", lambda *a, **k: None)
    assert recipe_synth.fetch_dealer_html("https://x.com") is None


def test_fetch_escalates_challenge_to_impersonation(monkeypatch):
    """The Cloudflare case: urllib sees a challenge, impersonation gets the real page."""
    body = (b"<html><body>Just a moment... Checking your browser before accessing. "
            + b"cf-challenge " * 200 + b"</body></html>")
    monkeypatch.setattr(synth_http, "open_url", lambda *a, **k: _FakeResp(body))
    monkeypatch.setattr(synth_http, "_fetch_impersonated", lambda *a, **k: "<html>real inventory</html>")
    assert recipe_synth.fetch_dealer_html("https://x.com") == "<html>real inventory</html>"


def test_fetch_escalates_http_error_to_impersonation(monkeypatch):
    """A 403 is the fingerprint rejection this whole path exists for."""
    import urllib.error

    def _raise(*a, **k):
        raise urllib.error.HTTPError("https://x.com", 403, "Forbidden", {}, None)

    monkeypatch.setattr(synth_http, "open_url", _raise)
    monkeypatch.setattr(synth_http, "_fetch_impersonated", lambda *a, **k: "<html>cleared</html>")
    assert recipe_synth.fetch_dealer_html("https://x.com") == "<html>cleared</html>"


def test_fetch_does_not_escalate_when_urllib_succeeds(monkeypatch):
    """Impersonation costs a request; the common path must not pay it."""
    body = b"<html><body>" + b"real dealership content " * 200 + b"</body></html>"
    monkeypatch.setattr(synth_http, "open_url", lambda *a, **k: _FakeResp(body))

    def _boom(*a, **k):
        raise AssertionError("must not escalate when the plain fetch worked")

    monkeypatch.setattr(synth_http, "_fetch_impersonated", _boom)
    assert "real dealership content" in (recipe_synth.fetch_dealer_html("https://x.com") or "")


def test_fetch_accepts_real_html(monkeypatch):
    body = b"<html><body>" + b"real dealership content " * 200 + b"</body></html>"
    monkeypatch.setattr(synth_http, "open_url", lambda *a, **k: _FakeResp(body))
    html = recipe_synth.fetch_dealer_html("https://x.com")
    assert html and "real dealership content" in html


def test_fetch_rejects_non_http_url():
    assert recipe_synth.fetch_dealer_html("ftp://x.com") is None


# ── New platform fixtures ─────────────────────────────────────────────────────

# Motive: RSC state carries the env creds + per-dealer dealer.id (quotes escaped).
_MOTIVE_HTML = (
    '<html><body><img src="https://images.app.ridemotive.com/abc"><script>'
    'self.__next_f.push([1,"x\\"env\\":{\\"ALGOLIA_APP_ID\\":\\"G58LKO3ETJ\\",'
    '\\"ALGOLIA_API_KEY\\":\\"cc3dce06acb2d9fc715bc10c9a624d80\\",'
    '\\"ALGOLIA_INVENTORY_INDEX\\":\\"production-inventory-\\"},'
    '\\"dealer\\":{\\"id\\":1192,\\"name\\":\\"North Park Lexus\\"}y"])'
    '</script></body></html>'
)

# dealer_alchemist: dv-framework theme, TypesenseInstantSearchAdapter config.
_DEALER_ALCHEMIST_HTML = (
    '<html><head><script src="https://bucket.dealervenom.com/dv-framework.js"></script>'
    '</head><body>dealeralchemist<script>'
    'var a = new TypesenseInstantSearchAdapter({ server: {'
    ' apiKey: "eQUa8iq30l8Tu908Drz9WKqar6tCJGd4",'
    " nodes: [{ host: 'hjnrb3s21408ezpfp.a1.typesense.net', port: 443, protocol: 'https' }] } });"
    ' var indexName = "vehicles-TOY42087";'
    '</script></body></html>'
)

_OVERFUEL_HTML = '<html><body><meta name="generator" content="Overfuel"><p>overfuel</p></body></html>'

# Overfuel SSR page: __NEXT_DATA__ with inventory.results + meta.total.
_OVERFUEL_SRP = (
    '<html><body><script id="__NEXT_DATA__" type="application/json">'
    '{"props":{"pageProps":{"inventory":{"meta":{"total":2},"results":['
    '{"vin":"1FTFW1E50NFA00001","year":2022,"make":"Ford","model":"F-150",'
    '"price":55000,"stocknumber":"A1","url":"/inventory/1FTFW1E50NFA00001"},'
    '{"vin":"1FTFW1E50NFA00002","year":2023,"make":"Ford","model":"F-150",'
    '"price":57000,"stocknumber":"A2","url":"/inventory/1FTFW1E50NFA00002"}]}}}}'
    '</script></body></html>'
)

_NABTHAT_HTML = '<html><body>powered by nabthat.com<p>hi</p></body></html>'

# nabthat SRP: schema.org Vehicle JSON-LD (same shape as Dealer eProcess).
_NABTHAT_SRP = (
    '<html><body>nabthat.com'
    '<script type="application/ld+json">{"@type":"Vehicle",'
    '"vehicleIdentificationNumber":"5TDKZ3DC0LS000001","vehicleModelDate":"2020",'
    '"brand":{"name":"Toyota"},"model":"Sienna","offers":{"price":0,"sku":"N1"}}</script>'
    '<script type="application/ld+json">{"@type":"Vehicle",'
    '"vehicleIdentificationNumber":"5TDKZ3DC0LS000002","vehicleModelDate":"2021",'
    '"brand":{"name":"Toyota"},"model":"Sienna","offers":{"price":0,"sku":"N2"}}</script>'
    '</body></html>'
)

_CHAPMAN_HTML = (
    '<html><body><img src="https://assets.chapmanchoice.com/img/dealers/cau.webp">'
    'chapmanapps.com</body></html>'
)

_CHAPMAN_ROWS = [
    {"vin": "1FA6P8TH0N5000001", "year": 2022, "make": "Ford", "model": "Mustang",
     "type": "N", "stockNumber": "C1", "pricing": {"msrp": 40000, "markupsTotal": 0,
     "discountsTotal": 1500, "rebatesAppliedTotal": 500}, "imageUrls": ["https://photos.chapmanchoice.com/1.jpg"]},
    {"vin": "1FA6P8TH0N5000002", "year": 2023, "make": "Ford", "model": "Bronco",
     "type": "U", "stockNumber": "C2", "pricing": {"msrp": 0}},
]

_JAZEL_HTML = (
    '<html><body><script>window.jzla5p={accountId:"6,7,8"};</script>'
    '<a href="https://jazelc.com">x</a>936 vehicles</body></html>'
)

# Jazel SSR SRP: per-card jzlSetVehicleInfoContext('VIN', {...}) calls.
_JAZEL_SRP = (
    '<html><body>2 vehicles'
    "<script>window.jzlSetVehicleInfoContext('1FTFW1E50NFB00001', "
    '{"vin":"1FTFW1E50NFB00001","year":2022,"make":"Ford","model":"F-150",'
    '"displayPrice":55000,"newOrUsed":"New","vdpLink":"/new/f150/1.htm",'
    '"image":"https://media-cdn-tango.jazelc.com/media/1"});</script>'
    "<script>window.jzlSetVehicleInfoContext('1FTFW1E50NFB00002', "
    '{"vin":"1FTFW1E50NFB00002","year":2023,"make":"Ford","model":"F-150",'
    '"displayPrice":57000,"newOrUsed":"Used","vdpLink":"/used/f150/2.htm",'
    '"image":"https://media-cdn-tango.jazelc.com/media/2"});</script>'
    '</body></html>'
)


# ── fingerprint: new platforms ────────────────────────────────────────────────


def test_fingerprint_motive():
    assert recipe_synth.fingerprint_platform(_MOTIVE_HTML, "https://www.northparklexus.com") == "motive_ridemotive"


def test_fingerprint_overfuel():
    assert recipe_synth.fingerprint_platform(_OVERFUEL_HTML, "https://x.com") == "overfuel"


def test_fingerprint_nabthat():
    assert recipe_synth.fingerprint_platform(_NABTHAT_HTML, "https://www.mossytoyota.com") == "nabthat"


def test_fingerprint_chapman():
    assert recipe_synth.fingerprint_platform(_CHAPMAN_HTML, "https://www.chapmanfordaz.com") == "chapman"


def test_fingerprint_jazel():
    assert recipe_synth.fingerprint_platform(_JAZEL_HTML, "https://www.5starford.com") == "jazel"


def test_dealer_alchemist_fingerprints_as_typesense():
    # dealer_alchemist shares the Typesense cluster, so it fingerprints as typesense.
    assert recipe_synth.fingerprint_platform(_DEALER_ALCHEMIST_HTML, "https://www.donmcgilltoyota.com") == "typesense"


# ── synthesize: Motive (Algolia) ──────────────────────────────────────────────


def test_synthesize_motive():
    from backend.scanner.recipes import PAGINATION_ALGOLIA

    r = recipe_synth.synthesize_recipe(
        "northparklexus-com", "https://www.northparklexus.com", _MOTIVE_HTML, "motive_ridemotive"
    )
    assert r is not None
    assert r.method == "POST"
    assert r.provider_hint == "motive_ridemotive"
    assert r.pagination == PAGINATION_ALGOLIA
    assert r.url == (
        "https://G58LKO3ETJ-dsn.algolia.net/1/indexes/production-inventory-global_price_desc/query"
    )
    assert r.auth_headers["X-Algolia-Application-Id"] == "G58LKO3ETJ"
    assert r.auth_headers["X-Algolia-API-Key"] == "cc3dce06acb2d9fc715bc10c9a624d80"
    body = json.loads(r.post_template)
    assert body["filters"] == 'is_active:true AND dealer_ids:"1192"'
    assert body["hitsPerPage"] == 1000
    assert body["page"] == 0


def test_synthesize_motive_needs_dealer_id():
    html = _MOTIVE_HTML.replace('\\"id\\":1192', '\\"nope\\":1')
    assert recipe_synth.synthesize_recipe("x-com", "https://x.com", html, "motive_ridemotive") is None


# ── synthesize: dealer_alchemist via the typesense template ───────────────────


def test_synthesize_dealer_alchemist_via_typesense():
    # No reference recipe injected: host/key/collection all come from the page's
    # TypesenseInstantSearchAdapter config via the extended _TS_* regexes.
    r = recipe_synth.synthesize_recipe(
        "donmcgilltoyota-com", "https://www.donmcgilltoyota.com", _DEALER_ALCHEMIST_HTML
    )
    assert r is not None
    assert r.provider_hint == "typesense"
    assert r.url == (
        "https://hjnrb3s21408ezpfp.a1.typesense.net/multi_search"
        "?x-typesense-api-key=eQUa8iq30l8Tu908Drz9WKqar6tCJGd4"
    )
    body = json.loads(r.post_template)
    # Falls back to a fresh single-search body when no reference recipe exists;
    # either way the collection must be this dealer's.
    dumped = r.post_template
    assert "vehicles-TOY42087" in dumped


# ── synthesize: Overfuel (HTML page-walk) ─────────────────────────────────────


def test_synthesize_overfuel(monkeypatch):
    from backend.scanner.recipes import PAGINATION_HTML_PAGE

    monkeypatch.setattr(overfuel, "_dep_fetch_html", lambda url: _OVERFUEL_SRP)
    r = recipe_synth.synthesize_recipe(
        "autoboutiqueflorida-com", "https://www.autoboutiqueflorida.com", _OVERFUEL_HTML, "overfuel"
    )
    assert r is not None
    assert r.method == "GET"
    assert r.provider_hint == "overfuel"
    assert r.pagination == PAGINATION_HTML_PAGE
    assert r.url == "https://www.autoboutiqueflorida.com/inventory"
    assert r.total_count == 2


def test_synthesize_overfuel_no_vehicles(monkeypatch):
    monkeypatch.setattr(overfuel, "_dep_fetch_html", lambda url: "<html><body>empty</body></html>")
    assert recipe_synth.synthesize_recipe(
        "x-com", "https://x.com", _OVERFUEL_HTML, "overfuel"
    ) is None


# ── synthesize: nabthat (HTML page-walk, dealer_eprocess parser) ──────────────


def test_synthesize_nabthat(monkeypatch):
    from backend.scanner.recipes import PAGINATION_HTML_PAGE

    def fake(url):
        # used path has vehicles; new path is empty -> single used recipe.
        return _NABTHAT_SRP if url.endswith("/inventory/used") else "<html>nabthat.com</html>"

    monkeypatch.setattr(nabthat, "_dep_fetch_html", fake)
    recipes = recipe_synth.synthesize_recipes(
        "mossytoyota-com", "https://www.mossytoyota.com", _NABTHAT_HTML, "nabthat"
    )
    assert len(recipes) == 1
    r = recipes[0]
    assert r.url == "https://www.mossytoyota.com/inventory/used"
    assert r.pagination == PAGINATION_HTML_PAGE
    assert r.provider_hint == "dealer_eprocess"


# ── synthesize: Chapman (flat JSON arrays, new + used) ────────────────────────


def test_synthesize_chapman(monkeypatch):
    from backend.scanner.recipes import PAGINATION_NONE

    monkeypatch.setattr(chapman, "_cosmos_get_json", lambda url: list(_CHAPMAN_ROWS))
    recipes = recipe_synth.synthesize_recipes(
        "chapmanfordaz-com", "https://www.chapmanfordaz.com", _CHAPMAN_HTML, "chapman"
    )
    assert len(recipes) == 2
    urls = {r.url for r in recipes}
    assert urls == {
        "https://apiv2.chapmanapps.com/inventory/cau/new",
        "https://apiv2.chapmanapps.com/inventory/cau/used",
    }
    for r in recipes:
        assert r.method == "GET"
        assert r.provider_hint == "chapman"
        assert r.pagination == PAGINATION_NONE
        assert r.total_count == 2


def test_synthesize_chapman_needs_arkona(monkeypatch):
    monkeypatch.setattr(chapman, "_cosmos_get_json", lambda url: list(_CHAPMAN_ROWS))
    html = "<html><body>chapmanapps.com but no arkona asset</body></html>"
    assert recipe_synth.synthesize_recipes("x-com", "https://x.com", html, "chapman") == []


# ── synthesize: Jazel (SSR path-walk) ─────────────────────────────────────────


def test_synthesize_jazel(monkeypatch):
    from backend.scanner.recipes import PAGINATION_JAZEL_SRP

    monkeypatch.setattr(jazel, "_dep_fetch_html", lambda url: _JAZEL_SRP)
    r = recipe_synth.synthesize_recipe(
        "5starford-com", "https://www.5starford.com", _JAZEL_HTML, "jazel"
    )
    assert r is not None
    assert r.method == "GET"
    assert r.provider_hint == "jazel"
    assert r.pagination == PAGINATION_JAZEL_SRP
    assert r.url == "https://www.5starford.com/inventory/all-vehicles/"


def test_jazel_url_for_page_path_walk():
    from backend.scanner.recipes import PAGINATION_JAZEL_SRP, EndpointRecipe, _url_for_page

    r = EndpointRecipe(
        dealer_id="d", url="https://www.5starford.com/inventory/all-vehicles/",
        method="GET", content_type="text/html", post_template=None,
        pagination=PAGINATION_JAZEL_SRP,
    )
    assert _url_for_page(r, 0) == "https://www.5starford.com/inventory/all-vehicles/"
    assert _url_for_page(r, 1) == "https://www.5starford.com/inventory/all-vehicles/srp-page-2/"
    assert _url_for_page(r, 2) == "https://www.5starford.com/inventory/all-vehicles/srp-page-3/"


# ── Universal browser-free fallback: generic schema.org Vehicle JSON-LD ────────

# A bespoke/luxury standalone SRP: no API-platform fingerprint, but it
# server-renders schema.org Vehicle JSON-LD with real VINs.
_CUSTOM_SRP_HTML = """
<html><head><title>Bespoke Motors</title>
<script type="application/ld+json">
{"@type":"Vehicle","vehicleIdentificationNumber":"1HGCM82633A004352",
 "brand":"Honda","model":"Accord","offers":{"@type":"Offer","price":"28995"}}
</script>
<script type="application/ld+json">
{"@type":"Car","vin":"5XYKTCA69FG566472","brand":"Kia","model":"Sorento",
 "offers":{"@type":"Offer","price":"14500"}}
</script>
</head><body>custom standalone site, no known platform markers</body></html>
"""

# Homepage with no vehicle JSON-LD — the fallback must probe SRP paths.
_CUSTOM_HOME_HTML = "<html><body>" + "welcome to bespoke motors " * 200 + "</body></html>"


def test_custom_srp_does_not_fingerprint_any_platform():
    # Precondition for the fallback: no API template claims this page.
    assert recipe_synth.fingerprint_platform(_CUSTOM_SRP_HTML, "https://bespokemotors.com") is None


def test_detect_html_harvest_from_supplied_html():
    n, src = recipe_synth.detect_html_harvest(
        "https://bespokemotors.com", _CUSTOM_SRP_HTML, min_vins=2
    )
    assert n == 2
    assert src == "https://bespokemotors.com"


def test_detect_html_harvest_probes_srp_when_home_bare(monkeypatch):
    # Homepage has no vehicles; the first probed SRP path yields them.
    def fake_fetch(url, **kwargs):
        return _CUSTOM_SRP_HTML if url.endswith("/inventory") else None

    monkeypatch.setattr(synth_registry, "fetch_dealer_html", fake_fetch)
    n, src = recipe_synth.detect_html_harvest(
        "https://bespokemotors.com", _CUSTOM_HOME_HTML, min_vins=2
    )
    assert n == 2
    assert src == "https://bespokemotors.com/inventory"


def test_detect_html_harvest_no_vehicles_returns_zero(monkeypatch):
    monkeypatch.setattr(synth_registry, "fetch_dealer_html", lambda *a, **k: None)
    n, src = recipe_synth.detect_html_harvest(
        "https://bespokemotors.com", _CUSTOM_HOME_HTML, min_vins=2
    )
    assert n == 0
    assert src is None


def test_detect_html_harvest_no_probe_when_disabled():
    n, src = recipe_synth.detect_html_harvest(
        "https://bespokemotors.com", _CUSTOM_HOME_HTML, min_vins=2, probe_srp=False
    )
    assert n == 0
    assert src is None


def test_pace_is_noop_by_default():
    # With SCANNER_SYNTH_FETCH_DELAY unset (module default 0) pacing must not sleep.
    import time as _t

    t0 = _t.monotonic()
    recipe_synth._pace()
    assert _t.monotonic() - t0 < 0.5


def test_synthesize_cosmos_yields_one_recipe_per_srp_section(monkeypatch):
    """Cherokee County Toyota 2026-09-23: used pageId 769890 (106 cars) and new
    pageId 769883 (282 cars); a used-only recipe replayed a third of the lot."""
    used_html = _COSMOS_SRP_HTML
    new_html = _COSMOS_SRP_HTML.replace("2483381", "2483399")

    def fake_fetch(url, **k):
        return new_html if "new" in url else used_html

    monkeypatch.setattr(dealeron_cosmos, "fetch_dealer_html", fake_fetch)
    recipes = recipe_synth.synthesize_recipes(
        "bellroadtoyota-com", "https://www.bellroadtoyota.com", _COSMOS_HOME_HTML, "dealer_on_cosmos"
    )
    urls = sorted(r.url.rsplit("/", 1)[-1] for r in recipes)
    assert urls == ["2483381", "2483399"]
    assert all(r.pagination == "cosmos_pt" for r in recipes)


# ── Cloudflare passive beacon vs real challenge (Honda of El Cajon, 2026-09-23) ─
#
# Cloudflare injects ``/cdn-cgi/challenge-platform/scripts/...`` into pages it
# served NORMALLY as a passive bot-management beacon. Counting it as a challenge
# made a 459KB genuine Dealer eProcess homepage read as unreachable.

_BEACON = "<script>s.src='/cdn-cgi/challenge-platform/scripts/precursor/main.js'</script>"


def test_beacon_alone_in_a_real_page_is_not_a_challenge():
    real = "<html><head><title>Honda Dealer El Cajon</title>" + _BEACON + "</head><body>" \
        + "dealereprocess real inventory " * 2000 + "</body></html>"
    assert recipe_synth.looks_like_challenge(real) is False


def test_beacon_in_thin_shell_is_a_challenge():
    shell = "<html><head>" + _BEACON + "</head><body>x</body></html>"
    assert recipe_synth.looks_like_challenge(shell) is True


def test_strong_marker_is_a_challenge_regardless_of_size():
    big = "<html><body>Just a moment..." + "padding " * 5000 + "</body></html>"
    assert recipe_synth.looks_like_challenge(big) is True


def test_fetch_accepts_real_page_carrying_beacon(monkeypatch):
    body = ("<html><head>" + _BEACON + "</head><body>"
            + "dealereprocess real inventory " * 2000 + "</body></html>").encode()
    monkeypatch.setattr(synth_http, "open_url", lambda *a, **k: _FakeResp(body))

    def _boom(*a, **k):
        raise AssertionError("beacon on a real page must not escalate")

    monkeypatch.setattr(synth_http, "_fetch_impersonated", _boom)
    html = recipe_synth.fetch_dealer_html("https://www.hondaofelcajon.com")
    assert html and "real inventory" in html


# ── DEP SRP fetch: same-site Referer + TLS-impersonation escalation ───────────


class _FakeRedirResp(_FakeResp):
    def __init__(self, body: bytes, final_url: str):
        super().__init__(body)
        self._final = final_url

    def geturl(self) -> str:
        return self._final


def test_dep_fetch_sends_same_site_referer(monkeypatch):
    seen = {}

    def fake_open(req, timeout=0):
        seen["headers"] = dict(req.header_items())
        return _FakeRedirResp(b"<html>ok</html>", req.full_url)

    monkeypatch.setattr(synth_http, "open_url", fake_open)
    html, final = recipe_synth._dep_fetch_page("https://www.hondaofelcajon.com/used-inventory/")
    assert html == "<html>ok</html>"
    assert final == "https://www.hondaofelcajon.com/used-inventory/"
    hdrs = {k.lower(): v for k, v in seen["headers"].items()}
    assert hdrs["referer"] == "https://www.hondaofelcajon.com/"
    assert hdrs["sec-fetch-site"] == "same-origin"


def test_dep_fetch_returns_redirect_target(monkeypatch):
    """/used-inventory/ 302s to /search/used/?tp=used; the walk must use THAT URL."""
    monkeypatch.setattr(
        synth_http, "open_url",
        lambda req, timeout=0: _FakeRedirResp(b"<html>srp</html>", "https://www.hondaofelcajon.com/search/used/?tp=used"),
    )
    html, final = recipe_synth._dep_fetch_page("https://www.hondaofelcajon.com/used-inventory/")
    assert html == "<html>srp</html>"
    assert final == "https://www.hondaofelcajon.com/search/used/?tp=used"


def test_dep_fetch_escalates_403_to_impersonation_with_referer(monkeypatch):
    import urllib.error

    def _raise(*a, **k):
        raise urllib.error.HTTPError("https://x.com", 403, "Forbidden", {}, None)

    captured = {}

    def fake_imp(url, *, timeout=25.0, headers=None, min_bytes=2000):
        captured["url"] = url
        captured["headers"] = headers or {}
        captured["min_bytes"] = min_bytes
        return "<html>cleared</html>"

    monkeypatch.setattr(synth_http, "open_url", _raise)
    monkeypatch.setattr(synth_http, "_fetch_impersonated", fake_imp)
    html, _ = recipe_synth._dep_fetch_page("https://www.hondaofelcajon.com/search/used/?tp=used")
    assert html == "<html>cleared</html>"
    assert captured["headers"]["Referer"] == "https://www.hondaofelcajon.com/"
    assert captured["min_bytes"] == 0  # a short past-the-last-result page is valid


def test_dep_fetch_does_not_escalate_404(monkeypatch):
    import urllib.error

    def _raise(*a, **k):
        raise urllib.error.HTTPError("https://x.com", 404, "Not Found", {}, None)

    def _boom(*a, **k):
        raise AssertionError("404 is not a fingerprint rejection")

    monkeypatch.setattr(synth_http, "open_url", _raise)
    monkeypatch.setattr(synth_http, "_fetch_impersonated", _boom)
    assert recipe_synth._dep_fetch_html("https://x.com/new-inventory/") is None


def test_dep_fetch_escalates_challenge_shell(monkeypatch):
    shell = b"<html><body>Just a moment... __cf_chl</body></html>"
    monkeypatch.setattr(synth_http, "open_url", lambda req, timeout=0: _FakeRedirResp(shell, req.full_url))
    monkeypatch.setattr(synth_http, "_fetch_impersonated", lambda url, **k: "<html>real</html>")
    assert recipe_synth._dep_fetch_html("https://x.com/search/used/?tp=used") == "<html>real</html>"


# ── synthesize: Dealer eProcess (redirected SRP + ct= page size) ──────────────


def _dep_srp(vins: list[str], total: int, page_sizes=(12, 24, 36, 48)) -> str:
    cards = "".join(
        '<script type="application/ld+json">{"@type":"Vehicle",'
        f'"vehicleIdentificationNumber":"{v}","vehicleModelDate":"2024",'
        '"brand":{"name":"Honda"},"model":"Civic","offers":{"price":25000,"sku":"S' + v[-3:] + '"}}</script>'
        for v in vins
    )
    # Live markup (Honda of El Cajon): the class also appears in inline CSS
    # before the control, and option values are SRP URLs carrying ct=N.
    css = "<style>.results_per_page_controls__select{width:3.25em}</style>"
    select = (
        '<select name="results_per_page_controls__select" class="results_per_page_controls__select thm">'
        + "".join(f'<option value="/search/used/?ct={n}&tp=used">{n}</option>' for n in page_sizes)
        + '<option value="/search/used/?ct=all&tp=used">All</option></select>'
    )
    return (f'<html><body>dealereprocess{css}<div data-vehicle_count="{total}"></div>'
            f'{select}{cards}</body></html>')


def _vins(prefix: str, n: int) -> list[str]:
    return [f"{prefix}{i:03d}" for i in range(n)]


def test_dep_page_size_reads_largest_numeric_option():
    assert recipe_synth._dep_page_size(_dep_srp([], 0)) == 48
    assert recipe_synth._dep_page_size(_dep_srp([], 0, page_sizes=(12, 24))) == 24
    assert recipe_synth._dep_page_size("<html>no control</html>") is None
    # Bare numeric option values are accepted too; ct=all never counts.
    bare = '<select class="results_per_page_controls__select"><option value="12">12</option>' \
           '<option value="24">24</option><option value="all">All</option></select>'
    assert recipe_synth._dep_page_size(bare) == 24
    css_only = "<style>.results_per_page_controls__select{}</style><select><option value='9'>9</option></select>"
    assert recipe_synth._dep_page_size(css_only) is None


def test_synthesize_dep_uses_redirect_target_and_honoured_page_size(monkeypatch):
    from backend.scanner.recipes import PAGINATION_DEP_SRP

    origin = "https://www.hondaofelcajon.com"
    calls: list[str] = []

    def fake(url):
        calls.append(url)
        if url.endswith("/used-inventory/"):
            return _dep_srp(_vins("2HGFC2F5XNH", 12), 56), origin + "/search/used/?tp=used"
        if url == origin + "/search/used/?tp=used&ct=48":
            return _dep_srp(_vins("2HGFC2F5XNH", 48), 56), url
        if url.endswith("/new-inventory/"):
            return _dep_srp(_vins("1HGCY1F3XRA", 12), 343), origin + "/search/new-honda/?tp=new"
        if url == origin + "/search/new-honda/?tp=new&ct=48":
            return _dep_srp(_vins("1HGCY1F3XRA", 48), 343), url
        raise AssertionError(f"unexpected fetch {url}")

    monkeypatch.setattr(dealer_eprocess, "_dep_fetch_page", fake)
    recipes = recipe_synth.synthesize_recipes("hondaofelcajon-com", origin, "<html>dealereprocess</html>", "dealer_eprocess")
    assert [r.url for r in recipes] == [
        origin + "/search/used/?tp=used&ct=48",
        origin + "/search/new-honda/?tp=new&ct=48",
    ]
    assert all(r.pagination == PAGINATION_DEP_SRP for r in recipes)
    assert [r.total_count for r in recipes] == [56, 343]
    assert len(calls) == 4


def test_synthesize_dep_falls_back_when_page_size_not_honoured(monkeypatch):
    origin = "https://www.example-dep.com"

    def fake(url):
        # Server ignores ct=: still 12 VINs.
        if url.endswith("/used-inventory/"):
            return _dep_srp(_vins("2HGFC2F5XNH", 12), 56), origin + "/used-inventory/"
        if "ct=48" in url:
            return _dep_srp(_vins("2HGFC2F5XNH", 12), 56), url
        return None, url

    monkeypatch.setattr(dealer_eprocess, "_dep_fetch_page", fake)
    recipes = recipe_synth.synthesize_recipes("x-com", origin, "<html>dealereprocess</html>", "dealer_eprocess")
    assert [r.url for r in recipes] == [origin + "/used-inventory/"]


def test_synthesize_dep_skips_page_size_probe_when_page1_is_whole_lot(monkeypatch):
    origin = "https://www.example-dep.com"
    calls: list[str] = []

    def fake(url):
        calls.append(url)
        if url.endswith("/used-inventory/"):
            return _dep_srp(_vins("2HGFC2F5XNH", 5), 5), url
        return None, url

    monkeypatch.setattr(dealer_eprocess, "_dep_fetch_page", fake)
    recipes = recipe_synth.synthesize_recipes("x-com", origin, "<html>dealereprocess</html>", "dealer_eprocess")
    assert [r.url for r in recipes] == [origin + "/used-inventory/"]
    assert not any("ct=" in c for c in calls)


def test_validate_dep_keeps_recipe_query_and_sets_p(monkeypatch):
    from backend.scanner.recipes import PAGINATION_DEP_SRP

    origin = "https://www.hondaofelcajon.com"
    pages = {
        1: _dep_srp(_vins("2HGFC2F5XNH", 48), 56),
        2: _dep_srp(_vins("2HGFC2F5XNH", 8), 56),  # disjoint from p=1 by construction below
    }
    pages[2] = _dep_srp([f"2HGFC2F5XNH9{i:02d}" for i in range(8)], 56)
    calls: list[str] = []

    def fake(url):
        calls.append(url)
        assert "tp=used" in url and "ct=48" in url, url
        m = __import__("re").search(r"[?&]p=(\d+)", url)
        return pages.get(int(m.group(1)), "<html>dealereprocess</html>")

    monkeypatch.setattr(synth_validate, "_dep_fetch_html", fake)
    recipe = EndpointRecipe(
        dealer_id="hondaofelcajon-com", url=origin + "/search/used/?tp=used&ct=48",
        method="GET", content_type="text/html", post_template=None, auth_headers={},
        pagination=PAGINATION_DEP_SRP, total_count=56, provider_hint="dealer_eprocess",
    )
    n = recipe_synth._validate_dep(recipe, origin, "hondaofelcajon-com", "Honda of El Cajon", max_pages=10)
    assert n == 56
    assert calls[0].endswith("?tp=used&ct=48&p=1")
    assert len(calls) == 2  # stops once total_count is reached
