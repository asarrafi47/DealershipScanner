"""Tests for endpoint replay recipes (store + promotion + pagination inference)."""
from __future__ import annotations

import pytest

from backend.scanner.network_observer import CapturedEndpoint
from backend.scanner.recipes import (
    PAGINATION_CARSCOMMERCE,
    PAGINATION_DEALER_COM,
    PAGINATION_NONE,
    PAGINATION_TYPESENSE,
    EndpointRecipe,
    infer_pagination,
    load_recipes,
    mark_stale,
    promote_from_ledger,
    save_recipes,
)


@pytest.fixture(autouse=True)
def _tmp_recipes_dir(tmp_path, monkeypatch):
    import backend.scanner.recipes as rec

    monkeypatch.setattr(rec, "RECIPES_DIR", tmp_path / "recipes")


def _ep(url, method="POST", rows=5, total=100, post='{"page":1,"perPage":20}', auth=None):
    return CapturedEndpoint(
        url=url, method=method, content_type="application/json",
        post_data_sample=post, reason="legacy", sniffed=False,
        vehicle_rows=rows, total_count=total, auth_headers=auth or {},
    )


@pytest.mark.parametrize("url,post,expect", [
    ("https://websites-search.api.carscommerce.inc/api/v1/listings/1/search",
     '{"page":1,"perPage":20}', PAGINATION_CARSCOMMERCE),
    ("https://abc.a1.typesense.net/multi_search", '{"searches":[]}', PAGINATION_TYPESENSE),
    ("https://www.dealer.com/api/widget/ws-inv-data/getInventory",
     '{"inventoryParameters":{"start":["20"]}}', PAGINATION_DEALER_COM),
    ("https://www.dealer.com/api/other", None, PAGINATION_NONE),
])
def test_infer_pagination(url, post, expect):
    assert infer_pagination(url, post) == expect


def test_promote_and_load_roundtrip():
    ep = _ep("https://websites-search.api.carscommerce.inc/api/v1/listings/1/search",
             auth={"authorization": "Bearer x"})
    n = promote_from_ledger("d1", "dealer_inspire", [ep])
    assert n == 1
    (r,) = load_recipes("d1")
    assert r.url == ep.url
    assert r.pagination == PAGINATION_CARSCOMMERCE
    assert r.auth_headers == {"authorization": "Bearer x"}
    assert r.provider_hint == "dealer_inspire"


def test_promote_skips_thin_endpoints():
    thin = _ep("https://x.example/api/one-vin", rows=1, total=None)
    assert promote_from_ledger("d2", "p", [thin]) == 0
    assert load_recipes("d2") == []


def test_promote_refreshes_stale_and_caps():
    eps = [_ep(f"https://api{i}.example/inv", rows=5 + i) for i in range(6)]
    promote_from_ledger("d3", "p", eps, max_recipes=4)
    assert len(load_recipes("d3")) == 4
    # mark one stale, re-promote same endpoint -> unstaled with fresh capture
    r = load_recipes("d3")[0]
    mark_stale("d3", r, "401")
    assert any(x.stale for x in load_recipes("d3"))
    fresh = _ep(r.url, rows=50)
    promote_from_ledger("d3", "p", [fresh])
    again = [x for x in load_recipes("d3") if x.key() == r.key()]
    assert again and not again[0].stale


def _dealercom_body(page_alias, config_id, start="0"):
    return (
        '{"siteId":"d","pageAlias":"%s","widgetName":"ws-inv-data",'
        '"inventoryParameters":{"start":["%s"]},'
        '"preferences":{"pageSize":"100","listing.config.id":"%s"}}'
        % (page_alias, start, config_id)
    )


def test_key_discriminates_dealercom_sections():
    """Same ws-inv-data URL, different section bodies -> distinct recipe keys."""
    url = "https://www.d.com/api/widget/ws-inv-data/getInventory"
    used = EndpointRecipe(dealer_id="x", url=url, method="POST", content_type="",
                          post_template=_dealercom_body("INVENTORY_LISTING_DEFAULT_AUTO_CERTIFIED_USED", "auto-certified-used"))
    new = EndpointRecipe(dealer_id="x", url=url, method="POST", content_type="",
                         post_template=_dealercom_body("INVENTORY_LISTING_DEFAULT_AUTO_NEW", "auto-new"))
    assert used.key() != new.key()
    # Different pages of the SAME section still share a key (start param ignored).
    new_pg2 = EndpointRecipe(dealer_id="x", url=url, method="POST", content_type="",
                             post_template=_dealercom_body("INVENTORY_LISTING_DEFAULT_AUTO_NEW", "auto-new", start="100"))
    assert new.key() == new_pg2.key()


def test_key_unchanged_for_bodyless_recipes():
    """CarsCommerce/Typesense have no pageAlias -> key stays URL-only (no regression)."""
    url = "https://x.example/api/v1/listings/1/search"
    a = EndpointRecipe(dealer_id="x", url=url, method="POST", content_type="", post_template='{"page":1}')
    b = EndpointRecipe(dealer_id="x", url=url, method="POST", content_type="", post_template='{"page":2}')
    assert a.key() == b.key() == ("POST", "x.example/api/v1/listings/1/search")


def test_promote_keeps_all_dealercom_sections():
    """Regression: new + used + certified bodies on ONE URL must all persist,
    not collapse to a single recipe (which dropped whole slices of the lot)."""
    url = "https://www.d.com/api/widget/ws-inv-data/getInventory"
    eps = [
        _ep(url, post=_dealercom_body("INVENTORY_LISTING_DEFAULT_AUTO_NEW", "auto-new"), total=734, rows=100),
        _ep(url, post=_dealercom_body("INVENTORY_LISTING_DEFAULT_AUTO_USED", "auto-used"), total=226, rows=100),
        _ep(url, post=_dealercom_body("INVENTORY_LISTING_DEFAULT_AUTO_CERTIFIED_USED", "auto-certified-used"), total=90, rows=90),
    ]
    n = promote_from_ledger("dsec", "dealer_dot_com", eps)
    assert n == 3
    aliases = {
        __import__("json").loads(r.post_template)["pageAlias"]
        for r in load_recipes("dsec")
    }
    assert aliases == {
        "INVENTORY_LISTING_DEFAULT_AUTO_NEW",
        "INVENTORY_LISTING_DEFAULT_AUTO_USED",
        "INVENTORY_LISTING_DEFAULT_AUTO_CERTIFIED_USED",
    }


def test_load_ignores_corrupt_file(tmp_path):
    import backend.scanner.recipes as rec

    rec.RECIPES_DIR.mkdir(parents=True, exist_ok=True)
    (rec.RECIPES_DIR / "d4.json").write_text("{not json", encoding="utf-8")
    assert load_recipes("d4") == []


def test_save_load_dataclass_roundtrip():
    r = EndpointRecipe(dealer_id="d5", url="https://a.example/x", method="GET",
                       content_type="application/json", post_template=None)
    save_recipes("d5", [r])
    (loaded,) = load_recipes("d5")
    assert loaded.url == r.url and loaded.method == "GET"


# ── alias resolution (URL-rekeyed dealers) ────────────────────────────────────
#
# These tests pin the FILE-side alias contract, so they run with the DB mirror
# disabled: the dev sqlite dealer_recipes table leaks rows across pytest
# sessions (save_recipes write-through), which would otherwise make alias
# lookups order-dependent.


@pytest.fixture()
def no_recipes_db(monkeypatch):
    monkeypatch.setenv("RECIPES_DB_DISABLED", "1")


def _write_alias_map(mapping):
    import json

    import backend.scanner.recipes as rec

    rec.RECIPES_DIR.mkdir(parents=True, exist_ok=True)
    (rec.RECIPES_DIR / rec.ALIASES_FILENAME).write_text(
        json.dumps(mapping) if isinstance(mapping, dict) else mapping, encoding="utf-8"
    )


def _seed_old_slug_file(old_id="hughwhitehonda-com", url="https://a.example/inv"):
    r = EndpointRecipe(dealer_id=old_id, url=url, method="GET",
                       content_type="application/json", post_template=None, saved_at=1.0)
    save_recipes(old_id, [r])
    return r


def test_alias_resolved_load(no_recipes_db):
    """Old-slug file + alias entry -> load_recipes(current id) finds the recipes."""
    import backend.scanner.recipes as rec

    r = _seed_old_slug_file("hughwhitehonda-com")
    _write_alias_map({"hughwhitehonda-com": "hughwhitehonda-net"})
    (loaded,) = load_recipes("hughwhitehonda-net")
    assert loaded.url == r.url
    # The old file itself was not moved by a read.
    assert (rec.RECIPES_DIR / "hughwhitehonda-com.json").exists()
    assert not (rec.RECIPES_DIR / "hughwhitehonda-net.json").exists()


def test_alias_never_shadows_current_slug_file(no_recipes_db):
    """A file under the CURRENT slug wins over an aliased old file."""
    _seed_old_slug_file("shadowhonda-com", url="https://old.example/inv")
    _write_alias_map({"shadowhonda-com": "shadowhonda-net"})
    cur = EndpointRecipe(dealer_id="shadowhonda-net", url="https://new.example/inv",
                         method="GET", content_type="", post_template=None, saved_at=2.0)
    save_recipes("shadowhonda-net", [cur])
    (loaded,) = load_recipes("shadowhonda-net")
    assert loaded.url == "https://new.example/inv"


def test_alias_write_goes_to_current_slug(no_recipes_db):
    """Saving under the current id migrates content forward: the new-slug file is
    written (never the old one) and later loads stop following the alias."""
    import backend.scanner.recipes as rec

    _seed_old_slug_file("fwdhonda-com", url="https://old.example/inv")
    _write_alias_map({"fwdhonda-com": "fwdhonda-net"})
    recipes = load_recipes("fwdhonda-net")  # read via alias
    for r in recipes:
        r.url = "https://new.example/inv"
        r.saved_at = 99.0
    save_recipes("fwdhonda-net", recipes)
    assert (rec.RECIPES_DIR / "fwdhonda-net.json").exists()
    (loaded,) = load_recipes("fwdhonda-net")
    assert loaded.url == "https://new.example/inv"
    # Old file may linger until the migration script runs, but it no longer wins.
    old_raw = (rec.RECIPES_DIR / "fwdhonda-com.json").read_text(encoding="utf-8")
    assert "old.example" in old_raw


def test_alias_corrupt_file_tolerated(no_recipes_db):
    """A corrupt _aliases.json means no aliasing — never an exception."""
    r = _seed_old_slug_file("corrupthonda-com")
    _write_alias_map("{not json!!")
    assert load_recipes("corrupthonda-net") == []
    # Non-dict JSON is equally ignored.
    _write_alias_map('["corrupthonda-com"]')
    assert load_recipes("corrupthonda-net") == []
    # And direct loads of the old id still work throughout.
    (loaded,) = load_recipes("corrupthonda-com")
    assert loaded.url == r.url


def test_no_alias_behavior_unchanged(no_recipes_db):
    """Without an alias file, unknown ids load empty and known ids load normally."""
    r = _seed_old_slug_file("noaliashonda-com")
    assert load_recipes("noaliashonda-net") == []
    (loaded,) = load_recipes("noaliashonda-com")
    assert loaded.url == r.url


def test_alias_file_ignored_when_target_file_missing(no_recipes_db):
    """An alias entry whose old FILE does not exist resolves to nothing."""
    _write_alias_map({"gone-com": "gone-net"})
    assert load_recipes("gone-net") == []


def test_resolve_alias_slug(no_recipes_db):
    from backend.scanner.recipes import resolve_alias_slug

    _write_alias_map({
        "hughwhitehonda-com": "hughwhitehonda-net",
        "self-com": "self-com",  # self-referential entries are ignored
    })
    assert resolve_alias_slug("hughwhitehonda-net") == "hughwhitehonda-com"
    assert resolve_alias_slug("self-com") is None
    assert resolve_alias_slug("unrelated-com") is None


# ── replay ────────────────────────────────────────────────────────────────────

import asyncio  # noqa: E402


def _vehicle(i):
    return {"vin": f"1HGBH41JXMN10{i:04d}", "year": 2024, "make": "Honda",
            "model": "Civic", "price": 30000 + i}


def _cc_recipe(dealer_id="d10", rows=20, total=45):
    ep = _ep("https://websites-search.api.carscommerce.inc/api/v1/listings/1/search",
             rows=rows, total=total, post='{"page":1,"perPage":20}',
             auth={"authorization": "Bearer k"})
    promote_from_ledger(dealer_id, "dealer_dot_com", [ep])
    return load_recipes(dealer_id)[0]


def test_replay_paginates_and_returns_records(monkeypatch):
    import backend.scanner.recipes as rec

    _cc_recipe("d10")
    pages = {1: [_vehicle(i) for i in range(20)],
             2: [_vehicle(20 + i) for i in range(20)],
             3: [_vehicle(40 + i) for i in range(5)]}
    calls = []

    def fake_request(recipe, body, base_url, url=None):
        page = body["page"]
        calls.append((page, dict(recipe.auth_headers)))
        return 200, {"inventory": pages.get(page, [])}

    monkeypatch.setattr(rec, "_replay_request", fake_request)
    hit = asyncio.run(rec.try_fetch_via_recipes("d10", "dealer_dot_com",
                                               "https://dealer.example", "Dealer"))
    assert hit is not None
    records, n_vins = hit
    assert len(records) == 3  # page 4 never requested: total_count reached
    assert n_vins == 45
    assert calls[0][1] == {"authorization": "Bearer k"}
    (r,) = load_recipes("d10")
    assert r.last_ok_at > 0


def test_replay_marks_stale_on_401(monkeypatch):
    import backend.scanner.recipes as rec

    _cc_recipe("d11")
    monkeypatch.setattr(rec, "_replay_request", lambda *a: (401, None))
    records = asyncio.run(rec.try_fetch_via_recipes("d11", "dealer_dot_com",
                                                    "https://dealer.example", "Dealer"))
    assert records is None
    (r,) = load_recipes("d11")
    assert r.stale and r.stale_reason == "http_401"


def test_replay_rejects_thin_results(monkeypatch):
    import backend.scanner.recipes as rec

    _cc_recipe("d12")
    monkeypatch.setattr(rec, "_replay_request",
                        lambda *a: (200, {"inventory": [_vehicle(1)]}))
    records = asyncio.run(rec.try_fetch_via_recipes("d12", "dealer_dot_com",
                                                    "https://dealer.example", "Dealer"))
    assert records is None  # 1 VIN < min_vehicles: browser scan proceeds


def test_replay_disabled_by_env(monkeypatch):
    import backend.scanner.recipes as rec

    _cc_recipe("d13")
    monkeypatch.setenv("SCANNER_RECIPE_FETCH", "0")
    monkeypatch.setattr(rec, "_replay_request",
                        lambda *a: (200, {"inventory": [_vehicle(i) for i in range(30)]}))
    assert asyncio.run(rec.try_fetch_via_recipes("d13", "dealer_dot_com",
                                                 "https://dealer.example", "Dealer")) is None


def test_mutate_for_page_shapes():
    from backend.scanner.recipes import _mutate_for_page

    cc = EndpointRecipe(dealer_id="d", url="https://x.carscommerce.inc/s", method="POST",
                        content_type="", post_template=None, pagination=PAGINATION_CARSCOMMERCE)
    assert _mutate_for_page(cc, {"page": 1, "perPage": 20}, 2)["page"] == 3

    ts = EndpointRecipe(dealer_id="d", url="https://x.typesense.net/multi_search", method="POST",
                        content_type="", post_template=None, pagination=PAGINATION_TYPESENSE)
    out = _mutate_for_page(ts, {"searches": [{"q": "*", "page": 1}]}, 1)
    assert out["searches"][0]["page"] == 2

    dc = EndpointRecipe(dealer_id="d", url="https://x.example/ws-inv-data/getInventory", method="POST",
                        content_type="", post_template=None, pagination=PAGINATION_DEALER_COM)
    out = _mutate_for_page(dc, {"preferences": {"pageSize": "24"}, "inventoryParameters": {}}, 2)
    assert out["inventoryParameters"]["start"] == ["48"]


def test_url_for_page_page_query():
    from backend.scanner.recipes import PAGINATION_PAGE_QUERY, _url_for_page

    # page_query mutates the URL: sets ?page=N (1-based), replacing any existing page.
    tv = EndpointRecipe(dealer_id="d", url="https://x.example/inventory-used.json", method="GET",
                        content_type="", post_template=None, pagination=PAGINATION_PAGE_QUERY)
    assert _url_for_page(tv, 0) == "https://x.example/inventory-used.json?page=1"
    assert _url_for_page(tv, 3) == "https://x.example/inventory-used.json?page=4"

    tv2 = EndpointRecipe(dealer_id="d", url="https://x.example/inv.json?page=9&x=1", method="GET",
                         content_type="", post_template=None, pagination=PAGINATION_PAGE_QUERY)
    got = _url_for_page(tv2, 1)
    assert "page=2" in got and "x=1" in got and "page=9" not in got

    # Non page_query paginations leave the URL untouched (no regression).
    cc = EndpointRecipe(dealer_id="d", url="https://x.carscommerce.inc/s", method="POST",
                        content_type="", post_template=None, pagination=PAGINATION_CARSCOMMERCE)
    assert _url_for_page(cc, 5) == "https://x.carscommerce.inc/s"


# ── field coverage: a VIN list is not an inventory feed ───────────────────────


def test_recipe_field_coverage_counts_only_positive_prices_and_nonblank_strings():
    from backend.scanner.recipes import recipe_field_coverage

    vs = [
        {"vin": "A", "price": 30000, "trim": "EX", "exterior_color": "Blue"},
        {"vin": "B", "price": 0, "trim": "", "exterior_color": None},
        {"vin": "C", "price": "Call", "trim": " ", "exterior_color": "Red"},
        {"vin": "D"},
    ]
    cov = recipe_field_coverage(vs)
    assert cov["n"] == 4
    assert cov["price"] == 0.25
    assert cov["trim"] == 0.25
    assert cov["exterior_color"] == 0.5
    assert recipe_field_coverage([]) == {"n": 0.0, "price": 0.0, "trim": 0.0, "exterior_color": 0.0}


def test_replay_reports_coverage_of_parsed_rows(monkeypatch):
    import backend.scanner.recipes as rec

    _cc_recipe("d20", rows=20, total=20)
    rows = [_vehicle(i) for i in range(20)]           # all priced, no trim/colour
    for v in rows[:10]:
        v["trim"] = "Sport"
    monkeypatch.setattr(rec, "_replay_request", lambda *a: (200, {"inventory": rows}))
    cov: dict = {}
    hit = asyncio.run(rec.try_fetch_via_recipes("d20", "dealer_dot_com",
                                               "https://dealer.example", "Dealer",
                                               coverage_out=cov))
    assert hit is not None and hit[1] == 20
    assert cov["n"] == 20 and cov["price"] == 1.0 and cov["trim"] == 0.5 and cov["exterior_color"] == 0.0


def test_replay_without_coverage_out_is_unchanged(monkeypatch):
    import backend.scanner.recipes as rec

    _cc_recipe("d21", rows=20, total=20)
    monkeypatch.setattr(rec, "_replay_request",
                        lambda *a: (200, {"inventory": [_vehicle(i) for i in range(20)]}))
    hit = asyncio.run(rec.try_fetch_via_recipes("d21", "dealer_dot_com",
                                               "https://dealer.example", "Dealer"))
    assert hit is not None and len(hit) == 2


@pytest.mark.parametrize(
    "vins,known,cov,ok,needle",
    [
        (100, 100, {"price": 0.9, "trim": 0.8, "exterior_color": 0.0}, True, "ok"),
        (100, 100, {"price": 0.9, "trim": 0.0, "exterior_color": 0.7}, True, "ok"),
        (60, 100, {"price": 1.0, "trim": 1.0, "exterior_color": 1.0}, False, "70%"),
        # The 2026-08-04 failure shape: every VIN, no price, no trim, no colour.
        (176, 176, {"price": 0.16, "trim": 0.0, "exterior_color": 0.2}, False, "price coverage"),
        (100, 100, {"price": 0.9, "trim": 0.1, "exterior_color": 0.2}, False, "trim/colour"),
        (100, 100, None, False, "price coverage"),
        (100, 0, {"price": 1.0, "trim": 1.0, "exterior_color": 1.0}, False, "no_known_lot"),
    ],
)
def test_recipe_yield_replaces_browser(monkeypatch, vins, known, cov, ok, needle):
    from backend.scanner.recipes import recipe_yield_replaces_browser

    monkeypatch.delenv("SCANNER_RECIPE_MIN_PRICE_COVERAGE", raising=False)
    monkeypatch.delenv("SCANNER_RECIPE_MIN_FIELD_COVERAGE", raising=False)
    got_ok, why = recipe_yield_replaces_browser(vins, known, cov)
    assert got_ok is ok
    assert needle in why


def test_recipe_coverage_thresholds_from_env(monkeypatch):
    from backend.scanner.recipes import recipe_yield_replaces_browser

    monkeypatch.setenv("SCANNER_RECIPE_MIN_PRICE_COVERAGE", "0.1")
    monkeypatch.setenv("SCANNER_RECIPE_MIN_FIELD_COVERAGE", "0.1")
    ok, _ = recipe_yield_replaces_browser(176, 176, {"price": 0.16, "trim": 0.0, "exterior_color": 0.2})
    assert ok is True


# ── coverage carried from ledger, rich feeds rank first, replay refreshes it ──


def test_promote_carries_field_coverage_and_ranks_rich_first(monkeypatch):
    monkeypatch.delenv("SCANNER_RECIPE_MIN_PRICE_COVERAGE", raising=False)
    monkeypatch.delenv("SCANNER_RECIPE_MIN_FIELD_COVERAGE", raising=False)
    thin = _ep("https://www.jordanford.net/api/KeyFeatures/GetKeyFeaturesByVins", method="GET",
               rows=50, total=None, post=None)
    thin.field_coverage = {"price": 0.0, "trim": 0.0, "exterior_color": 0.0}
    rich = _ep("https://www.jordanford.net/api/inventory/search", method="GET",
               rows=20, total=None, post=None)
    rich.field_coverage = {"price": 0.95, "trim": 0.8, "exterior_color": 0.9}
    assert promote_from_ledger("d30", "team_velocity", [thin, rich]) == 2
    first, second = load_recipes("d30")
    assert "inventory/search" in first.url          # rich wins despite fewer rows
    assert first.field_coverage["price"] == 0.95
    assert second.field_coverage == {"price": 0.0, "trim": 0.0, "exterior_color": 0.0}


def test_replay_persists_measured_coverage_on_recipe(monkeypatch):
    import backend.scanner.recipes as rec

    _cc_recipe("d31", rows=20, total=20)
    rows = [_vehicle(i) for i in range(20)]
    for v in rows:
        v["exterior_color"] = "Red"
    monkeypatch.setattr(rec, "_replay_request", lambda *a: (200, {"inventory": rows}))
    asyncio.run(rec.try_fetch_via_recipes("d31", "dealer_dot_com", "https://dealer.example", "Dealer"))
    (r,) = load_recipes("d31")
    assert r.field_coverage == {"price": 1.0, "trim": 0.0, "exterior_color": 1.0}


def test_recipe_rows_without_coverage_field_still_load(tmp_path):
    from backend.scanner.recipes import _rows_to_recipes

    (r,) = _rows_to_recipes([{"dealer_id": "d", "url": "https://x/y", "method": "GET",
                              "content_type": "json", "post_template": None}])
    assert r.field_coverage == {}


def test_last_known_vin_count_counts_listed_rows_only(monkeypatch):
    """Retired rows must not inflate the browser-skip denominator (Hiley VW
    2026-09-22: 338 feed VINs vs '624 known', 286 of them unlisted that morning)."""
    from backend.scanner import recipes as r

    seen: dict[str, str] = {}

    class _Cur:
        def execute(self, sql, params):
            seen["sql"] = sql
            seen["dealer"] = params[0]

        def fetchone(self):
            return (42,)

    class _Conn:
        def cursor(self):
            return _Cur()

        def close(self):
            pass

    monkeypatch.setattr("backend.db.inventory_pg.is_inventory_postgres", lambda: True)
    monkeypatch.setattr("backend.db.inventory_pg.pg_connect", lambda: _Conn())
    assert r.last_known_vin_count("dealer-x") == 42
    assert seen["dealer"] == "dealer-x"
    assert "listing_removed_at IS NULL" in seen["sql"]
    assert "listing_active" in seen["sql"]


def test_carscommerce_page_body_drops_featured_flag_and_pages_by_100():
    """Culver City Toyota 2026-09-23: captured body carried facetFilters
    {"custom_text_1": ["true"]} (a featured carousel) and replayed 30 of 529."""
    from backend.scanner import recipes as r

    rec = r.EndpointRecipe(dealer_id="d", url="https://websites-search.api.carscommerce.inc/api/v1/listings/1/search",
                           method="POST", content_type="json", post_template="{}", pagination=r.PAGINATION_CARSCOMMERCE)
    tpl = {"page": 1, "perPage": 20, "filters": {"type_slug": ["used"]},
           "facetFilters": {"custom_text_1": ["true"], "make": ["Toyota"]}}
    body = r._mutate_for_page(rec, tpl, 1)
    assert body["page"] == 2 and body["perPage"] == 100
    # 2026-09-25: make / type_slug pin one SRP section (Germain Toyota's capture
    # replayed 65 new Toyotas of a 401-car lot); they are dropped from both maps
    assert "facetFilters" not in body
    assert body["filters"] == {}
    body0 = r._mutate_for_page(rec, {"page": 1, "perPage": 20, "facetFilters": {"custom_text_1": ["true"]}}, 0)
    assert "facetFilters" not in body0


def test_cosmos_recipe_pages_with_pt_and_upgrades_old_files():
    from backend.scanner import recipes as r

    url = "https://www.cherokeecountytoyota.com/api/vhcliaa/vehicle-pages/cosmos/srp/vehicles/13028/769890"
    assert r.infer_pagination(url, None) == r.PAGINATION_COSMOS_PT
    recs = r._rows_to_recipes([{"dealer_id": "d", "url": url, "method": "GET", "content_type": "json",
                                "post_template": None, "pagination": "none"}])
    assert recs[0].pagination == r.PAGINATION_COSMOS_PT
    assert r._url_for_page(recs[0], 0).endswith("?pt=1&pn=96")
    assert r._url_for_page(recs[0], 1).endswith("?pt=2&pn=96")


def test_get_total_count_reads_carscommerce_meta_and_cosmos_paging():
    from backend.parsers.base import get_total_count

    assert get_total_count({"data": {}, "meta": {"pagination": {"total": 529, "count": 100}}}) == 529
    assert get_total_count({"Paging": {"PaginationDataModel": {"TotalCount": 106, "TotalPages": 2}}}) == 106
