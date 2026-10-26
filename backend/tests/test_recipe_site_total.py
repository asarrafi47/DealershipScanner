"""P5B.2: one site-total reader, HTML walks ignore the synth-time total, total-derived page cap."""
from __future__ import annotations

import asyncio
import re
from urllib.parse import parse_qs, urlparse

import pytest

import backend.scanner.recipes as rec
import backend.scanner.recipe_totals as rt
from backend.scanner import recipe_validation as rv
from backend.scanner.recipes import EndpointRecipe

D = "totals-com"


@pytest.fixture(autouse=True)
def _tmp_recipes_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(rec, "RECIPES_DIR", tmp_path / "recipes")
    monkeypatch.delenv("SCANNER_REPLAY_MAX_PAGES", raising=False)


def _vin(i):
    return f"1HGBH41JXMN{i:06d}"


def _install(monkeypatch, recipe, *, per_page, lot, shown_total=None, last_is_total_html=True):
    """A site with `lot` VINs served `per_page` per page; HTML payloads carry
    data-vehicle_count=`shown_total` when given. Returns the list of requested pages."""
    import backend.attribution as attribution

    requested: list[int] = []
    monkeypatch.setattr(rec, "load_recipes", lambda dealer_id: [recipe])
    monkeypatch.setattr(rec, "_persist_replay_success", lambda *a, **k: None)

    def page_of(url, body):
        if body is not None:
            return int(body.get("page") or 1)
        q = parse_qs(urlparse(url).query)
        return int((q.get("p") or q.get("page") or ["1"])[0])

    def fake_req(r, body, base_url, url=None):
        page = page_of(url or r.url, body)
        requested.append(page)
        lo = (page - 1) * per_page
        rows = list(range(lo, min(lo + per_page, lot)))
        if recipe.pagination in (rec.PAGINATION_DEP_SRP, rec.PAGINATION_HTML_PAGE):
            head = f'<div data-vehicle_count="{shown_total}"></div>' if shown_total else ""
            return 200, head + "|" + ",".join(map(str, rows))
        return 200, {"inventory": [{"vin": _vin(i)} for i in rows]}

    def fake_parse(provider, parsed, ctx, rejected_out, **kw):
        if isinstance(parsed, str):
            ids = [x for x in parsed.split("|", 1)[1].split(",") if x]
            return [{"vin": _vin(int(x))} for x in ids]
        return [dict(v) for v in parsed["inventory"]]

    monkeypatch.setattr(rec, "_replay_request", fake_req)
    monkeypatch.setattr(attribution, "parse_page", fake_parse)
    return requested


def _walk():
    out: dict = {}
    hit = asyncio.run(rec.try_fetch_via_recipes(D, "dealer_eprocess", "https://dealer.example", "Dealer",
                                                replay_out=out, union=True))
    return hit, out


def _html(pagination, url, total, provider="dealer_eprocess"):
    return EndpointRecipe(dealer_id=D, url=url, method="GET", content_type="", post_template=None, pagination=pagination,
                          provider_hint=provider, total_count=total)


def test_dep_walk_uses_the_page_count_not_the_stored_total(monkeypatch):
    r = _html(rec.PAGINATION_DEP_SRP, "https://dealer.example/used-vehicles/?p=1", total=56)
    req = _install(monkeypatch, r, per_page=48, lot=100, shown_total=100)
    hit, out = _walk()
    (w,) = out["recipes"]
    assert hit[1] == 100 and req == [1, 2, 3] and w["end"] == "total_reached" and w["total_count"] == 100


def test_html_walk_without_an_on_page_total_ignores_the_stored_one(monkeypatch):
    r = _html(rec.PAGINATION_HTML_PAGE, "https://dealer.example/inventory?page=1", total=30, provider="overfuel")
    req = _install(monkeypatch, r, per_page=20, lot=70)
    hit, out = _walk()
    (w,) = out["recipes"]
    assert hit[1] == 70 and w["end"] == "no_new_rows" and w["total_count"] is None and req == [1, 2, 3, 4, 5]


def test_paginated_list_feed_is_not_stopped_by_its_page_length(monkeypatch):
    r = EndpointRecipe(dealer_id=D, url="https://dealer.example/inventory.json?page=1", method="GET",
                       content_type="", post_template=None, pagination=rec.PAGINATION_PAGE_QUERY, provider_hint="generic_json")
    req = _install(monkeypatch, r, per_page=25, lot=60)
    hit, out = _walk()
    assert hit[1] == 60 and req[:3] == [1, 2, 3]


def _cc(total):
    return EndpointRecipe(dealer_id=D, url="https://websites-search.api.carscommerce.inc/api/v1/listings/1/search",
                          method="POST", content_type="application/json", post_template='{"page":1,"perPage":48}',
                          pagination=rec.PAGINATION_CARSCOMMERCE, provider_hint="carscommerce", total_count=total)


def test_total_3000_at_48_walks_63_pages_with_a_big_enough_cap(monkeypatch):
    req = _install(monkeypatch, _cc(3000), per_page=48, lot=5000)
    monkeypatch.setenv("SCANNER_REPLAY_MAX_PAGES", "63")
    hit, out = _walk()
    (w,) = out["recipes"]
    assert w["pages"] == 63 and w["end"] == "total_reached" and len(req) == 63
    assert hit[1] == 63 * 48


def test_cap_below_the_derived_pages_reports_page_cap_and_bounds_requests(monkeypatch):
    req = _install(monkeypatch, _cc(3000), per_page=48, lot=5000)
    monkeypatch.setenv("SCANNER_REPLAY_MAX_PAGES", "30")
    _, out = _walk()
    (w,) = out["recipes"]
    assert w["end"] == "page_cap" and len(req) == 30 and not w["complete"] and out["truncated"]


def test_default_cap_and_no_total_default():
    assert rec.replay_max_pages() == 150
    assert rec._walk_pages(None, 48, 150) == 40
    assert rec._walk_pages(5406, 48, 150) == 114
    assert rec._walk_pages(1338, 25, 150) == 55
    assert rec._walk_pages(100000, 5, 150) == 150
    assert rec._walk_pages(100, 48, 20) == 4


def test_total_readers_moved_and_reexported():
    assert rv.extract_site_total is rt.extract_site_total and rv.platform_of is rt.platform_of
    assert rv.SITE_TOTAL_EXTRACTORS is rt.SITE_TOTAL_EXTRACTORS


def _rec(url, pagination, provider, post=None):
    return EndpointRecipe(dealer_id=D, url=url, method="POST" if post else "GET", content_type="",
                          post_template=post, pagination=pagination, provider_hint=provider)


@pytest.mark.parametrize("recipe,payload,expect", [
    (_rec("https://websites-search.api.carscommerce.inc/api/v1/listings/1/search", rec.PAGINATION_CARSCOMMERCE,
          "carscommerce", "{}"), {"data": {"total_vehicle_count": 671}}, 671),
    (_rec("https://www.d.com/api/widget/ws-inv-data/getInventory", rec.PAGINATION_DEALER_COM, "dealer_dot_com", "{}"),
     {"pageInfo": {"totalCount": 157}}, 157),
    (_rec("https://abc.a1.typesense.net/multi_search", rec.PAGINATION_TYPESENSE, "typesense", "{}"),
     {"results": [{"found": 412}]}, 412),
    (_rec("https://www.d.com/api/cosmos/srp/vehicles?pt=1", rec.PAGINATION_COSMOS_PT, "dealer_on_cosmos"),
     {"Paging": {"PaginationDataModel": {"TotalCount": 282}}}, 282),
    (_rec("https://www.d.com/gs-vehicle/list?filter=All", rec.PAGINATION_HTML_PAGE, "autowall"),
     "<title>  93 Vehicles for Sale | Genesis</title>", 93),
    (_rec("https://www.d.com/used-vehicles/?p=1", rec.PAGINATION_DEP_SRP, "dealer_eprocess"),
     '<div data-vehicle_count="338"></div>', 338),
    (_rec("https://x.algolia.net/1/indexes/*/queries", "algolia_page", "motive_ridemotive", "{}"), {"nbHits": 555}, 555),
    (_rec("https://www.d.com/inventory/?page=1", rec.PAGINATION_HTML_PAGE, "html_cards"), "<html></html>", None),
])
def test_scan_and_set_gate_read_the_same_total(recipe, payload, expect):
    assert rt.replay_site_total(recipe, payload) == rv.extract_site_total(recipe, payload) == expect


@pytest.mark.parametrize("recipe", [
    _rec("https://www.d.com/wp-json/wp/v2/vehicles?per_page=100", rec.PAGINATION_PAGE_QUERY, "wp_vehicles_index"),
    _rec("https://www.d.com/feed?page=1", rec.PAGINATION_PAGE_QUERY, "generic_json"),
])
def test_list_length_readers_never_stop_a_paginated_walk(recipe):
    assert rt.replay_site_total(recipe, [{"vin": "x"}] * 42) is None
    assert rt.replay_site_total(recipe, {"vehicles": [{"vin": "x"}] * 42}) is None
