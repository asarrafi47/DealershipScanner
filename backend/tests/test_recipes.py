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
