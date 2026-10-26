"""P5B.1: the replay walk ends on no new RAW VIN, and the total check reads raw rows.

A group feed's other rooftops fill whole pages that the rooftop gate refuses; kept-only
termination read such a page as the end of the lot (shottenkirkhondarome: pages 5-6).
"""
from __future__ import annotations

import asyncio

import pytest

import backend.scanner.recipes as rec
from backend.scanner.network_observer import CapturedEndpoint
from backend.scanner.recipes import load_recipes, promote_from_ledger

CC_URL = "https://websites-search.api.carscommerce.inc/api/v1/listings/1/search"


@pytest.fixture(autouse=True)
def _tmp_recipes_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(rec, "RECIPES_DIR", tmp_path / "recipes")


def _vin(i):
    return f"1HGBH41JXMN10{i:04d}"


def _recipe(dealer_id, total):
    ep = CapturedEndpoint(
        url=CC_URL, method="POST", content_type="application/json", post_data_sample='{"page":1,"perPage":10}',
        reason="legacy", sniffed=False, vehicle_rows=10, total_count=total, auth_headers={"authorization": "Bearer k"},
    )
    promote_from_ledger(dealer_id, "dealer_dot_com", [ep])
    return load_recipes(dealer_id)[0]


def _walk(dealer_id):
    out: dict = {}
    hit = asyncio.run(rec.try_fetch_via_recipes(
        dealer_id, "dealer_dot_com", "https://dealer.example", "Dealer", replay_out=out, union=True))
    return hit, out


def _install(monkeypatch, store_pages, sibling_pages, per_page=10):
    """Page N holds `per_page` fresh VINs; the gate keeps them on `store_pages` and
    refuses them (sibling rooftops) on `sibling_pages`; later pages are empty."""
    import backend.attribution as attribution

    last = max(store_pages | sibling_pages)

    def fake_req(recipe, body, base_url, url=None):
        page = body["page"]
        if page > last:
            return 200, {"inventory": []}
        return 200, {"inventory": [{"vin": _vin((page - 1) * per_page + i), "page": page} for i in range(per_page)]}

    def fake_parse(provider, parsed, ctx, rejected_out, **kw):
        rows = [dict(v) for v in parsed["inventory"]]
        keep = [r for r in rows if r["page"] in store_pages]
        rejected_out.extend(r for r in rows if r["page"] not in store_pages)
        return keep

    monkeypatch.setattr(rec, "_replay_request", fake_req)
    monkeypatch.setattr(attribution, "parse_page", fake_parse)


def test_walk_passes_an_all_refused_page_and_collects_the_store_rows_after_it(monkeypatch):
    _recipe("g1", total=0)
    _install(monkeypatch, store_pages={1, 3}, sibling_pages={2})
    hit, out = _walk("g1")
    (w,) = out["recipes"]
    assert hit[1] == 20  # pages 1 and 3
    assert w["kept_vins"] == 20 and w["raw_vins"] == 30  # page 2's siblings are raw, never kept
    assert w["pages"] == 4 and w["end"] == "no_new_rows"
    kept = {v["vin"] for v in hit[2]} if len(hit) > 2 and hit[2] else set()
    assert not kept & {_vin(i) for i in range(10, 20)}


def test_total_stop_compares_raw_rows_with_a_group_total(monkeypatch):
    _recipe("g2", total=50)  # the feed's total counts every rooftop
    _install(monkeypatch, store_pages={1, 5}, sibling_pages={2, 3, 4})
    hit, out = _walk("g2")
    (w,) = out["recipes"]
    assert w["raw_vins"] == 50 and w["kept_vins"] == 20 and w["end"] == "total_reached" and w["complete"]
    assert hit[1] == 20


def test_kept_only_page_still_ends_on_a_page_that_adds_nothing(monkeypatch):
    _recipe("g3", total=0)
    _install(monkeypatch, store_pages={1, 2}, sibling_pages=set())
    _, out = _walk("g3")
    (w,) = out["recipes"]
    assert w["end"] == "no_new_rows" and w["pages"] == 3 and w["kept_vins"] == 20 and w["raw_vins"] == 20
