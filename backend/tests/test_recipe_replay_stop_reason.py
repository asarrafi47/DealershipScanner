"""P5A.2: the replay walk report (stop reason, completeness, in_union, scope hash).

``try_fetch_via_recipes(replay_out=...)`` is reporting only: every walk here also
asserts the return value the caller has always got. ``_replay_request`` is faked.
"""
from __future__ import annotations

import asyncio
import json
import sqlite3

import pytest

import backend.scanner.recipes as rec
from backend.scanner.network_observer import CapturedEndpoint
from backend.scanner.recipes import load_recipes, promote_from_ledger

CC_URL = "https://websites-search.api.carscommerce.inc/api/v1/listings/1/search"


@pytest.fixture(autouse=True)
def _tmp_recipes_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(rec, "RECIPES_DIR", tmp_path / "recipes")


def _vehicle(i):
    return {"vin": f"1HGBH41JXMN10{i:04d}", "year": 2024, "make": "Honda", "model": "Civic", "price": 30000 + i}


def _recipe(dealer_id, total, url=CC_URL):
    ep = CapturedEndpoint(
        url=url, method="POST", content_type="application/json", post_data_sample='{"page":1,"perPage":20}',
        reason="legacy", sniffed=False, vehicle_rows=20, total_count=total, auth_headers={"authorization": "Bearer k"},
    )
    promote_from_ledger(dealer_id, "dealer_dot_com", [ep])
    return load_recipes(dealer_id)[0]


def _walk(dealer_id, **kw):
    out: dict = {}
    hit = asyncio.run(rec.try_fetch_via_recipes(
        dealer_id, "dealer_dot_com", "https://dealer.example", "Dealer", replay_out=out, **kw))
    return hit, out


def _paged(per_page, last_page=None, fail_at=None):
    """Page N (1-based) holds `per_page` fresh VINs; pages past `last_page` are empty."""
    def fake(recipe, body, base_url, url=None):
        page = body["page"]
        if fail_at and page >= fail_at[0]:
            return fail_at[1], None
        if last_page is not None and page > last_page:
            return 200, {"inventory": []}
        return 200, {"inventory": [_vehicle((page - 1) * per_page + i) for i in range(per_page)]}
    return fake


def test_page_cap_is_incomplete_and_logged(monkeypatch, caplog):
    _recipe("c1", total=100000)
    monkeypatch.setattr(rec, "_replay_request", _paged(5))
    with caplog.at_level("WARNING", logger="scanner"):
        hit, out = _walk("c1")
    assert hit is not None and len(hit[0]) == rec._MAX_REPLAY_PAGES  # return value as before
    (w,) = out["recipes"]
    assert w["end"] == "page_cap" and not w["complete"] and w["in_union"]
    assert w["pages"] == rec._MAX_REPLAY_PAGES and w["kept_vins"] == 5 * rec._MAX_REPLAY_PAGES
    assert out["truncated"] is True and out["capped_recipes"] == 1 and out["source"] == "recipe"
    assert any("page_cap" in r.getMessage() for r in caplog.records)


def test_http_403_mid_walk_keeps_vins_and_is_incomplete(monkeypatch, caplog):
    _recipe("c2", total=100)
    monkeypatch.setattr(rec, "_replay_request", _paged(20, fail_at=(3, 403)))
    with caplog.at_level("WARNING", logger="scanner"):
        hit, out = _walk("c2")
    assert hit is not None and hit[1] == 40
    (w,) = out["recipes"]
    assert w["end"] == "http_403" and not w["complete"] and w["kept_vins"] == 40 and w["in_union"]
    assert out["truncated"] is True
    assert any("http_403" in r.getMessage() for r in caplog.records)


def test_total_reached_is_complete(monkeypatch):
    _recipe("c3", total=45)
    monkeypatch.setattr(rec, "_replay_request", _paged(15))
    hit, out = _walk("c3")
    assert hit[1] == 45
    (w,) = out["recipes"]
    assert w["end"] == "total_reached" and w["complete"] and w["total_count"] == 45
    assert out["truncated"] is False and out["capped_recipes"] == 0


def test_all_duplicate_page_is_exhausted_and_complete_without_total(monkeypatch):
    _recipe("c4", total=0)
    # pages 1-2 are fresh; page 3 repeats page 2
    def fake(recipe, body, base_url, url=None):
        page = min(body["page"], 2)
        return 200, {"inventory": [_vehicle((page - 1) * 12 + i) for i in range(12)]}
    monkeypatch.setattr(rec, "_replay_request", fake)
    hit, out = _walk("c4")
    assert hit[1] == 24
    (w,) = out["recipes"]
    assert w["end"] == "exhausted" and w["complete"] and not out["truncated"]


def test_401_on_page_one_is_auth_dead(monkeypatch):
    _recipe("c5", total=100)
    monkeypatch.setattr(rec, "_replay_request", lambda *a: (401, None))
    hit, out = _walk("c5")
    assert hit is None
    (w,) = out["recipes"]
    assert w["end"] == "auth_dead" and not w["complete"] and not w["in_union"] and w["raw_vins"] == 0
    assert out["truncated"] is True


def test_unparsed_and_failed_fetch_ends(monkeypatch):
    _recipe("c6", total=100)
    monkeypatch.setattr(rec, "_replay_request", _paged(20, fail_at=(2, 200)))
    _, out = _walk("c6")
    assert out["recipes"][0]["end"] == "unparsed_mid"

    _recipe("c7", total=100)
    monkeypatch.setattr(rec, "_replay_request", _paged(20, fail_at=(2, 0)))
    _, out = _walk("c7")
    assert out["recipes"][0]["end"] == "fetch_failed"


def test_low_yield_recipe_is_out_of_the_union_and_its_vins_are_reported(monkeypatch):
    # a complete 8-VIN walk (total 8) is dropped by the yield floor, not by incompleteness
    _recipe("c8", total=8)
    monkeypatch.setattr(rec, "_replay_request", _paged(8, last_page=1))
    hit, out = _walk("c8", union=True)
    assert hit is None
    (w,) = out["recipes"]
    assert w["complete"] and not w["in_union"] and w["kept_vins"] == 8
    assert out["low_yield_vins"] == sorted(_vehicle(i)["vin"] for i in range(8))
    assert out["truncated"] is False


def test_exhausted_far_short_of_total_and_single_page_of_large_total_are_incomplete(monkeypatch):
    _recipe("c9", total=500)
    def fake(recipe, body, base_url, url=None):
        page = min(body["page"], 3)
        return 200, {"inventory": [_vehicle((page - 1) * 16 + i) for i in range(16)]}
    monkeypatch.setattr(rec, "_replay_request", fake)
    _, out = _walk("c9")
    w = out["recipes"][0]
    assert w["end"] == "exhausted" and w["raw_vins"] == 48 and not w["complete"]

    # PAGINATION_NONE: one page of 100 against a site total of 500
    ep = CapturedEndpoint(url="https://dealer.example/api/inv", method="GET", content_type="application/json",
                          post_data_sample=None, reason="legacy", sniffed=False, vehicle_rows=100,
                          total_count=500, auth_headers={})
    promote_from_ledger("c10", "dealer_dot_com", [ep])
    assert load_recipes("c10")[0].pagination == rec.PAGINATION_NONE
    monkeypatch.setattr(rec, "_replay_request",
                        lambda *a: (200, {"inventory": [_vehicle(i) for i in range(100)]}))
    _, out = _walk("c10")
    w = out["recipes"][0]
    assert w["end"] == "single_page" and w["raw_vins"] == 100 and not w["complete"]
    assert out["truncated"] is True


def test_refused_rows_count_toward_raw_vins(monkeypatch):
    """raw_vins is kept plus refused, so a group feed that the rooftop gate empties still
    shows how much of the site total the walk saw."""
    import backend.attribution as attribution

    _recipe("c11", total=30)
    monkeypatch.setattr(rec, "_replay_request", _paged(15))

    def fake_parse(provider, parsed, ctx, rejected_out, **kw):
        rows = [dict(v) for v in parsed["inventory"]]
        rejected_out.extend(rows[10:])
        return rows[:10]
    monkeypatch.setattr(attribution, "parse_page", fake_parse)
    _, out = _walk("c11", union=True)
    w = out["recipes"][0]
    assert w["raw_vins"] == 45 and w["kept_vins"] == 30 and w["end"] == "total_reached"


def test_scope_hash_is_stable_across_pages_and_differs_by_section():
    a = _recipe("c12", total=100)
    h = rec.recipe_scope_hash(a, "Dealer")
    assert h == rec.recipe_scope_hash(a, "Dealer") and len(h) == 12
    b = rec.EndpointRecipe(
        dealer_id="c12", url=CC_URL, method="POST", content_type="application/json",
        post_template=json.dumps({"page": 1, "facetFilters": {"type_slug": ["used"]}}),
        pagination=rec.PAGINATION_CARSCOMMERCE,
    )
    c = rec.EndpointRecipe(**{**b.__dict__, "post_template": json.dumps({"page": 7, "facetFilters": {"type_slug": ["used"]}})})
    assert rec.recipe_scope_hash(b) == rec.recipe_scope_hash(c) != rec.recipe_scope_hash(a)


def test_report_does_not_change_the_return_value(monkeypatch):
    _recipe("c13", total=45)
    monkeypatch.setattr(rec, "_replay_request", _paged(15))
    plain = asyncio.run(rec.try_fetch_via_recipes("c13", "dealer_dot_com", "https://dealer.example", "Dealer"))
    hit, out = _walk("c13")
    assert plain is not None and plain[1] == hit[1] and len(plain[0]) == len(hit[0])
    assert out["recipes"]


def test_union_log_counts_recipes_and_pages(monkeypatch, caplog):
    _recipe("c14", total=45)
    monkeypatch.setattr(rec, "_replay_request", _paged(15))
    with caplog.at_level("INFO", logger="scanner"):
        _walk("c14", union=True)
    msg = [r.getMessage() for r in caplog.records if "combined across" in r.getMessage()]
    assert msg and "1 recipe replay(s), 3 page(s)" in msg[0]


def test_summary_json_carries_recipe_replay(tmp_path, monkeypatch):
    from backend.db import inventory_db as inv
    from backend.db.repositories import dealers_repo

    db_path = tmp_path / "t.db"
    monkeypatch.setenv("INVENTORY_DB_PATH", str(db_path))
    monkeypatch.setattr(inv, "DB_PATH", str(db_path))
    report = {"truncated": True, "capped_recipes": 1, "source": "recipe",
              "recipes": [{"key": "POST x", "end": "page_cap", "complete": False}]}
    dealers_repo.record_scan_outcomes(
        [{"dealer_id": "r-com", "recipe_replay": report}, {"dealer_id": "s-com"}], finished_at="2026-10-26T00:00:00Z")
    conn = sqlite3.connect(str(db_path))
    rows = {d: json.loads(s) for d, s in conn.execute("SELECT dealer_id, summary_json FROM scan_runs")}
    conn.close()
    assert rows["r-com"]["recipe_replay"] == report
    assert rows["s-com"]["recipe_replay"] is None


def test_feed_step_sets_recipe_replay_only_when_non_empty(monkeypatch):
    from backend.scanner.phases.dealer_run_steps.feed import fetch_recipe_feed
    from backend.scanner.phases.dealer_run_steps.state import DealerRun, new_result

    def _run():
        return DealerRun(
            dealer={"dealer_id": "f-com", "url": "https://dealer.example", "name": "F"}, name="F",
            url="https://dealer.example", provider="dealer_dot_com", dealer_id="f-com",
            result=new_result("f-com", "F", "dealer_dot_com"), t0=0.0,
        )

    async def _full(dealer_id, provider, base_url, name, *, replay_out=None, **kw):
        replay_out.update({"truncated": False, "recipes": []})
        return None

    async def _none(dealer_id, provider, base_url, name, **kw):
        return None

    async def _boom(dealer_id, provider, base_url, name, *, replay_out=None, **kw):
        raise RuntimeError("walk exploded")

    for fake, expect in ((_full, "report"), (_none, None), (_boom, "error")):
        monkeypatch.setattr(rec, "try_fetch_via_recipes", fake)
        run = _run()
        asyncio.run(fetch_recipe_feed(run))
        got = run.result.get("recipe_replay")
        if expect is None:
            assert got is None
        elif expect == "report":
            assert got["truncated"] is False
        else:
            assert got["error"] == "walk exploded" and got["truncated"] is True
