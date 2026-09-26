"""html_cards: data-vin card pages and RSC hydration payloads (Quantum Auto
Sales, 2026-09-26: 9 DOM cards + 199 escaped JSON objects in self.__next_f)."""
from __future__ import annotations

import pytest

from backend.parsers import parse
from backend.parsers.html_cards import detect
from backend.scanner import recipe_synth as rs


@pytest.fixture(autouse=True)
def _no_recipe_db(monkeypatch):
    """Recipe files only: load_recipes/save_recipes also sync Postgres recipe_store,
    which leaked test rows across runs (m-com came back with 3 recipes)."""
    from backend.scanner import recipe_store

    monkeypatch.setattr(recipe_store, "db_load_recipes", lambda *a, **k: [])
    monkeypatch.setattr(recipe_store, "db_save_recipes", lambda *a, **k: None)

_CARD = '<div class="inventory-image-wrapper" data-vin="{vin}"><a href="/inventory/used-cars-ACURA-TLX-2021-{tail}-X"><img src="https://cdn.x/{vin}.jpg"></a><button data-sales-price="31123.25" data-vin="{vin}">Explore</button></div>'
_RSC = ('self.__next_f.push([1,"{\\"vehicles\\":[{\\"vin\\":\\"%s\\",\\"year\\":2019,\\"make\\":\\"VOLKSWAGEN\\",\\"model\\":\\"GOLF\\",\\"trim\\":\\"TSI S WAGON 4D\\",'
        '\\"mileage\\":85933,\\"stockNo\\":\\"31247\\",\\"priceInet\\":\\"14995.00\\",\\"colorExterior\\":\\"WHITE\\",\\"cylinders\\":4,\\"imagesPath\\":\\"https://cdn.gma.to\\",'
        '\\"image1raw\\":\\"/data/a/b.jpg\\",\\"images\\":[{\\"large\\":\\"https://cdn.gma.to/fit-in/720x540/a/b.jpg\\"}],\\"vdpUrl\\":\\"https://www.q.com/inventory/used-cars-VOLKSWAGEN-GOLF-2019-510761-R\\"}]}"])')


def _page(n_cards: int, n_rsc: int) -> str:
    cards = "".join(_CARD.format(vin=f"19UUB6F49MA0{i:05d}", tail=f"{i:06d}") for i in range(n_cards))
    rsc = "".join(_RSC % f"3VWY57AU5KM5{i:05d}" for i in range(n_rsc))
    return "<html><body>" + cards + "<script>" + rsc + "</script></body></html>" + "x" * 2000


def test_detect_counts_cards_and_hydration_objects():
    assert not detect(_page(3, 0))
    assert detect(_page(12, 0))
    assert detect(_page(0, 12))


def test_parse_merges_cards_and_hydration_objects():
    rows = parse("html_cards", _page(2, 3), base_url="https://www.q.com", dealer_id="q-com", dealer_name="Q", dealer_url="https://www.q.com", rejected_out=[])
    assert len(rows) == 5
    card = next(r for r in rows if r["vin"].startswith("19UUB"))
    assert card["price"] == 31123.25 and card["make"] == "Acura" and card["model"] == "TLX" and card["year"] == 2021
    assert card["_detail_url"].startswith("https://www.q.com/inventory/used-cars-ACURA")
    rsc = next(r for r in rows if r["vin"].startswith("3VWY"))
    assert (rsc["year"], rsc["make"], rsc["model"], rsc["trim"]) == (2019, "VOLKSWAGEN", "GOLF", "TSI S WAGON 4D")
    assert rsc["price"] == 14995.0 and rsc["mileage"] == 85933 and rsc["stock_number"] == "31247" and rsc["cylinders"] == 4
    assert rsc["image_url"].startswith("https://cdn.gma.to/") and rsc["_detail_url"].endswith("510761-R")
    assert rsc["condition"] == "Used"


def test_synth_builds_html_page_recipe(monkeypatch):
    page = _page(0, 15)
    monkeypatch.setattr(rs, "_fetch_impersonated", lambda url, **k: page if url.endswith("/inventory/") else None)
    monkeypatch.setattr(rs, "_dep_fetch_html", lambda url: None)
    recipes = rs._synth_html_cards("q-com", "https://www.q.com", "<html>data-vin= /inventory</html>")
    assert len(recipes) == 1 and recipes[0].provider_hint == "html_cards" and recipes[0].url == "https://www.q.com/inventory/"
    assert recipes[0].vehicle_rows == 15
    assert rs._detect_html_cards(page, "https://www.q.com")


def test_ledger_html_fragment_promotes_as_html_cards_recipe(tmp_path, monkeypatch):
    from types import SimpleNamespace

    from backend.scanner import recipes as rc

    monkeypatch.setattr(rc, "RECIPES_DIR", tmp_path)
    ep = SimpleNamespace(url="https://www.m.com/inventory/ajax?cond=used", method="GET", content_type="text/html; charset=utf-8",
                         post_data_sample=None, vehicle_rows=24, total_count=None, auth_headers={}, field_coverage={},
                         reason="html_fragment_cards")
    assert rc.promote_from_ledger("m-com", "pixel_motion", [ep]) == 1
    r = rc.load_recipes("m-com")[0]
    assert r.provider_hint == "html_cards" and r.pagination == rc.PAGINATION_HTML_PAGE
    assert rc._url_for_page(r, 1).endswith("cond=used&page=2")


def test_json_envelope_with_html_cards_is_a_fragment():
    """PixelMotion VlpAjaxEndpoint.php: {"store": …, "html": "<cards>"} scored near_miss
    (vin_items=0) on mcpeeks-com 2026-09-26; the VINs live inside the string."""
    from backend.parsers.html_cards import markup_in_json
    from backend.scanner import network_observer as no

    body = {"store": "x", "html": _page(12, 0), "filters_html": "<ul></ul>", "time_elapsed_secs": 0.2}
    assert markup_in_json(body) == body["html"]
    assert no._html_fragment_in_json(body) == body["html"]
    assert no._html_fragment_in_json({"vehicles": [{"vin": f"19UUB6F49MA0{i:05d}"} for i in range(12)]}) is None
    rows = parse("html_cards", body, base_url="https://www.m.com", dealer_id="m-com", dealer_name="M", dealer_url="https://www.m.com", rejected_out=[])
    assert len(rows) == 12 and rows[0]["price"] == 31123.25


def test_html_page_walk_uses_the_captured_page_param_and_keeps_sections(tmp_path, monkeypatch):
    from types import SimpleNamespace

    from backend.scanner import recipes as rc

    monkeypatch.setattr(rc, "RECIPES_DIR", tmp_path)
    base = "https://www.m.com/wp-content/plugins/pm/VlpAjaxEndpoint.php?condition%5B%5D={c}&pp=24&inv_page=2&newUrl=%2Finventory%2F{c}%2F%3Finv_page%3D2&isAjax=true"
    eps = [SimpleNamespace(url=base.format(c=c), method="GET", content_type="application/json", post_data_sample=None, vehicle_rows=24,
                           total_count=None, auth_headers={}, field_coverage={}, reason="html_fragment_cards") for c in ("new", "used")]
    assert rc.promote_from_ledger("pm-sections-com", "pixel_motion", eps) == 2  # sections survive the (method, path) key
    r = next(x for x in rc.load_recipes("pm-sections-com") if "condition%5B%5D=new" in x.url)
    u3 = rc._url_for_page(r, 2)
    assert "inv_page=3" in u3 and "page=3" not in u3.replace("inv_page=3", "") and "inv_page%3D3" in u3
    assert rc._html_page_param("cond=used&page=4") == "page" and rc._html_page_param("a=1") == "page"
