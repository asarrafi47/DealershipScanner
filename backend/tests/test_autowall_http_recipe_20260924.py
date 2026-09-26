"""autoWALL over plain HTTP: /gs-vehicle/list?filter=All&page=N is a
server-rendered SRP (Long Chevrolet Buick GMC of Athens, 2026-09-24: 309 cars,
25 per page). The recipe replays it as an HTML page walk and the ``autowall``
provider parses the raw page."""
from __future__ import annotations

from backend.parsers import parse
from backend.scanner import recipe_synth as rs
from backend.scanner.recipes import PAGINATION_HTML_PAGE

_CARD = """
<div class="col vehicle-inventory-container" data-vin="{vin}">
  <div class="ibox"><div class="ibox-content product-box">
    <a href="/gs-vehicle/view?vehicleId={vid}&vn=2026 Buick Envision"><h4 class="vehicle-title">{title}</h4></a>
    <span class="price">$34,995</span>
    <span class="mileage">12 mi</span>
    <span class="condition">New</span>
  </div></div>
</div>
"""


def _page(n: int) -> str:
    cards = "".join(_CARD.format(vin=f"LRBFZPR43TD0{i:05d}", vid=17000 + i, title=f"2026 Buick Envision #{i}") for i in range(n))
    return f"<html><head><title>309  Vehicles for Sale in Athens | Long Chevrolet Buick GMC</title></head><body><div id='vehicleListRow'>{cards}</div></body></html>"


def test_detect_from_homepage_links():
    assert rs._detect_autowall('<a href="/gs-vehicle/list?filter=New">New</a>', "https://longofathens.com")
    assert rs._detect_autowall("<img src='/global-images/powered_by_autowall.png' alt='Powered by autoWall'/>", "https://longofathens.com")
    assert not rs._detect_autowall("<html>dealer</html>", "https://x.com")


def test_synth_builds_html_page_walk(monkeypatch):
    monkeypatch.setattr(rs, "_dep_fetch_html", lambda url: _page(25) if url.endswith("/gs-vehicle/list?filter=All") else None)
    r = rs._synth_autowall("longofathens-com", "https://longofathens.com", "<a href='/gs-vehicle/list?filter=All'>")
    assert r is not None
    assert r.url == "https://longofathens.com/gs-vehicle/list?filter=All"
    assert r.pagination == PAGINATION_HTML_PAGE and r.provider_hint == "autowall" and r.content_type == "text/html"
    assert r.vehicle_rows == 25 and r.total_count == 309


def test_synth_refuses_without_card_markup(monkeypatch):
    monkeypatch.setattr(rs, "_dep_fetch_html", lambda url: "<html><title>Long Of Athens Powered By autoWALL</title></html>")
    assert rs._synth_autowall("longofathens-com", "https://longofathens.com", "") is None


def test_autowall_provider_parses_raw_html():
    rows = parse("autowall", _page(3), base_url="https://longofathens.com", dealer_id="longofathens-com",
                 dealer_name="Long Chevrolet Buick GMC", dealer_url="https://longofathens.com", rejected_out=[])
    assert [r["vin"] for r in rows] == ["LRBFZPR43TD000000", "LRBFZPR43TD000001", "LRBFZPR43TD000002"]
    assert parse("autowall", {"inventory": [{"vin": "LRBFZPR43TD000009"}]}, base_url="", dealer_id="d", dealer_url="", rejected_out=[])[0]["vin"] == "LRBFZPR43TD000009"
