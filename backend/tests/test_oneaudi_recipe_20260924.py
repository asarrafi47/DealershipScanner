"""OneAudi (omnigraph.audi.com GraphQL) recipe: inputs read from the SSR page's
Apollo cache, offset pagination in the variables, StockCar rows parsed.
Evidence: workspace/dealer_logs/audihuntsville-com/discovery.md (2026-09-24)."""
from __future__ import annotations

import json
from urllib.parse import quote

from backend.parsers import parse
from backend.parsers.base import get_total_count
from backend.parsers.oneaudi import total_count
from backend.scanner import recipe_synth as rs
from backend.scanner.recipes import PAGINATION_GRAPHQL_OFFSET, _mutate_for_page


def _car(vin: str, used: bool = True) -> dict:
    return {"stockCar": {
        "vin": vin, "titleText": "2025 Audi Q8", "subtitleText": "Premium Plus 55 TFSI® quattro® Tiptronic®", "cartypeText": "U" if used else "N",
        "weblink": f"https://www.audihuntsville.com/en/inventory/vehicle/?isdealer&market=usuc&vehicleId={vin}",
        "modelInfo": {"genericModel": {"text": "Q8", "code": "AAEO"}, "modelyear": 2025},
        "preUse": {"code": "R", "text": "Used car"} if used else {"code": "N", "text": "New car"}, "qualityLabel": [],
        "carPrices": [{"type": "dealerDocFees", "price": {"value": 899}}, {"type": "final", "price": {"value": 62411}},
                      {"type": "list", "price": {"value": 70990}}, {"type": "sale", "price": {"value": 61512}}],
        "mileage": {"unitText": "miles", "value": {"number": 5918}},
        "colorInfo": {"exteriorColor": {"colorInfo": {"text": "Daytona Gray pearl effect"}, "baseColorInfo": {"text": "Gray"}},
                      "interiorColor": {"colorInfo": {"text": "Black with Rock Gray stitching"}, "baseColorInfo": {"text": "Black"}}},
        "driveText": "All-wheel drive", "engineInfo": {"fuel": {"text": "Gas"}}, "gearText": None,
        "images": [{"url": "https://vtpimages.audi.com/carimg2/5198/4757335198.jpg"}, {"url": "https://vtpimages.audi.com/carimg2/6560/4663586560.jpg"}],
        "dealer": {"id": "USA07B04", "name": "Audi Huntsville", "city": "Huntsville"},
        "dynamicAttributes": [{"id": "VEHICLE_ID", "value": "P1592"}, {"id": "URL_CARFAX", "value": "https://www.carfax.com/vehiclehistory/ar20/x"}],
    }}


def _payload(cars: list[dict], total: int = 133) -> dict:
    return {"data": {"stockCarSearch": {"resultNumber": total, "results": {"cars": cars}}}}


def test_parse_stockcar_rows():
    rows = parse("oneaudi", _payload([_car("WA1EVBF13SD035902"), _car("WA1EVBF13SD035903", used=False)]), base_url="https://www.audihuntsville.com",
                 dealer_id="audihuntsville-com", dealer_name="Audi Huntsville", dealer_url="https://www.audihuntsville.com", rejected_out=[])
    r = rows[0]
    assert (r["vin"], r["year"], r["make"], r["model"]) == ("WA1EVBF13SD035902", 2025, "Audi", "Q8")
    assert r["trim"].startswith("Premium Plus 55 TFSI") and "®" not in r["trim"]
    assert r["condition"] == "Used" and rows[1]["condition"] == "New"
    assert r["price"] == 62411.0 and r["msrp"] == 70990.0 and r["mileage"] == 5918
    assert r["exterior_color"] == "Daytona Gray pearl effect" and r["interior_color"].startswith("Black")
    assert r["stock_number"] == "P1592" and r["carfax_url"].startswith("https://www.carfax.com/")
    assert r["image_url"].endswith("4757335198.jpg") and len(r["gallery"]) == 2
    assert r["_detail_url"].endswith("vehicleId=WA1EVBF13SD035902")
    assert total_count(_payload([])) == 133 and get_total_count(_payload([])) == 133


def test_offset_pagination_mutates_variables():
    inputs = {"dealer": "07B04", "stat_import": "AGC_USA_JDP", "brand": "A", "country": "us", "language": "en"}
    body = rs._oneaudi_body(inputs, "USED")
    r = rs.EndpointRecipe(dealer_id="audihuntsville-com", url=rs._ONEAUDI_GRAPHQL, method="POST", content_type="application/json",
                          post_template=json.dumps(body), pagination=PAGINATION_GRAPHQL_OFFSET, provider_hint="oneaudi")
    p3 = _mutate_for_page(r, body, 2)
    assert p3["variables"]["sp"]["paging"] == {"limit": 48, "offset": 96}
    assert p3["variables"]["si"]["stockCarsType"] == "USED"
    assert body["variables"]["sp"]["paging"]["offset"] == 0  # template untouched
    assert p3["variables"]["sp"]["criteria"][0] == {"id": "dealer", "items": ["07B04"]}


def test_inputs_from_url_encoded_apollo_cache():
    cache = ('"stockCarSearch({\\"searchParameter\\":{\\"criteria\\":[{\\"id\\":\\"stat-import\\",\\"items\\":[\\"AGC_USA_JDP\\"]},'
             '{\\"id\\":\\"sold-order\\",\\"items\\":[\\"no\\"]},{\\"id\\":\\"dealer\\",\\"items\\":[\\"07B04\\"]}],'
             '\\"paging\\":{\\"limit\\":48,\\"offset\\":0}},\\"stockIdentifier\\":{\\"marketIdentifier\\":{\\"brand\\":\\"A\\",\\"country\\":\\"us\\",\\"language\\":\\"en\\"},'
             '\\"stockCarsType\\":\\"NEW\\"}})"')
    html = "<script>window.__APOLLO__ = " + quote(cache) + "</script>"
    assert rs._oneaudi_inputs(html) == {"dealer": "07B04", "stat_import": "AGC_USA_JDP", "brand": "A", "country": "us", "language": "en"}
    assert rs._oneaudi_inputs("<html>nothing</html>") is None


def test_synth_builds_new_and_used_recipes(monkeypatch):
    html = quote('"criteria":[{\\"id\\":\\"dealer\\",\\"items\\":[\\"07B04\\"]}]').replace("%5C%22", '\\"')
    monkeypatch.setattr(rs, "_dep_fetch_html", lambda url: None)
    monkeypatch.setattr(rs, "_fetch_impersonated", lambda url, **k: None)
    calls: list[dict] = []

    def fake_replay(recipe, body, origin, url=None):
        calls.append(body)
        t = body["variables"]["si"]["stockCarsType"]
        return 200, _payload([_car("WA1EVBF13SD035902", used=(t == "USED"))], total=133 if t == "NEW" else 210)

    monkeypatch.setattr(rs, "_replay_request", fake_replay)
    recipes = rs._synth_oneaudi("audihuntsville-com", "https://www.audihuntsville.com", '"id":"dealer","items":["07B04"]')
    assert [json.loads(r.post_template)["variables"]["si"]["stockCarsType"] for r in recipes] == ["NEW", "USED"]
    assert [r.total_count for r in recipes] == [133, 210]
    assert all(r.pagination == PAGINATION_GRAPHQL_OFFSET and r.provider_hint == "oneaudi" for r in recipes)
    assert "apollographql-client-name" in recipes[0].auth_headers
    assert rs._detect_oneaudi('<script src="https://oneaudi-falcon.prod.renderer.one.audi/static/client/client.js">', "https://www.audihuntsville.com")
