"""Per-vehicle endpoint recipes: capture during a browser visit, template by VIN,
replay over HTTP for other cars."""
from __future__ import annotations

import asyncio
import json

import pytest

from backend.scanner.vdp import vdp_recipes as vr

VIN_A = "1HGCY2F80TA030629"
VIN_B = "1HGCY2F63TA066331"


@pytest.fixture(autouse=True)
def _tmp_store(tmp_path, monkeypatch):
    monkeypatch.setattr(vr, "VDP_RECIPES_DIR", tmp_path / "vdp")
    monkeypatch.setattr(vr, "_POLITE_DELAY_S", 0.0)
    vr._CANDIDATES.clear()
    yield
    vr._CANDIDATES.clear()


def test_templatize_replaces_vin_and_stock_case_insensitively():
    url = f"https://d.example/api/vehicle/{VIN_A.lower()}?stock=T26A1234&x=1"
    out, hit = vr.templatize(url, VIN_A, "T26A1234")
    assert hit and out == "https://d.example/api/vehicle/{vin}?stock={stock}&x=1"
    out, hit = vr.templatize("https://d.example/api/config", VIN_A, "T26A1234")
    assert not hit


def test_templatize_ignores_short_numeric_stock():
    out, hit = vr.templatize("https://d.example/api/v/1234", VIN_A, "1234")
    assert not hit


def test_record_and_promote_keeps_only_car_specific_endpoints():
    ok = vr.record_candidate(
        "Test Motors", url=f"https://d.example/api/vehicle/{VIN_A}", method="GET", post_data=None,
        headers={"x-api-key": "k123", "cookie": "s=1"}, vin=VIN_A, stock="", score=40, ep_count=1, image_count=12,
    )
    assert ok
    assert not vr.record_candidate(  # nothing about the car in the request
        "Test Motors", url="https://d.example/api/site-config", method="GET", post_data=None,
        headers={}, vin=VIN_A, stock="", score=40, ep_count=1, image_count=0,
    )
    assert not vr.record_candidate(  # analytics host
        "Test Motors", url=f"https://www.google-analytics.com/collect?vin={VIN_A}", method="POST",
        post_data=None, headers={}, vin=VIN_A, stock="", score=40, ep_count=1, image_count=0,
    )
    n = vr.promote_candidates("test-motors", "Test Motors", "dealer_inspire")
    assert n == 1
    recs = vr.load_vdp_recipes("test-motors")
    assert recs[0].url_template == "https://d.example/api/vehicle/{vin}"
    assert recs[0].auth_headers == {"x-api-key": "k123"}
    assert recs[0].ep_hits == 1 and recs[0].image_hits == 1
    assert vr._CANDIDATES == {}


def test_promote_accumulates_hits_and_clears_stale():
    vr.save_vdp_recipes("test-motors", [vr.VdpRecipe(
        dealer_id="test-motors", url_template="https://d.example/api/vehicle/{vin}",
        hits=3, ep_hits=3, stale=True, stale_reason="x",
    )])
    vr.record_candidate(
        "Test Motors", url=f"https://d.example/api/vehicle/{VIN_B}", method="GET", post_data=None,
        headers={}, vin=VIN_B, stock="", score=40, ep_count=1, image_count=0,
    )
    vr.promote_candidates("test-motors", "Test Motors")
    r = vr.load_vdp_recipes("test-motors")[0]
    assert r.hits == 4 and r.ep_hits == 4 and r.stale is False


def _payload(vin: str) -> dict:
    return {
        "vehicle": {
            "vin": vin, "year": 2026, "make": "Honda", "model": "Accord", "trim": "EX-L Hybrid",
            "drivetrain": "FWD", "transmission": "CVT", "fuel_type": "Hybrid", "body_style": "Sedan",
            "engine": "2.0L I4 Hybrid", "exterior_color": "Platinum White Pearl", "price": 34990,
            "images": [f"https://cdn.example/{vin}/{i}.jpg" for i in range(10)],
        }
    }


def test_apply_vdp_recipes_fills_specs_and_gallery(monkeypatch):
    vr.save_vdp_recipes("test-motors", [vr.VdpRecipe(
        dealer_id="test-motors", url_template="https://d.example/api/vehicle/{vin}", ep_hits=2,
    )])
    calls: list[str] = []

    def fake_fetch(recipe, url, payload, base_url):
        calls.append(url)
        vin = url.rsplit("/", 1)[-1]
        return 200, _payload(vin)

    monkeypatch.setattr(vr, "_fetch_json", fake_fetch)
    v = {"vin": VIN_B, "_detail_url": f"https://d.example/vehicle/{VIN_B}/", "price": None,
         "drivetrain": "", "transmission": "", "fuel_type": "", "gallery": [], "description": ""}
    stats = asyncio.run(vr.apply_vdp_recipes([v], "test-motors", wants=lambda x: True, gallery_min=12))
    assert calls == [f"https://d.example/api/vehicle/{VIN_B}"]
    assert stats["fetched"] == 1 and stats["galleries_extended"] == 1
    assert v["drivetrain"] == "FWD" and v["transmission"] == "CVT"
    assert len(v["gallery"]) == 10
    r = vr.load_vdp_recipes("test-motors")[0]
    assert r.last_ok_at > 0 and r.fails == 0


def test_apply_vdp_recipes_marks_stale_after_repeated_failures(monkeypatch):
    vr.save_vdp_recipes("test-motors", [vr.VdpRecipe(
        dealer_id="test-motors", url_template="https://d.example/api/vehicle/{vin}",
    )])
    monkeypatch.setattr(vr, "_fetch_json", lambda recipe, url, payload, base_url: (403, None))
    cars = [{"vin": f"1HGCY2F8{i}TA03062{i}", "_detail_url": "https://d.example/v/x", "gallery": []} for i in range(5)]
    stats = asyncio.run(vr.apply_vdp_recipes(cars, "test-motors", wants=lambda x: True, gallery_min=12))
    assert stats["fetched"] == 0 and stats["stale_marked"] == 1
    assert vr.load_vdp_recipes("test-motors")[0].stale is True
    # a stale recipe is not replayed
    stats2 = asyncio.run(vr.apply_vdp_recipes(cars, "test-motors", wants=lambda x: True, gallery_min=12))
    assert stats2["recipes"] == 0


def test_apply_skips_vehicles_without_the_placeholder_value(monkeypatch):
    vr.save_vdp_recipes("test-motors", [vr.VdpRecipe(
        dealer_id="test-motors", url_template="https://d.example/api/stock/{stock}",
    )])
    monkeypatch.setattr(vr, "_fetch_json", lambda *a: (_ for _ in ()).throw(AssertionError("must not fetch")))
    v = {"vin": VIN_B, "_detail_url": "https://d.example/v/x", "gallery": []}
    stats = asyncio.run(vr.apply_vdp_recipes([v], "test-motors", wants=lambda x: True, gallery_min=12))
    assert stats["fetched"] == 0


def test_recipe_file_roundtrip_is_json(tmp_path):
    vr.save_vdp_recipes("test-motors", [vr.VdpRecipe(dealer_id="test-motors", url_template="https://d.example/{vin}")])
    raw = json.loads(vr._path("test-motors").read_text())
    assert raw[0]["url_template"] == "https://d.example/{vin}"
