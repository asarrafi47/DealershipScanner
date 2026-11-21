"""dealer_on (CDK / dealer.on) field extraction."""

from __future__ import annotations

from backend.parsers import dealer_dot_com, dealer_on


def _ford_like_inventory(*vehicles: dict) -> dict:
    return {"inventory": list(vehicles)}


def test_dealer_on_ford_like_colors_engine_mpg() -> None:
    v = {
        "vin": "1FTEW1EP5LFA12345",
        "year": 2020,
        "make": "Ford",
        "model": "F-150",
        "trim": "Lariat",
        "internetPrice": 42900,
        "mileage": 12000,
        "exteriorColor": "Oxford White",
        "interiorColor": "Medium Earth Gray",
        "engineDescription": "2.7L V6 EcoBoost",
        "fuelType": "Gasoline",
        "condition": "Used",
        "cylinders": 6,
        "mpgCity": 20,
        "mpgHighway": 26,
    }
    payload = _ford_like_inventory(v, dict(v, vin="1FTEW1EP5LFB23456"), dict(v, vin="1FTEW1EP5LFC34567"))
    rows = dealer_on.parse(payload, "https://dealer.example.com", "d1", "Test", "https://dealer.example.com")
    assert len(rows) >= 1
    r = next(x for x in rows if x.get("vin") == "1FTEW1EP5LFA12345")
    assert r.get("exterior_color") == "Oxford White"
    assert r.get("interior_color") == "Medium Earth Gray"
    assert r.get("engine_description") == "2.7L V6 EcoBoost"
    assert r.get("fuel_type") == "Gasoline"
    assert r.get("condition") == "Used"
    assert r.get("cylinders") == 6
    assert r.get("mpg_city") == 20
    assert r.get("mpg_highway") == 26


def test_dealer_on_tracking_attributes_exterior_mpg() -> None:
    v = {
        "vin": "3FMTK3SU6MMA90123",
        "year": 2021,
        "make": "Ford",
        "model": "Mustang Mach-E",
        "internetPrice": 55000,
        "mileage": 5000,
        "trackingAttributes": [
            {"name": "Exterior Paint", "value": "Rapid Red Metallic"},
            {"name": "EPA Est MPG City", "value": "105"},
            {"name": "EPA Est MPG Hwy", "value": "93"},
            {"name": "Engine", "value": "Electric Motor"},
        ],
    }
    payload = _ford_like_inventory(v, dict(v, vin="3FMTK3SU6MMB01234"), dict(v, vin="3FMTK3SU6MMC12345"))
    rows = dealer_on.parse(payload, "https://dealer.example.com", "d1", "Test", "https://dealer.example.com")
    r = rows[0]
    assert r.get("exterior_color") == "Rapid Red Metallic"
    assert r.get("mpg_city") == 105
    assert r.get("mpg_highway") == 93
    assert "Electric" in (r.get("engine_description") or "") or r.get("engine_description") == "Electric Motor"


def test_dealer_on_fuel_economy_nested() -> None:
    v = {
        "vin": "1FM5K8GC5LGA11111",
        "year": 2020,
        "make": "Ford",
        "model": "Explorer",
        "internetPrice": 38000,
        "mileage": 30000,
        "fuelEconomy": {"city": 21, "highway": "28"},
    }
    payload = _ford_like_inventory(v, dict(v, vin="1FM5K8GC5LGB22222"), dict(v, vin="1FM5K8GC5LGC33333"))
    rows = dealer_on.parse(payload, "https://dealer.example.com", "d1", "Test", "https://dealer.example.com")
    r = rows[0]
    assert r.get("mpg_city") == 21
    assert r.get("mpg_highway") == 28


def test_dealer_on_homenet_style_selling_internet_price() -> None:
    """Keffer-style payloads: stock + sellingPrice / internet_Price, not Dealer.com nesting."""
    base = {
        "year": 2016,
        "make": "Jeep",
        "model": "Cherokee",
        "stock": "J251107A",
        "sellingPrice": 12000,
        "internet_Price": 12000.0,
        "miles": 111117,
    }
    v1 = dict(base, vin="1C4PJMCS4GW131990")
    v2 = dict(base, vin="1C4PJMCS4GW131991", stock="J251107B")
    v3 = dict(base, vin="1C4PJMCS4GW131992", stock="J251107C")
    payload = _ford_like_inventory(v1, v2, v3)
    rows = dealer_on.parse(payload, "https://www.kefferjeep.com", "k", "Keffer", "https://www.kefferjeep.com")
    by_vin = {r["vin"]: r for r in rows}
    assert by_vin["1C4PJMCS4GW131990"]["price"] == 12000.0
    assert by_vin["1C4PJMCS4GW131990"].get("stock_number") == "J251107A"


def test_dealer_dot_com_parse_homenet_top_level_price() -> None:
    """Mis-tagged CDK inventory parsed as dealer_dot_com should still recover price."""
    vehicles = [
        {"vin": "1C4PJMCS4GW131990", "make": "Jeep", "model": "Cherokee", "year": 2016, "sellingPrice": 12000},
        {"vin": "1C4PJMCS4GW131991", "make": "Jeep", "model": "Cherokee", "year": 2016, "internet_Price": 12100},
        {"vin": "1C4PJMCS4GW131992", "make": "Jeep", "model": "Cherokee", "year": 2016, "sellingPrice": 12200},
    ]
    rows = dealer_dot_com.parse(
        {"inventory": vehicles},
        "https://www.kefferjeep.com",
        "k",
        "Keffer",
        "https://www.kefferjeep.com",
    )
    assert {r["vin"]: r["price"] for r in rows} == {
        "1C4PJMCS4GW131990": 12000,
        "1C4PJMCS4GW131991": 12100,
        "1C4PJMCS4GW131992": 12200,
    }


# ── cosmos per-car rooftop stamping (N8.1) ───────────────────────────────────
# Fixtures are the N1.2 cosmos map's trimmed live cards (identity + location fields).

import json
import os

import pytest

from backend.parsers import resolve_rooftop_attribution
from backend.scanner.scrapers.dealer_on import _extract_vehicles_from_srp_body

_COSMOS_DIR = os.path.join(os.path.dirname(__file__), "fixtures", "cosmos_rooftops")


def _cosmos_fixture(name: str) -> dict:
    with open(os.path.join(_COSMOS_DIR, name + ".json"), encoding="utf-8") as fh:
        return json.load(fh)


def _gate(rows: list[dict], monkeypatch, mode: str, **roster) -> tuple[list[dict], list[dict]]:
    import backend.parsers as parsers_mod

    monkeypatch.setenv("SCANNER_ROOFTOP_SCORER", "0" if mode == "legacy" else "1")
    monkeypatch.setattr(parsers_mod, "roster_name_aliases", lambda d: ())
    monkeypatch.setattr("backend.scanner.rooftop_ledger.note_refusal", lambda *a, **k: None, raising=False)
    monkeypatch.setattr("backend.scanner.rooftop_ledger.flush", lambda *a, **k: None, raising=False)
    return resolve_rooftop_attribution(rows, **roster)


@pytest.mark.parametrize("mode", ["legacy", "scorer"])
def test_cleveland_nissan_keeps_only_cleveland_cards(mode, monkeypatch) -> None:
    body = _cosmos_fixture("cleveland-nissan-com")
    rows = _extract_vehicles_from_srp_body(
        body, "https://www.cleveland-nissan.com", "cleveland-nissan-com", "Mtn View Nissan of Cleveland",
        "https://www.cleveland-nissan.com",
    )
    assert len(rows) == 16 and all("_rooftop" in r for r in rows)
    kept, rejected = _gate(
        rows, monkeypatch, mode, dealer_id="cleveland-nissan-com", dealer_name="Mtn View Nissan of Cleveland",
        dealer_url="https://www.cleveland-nissan.com", dealer_city="McDonald", dealer_state="TN",
    )
    assert {r["_rooftop"]["lot_id"] for r in kept} == {"25124"}
    assert len(kept) == 4 and len(rejected) == 12
    # the stock suffix the owner reads (CH/DA/BR) never appears on a kept row
    assert not any(str(r["stock_number"]).endswith(("CH", "DA", "BR")) for r in kept)


@pytest.mark.parametrize("mode", ["legacy", "scorer"])
def test_cosmos_own_cars_are_named_by_the_roster_even_when_the_lot_label_differs(mode, monkeypatch) -> None:
    body = _cosmos_fixture("cleveland-nissan-com")
    rows = _extract_vehicles_from_srp_body(
        body, "https://www.cleveland-nissan.com", "cleveland-nissan-com", "Cleveland Nissan", "https://www.cleveland-nissan.com"
    )
    own = [r for r in rows if r["_rooftop"].get("own_site")]
    assert own and all(r["_rooftop"]["name"] == "Cleveland Nissan" for r in own)
    assert all(r["_rooftop"]["alt_name"] == "Mtn View Nissan of Cleveland" for r in own)
    kept, _ = _gate(
        rows, monkeypatch, mode, dealer_id="cleveland-nissan-com", dealer_name="Cleveland Nissan",
        dealer_url="https://www.cleveland-nissan.com", dealer_city="McDonald", dealer_state="TN",
    )
    assert {r["_rooftop"]["lot_id"] for r in kept} == {"25124"}


@pytest.mark.parametrize("mode", ["legacy", "scorer"])
def test_bentleycadillac_page_of_sister_cards_keeps_none(mode, monkeypatch) -> None:
    body = _cosmos_fixture("bentleycadillac-com")
    sisters = [c for c in body["DisplayCards"] if c["VehicleCard"]["DealerLocatedAtId"] != c["VehicleCard"]["DealerId"]]
    page = []
    for i in range(24):  # a used-car page 1: 24 cards, none at Huntsville's own lot
        vc = dict(sisters[i % len(sisters)]["VehicleCard"])
        vc["VehicleVin"] = vc["VehicleVin"][:12] + f"{i:05d}"
        page.append({"VehicleCard": vc})
    rows = _extract_vehicles_from_srp_body(
        {"DisplayCards": page}, "https://www.bentleycadillac.com", "bentleycadillac-com", "Bentley Cadillac",
        "https://www.bentleycadillac.com",
    )
    assert len(rows) == 24
    kept, rejected = _gate(
        rows, monkeypatch, mode, dealer_id="bentleycadillac-com", dealer_name="Bentley Cadillac",
        dealer_url="https://www.bentleycadillac.com", dealer_city="Huntsville", dealer_state="AL",
    )
    assert kept == [] and len(rejected) == 24


def test_single_store_feed_is_untouched(monkeypatch) -> None:
    body = _cosmos_fixture("bentleycadillac-com")
    own_only = {"DisplayCards": [c for c in body["DisplayCards"] if c["VehicleCard"]["DealerLocatedAtId"] == c["VehicleCard"]["DealerId"]]}
    rows = _extract_vehicles_from_srp_body(own_only, "https://www.bentleycadillac.com", "bentleycadillac-com", "Bentley Cadillac", "https://www.bentleycadillac.com")
    kept, rejected = _gate(
        rows, monkeypatch, "scorer", dealer_id="bentleycadillac-com", dealer_name="Bentley Cadillac",
        dealer_url="https://www.bentleycadillac.com", dealer_city="Huntsville", dealer_state="AL",
    )
    assert len(kept) == len(rows) and not rejected


def test_card_without_location_fields_is_not_stamped() -> None:
    body = {"DisplayCards": [{"VehicleCard": {"VehicleVin": "1HGCM82633A004352", "VehicleYear": 2020, "VehicleMake": "Honda"}}]}
    rows = _extract_vehicles_from_srp_body(body, "https://x.com", "x-com", "X", "https://x.com")
    assert len(rows) == 1 and "_rooftop" not in rows[0]


def test_missing_own_site_id_keeps_rows_unstamped_and_logs(monkeypatch, caplog) -> None:
    from backend.scanner.scrapers import dealer_on as mod

    monkeypatch.setattr("backend.attribution.place.own_site_id", lambda d: "")
    mod._warned_unknown_site.discard("Hint Test")
    card = {"VehicleVin": "1HGCM82633A004352", "DealerLocatedAtId": 27897, "VehicleLocation": "Bentley Hyundai",
            "DealerLocatedAtCity": "Huntsville", "DealerLocatedAtState": "AL"}  # no DealerId
    caplog.set_level("INFO", logger=mod.logger.name)
    rows = _extract_vehicles_from_srp_body({"DisplayCards": [{"VehicleCard": card}]}, "https://x.com", "hint-test-com", "Hint Test", "https://x.com")
    assert "_rooftop" not in rows[0]
    assert any("no own-site hint" in r.getMessage() for r in caplog.records)


def test_learned_own_site_id_hint_stamps_cards_without_dealer_id(monkeypatch) -> None:
    monkeypatch.setattr("backend.attribution.place.own_site_id", lambda d: "27897")
    card = {"VehicleVin": "1HGCM82633A004352", "DealerLocatedAtId": 27897, "VehicleLocation": "Bentley Hyundai",
            "DealerLocatedAtCity": "Huntsville", "DealerLocatedAtState": "AL"}
    rows = _extract_vehicles_from_srp_body({"DisplayCards": [{"VehicleCard": card}]}, "https://x.com", "hint-com", "Bentley Hyundai", "https://x.com")
    assert rows[0]["_rooftop"]["own_site"] == "1" and rows[0]["_rooftop"]["lot_id"] == "27897"


def test_own_site_id_hint_roundtrip(monkeypatch) -> None:
    from backend.attribution import place

    store: dict = {}
    monkeypatch.setattr("backend.scanner.recipe_store.get_scan_hints", lambda d: dict(store.get(d, {})))
    monkeypatch.setattr("backend.scanner.recipe_store.set_scan_hints", lambda d, h, merge=True: store.setdefault(d, {}).update(h) or True)
    assert place.own_site_id("a-com") == ""
    assert place.learn_own_site_id("a-com", 27096) is True
    assert place.own_site_id("a-com") == "27096"
    assert place.learn_own_site_id("", "1") is False
