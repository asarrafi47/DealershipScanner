"""N21.1 (D-N18): the stored price is the advertised price before taxes and
government fees; MSRP goes only to ``msrp``. Fixtures follow the live values
named in the plan (bentleygmc 65,194 vs MSRP 67,445; cdodge 59,330 vs 70,390;
clevelandford 63,009; jackyjonessweetwater 94,424; secretcitycdjr 52,085)."""
from __future__ import annotations

import pytest

from backend.parsers import dealer_dot_com, dealer_on
from backend.parsers.dealer_dot_com import _extract_msrp_dealer_com, _extract_price_dealer_com
from backend.scanner.scrapers.dealer_on import _map_vehicle_card


def _card(**over) -> dict:
    vc = {"VehicleVin": "SCBCP7ZA5NC012345", "VehicleYear": 2026, "VehicleMake": "GMC", "VehicleModel": "Sierra 1500",
          "VehicleCondition": "New", "VehicleMsrp": 67445, "VehicleInternetPrice": 65194}
    vc.update(over)
    return vc


def _row(vc):
    return _map_vehicle_card(vc, "https://www.example.com", "d1", "Dealer", "https://www.example.com")


@pytest.mark.parametrize("msrp,adv", [(67445, 65194), (70390, 59330), (64000, 63009), (99000, 94424), (54000, 52085)])
def test_cosmos_price_is_internet_price_msrp_separate(msrp, adv):
    r = _row(_card(VehicleMsrp=msrp, VehicleInternetPrice=adv))
    assert r["price"] == adv
    assert r["msrp"] == msrp


def test_cosmos_no_internet_price_leaves_price_unset_not_msrp():
    for blank in (0, None, "", "0"):
        r = _row(_card(VehicleInternetPrice=blank))
        assert r["price"] is None
        assert r["msrp"] == 67445


def _dcom(**over) -> dict:
    item = {
        "vin": "1GTU9DED5NZ123456", "year": 2026, "make": "Chevrolet", "model": "Silverado", "condition": "New",
        "type": "new", "certified": False,
        "trackingPricing": {"internetPrice": "70390", "msrp": "$70,390"},
        "pricing": {
            "msrp": "70390", "retailPrice": "$70,390",
            "dprice": [
                {"label": "MSRP", "typeClass": "msrp", "value": "$70,390"},
                {"label": "Advertised Price", "typeClass": "askingPrice", "value": "$59,330", "isFinalPrice": True},
            ],
        },
    }
    item.update(over)
    return item


def test_dealer_com_final_price_row_beats_pre_offer_internet_price():
    item = _dcom()
    assert _extract_price_dealer_com(item) == 59330
    assert _extract_msrp_dealer_com(item) == 70390


def test_dealer_com_msrp_never_becomes_price():
    item = _dcom(trackingPricing={"msrp": "$70,390"}, pricing={"msrp": "70390", "retailPrice": "$70,390"})
    assert _extract_price_dealer_com(item) == 0
    item["trackingAttributes"] = [{"name": "msrp", "value": "70390"}]
    assert _extract_price_dealer_com(item) == 0


def test_dealer_com_used_retail_price_is_asking_price():
    used = _dcom(condition="Used", type="used", trackingPricing={}, pricing={"retailPrice": "$31,500"})
    assert _extract_price_dealer_com(used) == 31500


def test_dealer_com_final_flag_on_msrp_row_ignored():
    item = _dcom(pricing={"dprice": [{"typeClass": "msrp", "value": "$70,390", "isFinalPrice": True}]},
                 trackingPricing={"internetPrice": "65194"})
    assert _extract_price_dealer_com(item) == 65194


def test_dealer_com_parse_row_carries_both():
    row = dealer_dot_com._map_vehicle(_dcom(), "https://x.example.com", "d1", "D", "https://x.example.com")
    assert row["price"] == 59330 and row["msrp"] == 70390


def test_dealer_on_json_msrp_only_is_not_price():
    v = {"vin": "1FTEW1EP5LFA12345", "year": 2020, "make": "Ford", "model": "F-150", "msrp": 61000,
         "pricing": {"msrp": 61000}}
    args = ("https://d.example.com", "d1", "T", "https://d.example.com")
    r = dealer_on._map_vehicle(v, *args)
    assert not r.get("price")
    assert r["msrp"] == 61000
    r2 = dealer_on._map_vehicle(dict(v, internetPrice=58500), *args)
    assert r2["price"] == 58500 and r2["msrp"] == 61000
