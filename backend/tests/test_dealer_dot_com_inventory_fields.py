"""Dealer.com inventory JSON: description and Carfax without VDP."""

from __future__ import annotations

from backend.parsers import dealer_dot_com


def test_inventory_extended_description():
    obj = {
        "vin": "WBA43DA00SCU34030",
        "year": 2025,
        "make": "BMW",
        "model": "X5",
        "internetPrice": 75000,
        "extendedDescription": "Irvine BMW proudly presents this exclusive service loaner with premium features.",
    }
    row = dealer_dot_com._map_vehicle(obj, "https://www.irvinebmw.com", "irvinebmw-com", "Irvine BMW", "https://www.irvinebmw.com")
    assert row is not None
    assert "Irvine BMW proudly presents" in (row.get("description") or "")


def test_inventory_carfax_from_token():
    obj = {
        "vin": "1HGBH41JXMN109186",
        "year": 2021,
        "make": "Honda",
        "model": "Accord",
        "carfax_token": "abc123",
    }
    row = dealer_dot_com._map_vehicle(obj, "https://dealer.example.com", "d1", "Test", "https://dealer.example.com")
    assert row is not None
    assert row.get("carfax_url") == "https://vhr.carfax.com/main?vin=1HGBH41JXMN109186"


def test_inventory_carfax_from_badge_signal():
    obj = {
        "vin": "1HGBH41JXMN109186",
        "year": 2021,
        "make": "Honda",
        "model": "Accord",
        "callout": ["Carfax 1-Owner"],
    }
    row = dealer_dot_com._map_vehicle(obj, "https://dealer.example.com", "d1", "Test", "https://dealer.example.com")
    assert row is not None
    assert row.get("carfax_url") == "https://vhr.carfax.com/main?vin=1HGBH41JXMN109186"


# ── ws-inv-data routing whatever the provider label says (N8.2) ──────────────

import json
import os

import pytest

from backend.parsers import generic_json, parse

_SONIC_PAGE = os.path.join(os.path.dirname(__file__), "fixtures", "ddc_ws_inv_data_sonic_page.json")


def _sonic_page() -> dict:
    with open(_SONIC_PAGE, encoding="utf-8") as fh:
        return json.load(fh)


@pytest.mark.parametrize("provider", ["unknown", "generic_json", "", "dealer_dot_com", "carscommerce"])
def test_ws_inv_data_payload_stamps_rooftop_on_every_row_whatever_the_provider(provider) -> None:
    rows = parse(
        provider, _sonic_page(), base_url="https://www.bmwofmonrovia.net", dealer_id="bmwofmonrovia-net",
        dealer_name="BMW of Monrovia", dealer_url="https://www.bmwofmonrovia.net", rejected_out=[],
    )
    # the gate keeps this store's 9; the stamp must exist on all 24 before it decides
    rows_all = parse(
        provider, _sonic_page(), base_url="https://www.bmwofmonrovia.net", dealer_id="bmwofmonrovia-net",
        dealer_name="BMW of Monrovia", dealer_url="https://www.bmwofmonrovia.net",
    )
    assert len(rows_all) == 24 and sum(1 for r in rows_all if r.get("_rooftop")) == 24
    assert len(rows) == 9 and {r["_rooftop"]["key"] for r in rows} == {"sonicbmwmonrovia"}


def test_generic_json_does_not_claim_a_ws_inv_data_body() -> None:
    assert generic_json.detect(_sonic_page()) is None
    assert generic_json.parse(_sonic_page(), base_url="https://x.com", dealer_id="d") == []


def test_generic_json_still_claims_a_flat_feed() -> None:
    flat = {"vehicles": [{"vin": f"1HGCM82633A00{i:04d}", "year": 2020, "make": "Honda", "model": "Accord", "asking_price": 20000 + i} for i in range(4)]}
    assert len(generic_json.parse(flat, base_url="https://x.com", dealer_id="d")) == 4
    assert dealer_dot_com.detect_ws_inventory(flat) is False


def test_detect_ws_inventory_needs_the_dealer_com_shape() -> None:
    assert dealer_dot_com.detect_ws_inventory(_sonic_page()) is True
    assert dealer_dot_com.detect_ws_inventory({"inventory": [{"vin": "1HGCM82633A004352", "make": "Honda"}]}) is False
    assert dealer_dot_com.detect_ws_inventory({"inventory": [{"vin": "1HGCM82633A004352", "accountId": "a"}]}) is True
    assert dealer_dot_com.detect_ws_inventory([]) is False and dealer_dot_com.detect_ws_inventory({"inventory": []}) is False
