"""KBB IDWS client: normalization + patch building (HTTP mocked)."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from backend.kbb_idws import (
    extract_valuation_numbers,
    patch_from_refresh_result,
    refresh_kbb_for_vehicle_row,
)


def test_extract_valuation_numbers_nested():
    payload = {
        "pricing": {
            "usedCarFairPurchasePrice": 28450,
            "usedCarFairMarketRangeLow": 27000,
            "usedCarFairMarketRangeHigh": 29900,
            "privatePartyValue": 26200,
            "tradeInValue": 24800,
        }
    }
    n = extract_valuation_numbers(payload)
    assert n["fair_purchase"] == 28450
    assert n["range_low"] == 27000
    assert n["range_high"] == 29900
    assert n["private_party"] == 26200
    assert n["trade_in"] == 24800


def test_extract_valuation_numbers_swaps_inverted_range():
    n = extract_valuation_numbers({"a": {"fairMarketRangeLow": 31000, "fairMarketRangeHigh": 29000}})
    assert n["range_low"] == 29000
    assert n["range_high"] == 31000


def test_refresh_kbb_for_vehicle_row_ok(monkeypatch):
    monkeypatch.setenv("KBB_API_KEY", "test-secret-key")
    monkeypatch.setenv("KBB_DEFAULT_ZIP", "92618")

    decode_body = {"vehicleId": 424242, "year": 2022}
    values_body = {
        "usedCarFairPurchasePrice": 25000,
        "usedCarFairMarketRangeLow": 24000,
        "usedCarFairMarketRangeHigh": 26000,
    }

    def fake_get(url, timeout=30):
        m = MagicMock()
        m.json = MagicMock()
        if "/vehicle/vin/" in url:
            m.status_code = 200
            m.json.return_value = decode_body
        elif "/vehicle/values" in url:
            m.status_code = 200
            m.json.return_value = values_body
        else:
            m.status_code = 404
            m.json.return_value = {"error": "unexpected", "url": url}
        return m

    row = {"vin": "1HGBH41JXMN109186", "mileage": 45000, "zip_code": None}

    with patch("backend.kbb_idws.requests.Session.get", side_effect=fake_get):
        res = refresh_kbb_for_vehicle_row(row)

    assert res.ok
    assert res.message == "ok"
    patch_d = patch_from_refresh_result(res)
    assert patch_d["kbb_fair_purchase"] == 25000
    assert patch_d["kbb_range_low"] == 24000
    assert patch_d["kbb_range_high"] == 26000
    assert patch_d["kbb_fetched_at"]
    snap = json.loads(patch_d["kbb_snapshot_json"])
    assert snap["vin"] == "1HGBH41JXMN109186"
    assert snap["normalized"]["fair_purchase"] == 25000


def test_refresh_without_api_key():
    row = {"vin": "1HGBH41JXMN109186", "mileage": 1, "zip_code": "92618"}
    res = refresh_kbb_for_vehicle_row(row)
    assert not res.ok
    assert res.message == "kbb_api_key_missing"


@pytest.mark.parametrize(
    "vin",
    ("", "SHORT", "1HGBH41JXMN10918!", "1HGBH41JXMN10918"),
)
def test_refresh_invalid_vin(monkeypatch, vin):
    monkeypatch.setenv("KBB_API_KEY", "x")
    res = refresh_kbb_for_vehicle_row({"vin": vin, "mileage": 1, "zip_code": "92618"})
    assert not res.ok
    assert res.message == "invalid_vin"


def test_serialize_car_kbb_vs_listing():
    from backend.utils.car_serialize import serialize_car_for_api

    row = {
        "id": 1,
        "vin": "1HGBH41JXMN109186",
        "title": "Test",
        "year": 2020,
        "make": "Honda",
        "model": "Accord",
        "trim": None,
        "price": 23500,
        "mileage": 1000,
        "kbb_fair_purchase": 25000,
        "kbb_range_low": 24000,
        "kbb_range_high": 26000,
        "kbb_fetched_at": "2026-04-20T12:00:00+00:00",
    }
    out = serialize_car_for_api(row, include_verified=False)
    assert out.get("kbb") is not None
    assert out["kbb"]["vs_listing_code"] == "below_kbb_range"
    assert "Below" in (out["kbb"]["vs_listing_label"] or "")
