"""Tests for shared parser helpers (vehicle list discovery)."""

from __future__ import annotations

from backend.parsers.base import extract_price, find_vehicle_list


def test_find_vehicle_list_prefers_more_vins() -> None:
    small = [{"vin": "1" * 17}, {"vin": "2" * 17}, {"vin": "3" * 17}]
    big = [{"vin": "A" * 17} for _ in range(10)]
    payload = {"compare": small, "inventory": big}
    got = find_vehicle_list(payload)
    assert got is big


def test_find_vehicle_list_tiebreak_longer_list() -> None:
    a = [{"vin": "1" * 17}, {"vin": "2" * 17}, {"vin": "3" * 17}, {"x": 1}]
    b = [{"vin": "1" * 17}, {"vin": "2" * 17}, {"vin": "3" * 17}]
    payload = {"first": a, "second": b}
    got = find_vehicle_list(payload)
    assert got is a


def test_find_vehicle_list_nested_returns_none_when_below_min() -> None:
    assert find_vehicle_list({"items": [{"vin": "1" * 17}, {"vin": "2" * 17}]}, min_vin_count=3) is None


def test_extract_price_homenet_selling_and_internet_price() -> None:
    """CDK / homenet vehicle API often uses sellingPrice and internet_Price (underscore)."""
    assert extract_price({"sellingPrice": 12000}) == 12000.0
    assert extract_price({"internet_Price": 12999.5}) == 12999.5
    assert extract_price({"internet_price": 11_500}) == 11500.0
