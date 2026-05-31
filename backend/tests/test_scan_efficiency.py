"""Tests for scanner throughput helpers."""

from backend.scanner.scan_efficiency import (
    INVENTORY_PATHS_CORE,
    intercept_feed_is_sufficient,
    inventory_paths_for_dealer,
    scanner_fast_mode_enabled,
)


def test_inventory_paths_core_default():
    paths = inventory_paths_for_dealer({"provider": "dealer_dot_com"})
    assert paths == list(INVENTORY_PATHS_CORE)
    assert len(paths) == 3


def test_inventory_paths_extended_env(monkeypatch):
    monkeypatch.setenv("SCANNER_INVENTORY_PATHS", "extended")
    paths = inventory_paths_for_dealer({})
    assert len(paths) > 3


def test_intercept_feed_sufficient_with_total_count():
    body = {
        "inventory": [{"vin": f"VIN{i:013d}"} for i in range(100)],
        "totalCount": 100,
    }
    records = [("https://dealer.example/api/getInventory", body)]
    assert intercept_feed_is_sufficient(records, "https://dealer.example", 95)


def test_intercept_feed_sufficient_partial():
    body = {
        "inventory": [{"vin": f"VIN{i:013d}"} for i in range(20)],
        "totalCount": 200,
    }
    records = [("https://dealer.example/api/getInventory", body)]
    assert not intercept_feed_is_sufficient(records, "https://dealer.example", 20)


def test_scanner_fast_mode_env(monkeypatch):
    monkeypatch.setenv("SCANNER_FAST_MODE", "1")
    assert scanner_fast_mode_enabled()
