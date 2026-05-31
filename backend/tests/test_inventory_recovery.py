"""Tests for multi-strategy inventory recovery helpers."""

from __future__ import annotations

from backend.scanner.inventory_recovery import (
    RecoveryContext,
    should_run_platform_recovery,
    unique_vin_count,
    _prefer_new_vehicles,
)


def _ctx(*, vehicles=None, intercepts=None):
    return RecoveryContext(
        page=None,
        base_url="https://www.example-dealer.com",
        dealer_id="example-com",
        dealer_name="Example Dealer",
        dealer_url="https://www.example-dealer.com",
        provider="dealer_dot_com",
        intercept_records=intercepts or [],
        path_htmls=[],
        vehicles=vehicles or [],
        parse_fn=lambda _raw: [],
    )


def test_unique_vin_count_dedupes():
    rows = [{"vin": "1" * 17}, {"vin": "1" * 17}, {"vin": "2" * 17}]
    assert unique_vin_count(rows) == 2


def test_prefer_new_vehicles_takes_larger_set():
    cur = [{"vin": "1" * 17}]
    new = [{"vin": "2" * 17}, {"vin": "3" * 17}]
    out, replaced = _prefer_new_vehicles(cur, new, "dealer_inspire_algolia")
    assert replaced is True
    assert unique_vin_count(out) == 2


def test_should_run_when_empty():
    assert should_run_platform_recovery(_ctx(vehicles=[])) is True


def test_should_run_when_algolia_hint_thin():
    hits = [{"vin": f"{i:017d}"} for i in range(20)]
    intercepts = [
        (
            "https://x.algolia.net/q",
            {"results": [{"hits": hits, "nbHits": 387}]},
        )
    ]
    thin = [{"vin": f"{i:017d}"} for i in range(5)]
    assert should_run_platform_recovery(_ctx(vehicles=thin, intercepts=intercepts)) is True


def test_should_run_algolia_floor_when_feed_looks_small():
    hits = [{"vin": f"{i:017d}"} for i in range(20)]
    intercepts = [
        (
            "https://x.algolia.net/q",
            {"results": [{"hits": hits, "nbHits": 53}]},
        )
    ]
    rows = [{"vin": f"{i:017d}"} for i in range(53)]
    assert should_run_platform_recovery(_ctx(vehicles=rows, intercepts=intercepts)) is True
