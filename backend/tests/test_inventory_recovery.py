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
        base_url="https://www.example-dealer.com",
        dealer_id="example-com",
        dealer_name="Example Dealer",
        dealer_url="https://www.example-dealer.com",
        provider="dealer_dot_com",
        intercept_records=intercepts or [],
        vehicles=vehicles or [],
        parse_fn=lambda _raw: [],
    )


def test_unique_vin_count_dedupes():
    rows = [{"vin": "1" * 17}, {"vin": "1" * 17}, {"vin": "2" * 17}]
    assert unique_vin_count(rows) == 2


def test_prefer_new_vehicles_takes_larger_set():
    cur = [{"vin": "1" * 17}]
    new = [{"vin": "2" * 17}, {"vin": "3" * 17}]
    out, replaced = _prefer_new_vehicles(cur, new, "shopperexpress_api")
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
    rows = [{"vin": f"{i:017d}", "price": 21000} for i in range(53)]
    assert should_run_platform_recovery(_ctx(vehicles=rows, intercepts=intercepts)) is False


def test_should_run_when_rows_lack_price():
    """Plausible row counts with ~no prices mean the primary capture was junk."""
    hits = [{"vin": f"{i:017d}"} for i in range(20)]
    intercepts = [
        (
            "https://x.algolia.net/q",
            {"results": [{"hits": hits, "nbHits": 53}]},
        )
    ]
    rows = [{"vin": f"{i:017d}"} for i in range(53)]
    assert should_run_platform_recovery(_ctx(vehicles=rows, intercepts=intercepts)) is True


def test_prefer_new_vehicles_fills_prices_from_smaller_candidate():
    cur = [
        {"vin": "1" * 17, "price": None, "mileage": 0},
        {"vin": "2" * 17, "price": None},
        {"vin": "3" * 17, "price": None},
    ]
    cand = [
        {"vin": "1" * 17, "price": 45000, "exterior_color": "Carmine"},
        {"vin": "2" * 17, "price": 32000},
    ]
    out, replaced = _prefer_new_vehicles(cur, cand, "dealer_eprocess_json")
    assert replaced is False and len(out) == 3
    by_vin = {r["vin"]: r for r in out}
    assert by_vin["1" * 17]["price"] == 45000
    assert by_vin["1" * 17]["exterior_color"] == "Carmine"
    assert by_vin["2" * 17]["price"] == 32000
    assert by_vin["3" * 17].get("price") is None


def test_recovery_strategy_names_hint_selects_matching_strategy():
    from backend.scanner.inventory_recovery import recovery_strategy_names

    assert recovery_strategy_names({"dealer_eprocess"}) == ["dealer_eprocess_json"]
    # A hint no surviving strategy serves (Algolia analytics calls) keeps the full chain.
    assert recovery_strategy_names({"algolia", "dealer_inspire"}) == ["shopperexpress_api", "dealer_eprocess_json"]


def test_recovery_strategy_names_unknown_full_chain():
    from backend.scanner.inventory_recovery import RECOVERY_STRATEGY_ORDER, recovery_strategy_names

    assert recovery_strategy_names(set()) == list(RECOVERY_STRATEGY_ORDER)


def test_recovery_strategy_names_cached_winner_first():
    from backend.scanner.inventory_recovery import recovery_strategy_names

    chain = recovery_strategy_names(set(), cached_strategy="dealer_eprocess_json")
    assert chain == ["dealer_eprocess_json", "shopperexpress_api"]


def test_recovery_chain_is_http_only_and_page_free():
    import dataclasses

    from backend.scanner import inventory_recovery as ir

    assert set(ir.RECOVERY_STRATEGY_ORDER) == {"shopperexpress_api", "dealer_eprocess_json"}
    fields = {f.name for f in dataclasses.fields(ir.RecoveryContext)}
    assert "page" not in fields and "path_htmls" not in fields
    assert not hasattr(ir, "_html_and_next_data") and not hasattr(ir, "_parse_jsonld_listing_html")


def test_should_run_when_scrape_is_far_below_last_listed_count(monkeypatch):
    """Culver City Toyota 2026-09-22: 107 priced VINs captured vs 383 listed the day
    before; the priced-share check called it sufficient and skipped recovery."""
    import backend.scanner.inventory_recovery as ir

    rows = [{"vin": f"{i:017d}", "price": 21000} for i in range(107)]
    intercepts = [("https://www.example-dealer.com/api/inv", {"vehicles": rows})]
    monkeypatch.setattr(ir, "_last_known_active_vins", lambda dealer_id: 383)
    assert should_run_platform_recovery(_ctx(vehicles=rows, intercepts=intercepts)) is True
    monkeypatch.setattr(ir, "_last_known_active_vins", lambda dealer_id: 120)
    assert should_run_platform_recovery(_ctx(vehicles=rows, intercepts=intercepts)) is False

