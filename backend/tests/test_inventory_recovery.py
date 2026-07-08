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
    out, replaced = _prefer_new_vehicles(cur, cand, "jsonld_listing_html")
    assert replaced is False and len(out) == 3
    by_vin = {r["vin"]: r for r in out}
    assert by_vin["1" * 17]["price"] == 45000
    assert by_vin["1" * 17]["exterior_color"] == "Carmine"
    assert by_vin["2" * 17]["price"] == 32000
    assert by_vin["3" * 17].get("price") is None


def test_jsonld_parser_dealer_group_shape():
    """Dealer-group platform LD: float-string price, ImageObject, vehicle-level
    itemCondition, structured brand/model/vehicleModelDate, name in itemOffered."""
    import json
    from types import SimpleNamespace

    from backend.scanner.inventory_recovery import _parse_jsonld_listing_html

    ld = {
        "@context": "http://schema.org/",
        "@type": "Vehicle",
        "vehicleIdentificationNumber": "WP0BB2A98PS233151",
        "model": "911",
        "brand": "Porsche",
        "vehicleModelDate": "2023",
        "itemCondition": "https://schema.org/UsedCondition",
        "color": "Carmine",
        "vehicleInteriorColor": "Black",
        "mileageFromOdometer": {"@type": "QuantitativeValue", "value": "24179", "unitCode": "SMI"},
        "image": {"@type": "ImageObject", "contentUrl": "https://cdn.example.com/car.jpg"},
        "offers": {
            "@type": "Offer",
            "itemOffered": {"@type": "Thing", "name": "2023  Porsche 911 4S"},
            "price": "162113.0",
            "priceCurrency": "USD",
            "url": "https://www.example-dealer.com/viewdetails/used/wp0bb2a98ps233151/x",
        },
    }
    html = f'<html><script type="application/ld+json">{json.dumps(ld)}</script></html>'
    ctx = SimpleNamespace(
        path_htmls=[html],
        dealer_name="Example Dealer",
        dealer_url="https://www.example-dealer.com",
        dealer_id="example-com",
    )
    rows = _parse_jsonld_listing_html(ctx)
    assert len(rows) == 1
    v = rows[0]
    assert v["price"] == 162113
    assert v["year"] == 2023
    assert v["make"] == "Porsche"
    assert v["model"] == "911"
    assert v["mileage"] == 24179
    assert v["condition"] == "Used"
    assert v["exterior_color"] == "Carmine"
    assert v["interior_color"] == "Black"
    assert v["image_url"] == "https://cdn.example.com/car.jpg"


def test_recovery_strategy_names_algolia_only():
    from backend.scanner.inventory_recovery import recovery_strategy_names

    chain = recovery_strategy_names({"algolia", "dealer_inspire"})
    # jsonld_listing_html reads already-captured page HTML, so it stays in the
    # chain regardless of platform hints (like html_next_data).
    assert chain == ["dealer_inspire_algolia", "html_next_data", "jsonld_listing_html"]


def test_recovery_strategy_names_unknown_full_chain():
    from backend.scanner.inventory_recovery import RECOVERY_STRATEGY_ORDER, recovery_strategy_names

    assert recovery_strategy_names(set()) == list(RECOVERY_STRATEGY_ORDER)


def test_recovery_strategy_names_cached_winner_first():
    from backend.scanner.inventory_recovery import recovery_strategy_names

    chain = recovery_strategy_names(
        {"algolia", "dealer_inspire"},
        cached_strategy="dealer_inspire_algolia",
    )
    assert chain[0] == "dealer_inspire_algolia"
