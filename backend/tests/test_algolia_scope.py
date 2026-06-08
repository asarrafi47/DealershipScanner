"""Algolia production-index scoping for DealerInspire recovery."""

from __future__ import annotations

from backend.scanner.scrapers.algolia_scope import (
    encode_algolia_filter_param,
    infer_algolia_filters,
    manifest_algolia_scope,
    post_filter_algolia_hits,
)


def _crevier_sample_hits() -> list[dict]:
    return (
        [{"api_id": "MP7367", "in_transit": "On Lot", "make": "BMW", "type": "New"}] * 40
        + [{"api_id": "36721-01", "in_transit": "In-Transit", "make": "BMW", "type": "New"}] * 20
        + [{"api_id": "MP7366", "in_transit": "On Lot", "make": "MINI", "type": "Pre-Owned"}] * 5
        + [{"api_id": "MP7367", "in_transit": "On Lot", "make": "Honda", "type": "Pre-Owned"}] * 3
    )


def test_manifest_algolia_scope_on_lot():
    dealer = {"algolia_scope": "on_lot"}
    filt = infer_algolia_filters(
        dealer,
        index_name="crevierbmw-sbm0125_production_inventory",
        sample_hits=_crevier_sample_hits(),
    )
    assert filt == 'in_transit:"On Lot"'


def test_manifest_on_lot_without_probe_sample():
    dealer = {"algolia_scope": "on_lot"}
    filt = infer_algolia_filters(
        dealer,
        index_name="crevierbmw-sbm0125_production_inventory",
        sample_hits=[],
    )
    assert filt == 'in_transit:"On Lot"'


def test_manifest_algolia_filters_override():
    dealer = {"algolia_scope": "on_lot", "algolia_filters": "api_id:MP7367"}
    filt = infer_algolia_filters(
        dealer,
        index_name="crevierbmw-sbm0125_production_inventory",
        sample_hits=_crevier_sample_hits(),
    )
    assert filt == "api_id:MP7367"


def test_auto_scope_production_index(monkeypatch):
    monkeypatch.setenv("SCANNER_ALGOLIA_AUTO_SCOPE", "1")
    filt = infer_algolia_filters(
        None,
        index_name="crevierbmw-sbm0125_production_inventory",
        sample_hits=_crevier_sample_hits(),
    )
    assert filt == 'in_transit:"On Lot"'


def test_auto_scope_disabled(monkeypatch):
    monkeypatch.setenv("SCANNER_ALGOLIA_AUTO_SCOPE", "0")
    filt = infer_algolia_filters(
        None,
        index_name="crevierbmw-sbm0125_production_inventory",
        sample_hits=_crevier_sample_hits(),
    )
    assert filt == ""


def test_primary_rooftop_scope():
    dealer = {"algolia_scope": "primary_rooftop"}
    filt = infer_algolia_filters(
        dealer,
        index_name="example_production_inventory",
        sample_hits=_crevier_sample_hits(),
    )
    assert filt == "api_id:MP7367"


def test_post_filter_bmw_make_guard():
    dealer = {"name": "Crevier BMW", "url": "https://www.crevierbmw.com", "algolia_make_guard": True}
    hits = [
        {"make": "BMW", "vin": "1"},
        {"make": "Honda", "vin": "2"},
        {"make": "MINI", "vin": "3"},
    ]
    kept = post_filter_algolia_hits(hits, dealer)
    assert [h["vin"] for h in kept] == ["1", "3"]


def test_encode_algolia_filter_param_quotes():
    assert encode_algolia_filter_param('in_transit:"On Lot"') == "in_transit%3A%22On%20Lot%22"


def test_manifest_algolia_scope_invalid():
    assert manifest_algolia_scope({"algolia_scope": "not_real"}) is None
