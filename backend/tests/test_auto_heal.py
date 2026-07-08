"""Quality-driven auto-heal: detection thresholds, targeting, and the price HTML parser."""
from __future__ import annotations

import json

import pytest

from backend.scanner.post_scan.auto_heal import (
    deficient_fields,
    run_auto_heal_for_dealer,
)
from backend.scanner.post_scan.coverage_report import compute_dealer_coverage
from backend.scanner.utils.vdp_spec_parse import parse_price_from_listing_html


def _lot(n_priced: int, n_unpriced: int) -> list[dict]:
    rows = []
    for i in range(n_priced):
        rows.append({"vin": f"1HGBH41JXMN1{i:05d}", "price": 25000, "exterior_color": "Blue",
                     "interior_color": "Black", "mileage": 100 + i, "trim": "EX"})
    for i in range(n_unpriced):
        rows.append({"vin": f"2HGBH41JXMN1{i:05d}", "price": None, "exterior_color": "Blue",
                     "interior_color": "Black", "mileage": 100 + i, "trim": "EX"})
    return rows


def test_deficient_fields_flags_low_price_coverage():
    cov = compute_dealer_coverage(_lot(2, 18), dealer_id="d")
    fields = deficient_fields(cov)
    assert "price" in fields
    assert "exterior_color" not in fields


def test_deficient_fields_clean_lot_flags_nothing():
    cov = compute_dealer_coverage(_lot(20, 0), dealer_id="d")
    assert deficient_fields(cov) == {}


def test_auto_heal_targets_only_deficient_vins(monkeypatch):
    calls = {}

    def _fake_gap_fill(vins):
        calls["vins"] = vins
        return {"vins_input": len(vins), "rows_patched": len(vins), "web_fetch_ok": 0,
                "structured_backfill_applied": 0}

    import backend.scanner.post_scan.gap_fill as gf

    monkeypatch.setattr(gf, "run_listing_gap_fill_for_vins", _fake_gap_fill)
    rows = _lot(2, 18)
    cov = compute_dealer_coverage(rows, dealer_id="d")
    out = run_auto_heal_for_dealer("d", "Dealer", cov, rows)
    assert out is not None
    assert out["vins_targeted"] == 18
    assert all(v.startswith("2HGBH41JXMN1") for v in calls["vins"])
    assert "price" in out["deficient_fields"]


def test_auto_heal_skips_small_lots_and_when_disabled(monkeypatch):
    rows = _lot(0, 5)
    cov = compute_dealer_coverage(rows, dealer_id="d")
    assert run_auto_heal_for_dealer("d", "Dealer", cov, rows) is None

    monkeypatch.setenv("SCANNER_AUTO_HEAL", "0")
    rows = _lot(2, 18)
    cov = compute_dealer_coverage(rows, dealer_id="d")
    assert run_auto_heal_for_dealer("d", "Dealer", cov, rows) is None


def test_parse_price_from_listing_html_jsonld():
    ld = {"@type": "Vehicle", "offers": {"@type": "Offer", "price": "27995.0"}}
    html = f'<script type="application/ld+json">{json.dumps(ld)}</script>'
    assert parse_price_from_listing_html(html) == 27995.0


def test_parse_price_from_listing_html_meta_fallback():
    html = '<meta itemprop="price" content="31,500">'
    assert parse_price_from_listing_html(html) == 31500.0


def test_parse_price_ignores_free_text_and_junk_amounts():
    # Dollar amounts in prose must never be mistaken for the price.
    html = "<p>Save $2,000 today! Doc fee $85. Call for price.</p>"
    assert parse_price_from_listing_html(html) is None
    # Clamp rejects implausible values even from structured data.
    ld = {"@type": "Vehicle", "offers": {"@type": "Offer", "price": "85"}}
    html2 = f'<script type="application/ld+json">{json.dumps(ld)}</script>'
    assert parse_price_from_listing_html(html2) is None
