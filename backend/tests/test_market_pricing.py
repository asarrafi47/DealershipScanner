"""Unit tests for backend.intelligence.market_pricing.

Pure-Python coverage of the bucketing primitives and the offline scoring path
(``deal_score`` fed a pre-fetched band), so no database is required.
"""

from __future__ import annotations

from backend.intelligence import market_pricing as mp


# --- bucketing primitives -------------------------------------------------


def test_normalize_condition():
    assert mp.normalize_condition("New") == "new"
    assert mp.normalize_condition("new") == "new"
    assert mp.normalize_condition("Used") == "used"
    assert mp.normalize_condition("Certified Pre-Owned") == "used"
    assert mp.normalize_condition("Certified") == "used"
    assert mp.normalize_condition(None) == "used"
    assert mp.normalize_condition("") == "used"


def test_mileage_band_low_buckets():
    assert mp.mileage_band_low(0) == 0
    assert mp.mileage_band_low(9_999) == 0
    assert mp.mileage_band_low(10_000) == 10_000
    assert mp.mileage_band_low(34_500) == 30_000
    assert mp.mileage_band_low(95_000) == 90_000


def test_mileage_band_low_edge_cases():
    # cap collapses very-high mileage into one top band
    assert mp.mileage_band_low(250_000) == mp.MILEAGE_BAND_CAP
    assert mp.mileage_band_low(mp.MILEAGE_BAND_CAP) == mp.MILEAGE_BAND_CAP
    # missing / junk / negative -> floor band
    assert mp.mileage_band_low(None) == 0
    assert mp.mileage_band_low("nope") == 0
    assert mp.mileage_band_low(-5) == 0


def test_mileage_band_label():
    assert mp.mileage_band_label(0) == "0k-10k"
    assert mp.mileage_band_label(30_000) == "30k-40k"
    assert mp.mileage_band_label(mp.MILEAGE_BAND_CAP) == "200k+"


def test_config_key_normalizes_and_guards():
    assert mp.config_key(2026, "Toyota", "Camry", "SE") == (2026, "toyota", "camry", "se")
    assert mp.config_key("2026", " BMW ", "X3", None) == (2026, "bmw", "x3", "")
    # unusable configs
    assert mp.config_key(None, "Toyota", "Camry", "SE") is None
    assert mp.config_key(2026, "", "Camry", "SE") is None
    assert mp.config_key(2026, "Toyota", "", "SE") is None


# --- scoring (offline: pre-fetched band) ----------------------------------


def _band(median=30_000.0, n=40, dealers=6):
    return {
        "band_median": median,
        "band_p25": median * 0.95,
        "band_p75": median * 1.05,
        "sample_count": n,
        "dealer_count": dealers,
        "mileage_band_label": "0k-10k",
        "condition": "new",
    }


def test_deal_score_below_market():
    score = mp.deal_score({"price": 27_000}, band=_band(30_000))
    assert score["label"] == mp.LABEL_BELOW
    assert score["delta"] == -3_000.0
    assert score["pct_from_median"] == -10.0
    assert score["band_median"] == 30_000.0


def test_deal_score_above_market():
    score = mp.deal_score({"price": 33_000}, band=_band(30_000))
    assert score["label"] == mp.LABEL_ABOVE
    assert score["delta"] == 3_000.0
    assert score["pct_from_median"] == 10.0


def test_deal_score_at_market_within_threshold():
    # +3% is inside the +/-5% at-market band
    score = mp.deal_score({"price": 30_900}, band=_band(30_000))
    assert score["label"] == mp.LABEL_AT
    assert score["pct_from_median"] == 3.0


def test_deal_score_at_market_boundary_is_inclusive():
    # exactly +5% is still "at market" (strictly-greater trips "above")
    score = mp.deal_score({"price": 31_500}, band=_band(30_000))
    assert score["pct_from_median"] == 5.0
    assert score["label"] == mp.LABEL_AT


def test_deal_score_insufficient_when_no_band():
    score = mp.deal_score({"price": 30_000}, band=None)
    assert score["label"] == mp.LABEL_INSUFFICIENT
    assert score["delta"] is None
    assert score["pct_from_median"] is None
    assert score["band_median"] is None


def test_deal_score_insufficient_when_no_price():
    # a band exists but the listing has no usable price
    score = mp.deal_score({"price": None}, band=_band(30_000))
    assert score["label"] == mp.LABEL_INSUFFICIENT
    assert score["delta"] is None


def test_deal_score_custom_threshold_widens_at_market():
    # 8% would be "above" at the default 5%, but "at market" with a 10% window
    score = mp.deal_score({"price": 32_400}, band=_band(30_000), at_market_pct=10.0)
    assert score["pct_from_median"] == 8.0
    assert score["label"] == mp.LABEL_AT
