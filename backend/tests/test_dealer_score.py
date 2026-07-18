"""Unit tests for backend.intelligence.dealer_score (pure functions only)."""

import math

import pytest

from backend.intelligence import dealer_score as ds
from backend.intelligence import market_pricing as mp


# --- reputation_score ---------------------------------------------------------
def test_reputation_score_none_without_rating():
    assert ds.reputation_score(None, 100) is None
    assert ds.reputation_score(0, 100) is None
    assert ds.reputation_score("n/a", 100) is None


def test_reputation_score_shrinks_thin_reviews():
    # A perfect 5.0 from 3 reviews should score below a 4.6 from 900.
    thin = ds.reputation_score(5.0, 3)
    deep = ds.reputation_score(4.6, 900)
    assert thin is not None and deep is not None
    assert thin < deep


def test_reputation_score_zero_reviews_is_prior():
    # With no reviews, the shrunk rating equals the prior mean regardless of stars.
    expected = round(ds.REP_PRIOR_MEAN / ds.REP_MAX_STARS * 100.0, 2)
    assert ds.reputation_score(5.0, 0) == expected
    assert ds.reputation_score(1.0, 0) == expected


def test_reputation_score_bounded_0_100():
    assert 0.0 <= ds.reputation_score(5.0, 100000) <= 100.0
    assert 0.0 <= ds.reputation_score(1.0, 100000) <= 100.0


# --- pricing_score ------------------------------------------------------------
def test_pricing_score_none_passthrough():
    assert ds.pricing_score(None) is None


def test_pricing_score_direction_and_pivots():
    assert ds.pricing_score(0.0) == 50.0
    assert ds.pricing_score(-ds.PRICING_PIVOT_PCT) == 100.0
    assert ds.pricing_score(ds.PRICING_PIVOT_PCT) == 0.0
    # Cheaper than market always beats pricier than market.
    assert ds.pricing_score(-5.0) > ds.pricing_score(5.0)


def test_pricing_score_clamped():
    assert ds.pricing_score(-100.0) == 100.0
    assert ds.pricing_score(100.0) == 0.0


# --- inventory_score ----------------------------------------------------------
def test_inventory_score_zero_and_monotonic():
    assert ds.inventory_score(0) == 0.0
    assert ds.inventory_score(10) < ds.inventory_score(100) < ds.inventory_score(1000)


def test_inventory_score_bounded():
    assert 0.0 <= ds.inventory_score(100000) <= 100.0


# --- aggregate_pricing --------------------------------------------------------
def _deal(pct, delta, label):
    return {"pct_from_median": pct, "delta": delta, "label": label}


def test_aggregate_pricing_skips_insufficient():
    scores = [
        _deal(-10.0, -2000.0, mp.LABEL_BELOW),
        _deal(2.0, 400.0, mp.LABEL_AT),
        {"label": mp.LABEL_INSUFFICIENT, "pct_from_median": None, "delta": None},
    ]
    out = ds.aggregate_pricing(scores)
    assert out["scored_listings"] == 2
    assert out["priced_listings"] == 3
    assert out["median_pct_from_market"] == pytest.approx(-4.0)
    assert out["below_market_share"] == pytest.approx(0.5)


def test_aggregate_pricing_all_insufficient():
    scores = [{"label": mp.LABEL_INSUFFICIENT, "pct_from_median": None, "delta": None}]
    out = ds.aggregate_pricing(scores)
    assert out["median_pct_from_market"] is None
    assert out["below_market_share"] is None
    assert out["scored_listings"] == 0
    assert out["priced_listings"] == 1


def test_aggregate_pricing_empty():
    out = ds.aggregate_pricing([])
    assert out["median_pct_from_market"] is None
    assert out["scored_listings"] == 0


# --- composite_score ----------------------------------------------------------
def test_composite_all_present_is_weighted_mean():
    got = ds.composite_score(80.0, 60.0, 40.0)
    w = ds.DEFAULT_WEIGHTS
    expected = (w["reputation"] * 80 + w["pricing"] * 60 + w["inventory"] * 40) / sum(w.values())
    assert got == pytest.approx(round(expected, 2))


def test_composite_missing_reputation_renormalizes():
    # Drops reputation, blends pricing+inventory on their own weights.
    got = ds.composite_score(None, 60.0, 40.0)
    w = ds.DEFAULT_WEIGHTS
    expected = (w["pricing"] * 60 + w["inventory"] * 40) / (w["pricing"] + w["inventory"])
    assert got == pytest.approx(round(expected, 2))


def test_composite_only_inventory():
    assert ds.composite_score(None, None, 42.0) == pytest.approx(42.0)


def test_composite_all_missing_is_none():
    assert ds.composite_score(None, None, None) is None


# --- band matching reuses pricing primitives ----------------------------------
def test_band_key_matches_lookup_shape():
    car = {"year": 2021, "make": "Toyota", "model": "Camry", "trim": "LE", "mileage": 34_500, "condition": "Used"}
    key = ds._band_key(car)
    # Same tuple order / normalization the pricing lookup keys on.
    assert key == (
        "toyota",
        "camry",
        "le",
        2021,
        mp.normalize_condition("Used"),
        mp.mileage_band_low(34_500),
    )


def test_band_key_none_when_unkeyable():
    assert ds._band_key({"make": "Toyota", "model": "Camry"}) is None  # no year


def test_score_listings_uses_matched_band():
    car = {"year": 2021, "make": "toyota", "model": "camry", "trim": "le",
           "mileage": 30_000, "condition": "used", "price": 20_000.0}
    key = ds._band_key(car)
    band_index = {
        key: {
            "band_median": 25_000.0,
            "band_p25": 23_000.0,
            "band_p75": 27_000.0,
            "sample_count": 12,
            "dealer_count": 4,
            "mileage_band_label": "30k-40k",
            "condition": "used",
        }
    }
    scored = ds.score_listings([car], band_index)
    assert len(scored) == 1
    assert scored[0]["label"] == mp.LABEL_BELOW  # 20k vs 25k median
    assert scored[0]["pct_from_median"] == pytest.approx(-20.0)


def test_score_listings_insufficient_without_band():
    car = {"year": 2021, "make": "toyota", "model": "camry", "trim": "le",
           "mileage": 30_000, "condition": "used", "price": 20_000.0}
    scored = ds.score_listings([car], {})
    assert scored[0]["label"] == mp.LABEL_INSUFFICIENT


# --- inventory_mix ------------------------------------------------------------
def test_inventory_mix_counts_new_used_and_models():
    listings = [
        {"make": "Toyota", "model": "Camry", "condition": "New", "price": 30000},
        {"make": "Toyota", "model": "Camry", "condition": "Used", "price": 20000},
        {"make": "Honda", "model": "Civic", "condition": "Certified", "price": 0},
    ]
    mix = ds.inventory_mix(listings)
    assert mix["size"] == 3
    assert mix["new_count"] == 1
    assert mix["used_count"] == 2
    assert mix["distinct_models"] == 2
    assert mix["priced_count"] == 2


# --- build_dealer_score integration (pure, in-memory) -------------------------
def test_build_dealer_score_end_to_end():
    car = {"dealer_id": "acme-com", "dealer_name": "Acme Motors", "year": 2021,
           "make": "toyota", "model": "camry", "trim": "le", "mileage": 30_000,
           "condition": "used", "price": 20_000.0}
    key = ds._band_key(car)
    band_index = {key: {"band_median": 25_000.0, "band_p25": 23_000.0, "band_p75": 27_000.0,
                        "sample_count": 12, "dealer_count": 4, "mileage_band_label": "30k-40k",
                        "condition": "used"}}
    reputation = {"name": "Acme Motors", "google_rating": 4.6, "google_review_count": 500, "oem_brand": "toyota"}
    r = ds.build_dealer_score("acme-com", [car], reputation, band_index)

    assert r["dealer_id"] == "acme-com"
    assert r["dealer_name"] == "Acme Motors"
    assert r["oem_brand"] == "toyota"
    assert r["reputation"]["score"] is not None and r["reputation"]["matched"] is True
    assert r["pricing_aggressiveness"]["median_pct_from_market"] == pytest.approx(-20.0)
    assert r["inventory"]["size"] == 1
    assert r["composite"] is not None
    # Cheap + well-reviewed => a strong composite.
    assert r["composite"] > 50


def test_build_dealer_score_no_reputation_still_scores():
    car = {"dealer_id": "acme-com", "dealer_name": "Acme", "year": 2021, "make": "toyota",
           "model": "camry", "trim": "le", "mileage": 30_000, "condition": "used", "price": 20_000.0}
    r = ds.build_dealer_score("acme-com", [car], None, {})
    assert r["reputation"]["score"] is None
    assert r["reputation"]["matched"] is False
    # No reputation, no scoreable band -> composite rests on inventory alone.
    assert r["composite"] == pytest.approx(r["inventory"]["score"])
    assert r["dealer_name"] == "Acme"
