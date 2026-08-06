"""
Cohort-relative price plausibility guard (backend/utils/price_plausibility.py)
and its wiring into serialize_car_for_api / serialize_car_for_listings_grid.

Fixture numbers below are the real cohort medians measured 2026-08-05 (see the
module docstring) — not invented — so these tests double as a record of the
audit that motivated the guard.
"""
from __future__ import annotations

import time

from backend.utils import price_plausibility as pp
from backend.utils.car_serialize import serialize_car_for_api, serialize_car_for_listings_grid


def _seed_cache(monkeypatch, trim_year=None, by_model=None):
    monkeypatch.setattr(pp, "_trim_year_medians", trim_year or {})
    monkeypatch.setattr(pp, "_model_medians", by_model or {})
    monkeypatch.setattr(pp, "_loaded_at", time.monotonic())


# ---------------------------------------------------------------------------
# implausible_price() unit tests
# ---------------------------------------------------------------------------

def test_flags_the_confirmed_broken_bronco(monkeypatch):
    """2026 Ford Bronco Base id 160558: $449,150 vs a 43-peer cohort median of
    $46,039 (9.8x) — the canonical broken row this guard was written for."""
    _seed_cache(
        monkeypatch,
        trim_year={("ford", "bronco", "base", 2026): (46039.0, 44)},  # 43 peers + this row
    )
    car = {"make": "Ford", "model": "Bronco", "trim": "Base", "year": 2026}
    assert pp.implausible_price(car, 449150.0) is True


def test_lone_exotic_with_no_peers_is_never_flagged(monkeypatch):
    """McLaren 765LT id 925357: $699,900 is the ONLY active 765LT in the fleet.
    No peers means no evidence either way — the guard must default to trusting
    the stored price rather than inventing a verdict."""
    _seed_cache(
        monkeypatch,
        trim_year={("mclaren", "765lt", "coupe", 2021): (699900.0, 1)},
        by_model={("mclaren", "765lt"): (699900.0, 1)},
    )
    car = {"make": "McLaren", "model": "765LT", "trim": "Coupe", "year": 2021}
    assert pp.implausible_price(car, 699900.0) is False


def test_legitimate_dealer_markup_stays_within_cohort_ratio(monkeypatch):
    """Mercedes-Benz of Ontario G 63 at $500,080 against a 110-car G-Class
    cohort median of $202,474.50 (~2.5x) — confirmed-real ADM (repeat-scraped,
    stable across three sweeps), not a parser artifact. Must not be flagged."""
    _seed_cache(
        monkeypatch,
        by_model={("mercedes-benz", "g-class"): (202474.5, 110)},
    )
    car = {"make": "Mercedes-Benz", "model": "G-Class", "trim": "AMG® G 63 SUV", "year": 2026}
    assert pp.implausible_price(car, 500080.0) is False


def test_thin_trim_year_cohort_falls_back_to_model_level(monkeypatch):
    """Cohort at the exact (year, make, model, trim) is too thin (< 5 peers) —
    fall back to the coarser (make, model) cohort instead of judging on noise."""
    _seed_cache(
        monkeypatch,
        trim_year={("ford", "bronco", "base", 2026): (449150.0, 1)},  # only this row itself
        by_model={("ford", "bronco"): (52813.5, 968)},
    )
    car = {"make": "Ford", "model": "Bronco", "trim": "Base", "year": 2026}
    assert pp.implausible_price(car, 449150.0) is True


def test_no_cohort_data_at_all_is_not_flagged(monkeypatch):
    _seed_cache(monkeypatch)
    car = {"make": "Rivian", "model": "R1T", "trim": "Adventure", "year": 2024}
    assert pp.implausible_price(car, 89999.0) is False


def test_missing_make_or_model_is_not_flagged(monkeypatch):
    _seed_cache(monkeypatch, by_model={("ford", "bronco"): (52813.5, 968)})
    assert pp.implausible_price({"model": "Bronco"}, 449150.0) is False
    assert pp.implausible_price({"make": "Ford"}, 449150.0) is False


def test_zero_or_missing_price_is_not_flagged(monkeypatch):
    _seed_cache(monkeypatch, by_model={("ford", "bronco"): (52813.5, 968)})
    car = {"make": "Ford", "model": "Bronco"}
    assert pp.implausible_price(car, 0.0) is False


# ---------------------------------------------------------------------------
# Wiring into the two public serializers
# ---------------------------------------------------------------------------

def test_serialize_car_for_api_suppresses_implausible_price(monkeypatch):
    monkeypatch.setattr(pp, "implausible_price", lambda car, price: price == 449150.0)
    row = {
        "vin": "1FMDE6AH7TLB38434",
        "year": 2026,
        "make": "Ford",
        "model": "Bronco",
        "trim": "Base",
        "price": 449150.0,
    }
    out = serialize_car_for_api(row, include_verified=False)
    assert out["price"] is None


def test_serialize_car_for_api_keeps_legitimate_exotic_price(monkeypatch):
    monkeypatch.setattr(pp, "implausible_price", lambda car, price: False)
    row = {
        "vin": "SBM14FCA9MW004349",
        "year": 2021,
        "make": "McLaren",
        "model": "765LT",
        "trim": "Coupe",
        "price": 699900.0,
    }
    out = serialize_car_for_api(row, include_verified=False)
    assert out["price"] == 699900.0


def test_serialize_car_for_listings_grid_suppresses_implausible_price(monkeypatch):
    monkeypatch.setattr(pp, "implausible_price", lambda car, price: price == 449150.0)
    row = {
        "id": 160558,
        "vin": "1FMDE6AH7TLB38434",
        "year": 2026,
        "make": "Ford",
        "model": "Bronco",
        "trim": "Base",
        "price": 449150.0,
    }
    out = serialize_car_for_listings_grid(row)
    assert out["price"] is None


def test_serialize_car_for_listings_grid_keeps_legitimate_exotic_price(monkeypatch):
    monkeypatch.setattr(pp, "implausible_price", lambda car, price: False)
    row = {
        "id": 925357,
        "vin": "SBM14FCA9MW004349",
        "year": 2021,
        "make": "McLaren",
        "model": "765LT",
        "trim": "Coupe",
        "price": 699900.0,
    }
    out = serialize_car_for_listings_grid(row)
    assert out["price"] == 699900.0
