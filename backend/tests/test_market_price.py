from __future__ import annotations

from backend.utils.market_price import (
    CohortIndex,
    _trim_key,
    attach_market_to_listing_cars,
    cohort_key,
    get_cohort_index,
    market_price_for_car,
    mileage_band,
    trim_price_stats_for_client,
)


def _sample_index(*, geo_label: str = "nationwide inventory") -> CohortIndex:
    groups = {
        ("toyota", "camry", "se", 2022, "0-25k"): [28000.0, 29000.0, 31000.0, 31000.0],
        ("toyota", "camry", "se", 2021, "25-50k"): [26000.0, 27000.0, 28000.0],
    }
    return CohortIndex(dict(groups), geo_label=geo_label, region_count=7)


def test_mileage_band_buckets():
    assert mileage_band(10000) == "0-25k"
    assert mileage_band(40000) == "25-50k"
    assert mileage_band(90000) == "75-100k"
    assert mileage_band(150000) == "100k+"
    assert mileage_band(None) == "unknown"


def test_bmw_trim_key_merges_drivetrain_and_dealer_labels():
    assert _trim_key("BMW", "3 Series", "330i xDrive") == ("bmw", "3 series", "330i")
    assert _trim_key("BMW", "X5", "xDrive40i") == ("bmw", "x5", "40i")
    assert _trim_key("BMW", "X5", "X5 40i") == ("bmw", "x5", "40i")


def test_market_price_below_average(monkeypatch):
    monkeypatch.setattr(
        "backend.utils.market_price.get_cohort_index",
        lambda **_: _sample_index(),
    )
    out = market_price_for_car(
        {
            "make": "Toyota",
            "model": "Camry",
            "trim": "SE",
            "year": 2022,
            "mileage": 12000,
            "price": 27000,
        }
    )
    assert out is not None
    assert out["vs_market"] == "below_market"
    assert out["avg_price"] == 29750.0
    assert out["delta_pct"] < 0
    assert out["mileage_band"] == "0-25k"
    assert "cohort_label" in out


def test_market_price_near_average(monkeypatch):
    monkeypatch.setattr(
        "backend.utils.market_price.get_cohort_index",
        lambda **_: _sample_index(),
    )
    out = market_price_for_car(
        {
            "make": "Toyota",
            "model": "Camry",
            "trim": "SE",
            "year": 2022,
            "mileage": 12000,
            "price": 30100,
        }
    )
    assert out is not None
    assert out["vs_market"] == "near_market"


def test_market_price_year_fallback(monkeypatch):
    monkeypatch.setattr(
        "backend.utils.market_price.get_cohort_index",
        lambda **_: _sample_index(),
    )
    out = market_price_for_car(
        {
            "make": "Toyota",
            "model": "Camry",
            "trim": "SE",
            "year": 2023,
            "mileage": 12000,
            "price": 32000,
        }
    )
    assert out is not None
    assert out["avg_price"] == 29750.0


def test_market_price_no_stats(monkeypatch):
    monkeypatch.setattr(
        "backend.utils.market_price.get_cohort_index",
        lambda **_: CohortIndex({}, geo_label="nationwide inventory", region_count=0),
    )
    assert market_price_for_car(
        {
            "make": "Toyota",
            "model": "Camry",
            "trim": "SE",
            "year": 2022,
            "mileage": 12000,
            "price": 30000,
        }
    ) is None


def test_trim_price_stats_for_client(monkeypatch):
    monkeypatch.setattr(
        "backend.utils.market_price.get_cohort_index",
        lambda **_: _sample_index(geo_label="within 50 mi of 28202"),
    )
    client = trim_price_stats_for_client(zip_code="28202", radius_miles=50)
    key = cohort_key("Toyota", "Camry", "SE", 2022, "0-25k")
    assert key in client["cohorts"]
    assert client["cohorts"][key]["avg_price_display"] == "$29,750"
    assert client["geo_label"] == "within 50 mi of 28202"
    assert client["min_samples"] >= 2


def test_attach_market_to_listing_cars(monkeypatch):
    monkeypatch.setattr(
        "backend.utils.market_price.get_cohort_index",
        lambda **_: _sample_index(),
    )
    cars = [
        {
            "make": "Toyota",
            "model": "Camry",
            "trim": "SE",
            "year": 2022,
            "mileage": 12000,
            "price": 33000,
        },
        {
            "make": "Honda",
            "model": "Accord",
            "trim": "Sport",
            "year": 2022,
            "mileage": 12000,
            "price": 28000,
        },
    ]
    attach_market_to_listing_cars(cars)
    assert cars[0]["market"]["vs_market"] == "above_market"
    assert cars[0]["market"]["avg_price_display"] == "$29,750"
    assert cars[1]["market"] is None


def test_geo_filter_excludes_distant_listings(monkeypatch):
    from backend.utils import market_price as mp

    mp._cohort_cache.clear()
    rows = [
        {
            "make": "Toyota",
            "model": "Camry",
            "trim": "SE",
            "year": 2022,
            "mileage": 10000,
            "price": 30000,
            "zip_code": "28202",
            "dealer_url": "",
        },
        {
            "make": "Toyota",
            "model": "Camry",
            "trim": "SE",
            "year": 2022,
            "mileage": 10000,
            "price": 31000,
            "zip_code": "30301",
            "dealer_url": "",
        },
    ]
    monkeypatch.setattr("backend.utils.market_price._load_active_listing_rows", lambda: rows)
    monkeypatch.setattr(
        "backend.db.geo.zip_to_coords",
        lambda z: (35.2271, -80.8431) if z == "28202" else (33.7490, -84.3880),
    )
    monkeypatch.setattr("backend.utils.market_price._load_dealer_geo", lambda: {})

    idx = get_cohort_index(zip_code="28202", radius_miles=25)
    assert idx.region_count == 1
    assert len(idx.groups.get(("toyota", "camry", "se", 2022, "0-25k"), [])) == 1
