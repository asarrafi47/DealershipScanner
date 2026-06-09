"""EPA original range resolution for EV battery intelligence."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.intelligence import ev_range_estimates as ev_range


def test_parse_epa_range_from_text() -> None:
    assert ev_range.parse_epa_range_from_text("330 miles EPA estimated range") == 330
    assert ev_range.parse_epa_range_from_text("227 mi range, dual electric motors") == 227
    assert ev_range.parse_epa_range_from_text("no range here") is None


def test_score_epa_model_match() -> None:
    score = ev_range._score_epa_model_match(
        listing_model="Model Y",
        listing_trim="Long Range",
        epa_model="Model Y Long Range AWD",
    )
    assert score >= 30


def test_lookup_epa_range_miles_from_cache(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cache = {
        "source": "test",
        "rows": [
            {
                "year": 2023,
                "make": "Tesla",
                "make_n": "tesla",
                "model": "Model Y Long Range AWD",
                "range_miles": 330,
            }
        ],
    }
    path = tmp_path / "epa_ev_range_miles.json"
    path.write_text(json.dumps(cache), encoding="utf-8")
    monkeypatch.setattr(ev_range, "_EPA_RANGE_CACHE_PATH", path)
    ev_range._load_epa_ev_range_rows.cache_clear()
    assert ev_range.lookup_epa_range_miles(2023, "Tesla", "Model Y", "Long Range") == 330


def test_lookup_epa_range_miles_token_match(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cache = {
        "source": "test",
        "rows": [
            {
                "year": 2024,
                "make": "Audi",
                "make_n": "audi",
                "model": "Q4 Sportback 55 e-tron quattro",
                "range_miles": 258,
            }
        ],
    }
    path = tmp_path / "epa_ev_range_miles.json"
    path.write_text(json.dumps(cache), encoding="utf-8")
    monkeypatch.setattr(ev_range, "_EPA_RANGE_CACHE_PATH", path)
    ev_range._load_epa_ev_range_rows.cache_clear()
    assert (
        ev_range.lookup_epa_range_miles(2024, "Audi", "Q4 e-tron Sportback", "Premium 55 quattro")
        == 258
    )


def test_range_miles_from_trim_adds(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    trim_dir = tmp_path / "trim_adds_by_year"
    trim_dir.mkdir()
    payload = {
        "adds_by_trim": {
            "GT": [
                "98.8-kWh Extended Range battery with at least 235 miles EPA-estimated range",
            ]
        }
    }
    (trim_dir / "2021__ford__mustangmache.json").write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setattr(ev_range, "TRIM_ADDS_BY_YEAR_DIR", trim_dir)
    car = {
        "year": 2021,
        "make": "Ford",
        "model": "Mustang Mach-E",
        "trim": "GT",
    }
    assert ev_range._range_miles_from_trim_adds(car) == 235


def test_resolve_factory_epa_range_prefers_listing_text(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ev_range, "lookup_epa_range_miles", lambda *a, **k: 300)
    car = {
        "fuel_type": "Electric",
        "year": 2023,
        "make": "Tesla",
        "model": "Model Y",
        "trim": "Long Range",
        "description": "Dual Motor AWD. 330 miles EPA estimated range.",
    }
    assert ev_range.resolve_factory_epa_range(car) == 330
