"""EIA cached fuel lookup for TCO."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

import backend.main as main_mod


@pytest.fixture()
def live_gas_prices_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    payload = {
        "source": "eia_api_v2",
        "national": {
            "region_name": "National Average",
            "regular": 3.24,
            "premium": 4.01,
            "electricity_rate": 0.176,
        },
        "states": {
            "NC": {
                "region_name": "North Carolina",
                "regular": 3.12,
                "premium": 3.98,
                "electricity_rate": 0.16,
            },
            "CA": {
                "region_name": "California",
                "regular": 4.55,
                "premium": 6.26,
                "electricity_rate": 0.3335,
            },
        },
    }
    path = tmp_path / "live_gas_prices.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setattr(main_mod, "_LIVE_GAS_PRICES_PATH", path)
    monkeypatch.setattr(main_mod, "_live_gas_prices_cache", None)
    monkeypatch.setattr(main_mod, "_live_gas_prices_cache_mtime", None)
    return path


def test_resolve_live_gas_lookup_state_tier(live_gas_prices_file: Path) -> None:
    resolved = main_mod._resolve_live_gas_lookup("CA", "premium")
    assert resolved is not None
    rate, region = resolved
    assert rate == 6.26
    assert region == "California"


def test_resolve_live_gas_lookup_falls_back_to_national(live_gas_prices_file: Path) -> None:
    resolved = main_mod._resolve_live_gas_lookup("TX", "regular")
    assert resolved is not None
    rate, region = resolved
    assert rate == 3.24
    assert region == "National Average"


def test_resolve_live_electricity_lookup_state(live_gas_prices_file: Path) -> None:
    rate, region = main_mod._resolve_live_electricity_lookup("NC")
    assert rate == 0.16
    assert region == "North Carolina"


def test_api_fuel_lookup_route(live_gas_prices_file: Path) -> None:
    client = main_mod.app.test_client()
    resp = client.get("/api/fuel/lookup?state=NC&fuel_tier=regular")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["rate"] == 3.12
    assert data["electricity_rate"] == 0.16
    assert data["region_name"] == "North Carolina"
    assert data["state"] == "NC"


def test_api_fuel_lookup_zip_code(live_gas_prices_file: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import backend.db.geo as geo_mod

    monkeypatch.setattr(
        geo_mod,
        "us_postal_meta_for_zip",
        lambda _zip: {"postal_code": "90210", "state_code": "CA", "place_name": "Beverly Hills"},
    )
    client = main_mod.app.test_client()
    resp = client.get("/api/fuel/lookup?zip_code=90210&fuel_tier=premium")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["rate"] == 6.26
    assert data["electricity_rate"] == 0.3335
    assert data["region_name"] == "California"
    assert data["state"] == "CA"
    assert data["zip_code"] == "90210"


def test_api_fuel_lookup_missing_file_uses_fallback(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    missing = tmp_path / "missing.json"
    monkeypatch.setattr(main_mod, "_LIVE_GAS_PRICES_PATH", missing)
    monkeypatch.setattr(main_mod, "_live_gas_prices_cache", None)
    monkeypatch.setattr(main_mod, "_live_gas_prices_cache_mtime", None)
    client = main_mod.app.test_client()
    resp = client.get("/api/fuel/lookup?state=NC&fuel_tier=regular")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["rate"] == 3.94
    assert data["electricity_rate"] == 0.176
    assert data["source"] == "eia_fallback_2026"
