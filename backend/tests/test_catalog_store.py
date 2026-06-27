"""Tests for catalog_store option-row adapter."""

from __future__ import annotations

from backend.enrichment import catalog_store as cs


def test_fetch_catalog_option_rows_empty_without_db(monkeypatch) -> None:
    monkeypatch.setattr(cs, "_fetch_rows", lambda *args, **kwargs: [])
    assert cs.fetch_catalog_option_rows(2024, "Toyota", "Camry") == []


def test_fetch_catalog_option_rows_builds_csv_shape(monkeypatch) -> None:
    def fake_fetch(sql: str, params: tuple) -> list[dict]:
        if "catalog_trims" in sql:
            return [{"id": 1, "trim": "LE", "make": "Toyota", "model": "Camry"}]
        if "catalog_packages" in sql:
            return [{"package_name": "Tech Pkg", "feature_name": "Blind spot monitor"}]
        if "catalog_options" in sql:
            return [{"option_name": "Floor mats", "description": "All-weather"}]
        if "catalog_exterior_colors" in sql:
            return [{"color_name": "White"}]
        return []

    monkeypatch.setattr(cs, "_fetch_rows", fake_fetch)
    rows = cs.fetch_catalog_option_rows(2024, "Toyota", "Camry")
    assert len(rows) == 1
    assert rows[0]["Trim"] == "LE"
    assert "Tech Pkg" in rows[0]["Packages"]
    assert "Blind spot" in rows[0]["packageDetails"]
