"""Dev scan-lab helpers (manifest summary, car listing)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.dev import scan_lab as sl


@pytest.fixture
def tiny_manifest(tmp_path: Path, monkeypatch):
    mp = tmp_path / "manifest.json"
    mp.write_text(
        json.dumps(
            [
                {"name": "Test Dealer", "url": "https://example.com", "dealer_id": "example-com"},
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("SCAN_LAB_MANIFEST", str(mp))
    sl.invalidate_scan_lab_summary_cache()
    return mp


def test_manifest_lab_config(tiny_manifest):
    cfg = sl.manifest_lab_config()
    assert cfg["dealer_count"] == 1
    assert cfg["dealer_ids"] == ["example-com"]
    assert cfg["zip_code"] == "92694"


def test_inventory_summary_empty_manifest(tmp_path, monkeypatch):
    mp = tmp_path / "empty.json"
    mp.write_text("[]", encoding="utf-8")
    monkeypatch.setenv("SCAN_LAB_MANIFEST", str(mp))
    sl.invalidate_scan_lab_summary_cache()
    summary = sl.inventory_summary_for_manifest(force_refresh=True)
    assert summary["total_cars"] == 0
    assert summary["dealer_count"] == 0


def test_list_scan_lab_cars_paginated_shape(tiny_manifest):
    page = sl.list_scan_lab_cars(limit=10, offset=0)
    assert "cars" in page
    assert "total" in page
    assert "limit" in page
    assert "offset" in page
    assert isinstance(page["cars"], list)
