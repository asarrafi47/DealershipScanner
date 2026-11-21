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


@pytest.fixture
def lab_pg(pg_db, monkeypatch):
    """pg_db wired as the lab database; the inventory DSN is blanked so the lab
    DSN is not 'the same database as the inventory' (a real lab is a second DB)."""
    monkeypatch.setenv("SCAN_LAB_DATABASE_URL", pg_db.dsn)
    monkeypatch.setenv("INVENTORY_DATABASE_URL", "")
    sl.invalidate_scan_lab_summary_cache()
    return pg_db


@pytest.mark.pg
def test_get_scan_lab_car_by_id_reads_lab_database(tiny_manifest, lab_pg):
    lab_pg.add_cars([{"dealer_id": "example-com", "dealer_name": "Test Dealer", "make": "BMW", "model": "X3"}])
    with sl.scan_lab_db_conn() as conn:
        cid = conn.execute("SELECT id FROM cars").fetchone()[0]
    car = sl.get_scan_lab_car_by_id(cid)
    assert car is not None
    assert car["make"] == "BMW"
    assert sl.get_scan_lab_car_by_id(cid + 999) is None


@pytest.mark.pg
def test_lab_list_summary_patch_and_counts(tiny_manifest, lab_pg):
    lab_pg.add_cars([
        {"dealer_id": "example-com", "dealer_name": "Test Dealer", "make": "BMW", "model": "X3"},
        {"dealer_id": "example-com", "dealer_name": "Test Dealer", "make": "Audi", "model": "A4"},
        {"dealer_id": "other-com", "dealer_name": "Other", "make": "BMW", "model": "M3"},
    ])
    page = sl.list_scan_lab_cars(limit=10)
    assert page["total"] == 2
    assert len(sl.list_scan_lab_cars(q="audi")["cars"]) == 1
    summary = sl.inventory_summary_for_manifest(force_refresh=True)
    assert summary["total_cars"] == 2
    car_id = page["cars"][0]["id"]
    assert sl.patch_scan_lab_car(car_id, {"trim": "xDrive"})
    assert sl.get_scan_lab_car_by_id(car_id)["trim"] == "xDrive"
    counts = sl.db_table_counts()
    assert counts["cars"] == 3
    assert counts["dealer_scan_profile"] >= -1  # absent table reports -1, never raises


@pytest.mark.pg
def test_list_scan_lab_cars_paginated_shape(tiny_manifest, lab_pg):
    page = sl.list_scan_lab_cars(limit=10, offset=0)
    assert "cars" in page
    assert "total" in page
    assert "limit" in page
    assert "offset" in page
    assert isinstance(page["cars"], list)
