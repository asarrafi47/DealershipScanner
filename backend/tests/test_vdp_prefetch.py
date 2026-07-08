"""Pre-VDP prefetch: DB carry-forward of immutable fields and HTTP structured-data fill."""
from __future__ import annotations

import asyncio
import json
import sqlite3
from pathlib import Path

import pytest

from backend.db import inventory_db
from backend.db.inventory_db import init_inventory_db
from backend.scanner.vdp import prefetch as pf


@pytest.fixture()
def seeded_db(monkeypatch, tmp_path: Path):
    dbp = tmp_path / "inv_prefetch.db"
    monkeypatch.setattr(inventory_db, "DB_PATH", str(dbp))
    init_inventory_db()
    conn = sqlite3.connect(str(dbp))
    conn.execute(
        """INSERT INTO cars (vin, year, make, model, trim, price, mileage,
               exterior_color, interior_color, transmission, drivetrain, body_style,
               fuel_type, cylinders, mpg_city, description, gallery, dealer_id, listing_active)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)""",
        (
            "1HGBH41JXMN109186", 2022, "Honda", "Accord", "EX-L", 24500, 31000,
            "Platinum White", "Black", "CVT", "FWD", "Sedan",
            "Gasoline", 4, 30, "A well-kept Accord with service records and more text here.",
            json.dumps(["https://img.example/1.jpg", "https://img.example/2.jpg"]),
            "test-dealer-com",
        ),
    )
    conn.commit()
    conn.close()
    return dbp


def test_db_merge_fills_immutable_fields_but_never_price_or_mileage(seeded_db):
    v = {"vin": "1HGBH41JXMN109186", "price": None, "mileage": 0,
         "exterior_color": "", "transmission": None, "gallery": []}
    stats = pf.merge_known_fields_from_db([v], "test-dealer-com")
    assert stats["vins_known"] == 1 and stats["vehicles_touched"] == 1
    assert v["exterior_color"] == "Platinum White"
    assert v["transmission"] == "CVT"
    assert v["drivetrain"] == "FWD"
    assert v["cylinders"] == 4
    assert len(v["gallery"]) == 2
    # The accuracy contract: mutable fields are never carried forward.
    assert v["price"] is None
    assert v["mileage"] == 0


def test_db_merge_never_overwrites_fresh_values(seeded_db):
    v = {"vin": "1HGBH41JXMN109186", "exterior_color": "Repainted Red", "transmission": "Manual"}
    pf.merge_known_fields_from_db([v], "test-dealer-com")
    assert v["exterior_color"] == "Repainted Red"
    assert v["transmission"] == "Manual"


def test_db_merge_is_dealer_scoped(seeded_db):
    v = {"vin": "1HGBH41JXMN109186", "exterior_color": ""}
    stats = pf.merge_known_fields_from_db([v], "other-dealer-com")
    assert stats["vins_known"] == 0
    assert v["exterior_color"] == ""


def test_http_prefetch_fills_from_structured_page(monkeypatch):
    ld = {"@type": "Vehicle", "offers": {"@type": "Offer", "price": "19995.0"}}
    html = (
        f'<script type="application/ld+json">{json.dumps(ld)}</script>'
        "<li>Exterior Color: Deep Blue</li><li>Interior Color: Gray</li>"
    )
    monkeypatch.setattr(pf, "_fetch_html", lambda url: html)
    v = {"vin": "1HGBH41JXMN109186", "_detail_url": "https://d.example/car", "price": None,
         "exterior_color": "", "interior_color": ""}
    stats = asyncio.run(pf.http_prefetch_missing_fields([v]))
    assert stats["fetched"] == 1
    assert v["price"] == 19995
    assert v["exterior_color"] == "Deep Blue"
    assert v["interior_color"] == "Gray"


def test_http_prefetch_fetch_failure_changes_nothing(monkeypatch):
    monkeypatch.setattr(pf, "_fetch_html", lambda url: None)
    v = {"vin": "1HGBH41JXMN109186", "_detail_url": "https://d.example/car", "price": None}
    stats = asyncio.run(pf.http_prefetch_missing_fields([v]))
    assert stats["fetched"] == 0
    assert v["price"] is None


def test_http_prefetch_skips_complete_vehicles(monkeypatch):
    def _boom(url):  # pragma: no cover - must not be reached
        raise AssertionError("complete vehicle must not be fetched")

    monkeypatch.setattr(pf, "_fetch_html", _boom)
    v = {"vin": "1HGBH41JXMN109186", "_detail_url": "https://d.example/car",
         "price": 21000, "exterior_color": "Blue", "interior_color": "Black",
         "engine_description": "2.0L I4", "transmission": "CVT", "drivetrain": "FWD",
         "fuel_type": "Gasoline", "body_style": "Sedan"}
    stats = asyncio.run(pf.http_prefetch_missing_fields([v]))
    assert stats["candidates"] == 0


def test_env_gates_disable_layers(monkeypatch, seeded_db):
    monkeypatch.setenv("SCANNER_VDP_DB_MERGE", "0")
    monkeypatch.setenv("SCANNER_VDP_HTTP_FIRST", "0")
    v = {"vin": "1HGBH41JXMN109186", "exterior_color": "", "price": None,
         "_detail_url": "https://d.example/car"}
    out = asyncio.run(pf.prefetch_before_vdp([v], "test-dealer-com", "Test Dealer"))
    assert out is None
    assert v["exterior_color"] == ""
