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
         "fuel_type": "Gasoline", "body_style": "Sedan",
         "description": "Dealer notes long enough to count as a real description paragraph.",
         "gallery": [f"https://img.example/{i}.jpg" for i in range(12)],
         "stock_number": "U1", "carfax_url": "https://www.carfax.com/vehiclehistory/x", "condition": "Used"}
    stats = asyncio.run(pf.http_prefetch_missing_fields([v]))
    assert stats["candidates"] == 0


def _complete_but_thin(**over):
    v = {"vin": "1HGBH41JXMN109186", "_detail_url": "https://d.example/car",
         "price": 21000, "exterior_color": "Blue", "interior_color": "Black",
         "engine_description": "2.0L I4", "transmission": "CVT", "drivetrain": "FWD",
         "fuel_type": "Gasoline", "body_style": "Sedan", "description": "", "gallery": [],
         "stock_number": "U1", "carfax_url": "https://www.carfax.com/vehiclehistory/x", "condition": "Used"}
    v.update(over)
    return v


def test_http_prefetch_fills_description_and_gallery_from_html(monkeypatch):
    """The two fields that drove 92% of browser VDP visits in the 2026-09-22 lab."""
    imgs = "".join(f'<img src="https://cdn.example/photos/{i}.jpg">' for i in range(8))
    html = (
        "<html><body><h2>Description</h2><p>This one-owner sedan comes with heated seats, "
        "a clean history report and a full set of service records from our shop.</p>"
        f"<div class='gallery'>{imgs}</div></body></html>"
    )
    monkeypatch.setattr(pf, "_fetch_html", lambda url: html)
    v = _complete_but_thin()
    stats = asyncio.run(pf.http_prefetch_missing_fields([v]))
    assert stats["candidates"] == 1 and stats["fetched"] == 1
    assert stats["descriptions_filled"] == 1
    assert "heated seats" in v["description"]
    assert stats["galleries_extended"] == 1
    assert len(v["gallery"]) == 8 and all(u.startswith("https://") for u in v["gallery"])


def test_http_prefetch_gallery_only_grows(monkeypatch):
    html = ('<img src="https://cdn.example/a.jpg"><img src="https://cdn.example/b.jpg">'
            + "<!-- " + "pad " * 120 + "-->")  # the harvester ignores pages under 400 bytes
    monkeypatch.setattr(pf, "_fetch_html", lambda url: html)
    v = _complete_but_thin(description="x" * 60, gallery=["https://cdn.example/a.jpg", "https://cdn.example/z.jpg"])
    asyncio.run(pf.http_prefetch_missing_fields([v]))
    assert v["gallery"][:2] == ["https://cdn.example/a.jpg", "https://cdn.example/z.jpg"]
    assert "https://cdn.example/b.jpg" in v["gallery"]


def test_http_prefetch_short_description_is_not_taken(monkeypatch):
    html = "<h2>Description</h2><p>Nice car.</p>"
    monkeypatch.setattr(pf, "_fetch_html", lambda url: html)
    v = _complete_but_thin(gallery=[f"https://img.example/{i}.jpg" for i in range(12)])
    stats = asyncio.run(pf.http_prefetch_missing_fields([v]))
    assert stats["descriptions_filled"] == 0
    assert v["description"] == ""


def test_env_gates_disable_layers(monkeypatch, seeded_db):
    monkeypatch.setenv("SCANNER_VDP_DB_MERGE", "0")
    monkeypatch.setenv("SCANNER_VDP_HTTP_FIRST", "0")
    v = {"vin": "1HGBH41JXMN109186", "exterior_color": "", "price": None,
         "_detail_url": "https://d.example/car"}
    out = asyncio.run(pf.prefetch_before_vdp([v], "test-dealer-com", "Test Dealer"))
    assert out is None
    assert v["exterior_color"] == ""


def test_db_merge_never_carries_cylinders_that_contradict_new_engine_text(seeded_db):
    # Prior row says 4 cylinders; the fresh scrape's own text says V6. The stored
    # count is enrichment-written on thousands of rows, so the text wins and the
    # value is left for the read path (VIN decode / text) to settle.
    v = {"vin": "1HGBH41JXMN109186", "engine_description": "3.5L V6", "cylinders": None}
    pf.merge_known_fields_from_db([v], "test-dealer-com")
    assert v.get("cylinders") is None


def test_db_merge_still_carries_cylinders_that_agree(seeded_db):
    v = {"vin": "1HGBH41JXMN109186", "engine_description": "2.0L I4", "cylinders": None}
    pf.merge_known_fields_from_db([v], "test-dealer-com")
    assert v["cylinders"] == 4


def test_http_prefetch_stops_after_three_silent_attempts_on_a_host(monkeypatch):
    """A host that returns no status and no body (tarpit) must not consume the wall
    clock: Bill Luke 0/800 pages in 300 s on 2026-09-24."""
    calls: list[str] = []

    def _silent(url):
        calls.append(url)
        return None

    monkeypatch.setattr(pf, "_fetch_html", _silent)
    monkeypatch.setattr(pf, "_HTTP_CONCURRENCY", 1)
    vs = [{"vin": f"1HGBH41JXMN10{i:04d}", "_detail_url": f"https://tarpit.example/car{i}", "price": None} for i in range(12)]
    stats = asyncio.run(pf.http_prefetch_missing_fields(vs))
    assert stats["fetched"] == 0
    assert stats.get("dead_host") == "tarpit.example"
    assert stats.get("dead_host_skipped", 0) >= 6
    assert len(calls) < 12


def test_http_prefetch_does_not_kill_a_host_that_answers(monkeypatch):
    seen = {"n": 0}

    def _flaky(url):
        seen["n"] += 1
        return None if seen["n"] % 2 else "<html>" + "x" * 500 + "</html>"

    monkeypatch.setattr(pf, "_fetch_html", _flaky)
    monkeypatch.setattr(pf, "_HTTP_CONCURRENCY", 1)
    vs = [{"vin": f"1HGBH41JXMN10{i:04d}", "_detail_url": f"https://ok.example/car{i}", "price": None} for i in range(10)]
    stats = asyncio.run(pf.http_prefetch_missing_fields(vs))
    assert "dead_host" not in stats
    assert stats["fetched"] >= 4

