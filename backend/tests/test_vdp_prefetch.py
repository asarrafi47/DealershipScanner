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
         "fuel_type": "Gasoline", "body_style": "Sedan", "trim": "EX",
         "description": "Dealer notes long enough to count as a real description paragraph.",
         "gallery": [f"https://img.example/{i}.jpg" for i in range(12)],
         "stock_number": "U1", "carfax_url": "https://www.carfax.com/vehiclehistory/x", "condition": "Used"}
    stats = asyncio.run(pf.http_prefetch_missing_fields([v]))
    assert stats["candidates"] == 0


def _complete_but_thin(**over):
    v = {"vin": "1HGBH41JXMN109186", "_detail_url": "https://d.example/car",
         "price": 21000, "exterior_color": "Blue", "interior_color": "Black",
         "engine_description": "2.0L I4", "transmission": "CVT", "drivetrain": "FWD",
         "fuel_type": "Gasoline", "body_style": "Sedan", "trim": "EX", "description": "", "gallery": [],
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



# ---------------------------------------------------------------- shared (stock-art) gallery URLs

CAI = "https://assets.cai-media-management.com/resize/1024x1024/common-vehicle-media/"


def _cai(tag: str) -> str:
    return CAI + f"{tag:0>8}-864e-4a57-9f7a-b6a691ec3267.jpg"


def test_shared_gallery_urls_are_dropped_across_the_batch_and_hero_follows():
    """autoWALL library art: one cai-media UUID on many cars of the same store.
    The URL looks exactly like a Team Velocity photo, so only the batch can tell."""
    stock = _cai("aaaaaaaa")
    twice = _cai("bbbbbbbb")  # on 3 cars = 2 others: kept
    vehicles = [
        {"vin": f"VIN{i}", "image_url": stock, "gallery": ["/static/placeholder.svg", stock, _cai(f"{i}")]}
        for i in range(5)
    ]
    for v in vehicles[:3]:
        v["gallery"].append(twice)
    hero_only = {"vin": "VIN9", "image_url": stock, "gallery": [stock]}
    vehicles.append(hero_only)
    stats = pf.drop_shared_gallery_urls(vehicles)
    assert stats == {"urls_dropped": 1, "vehicles_touched": 6, "removals": 6}
    for i, v in enumerate(vehicles[:5]):
        assert stock not in v["gallery"]
        assert v["gallery"][:2] == ["/static/placeholder.svg", _cai(f"{i}")]  # placeholder untouched, order kept
        assert v["image_url"] == _cai(f"{i}")
    assert all(twice in v["gallery"] for v in vehicles[:3])
    assert hero_only["gallery"] == [] and hero_only["image_url"] == stock  # nothing real left: hero unchanged


def test_unique_team_velocity_photos_survive_the_shared_check():
    vehicles = [
        {"vin": f"VIN{i}", "image_url": _cai(f"{i}00"), "gallery": [_cai(f"{i}{j:02d}") for j in range(12)]}
        for i in range(6)
    ]
    before = [list(v["gallery"]) for v in vehicles]
    assert pf.drop_shared_gallery_urls(vehicles) == {"urls_dropped": 0, "vehicles_touched": 0, "removals": 0}
    assert [v["gallery"] for v in vehicles] == before
    # a batch too small to prove sharing is left alone
    tiny = [{"gallery": [_cai("ffffffff")]} for _ in range(3)]
    assert pf.drop_shared_gallery_urls(tiny)["removals"] == 0


def test_prefetch_before_vdp_runs_the_shared_check_even_with_both_layers_off(monkeypatch):
    monkeypatch.setenv("SCANNER_VDP_DB_MERGE", "0")
    monkeypatch.setenv("SCANNER_VDP_HTTP_FIRST", "0")
    stock = _cai("aaaaaaaa")
    vehicles = [{"vin": f"VIN{i}", "image_url": stock, "gallery": [stock, _cai(f"{i}")]} for i in range(4)]
    out = asyncio.run(pf.prefetch_before_vdp(vehicles, "autowall-store-com", "autoWALL store"))
    assert out == {"gallery_shared": {"urls_dropped": 1, "vehicles_touched": 4, "removals": 4}}
    assert all(v["gallery"] == [_cai(f"{i}")] and v["image_url"] == _cai(f"{i}") for i, v in enumerate(vehicles))


# ---------------------------------------------------------------- galleries_extended counts real photos gained

LOGO = "https://widget.buyercall.com/offerlogix/img/CD-full-dark-transp.png"


def _photo_page(*urls: str) -> str:
    return "".join(f'<img src="{u}">' for u in urls) + "<!-- " + "pad " * 120 + "-->"


def test_gallery_added_counts_real_photos_not_list_length(monkeypatch):
    """[placeholder, logo] + one photo used to give len(merged) - len(cur) = -1,
    a truthy per-row stat; [placeholder, real1] + real2 gave 0 and was not counted."""
    monkeypatch.setenv("SCANNER_VDP_HTTP_FIRST_GALLERY_MIN", "8")
    photo, photo2 = "https://cdn.example/photos/1.jpg", "https://cdn.example/photos/2.jpg"
    v = {"vin": "1HGBH41JXMN109186", "gallery": ["/static/placeholder.svg", LOGO],
         "image_url": "/static/placeholder.svg", "description": "x" * 400}
    assert pf._apply_description_and_gallery(v, _photo_page(photo), "https://d.example/car") == 1
    assert v["gallery"] == [photo] and v["image_url"] == photo
    assert v["_gallery_http_prefetch_added"] == 1

    v2 = {"vin": "1HGBH41JXMN109186", "gallery": ["/static/placeholder.svg", photo],
          "image_url": photo, "description": "x" * 400}
    assert pf._apply_description_and_gallery(v2, _photo_page(photo2), "https://d.example/car") == 1
    assert v2["gallery"] == [photo, photo2] and v2["_gallery_http_prefetch_added"] == 1

    # the same page twice: nothing new -> no stat, gallery untouched
    v3 = {"vin": "1HGBH41JXMN109186", "gallery": [photo], "image_url": photo, "description": "x" * 400}
    assert pf._apply_description_and_gallery(v3, _photo_page(photo), "https://d.example/car") == 0
    assert "_gallery_http_prefetch_added" not in v3 and v3["gallery"] == [photo]


def test_galleries_extended_stat_from_placeholder_and_logo_rows(monkeypatch):
    monkeypatch.setenv("SCANNER_VDP_HTTP_FIRST_GALLERY_MIN", "8")
    photo = "https://cdn.example/photos/1.jpg"
    monkeypatch.setattr(pf, "_fetch_html", lambda url: _photo_page(photo))
    a = _complete_but_thin(description="x" * 60, gallery=["/static/placeholder.svg", LOGO], image_url="/static/placeholder.svg")
    b = _complete_but_thin(description="x" * 60, gallery=["/static/placeholder.svg", photo], image_url=photo)
    stats = asyncio.run(pf.http_prefetch_missing_fields([a, b]))
    assert stats["fetched"] == 2
    assert stats["galleries_extended"] == 1  # a gained a photo; b already had it
    assert a["gallery"] == [photo] and a["image_url"] == photo
    assert b["gallery"] == ["/static/placeholder.svg", photo]  # nothing new landed: the merge path did not run
    assert "_gallery_http_prefetch_added" not in a and "_gallery_http_prefetch_added" not in b
