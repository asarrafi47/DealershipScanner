"""F12 (docs/DATA_COMPLETENESS_FINDINGS_2026_09_28.md): used / CPO rows stored
mileage = 0 when the feed had no odometer (parsers defaulted to 0, the upsert
stored 0, _mileage_blank accepted 0), so 1,307 rows hid the gap (crownlexus:
VDP shows 10,914 where 0 was stored). No odometer must be NULL, and 0 on a
used row is blank."""
from __future__ import annotations

import sqlite3
from unittest.mock import patch

import pytest

from backend.parsers import dealer_dot_com
from backend.parsers.base import extract_mileage
from backend.parsers.dealer_dot_com import _extract_mileage_dealer_com
from backend.utils.listing_completeness import (
    _mileage_blank,
    is_car_incomplete_for_public_listings,
    listing_missing_field_codes,
)


def test_base_extract_mileage_none_when_absent():
    assert extract_mileage({"vin": "X"}) is None
    assert extract_mileage({"odometer": ""}) is None
    assert extract_mileage({"odometer": "10,914"}) == 10914
    assert extract_mileage({"attributes": {"mileage": 5}}) == 5
    assert extract_mileage({"mileage": 0}) == 0  # an explicit 0 is a reading (new car)


def test_dealer_dot_com_mileage_none_when_no_odometer():
    assert _extract_mileage_dealer_com({"vin": "X", "trackingAttributes": [{"name": "engine", "value": "2.5L"}]}) is None
    assert _extract_mileage_dealer_com({"trackingAttributes": [{"name": "odometer", "value": "10914"}]}) == 10914
    assert _extract_mileage_dealer_com({"trackingAttributes": [{"name": "odometer", "value": ""}], "odometer": 0}) == 0
    row = dealer_dot_com._map_vehicle({"vin": "JTHY3JBH2P2000001", "year": 2023, "make": "Lexus", "model": "RX 350",
                                       "internetPrice": 47000, "condition": "Used", "type": "used"},
                                      "https://www.crownlexus.com", "crownlexus-com", "Crown Lexus", "https://www.crownlexus.com")
    assert row["mileage"] is None


def test_mileage_blank_treats_zero_on_used_as_missing():
    assert _mileage_blank({"mileage": 0, "condition": "Used"})
    assert _mileage_blank({"mileage": 0, "condition": "Certified Pre-Owned"})
    assert _mileage_blank({"mileage": 0, "condition": None, "is_cpo": 1})
    # IL-01 / D-N12: no odometer on a New, non-CPO row is not a gap
    assert not _mileage_blank({"mileage": None, "condition": "New"})
    assert not _mileage_blank({"mileage": 0, "condition": "New"})
    assert not _mileage_blank({"mileage": 0, "condition": None})
    assert not _mileage_blank({"mileage": 10914, "condition": "Used"})


# IL-01 (docs/NEXT_PHASES_PLAN_2026_10.md N2.1, owner decision D-N12 2026-10-18):
# a New, non-CPO car whose feed gives no odometer is complete; used / certified
# rows keep the F12 rule (NULL, empty and 0 are all missing).
@pytest.mark.parametrize(
    ("row", "missing"),
    [
        pytest.param({"mileage": None, "condition": "New"}, False, id="new-null"),
        pytest.param({"mileage": "", "condition": "New"}, False, id="new-empty"),
        pytest.param({"mileage": "  ", "condition": "new", "is_cpo": 0}, False, id="new-blank-str-cpo0"),
        pytest.param({"mileage": 0, "condition": "New"}, False, id="new-zero"),
        pytest.param({"mileage": 12, "condition": "New"}, False, id="new-12"),
        pytest.param({"mileage": None, "condition": "New", "is_cpo": 1}, True, id="new-cpo-null"),
        pytest.param({"mileage": None, "condition": "Certified Pre-Owned"}, True, id="cpo-null"),
        pytest.param({"mileage": None, "condition": "CPO"}, True, id="cpo-token-null"),
        pytest.param({"mileage": 0, "condition": "Used"}, True, id="used-zero"),
        pytest.param({"mileage": None, "condition": "Used"}, True, id="used-null"),
        pytest.param({"mileage": "", "condition": "Pre-Owned"}, True, id="preowned-empty"),
        pytest.param({"mileage": 10914, "condition": "Used"}, False, id="used-reading"),
        pytest.param({"mileage": None, "condition": None}, False, id="blank-condition-null"),
        pytest.param({"mileage": None, "condition": ""}, False, id="empty-condition-null"),
        pytest.param({"mileage": None}, False, id="no-condition-key-null"),
        pytest.param({"mileage": "n/a", "condition": "New"}, True, id="new-unparseable"),
    ],
)
def test_il01_mileage_blank_by_condition(row, missing):
    assert _mileage_blank(row) is missing


def _complete_row(**overrides):
    row = {"vin": "1HGCM82633A004352", "title": "2026 Honda Accord EX", "year": 2026, "make": "Honda",
           "model": "Accord", "trim": "EX", "price": 30000, "mileage": None, "transmission": "CVT",
           "drivetrain": "FWD", "fuel_type": "Gasoline", "exterior_color": "Crystal Black",
           "interior_color": "Gray", "image_url": "https://example.com/hero.jpg", "gallery": [],
           "body_style": "Sedan", "engine_description": "1.5L turbo I4", "condition": "New", "cylinders": 4}
    row.update(overrides)
    return row


@patch("backend.utils.listing_completeness.prepare_car_detail_context", return_value={"verified_specs": {}})
def test_il01_new_null_mileage_is_not_a_listing_code(_prep):
    assert "mileage" not in listing_missing_field_codes(_complete_row(), for_public_filter=True)
    assert "mileage" not in listing_missing_field_codes(_complete_row(), for_public_filter=False)
    assert is_car_incomplete_for_public_listings(_complete_row()) is False


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"condition": "New", "is_cpo": 1}, id="new-cpo"),
        pytest.param({"condition": "Used"}, id="used-null"),
        pytest.param({"condition": "Used", "mileage": 0}, id="used-zero"),
    ],
)
@patch("backend.utils.listing_completeness.prepare_car_detail_context", return_value={"verified_specs": {}})
def test_il01_used_and_cpo_keep_the_mileage_code(_prep, overrides):
    row = _complete_row(**overrides)
    assert "mileage" in listing_missing_field_codes(row, for_public_filter=True)
    assert is_car_incomplete_for_public_listings(row) is True


@patch("backend.utils.listing_completeness.prepare_car_detail_context", return_value={"verified_specs": {}})
def test_il01_blank_condition_null_mileage_flags_condition_not_mileage(_prep):
    codes = listing_missing_field_codes(_complete_row(condition=None, title="2026 Honda Accord"),
                                        for_public_filter=True)
    assert "mileage" not in codes


def _row(vin, mileage, condition="Used"):
    return {"vin": vin, "title": f"2023 Lexus RX 350 {vin[-3:]}", "year": 2023, "make": "Lexus", "model": "RX 350",
            "trim": "Premium", "price": 47000, "mileage": mileage, "dealer_name": "Crown Lexus", "dealer_id": "crownlexus-com",
            "dealer_url": "https://www.crownlexus.com/", "condition": condition, "fuel_type": "Gasoline"}


def test_upsert_stores_null_not_zero_and_keeps_real_readings(tmp_path, monkeypatch):
    import backend.db.inventory_db as inventory_db
    from backend.scanner.database import upsert_vehicles

    db_path = tmp_path / "f12.db"
    monkeypatch.setenv("INVENTORY_DB_PATH", str(db_path))
    monkeypatch.setattr(inventory_db, "DB_PATH", str(db_path))

    upsert_vehicles([_row("JTHY3JBH2P2000001", None), _row("JTHY3JBH2P2000002", 10914), _row("JTHY3JBH2P2000003", 0, "New")])
    conn = sqlite3.connect(str(db_path))
    got = dict(conn.execute("SELECT vin, mileage FROM cars").fetchall())
    assert got["JTHY3JBH2P2000001"] is None
    assert got["JTHY3JBH2P2000002"] == 10914
    assert got["JTHY3JBH2P2000003"] == 0
    conn.close()

    # rescan: no odometer must not wipe a real reading; an explicit 0 on the new car stays 0
    upsert_vehicles([_row("JTHY3JBH2P2000002", None), _row("JTHY3JBH2P2000003", 0, "New")])
    conn = sqlite3.connect(str(db_path))
    got = dict(conn.execute("SELECT vin, mileage FROM cars").fetchall())
    assert got["JTHY3JBH2P2000002"] == 10914 and got["JTHY3JBH2P2000003"] == 0
    # a stale default 0 on a used row is replaced by NULL when the feed still has no odometer
    conn.execute("UPDATE cars SET mileage = 0 WHERE vin = 'JTHY3JBH2P2000002'")
    conn.commit(); conn.close()
    upsert_vehicles([_row("JTHY3JBH2P2000002", None)])
    conn = sqlite3.connect(str(db_path))
    assert conn.execute("SELECT mileage FROM cars WHERE vin='JTHY3JBH2P2000002'").fetchone()[0] is None
    conn.close()


# D-N12 display: the car page shows no mileage line on a New, non-CPO car with no
# odometer figure; used and CPO cars keep "Mileage not listed".
def _render_car_page(monkeypatch, tmp_path, app_factory, **overrides):
    from backend.routes import cars_pages

    main = app_factory(ALLOW_DEFAULT_APP_USER="0", RATE_LIMIT_SQLITE_PATH=str(tmp_path / "rate_limits.db"))
    car = {"id": 77, "vin": "1HGCM82633A004352", "year": 2026, "make": "Honda", "model": "Accord",
           "title": "2026 Honda Accord EX", "trim": "EX", "price": 30000, "mileage": None, "condition": "New",
           "dealer_id": "demo", "inactive": 0, "gallery": [], "history_highlights": []}
    car.update(overrides)
    monkeypatch.setattr(cars_pages, "get_car_by_id", lambda *_a, **_k: dict(car))
    monkeypatch.setattr(cars_pages, "prepare_car_detail_context",
                        lambda _raw: {"verified_specs": {}, "gallery_images": [], "packages_panel_has_content": False})
    with main.app.test_client() as client:
        rv = client.get("/car/77")
    assert rv.status_code == 200
    return rv.get_data(as_text=True)


def _mileage_markers(html):
    return {
        "unlisted_hero": "Mileage not listed" in html,
        "glance_tile": "<dt>Mileage</dt>" in html,
        "spec_row": '<span class="spec-label">Mileage</span>' in html,
    }


def test_car_page_new_null_mileage_shows_no_mileage_line(monkeypatch, tmp_path, app_factory):
    html = _render_car_page(monkeypatch, tmp_path, app_factory)
    assert _mileage_markers(html) == {"unlisted_hero": False, "glance_tile": False, "spec_row": False}
    assert "car-detail-headline-mileage" not in html


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"condition": "Used", "year": 2021, "title": "2021 Honda Accord EX"}, id="used-null"),
        pytest.param({"condition": "Used", "year": 2021, "title": "2021 Honda Accord EX", "mileage": 0},
                     id="used-zero"),
        pytest.param({"condition": "New", "is_cpo": 1}, id="new-cpo-null"),
    ],
)
def test_car_page_used_and_cpo_keep_mileage_not_listed(monkeypatch, tmp_path, app_factory, overrides):
    html = _render_car_page(monkeypatch, tmp_path, app_factory, **overrides)
    markers = _mileage_markers(html)
    assert markers["unlisted_hero"] and markers["glance_tile"]


def test_car_page_new_with_odometer_still_shows_it(monkeypatch, tmp_path, app_factory):
    html = _render_car_page(monkeypatch, tmp_path, app_factory, mileage=12)
    markers = _mileage_markers(html)
    assert markers == {"unlisted_hero": False, "glance_tile": True, "spec_row": True}
    assert "car-detail-headline-mileage__value" in html
