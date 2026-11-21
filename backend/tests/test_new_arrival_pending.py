"""N6.4 (D-N13): the new-arrival "pending" class, its index columns, the display and the report.

Day-boundary tests pin the window definition: (the row's latest ``scraped_at`` - the gap's
``first_seen_at``) <= N days, never the wall clock. The template test pins the design rule:
"Details coming" is one plain line of text, never a card, box or pill.
"""
from __future__ import annotations

import re
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from backend.db import incomplete_listings_db as ild
from backend.scripts.incomplete_report import compute_report, format_report
from backend.utils.listing_completeness import (
    inside_new_arrival_window,
    is_pending_arrival,
)

_ROOT = Path(__file__).resolve().parents[2]
T0 = datetime(2026, 9, 25, 12, 0, tzinfo=timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.replace(tzinfo=None).isoformat() + "Z"


# -- window definition ---------------------------------------------------------


def test_window_boundary_is_inclusive_at_n_days():
    assert inside_new_arrival_window(_iso(T0), _iso(T0 + timedelta(days=3)), 3) is True
    assert inside_new_arrival_window(_iso(T0), _iso(T0 + timedelta(days=3, seconds=1)), 3) is False
    assert inside_new_arrival_window(_iso(T0), _iso(T0 + timedelta(days=2, hours=23)), 3) is True
    assert inside_new_arrival_window(_iso(T0), _iso(T0), 3) is True


def test_window_follows_the_n_argument():
    last = _iso(T0 + timedelta(days=5))
    assert inside_new_arrival_window(_iso(T0), last, 3) is False
    assert inside_new_arrival_window(_iso(T0), last, 5) is True


def test_window_is_measured_against_the_latest_scrape_not_the_wall_clock():
    # Both dates are weeks in the past: a wall-clock window would put no car inside it.
    assert inside_new_arrival_window("2026-09-25T10:00:00Z", "2026-09-27T10:00:00Z", 3) is True


@pytest.mark.parametrize(
    "first,last",
    [
        (None, "2026-09-27"),
        ("2026-09-25", None),
        ("", "2026-09-27"),
        ("garbage", "2026-09-27"),
        ("2026-09-27", "2026-09-25"),  # scrape older than first sighting: not an arrival
    ],
)
def test_missing_or_inconsistent_timestamps_are_outside_the_window(first, last):
    assert inside_new_arrival_window(first, last, 3) is False


def test_mixed_timestamp_shapes_compare():
    assert inside_new_arrival_window("2026-09-25 12:00:00", "2026-09-26T12:00:00+00:00", 3) is True
    assert inside_new_arrival_window(T0, T0 + timedelta(days=1), 3) is True


def test_pending_needs_a_price_or_photo_only_gap_inside_the_window():
    first, last = _iso(T0), _iso(T0 + timedelta(days=1))
    assert is_pending_arrival(["price"], first, last) is True
    assert is_pending_arrival(["price", "images"], first, last) is True
    assert is_pending_arrival(["price", "trim"], first, last) is False
    assert is_pending_arrival(["trim"], first, last) is False
    assert is_pending_arrival([], first, last) is False
    assert is_pending_arrival(["price"], first, _iso(T0 + timedelta(days=4))) is False


# -- index columns, sync and readers -------------------------------------------


@pytest.fixture
def index_db(tmp_path, monkeypatch):
    monkeypatch.setattr(ild, "DB_PATH", str(tmp_path / "incomplete_listings.db"))
    monkeypatch.setattr(ild, "ensure_incomplete_index_built", lambda: None)
    return tmp_path


def _sync(car, missing):
    with patch("backend.db.inventory_db.get_car_by_id", return_value=car), patch.object(
        ild, "listing_missing_field_codes", return_value=missing
    ):
        ild.sync_incomplete_listing_for_car_id(car["id"])


def _row(car_id):
    conn = ild.get_conn()
    try:
        return conn.execute(
            "SELECT first_seen_at, last_scraped_at, scans_survived, pending FROM incomplete_listings "
            "WHERE car_id = ?", (car_id,)
        ).fetchone()
    finally:
        conn.close()


def _car(car_id, first, last):
    return {"id": car_id, "vin": f"VIN{car_id}", "listing_active": 1,
            "first_seen_at": _iso(first), "scraped_at": _iso(last)}


def test_sync_records_first_seen_scans_and_pending(index_db):
    _sync(_car(1, T0, T0 + timedelta(days=1)), ["price"])
    assert _row(1) == (_iso(T0), _iso(T0 + timedelta(days=1)), 1, 1)
    # Same scrape again: not a new scan.
    _sync(_car(1, T0, T0 + timedelta(days=1)), ["price"])
    assert _row(1)[2] == 1
    # A later scan that still has the gap.
    _sync(_car(1, T0, T0 + timedelta(days=2)), ["price"])
    assert _row(1)[2:] == (2, 1)
    # Past the window: incomplete, and it keeps counting.
    _sync(_car(1, T0, T0 + timedelta(days=4)), ["price"])
    assert _row(1)[2:] == (3, 0)


def test_non_price_gap_is_never_pending(index_db):
    _sync(_car(2, T0, T0), ["price", "trim"])
    assert _row(2)[3] == 0


def test_readers_split_incomplete_from_pending(index_db):
    _sync(_car(1, T0, T0 + timedelta(days=1)), ["price"])  # pending
    _sync(_car(2, T0, T0 + timedelta(days=9)), ["price"])  # incomplete
    assert ild.get_incomplete_car_id_set() == {2}
    assert ild.get_pending_car_id_set() == {1}
    assert ild.is_car_pending(1) is True and ild.is_car_pending(2) is False
    assert ild.get_missing_field_codes_for_car_id(1) == []
    assert ild.get_missing_field_codes_for_car_id(2) == ["price"]


def test_old_four_column_index_table_gains_the_columns(index_db):
    conn = sqlite3.connect(ild.DB_PATH)
    conn.execute("CREATE TABLE incomplete_listings (car_id INTEGER PRIMARY KEY NOT NULL, vin TEXT NOT NULL, "
                 "missing_fields_json TEXT NOT NULL, updated_at TEXT NOT NULL)")
    conn.execute("INSERT INTO incomplete_listings VALUES (7, 'V', '[\"trim\"]', 'x')")
    conn.commit()
    conn.close()
    assert ild.get_incomplete_car_id_set() == {7}
    assert _row(7) == (None, None, 1, 0)


def test_rebuild_rows_carry_scans_forward_and_mark_pending():
    cars = [_car(1, T0, T0 + timedelta(days=1)), _car(2, T0, T0 + timedelta(days=8))]
    with patch("backend.enrichment.knowledge_engine.merge_verified_specs", return_value={}), patch(
        "backend.enrichment.knowledge_engine.prime_vpic_cache"
    ), patch.object(ild, "listing_missing_field_codes", return_value=["price"]):
        rows = ild._missing_rows(cars, "2026-10-01T00:00:00+00:00", {2: (4, _iso(T0 + timedelta(days=7)))})
    by_id = {r[0]: r for r in rows}
    assert by_id[1][4:] == (_iso(T0), _iso(T0 + timedelta(days=1)), 1, 1)
    assert by_id[2][6:] == (5, 0)  # prior 4 scans + this newer scrape; outside the window


def test_rules_version_was_bumped_so_the_fleet_rebuilds():
    assert ild.INDEX_RULES_VERSION.endswith("n6.4")


# -- the report ----------------------------------------------------------------


def test_report_splits_inside_and_outside_rates():
    cars = [
        (1, _iso(T0), _iso(T0 + timedelta(days=1))),   # inside, pending
        (2, _iso(T0), _iso(T0 + timedelta(days=2))),   # inside, complete
        (3, _iso(T0), _iso(T0 + timedelta(days=3))),   # inside, incomplete (trim gap)
        (4, _iso(T0), _iso(T0 + timedelta(days=9))),   # outside, incomplete
        (5, _iso(T0), _iso(T0 + timedelta(days=9))),   # outside, complete
        (6, _iso(T0), _iso(T0 + timedelta(days=9))),   # outside, complete
    ]
    rep = compute_report(cars, {1: 1, 3: 0, 4: 0}, 3)
    assert rep["inside"] == {"cars": 3, "incomplete": 1, "pending": 1, "rate_pct": 66.67,
                             "incomplete_only_pct": 33.33}
    assert rep["outside"]["cars"] == 3 and rep["outside"]["incomplete"] == 1
    assert rep["outside"]["rate_pct"] == 33.33
    text = format_report(rep)
    assert "inside" in text and "outside" in text


# -- design rule: no box around "details coming" -------------------------------


def test_car_page_line_is_one_plain_paragraph():
    src = (_ROOT / "frontend" / "templates" / "car.html").read_text(encoding="utf-8")
    lines = [ln.strip() for ln in src.splitlines() if "Details coming" in ln]
    assert lines == ['<p class="car-detail-location-note">Details coming</p>']
    assert "{% if details_pending %}" in src


def test_grid_card_line_is_one_plain_paragraph():
    js = (_ROOT / "frontend" / "static" / "listings" / "card.js").read_text(encoding="utf-8")
    hits = [ln.strip() for ln in js.splitlines() if "Details coming" in ln]
    assert hits == ['? `<p class="result-meta">Details coming</p>`']
    box = re.compile(r"class=\"[^\"]*(card|box|pill|badge|chip|panel|tag)[^\"]*\"[^>]*>Details coming")
    assert not box.search(js)
