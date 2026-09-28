"""assess() must judge and reconcile on rows stamped to the store, not the raw feed (2026-09-28).

MB Beverly Hills' recipe answers with the whole Fletcher Jones group feed (1,521 rows);
the rooftop gate keeps 2. The old assess called that run "inaccurate" (accepted) on the
feed count and reconcile_dealer then retired the store's 158 real cars.
"""
from __future__ import annotations

import json
import sqlite3

import pytest

from backend.scripts import dealer_pipeline as dp

SINCE = "2026-09-28T14:00:00+00:00"


def _db(stamped_rows: int, old_rows: int, feed_rows: int) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE cars (id INTEGER PRIMARY KEY, dealer_id TEXT, vin TEXT, condition TEXT, dealer_name TEXT, zip_code TEXT, scraped_at TEXT, listing_active INTEGER, listing_removed_at TEXT)")
    conn.execute("CREATE TABLE scan_runs (id INTEGER PRIMARY KEY, dealer_id TEXT, provider TEXT, finished_at TEXT, duration_seconds REAL, inventory_rows INTEGER, upserted INTEGER, error TEXT, summary_json TEXT)")
    for i in range(old_rows):
        conn.execute("INSERT INTO cars (dealer_id, vin, condition, dealer_name, zip_code, scraped_at, listing_active) VALUES (?,?,?,?,?,?,1)",
                     ("mb-com", f"OLD{i:014d}", "used", "MB Beverly Hills", "90210", "2026-09-20T00:00:00+00:00"))
    for i in range(stamped_rows):
        conn.execute("INSERT INTO cars (dealer_id, vin, condition, dealer_name, zip_code, scraped_at, listing_active) VALUES (?,?,?,?,?,?,1)",
                     ("mb-com", f"NEW{i:014d}", "used", "MB Beverly Hills", "90210", "2026-09-28T14:56:00+00:00"))
    summary = {"capture_coverage": {"n": feed_rows, "price": 1.0, "trim": 1.0, "exterior_color": 1.0}}
    conn.execute("INSERT INTO scan_runs (dealer_id, provider, finished_at, duration_seconds, inventory_rows, upserted, error, summary_json) VALUES (?,?,?,?,?,?,?,?)",
                 ("mb-com", "dealer_dot_com", "2026-09-28T14:56:38+00:00", 200.0, feed_rows, feed_rows, None, json.dumps(summary)))
    conn.commit()
    return conn


def test_group_feed_with_two_stamped_rows_is_no_rows():
    conn = _db(stamped_rows=2, old_rows=158, feed_rows=1521)
    out = dp.assess(conn, "mb-com", SINCE, known_before=160, recipe_info={"had_recipes": 1})
    assert out["verdict"] == "no_rows"
    assert out["rows_stamped"] == 2
    assert "rooftop gate" in out["reason"]
    # and reconcile refuses to retire on that verdict
    rec = dp.reconcile_dealer(conn, "mb-com", SINCE, 160, out["rows_stamped"], out["verdict"])
    assert not rec["eligible"] and rec["retired"] == 0
    assert conn.execute("SELECT COUNT(*) FROM cars WHERE listing_active = 1").fetchone()[0] == 160


def test_reconcile_on_stamped_count_not_feed_count():
    conn = _db(stamped_rows=2, old_rows=158, feed_rows=1521)
    # even if a caller passed an accepted verdict, the stamped count is below the share floor
    rec = dp.reconcile_dealer(conn, "mb-com", SINCE, 160, 2, "inaccurate")
    assert not rec["eligible"]
    assert "2 rows" in rec["reason"]


def test_scoped_group_store_with_full_yield_still_accepted():
    # Hendrick-style: big group feed, but the gate keeps the store's whole lot
    conn = _db(stamped_rows=400, old_rows=380, feed_rows=11000)
    out = dp.assess(conn, "mb-com", SINCE, known_before=380, recipe_info={"had_recipes": 1})
    assert out["verdict"] != "no_rows"
    assert out["rows_stamped"] == 400
