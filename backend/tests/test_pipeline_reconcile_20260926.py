"""dealer_pipeline reconcile: retire rows a full, accepted run did not return —
never on a partial run. 2026-09-26: 194,811 active rows, 0 ever retired, 45,265
last seen before September."""
from __future__ import annotations

import sqlite3

import pytest

from backend.scripts import dealer_pipeline as dp


@pytest.fixture()
def conn(tmp_path):
    c = sqlite3.connect(str(tmp_path / "cars.db"))
    c.execute("CREATE TABLE cars (id INTEGER PRIMARY KEY, dealer_id TEXT, vin TEXT, scraped_at TEXT, listing_active INTEGER DEFAULT 1, listing_removed_at TEXT)")
    rows = [("d", f"VIN{i:014d}", "2026-07-10T00:00:00Z") for i in range(6)]          # July ghosts
    rows += [("d", f"VIN{i:014d}", "2026-09-26T01:00:00Z") for i in range(6, 16)]     # this run
    rows += [("other", "OTHERVIN000000001", "2026-07-10T00:00:00Z")]
    c.executemany("INSERT INTO cars (dealer_id, vin, scraped_at) VALUES (?,?,?)", rows)
    c.commit()
    return c


def _active(conn, dealer="d"):
    return conn.execute("SELECT COUNT(*) FROM cars WHERE dealer_id=? AND COALESCE(listing_active,1)=1", (dealer,)).fetchone()[0]


def test_full_accepted_run_retires_unseen_rows(conn):
    out = dp.reconcile_dealer(conn, "d", "2026-09-26T00:00:00Z", baseline=12, rows_this_run=25, verdict="inaccurate")
    assert out["eligible"] and out["stale"] == 6 and out["retired"] == 6
    assert _active(conn) == 10 and _active(conn, "other") == 1
    removed = conn.execute("SELECT COUNT(*) FROM cars WHERE dealer_id='d' AND listing_active=0 AND listing_removed_at IS NOT NULL").fetchone()[0]
    assert removed == 6


def test_partial_run_never_retires(conn):
    out = dp.reconcile_dealer(conn, "d", "2026-09-26T00:00:00Z", baseline=100, rows_this_run=30, verdict="ok")
    assert not out["eligible"] and out["retired"] == 0 and "60%" in out["reason"]
    assert _active(conn) == 16
    out = dp.reconcile_dealer(conn, "d", "2026-09-26T00:00:00Z", baseline=12, rows_this_run=25, verdict="no_rows")
    assert not out["eligible"] and _active(conn) == 16


def test_dry_run_reports_only(conn):
    out = dp.reconcile_dealer(conn, "d", "2026-09-26T00:00:00Z", baseline=12, rows_this_run=25, verdict="ok", dry_run=True)
    assert out["eligible"] and out["stale"] == 6 and out["retired"] == 0
    assert _active(conn) == 16


def test_zero_baseline_falls_back_and_small_runs_never_retire(conn):
    # Gunn Honda 2026-09-26: no scan in 30 days (baseline 0), an 8-row run retired 382 cars
    out = dp.reconcile_dealer(conn, "d", "2026-09-26T00:00:00Z", baseline=0, rows_this_run=8, verdict="ok")
    assert not out["eligible"] and "absolute floor" in out["reason"] and _active(conn) == 16
    # 25 rows against 6 July ghosts (90-day window empty -> all active rows = 6): eligible
    out = dp.reconcile_dealer(conn, "d", "2026-09-26T00:00:00Z", baseline=0, rows_this_run=25, verdict="ok")
    assert out["eligible"] and out["baseline_fallback"] == 6 and out["retired"] == 6
