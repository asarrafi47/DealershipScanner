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


# ---- N9.12: honest accounting, ok gates, guard-only routing ----------------------------------

WALK = {"recipes": [{"complete": True, "total_count": 90, "raw_vins": 90, "end": "no_new_rows"}]}


def _run_db(stamped: int, feed: int, *, prior_rows: int = 0, vin_refused: int = 0, owners=None, replay=WALK,
            placeholder: bool = False, dealer: str = "d-com") -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE cars (id INTEGER PRIMARY KEY, dealer_id TEXT, vin TEXT, condition TEXT, dealer_name TEXT, zip_code TEXT, "
                 "scraped_at TEXT, listing_active INTEGER, image_url TEXT)")
    conn.execute("CREATE TABLE scan_runs (id INTEGER PRIMARY KEY, dealer_id TEXT, provider TEXT, finished_at TEXT, duration_seconds REAL, "
                 "inventory_rows INTEGER, upserted INTEGER, error TEXT, summary_json TEXT)")
    for i in range(stamped):
        conn.execute("INSERT INTO cars (dealer_id, vin, condition, dealer_name, zip_code, scraped_at, listing_active, image_url) VALUES (?,?,?,?,?,?,1,?)",
                     (dealer, f"V{i:016d}", "new" if i % 2 else "used", "D", "28202", "2026-09-28T14:56:00+00:00",
                      "/static/placeholder.svg" if placeholder else f"https://img/{i}.jpg"))
    if prior_rows:
        conn.execute("INSERT INTO scan_runs (dealer_id, provider, finished_at, duration_seconds, inventory_rows, upserted, error, summary_json) VALUES (?,?,?,?,?,?,?,?)",
                     (dealer, "p", "2026-09-20T00:00:00+00:00", 60.0, prior_rows, prior_rows, None, "{}"))
    summary = {"capture_coverage": {"n": feed, "price": 1.0, "trim": 1.0, "exterior_color": 1.0},
               "vin_owner_conflicts": vin_refused, "vin_owner_conflict_owners": owners or {}, "recipe_replay": replay}
    conn.execute("INSERT INTO scan_runs (dealer_id, provider, finished_at, duration_seconds, inventory_rows, upserted, error, summary_json) VALUES (?,?,?,?,?,?,?,?)",
                 (dealer, "p", "2026-09-28T14:56:38+00:00", 200.0, feed, stamped, None, json.dumps(summary)))
    conn.commit()
    return conn


def _assess(conn, known=0):
    return dp.assess(conn, "d-com", SINCE, known_before=known, recipe_info={"had_recipes": 1})


def test_accounting_keys_and_guard_named_in_reason():
    conn = _run_db(14, 862, vin_refused=848, owners={"cleveland-nissan-com": 848})
    out = _assess(conn)
    assert out["verdict"] == "no_rows" and out["guard_only"]
    assert (out["rows_captured"], out["rows_refused_vin_owner"], out["rows_refused_rooftop"], out["rows_written"]) == (862, 848, 0, 14)
    assert "VIN-owner guard refused 848" in out["reason"] and "cleveland-nissan-com" in out["reason"]
    assert "rooftop gate refused the rest" not in out["reason"]
    assert out["reason"].count("owner guard") + out["reason"].count("owner-guard") <= 1


def test_guard_only_no_rows_is_not_routed_to_resynth():
    from backend.scanner.pipeline.lifecycle import route_verdict

    out = _assess(_run_db(6, 162, vin_refused=156, owners={"cumberlandkia-com": 156}))
    route = route_verdict({"dealer_id": "d-com", "url": "https://d.example"}, out, recipe_status="")
    assert route["action"] == "none" and route["trigger"] == "vin_owner_guard_only"
    # a plain empty replay still goes to the lifecycle
    empty = {"dealer_id": "d-com", "verdict": "no_rows", "reason": "recipe replay yielded 0 rows", "recipe": {}}
    assert route_verdict({"dealer_id": "d-com", "url": "https://d.example"}, empty, recipe_status="")["action"] == "lifecycle"


def test_clean_run_is_ok():
    assert _assess(_run_db(90, 90, prior_rows=100))["verdict"] == "ok"


def test_coverage_drop_over_30_percent_is_thin_and_still_verified():
    out = _assess(_run_db(90, 90, prior_rows=773))  # kearnymesasubaru: 773 -> 90
    assert out["verdict"] == "thin" and out["reason"].startswith("coverage_drop")
    assert "accuracy" in out  # thin still runs the NHTSA verification


def test_missing_site_total_check_blocks_ok():
    out = _assess(_run_db(90, 90, replay={}))
    assert out["verdict"] == "thin" and out["reason"].startswith("no_site_total_check")


def test_all_placeholder_photos_block_ok():
    out = _assess(_run_db(379, 379, placeholder=True))  # groovetoyota / mymetrohonda shape
    assert out["verdict"] == "thin" and out["reason"].startswith("placeholder_photos")


def test_error_dealer_without_scan_run_counts_zeros():
    conn = _run_db(0, 0)
    conn.execute("DELETE FROM scan_runs")
    out = _assess(conn)
    assert out["verdict"] == "error"
    assert (out["rows"], out["rows_captured"], out["rows_written"]) == (0, 0, 0)
