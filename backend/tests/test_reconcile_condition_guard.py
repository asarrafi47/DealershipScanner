"""Scanner-level reconcile never retires a condition the run returned nothing for (2026-09-28)."""
from __future__ import annotations

import sqlite3

from backend.scanner import inventory_reconcile as ir


def _vin(i: int) -> str:
    return f"5NPE34AF{i:09d}"[:17].ljust(17, "0")


def _db(new_rows: int, used_rows: int) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE cars (id INTEGER PRIMARY KEY, dealer_id TEXT, dealer_url TEXT, vin TEXT, condition TEXT, listing_active INTEGER, listing_removed_at TEXT)")
    i = 0
    for _ in range(new_rows):
        conn.execute("INSERT INTO cars (dealer_id, dealer_url, vin, condition, listing_active) VALUES ('parksidekia-com','https://www.parksidekia.com',?, 'New', 1)", (_vin(i),)); i += 1
    for _ in range(used_rows):
        conn.execute("INSERT INTO cars (dealer_id, dealer_url, vin, condition, listing_active) VALUES ('parksidekia-com','https://www.parksidekia.com',?, 'Used', 1)", (_vin(i),)); i += 1
    conn.commit()
    return conn


def test_new_only_replay_keeps_the_used_rows(monkeypatch):
    monkeypatch.setattr(ir, "ensure_cars_table_columns", lambda cur: None)
    conn = _db(new_rows=300, used_rows=200)
    scraped = {_vin(i) for i in range(300)}  # every new car re-seen, zero used
    stats = {"deduped_rows": 300}
    out = ir.reconcile_dealer_inventory_after_scan(
        "parksidekia-com", "https://www.parksidekia.com", scraped, stats,
        scraped_conditions={"new"}, _conn=conn,
    )
    assert out["ran"] is True
    assert out["marked_inactive"] == 0
    assert out["kept_missing_condition"] == 200
    assert conn.execute("SELECT COUNT(*) FROM cars WHERE listing_active = 1").fetchone()[0] == 500


def test_both_conditions_returned_still_retires_missing_cars(monkeypatch):
    monkeypatch.setattr(ir, "ensure_cars_table_columns", lambda cur: None)
    conn = _db(new_rows=300, used_rows=200)
    scraped = {_vin(i) for i in range(300)} | {_vin(i) for i in range(300, 450)}  # 50 used cars gone
    stats = {"deduped_rows": 450}
    out = ir.reconcile_dealer_inventory_after_scan(
        "parksidekia-com", "https://www.parksidekia.com", scraped, stats,
        scraped_conditions={"new", "used"}, _conn=conn,
    )
    assert out["marked_inactive"] == 50
    assert "kept_missing_condition" not in out


def test_condition_buckets():
    assert ir.condition_bucket("New") == "new"
    assert ir.condition_bucket("Certified Pre-Owned") == "used"
    # blank is unknown, never used (P1A.1: assess's rows_used counts blanks as used)
    assert ir.condition_bucket("") == "unknown"
    assert ir.condition_bucket(None) == "unknown"
    assert ir.condition_bucket("   ") == "unknown"
    assert ir.condition_buckets_from_vehicles([{"condition": "new"}, {"condition": "CPO"}, {}]) == {"new", "used"}
    assert ir.condition_buckets_from_vehicles([{}, {"condition": ""}]) == set()
