"""Scanner dealer inventory reconciliation (soft-unlist stale VINs)."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

import backend.db.inventory_db as inventory_db
from backend.db.inventory_db import ensure_cars_table_columns, init_inventory_db, search_cars
from backend.scanner.inventory_reconcile import (
    normalize_scanner_vin,
    normalized_vin_set_from_vehicles,
    reconcile_dealer_inventory_after_scan,
)


def test_normalize_scanner_vin_basic() -> None:
    assert normalize_scanner_vin("  1hgbh41jxmn109186  ") == "1HGBH41JXMN109186"
    assert normalize_scanner_vin("short") is None
    assert normalize_scanner_vin("UNKNOWN1234567890") is None
    assert normalize_scanner_vin("unknown1234567890") is None
    assert normalize_scanner_vin("1HGBH41JXMN10918!") is None  # not alphanumeric


def test_normalized_vin_set_from_vehicles_dedupes() -> None:
    vins = normalized_vin_set_from_vehicles(
        [
            {"vin": "1HGBH41JXMN109186"},
            {"vin": "1hgbh41jxmn109186"},
            {"vin": "bad"},
            {"vin": "UNKNOWN-12345678901"},
        ]
    )
    assert vins == {"1HGBH41JXMN109186"}


def _memory_cars_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE cars (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vin TEXT UNIQUE NOT NULL,
            dealer_id TEXT,
            dealer_url TEXT,
            listing_active INTEGER,
            listing_removed_at TEXT
        )
        """
    )
    ensure_cars_table_columns(cur)
    conn.commit()
    return conn


def test_reconcile_marks_stale_vins_for_dealer() -> None:
    conn = _memory_cars_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO cars (vin, dealer_id, dealer_url, listing_active) VALUES (?, ?, ?, ?)",
        ("11111111111111111", "dealer-a", "https://dealer-a.com/", 1),
    )
    cur.execute(
        "INSERT INTO cars (vin, dealer_id, dealer_url, listing_active) VALUES (?, ?, ?, ?)",
        ("22222222222222222", "dealer-a", "https://dealer-a.com/", 1),
    )
    conn.commit()

    stats: dict = {"deduped_rows": 20, "error": None}
    scraped = {"11111111111111111"}
    out = reconcile_dealer_inventory_after_scan(
        "dealer-a",
        "https://dealer-a.com/",
        scraped,
        stats,
        _conn=conn,
    )
    assert out["ran"] is True
    assert out["marked_inactive"] == 1
    assert out["skipped_reason"] == "ok"

    cur.execute("SELECT listing_active FROM cars WHERE vin = ?", ("22222222222222222",))
    assert int(cur.fetchone()[0]) == 0
    cur.execute("SELECT listing_active FROM cars WHERE vin = ?", ("11111111111111111",))
    assert int(cur.fetchone()[0]) == 1
    conn.close()


def test_reconcile_skips_when_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SCANNER_RECONCILE", "0")
    conn = _memory_cars_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO cars (vin, dealer_id, listing_active) VALUES (?, ?, ?)",
        ("33333333333333333", "dealer-b", 1),
    )
    conn.commit()
    stats: dict = {"deduped_rows": 20, "error": None}
    out = reconcile_dealer_inventory_after_scan(
        "dealer-b",
        "https://dealer-b.com/",
        {"44444444444444444"},
        stats,
        _conn=conn,
    )
    assert out["ran"] is False
    assert out["marked_inactive"] == 0
    assert out["skipped_reason"] == "disabled"
    cur.execute("SELECT listing_active FROM cars WHERE vin = ?", ("33333333333333333",))
    assert int(cur.fetchone()[0]) == 1
    conn.close()


def test_reconcile_skips_below_min_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SCANNER_RECONCILE_MIN_ROWS", "50")
    conn = _memory_cars_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO cars (vin, dealer_id, listing_active) VALUES (?, ?, ?)",
        ("55555555555555555", "dealer-c", 1),
    )
    conn.commit()
    stats: dict = {"deduped_rows": 5, "error": None}
    out = reconcile_dealer_inventory_after_scan(
        "dealer-c",
        "https://dealer-c.com/",
        {"66666666666666666"},
        stats,
        _conn=conn,
    )
    assert out["ran"] is False
    assert out["marked_inactive"] == 0
    assert "below_min_rows" in (out["skipped_reason"] or "")
    cur.execute("SELECT listing_active FROM cars WHERE vin = ?", ("55555555555555555",))
    assert int(cur.fetchone()[0]) == 1
    conn.close()


def test_reconcile_skips_no_valid_scraped_vins() -> None:
    conn = _memory_cars_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO cars (vin, dealer_id, listing_active) VALUES (?, ?, ?)",
        ("77777777777777777", "dealer-d", 1),
    )
    conn.commit()
    stats: dict = {"deduped_rows": 20, "error": None}
    out = reconcile_dealer_inventory_after_scan(
        "dealer-d",
        "https://dealer-d.com/",
        set(),
        stats,
        _conn=conn,
    )
    assert out["skipped_reason"] == "no_valid_scraped_vins"
    cur.execute("SELECT listing_active FROM cars WHERE vin = ?", ("77777777777777777",))
    assert int(cur.fetchone()[0]) == 1
    conn.close()


def test_reconcile_skips_on_dealer_error() -> None:
    conn = _memory_cars_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO cars (vin, dealer_id, listing_active) VALUES (?, ?, ?)",
        ("88888888888888888", "dealer-e", 1),
    )
    conn.commit()
    stats: dict = {"deduped_rows": 20, "error": "timeout"}
    out = reconcile_dealer_inventory_after_scan(
        "dealer-e",
        "https://dealer-e.com/",
        {"99999999999999999"},
        stats,
        _conn=conn,
    )
    assert out["skipped_reason"] == "dealer_error"
    cur.execute("SELECT listing_active FROM cars WHERE vin = ?", ("88888888888888888",))
    assert int(cur.fetchone()[0]) == 1
    conn.close()


def test_search_cars_excludes_inactive_rows(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    dbp = tmp_path / "inv_reconcile_search.db"
    monkeypatch.setattr(inventory_db, "DB_PATH", str(dbp))
    init_inventory_db()
    conn = sqlite3.connect(str(dbp))
    cur = conn.cursor()
    now = "2026-01-01T00:00:00Z"
    cur.execute(
        """
        INSERT INTO cars (
            vin, title, year, make, model, trim, price, mileage,
            image_url, dealer_name, dealer_url, dealer_id, scraped_at,
            fuel_type, cylinders, transmission, drivetrain,
            exterior_color, interior_color, stock_number, gallery,
            listing_active, listing_removed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "CCCCCCCCCCCCCCCCC",
            "Active Car",
            2022,
            "Honda",
            "Civic",
            "LX",
            20000,
            1000,
            "https://example.com/a.jpg",
            "Test Dealer",
            "https://dealer.test/",
            "t-dealer",
            now,
            "Gas",
            4,
            "Automatic",
            "FWD",
            "Black",
            "Black",
            "S1",
            "[]",
            1,
            None,
        ),
    )
    cur.execute(
        """
        INSERT INTO cars (
            vin, title, year, make, model, trim, price, mileage,
            image_url, dealer_name, dealer_url, dealer_id, scraped_at,
            fuel_type, cylinders, transmission, drivetrain,
            exterior_color, interior_color, stock_number, gallery,
            listing_active, listing_removed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "DDDDDDDDDDDDDDDDD",
            "Inactive Car",
            2021,
            "Honda",
            "Accord",
            "EX",
            25000,
            5000,
            "https://example.com/b.jpg",
            "Test Dealer",
            "https://dealer.test/",
            "t-dealer",
            now,
            "Gas",
            4,
            "Automatic",
            "FWD",
            "White",
            "Gray",
            "S2",
            "[]",
            0,
            now,
        ),
    )
    conn.commit()
    conn.close()
    rows = search_cars(makes=["Honda"])
    vins = {r["vin"] for r in rows}
    assert "CCCCCCCCCCCCCCCCC" in vins
    assert "DDDDDDDDDDDDDDDDD" not in vins


def test_reconcile_does_not_touch_other_dealer() -> None:
    conn = _memory_cars_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO cars (vin, dealer_id, listing_active) VALUES (?, ?, ?)",
        ("AAAAAAAAAAAAAAAAA", "dealer-x", 1),
    )
    cur.execute(
        "INSERT INTO cars (vin, dealer_id, listing_active) VALUES (?, ?, ?)",
        ("BBBBBBBBBBBBBBBBB", "dealer-y", 1),
    )
    conn.commit()
    stats: dict = {"deduped_rows": 20, "error": None}
    reconcile_dealer_inventory_after_scan(
        "dealer-x",
        "https://dealer-x.com/",
        {"AAAAAAAAAAAAAAAAA"},
        stats,
        _conn=conn,
    )
    cur.execute("SELECT listing_active FROM cars WHERE vin = ?", ("BBBBBBBBBBBBBBBBB",))
    assert int(cur.fetchone()[0]) == 1
    conn.close()


def _seed(conn: sqlite3.Connection, dealer: str, n: int) -> list[str]:
    vins = [f"1HGBH41JXMN1{i:05d}" for i in range(n)]
    cur = conn.cursor()
    for v in vins:
        cur.execute(
            "INSERT INTO cars (vin, dealer_id, dealer_url, listing_active) VALUES (?, ?, ?, 1)",
            (v, dealer, f"https://{dealer}"),
        )
    conn.commit()
    return vins


def test_reconcile_refuses_partial_capture(monkeypatch: pytest.MonkeyPatch) -> None:
    """A scrape that re-sees under half the active lot must not unlist the rest."""
    monkeypatch.delenv("SCANNER_RECONCILE_MIN_COVERAGE", raising=False)
    conn = _memory_cars_conn()
    vins = _seed(conn, "gardenahonda-com", 100)
    seen = set(vins[:30])  # 30 of 100 re-seen, like the 443-of-1246 capture
    stats: dict = {"deduped_rows": 30}
    out = reconcile_dealer_inventory_after_scan(
        "gardenahonda-com", "https://gardenahonda-com", seen, stats, _conn=conn
    )
    assert out["ran"] is False
    assert out["skipped_reason"].startswith("low_coverage(matched=30,active=100")
    n_active = conn.execute("SELECT COUNT(*) FROM cars WHERE listing_active = 1").fetchone()[0]
    assert n_active == 100


def test_reconcile_runs_when_coverage_clears(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SCANNER_RECONCILE_MIN_COVERAGE", raising=False)
    conn = _memory_cars_conn()
    vins = _seed(conn, "d-ok", 100)
    seen = set(vins[:80])
    out = reconcile_dealer_inventory_after_scan("d-ok", "https://d-ok", seen, {"deduped_rows": 80}, _conn=conn)
    assert out["ran"] is True and out["marked_inactive"] == 20


def test_reconcile_coverage_gate_can_be_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SCANNER_RECONCILE_MIN_COVERAGE", "0")
    conn = _memory_cars_conn()
    vins = _seed(conn, "d-off", 100)
    out = reconcile_dealer_inventory_after_scan("d-off", "https://d-off", set(vins[:10]), {"deduped_rows": 10}, _conn=conn)
    assert out["ran"] is True and out["marked_inactive"] == 90
