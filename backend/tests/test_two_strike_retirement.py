"""P5B.3 / P5B.4 on SQLite: provenance rows (``listing_retirements``) and two-strike (``listing_miss_strikes``).

The Postgres twins live at the end of test_reconcile_pg.py (``pg`` tier).
"""
from __future__ import annotations

import sqlite3

import pytest

from backend.db import retirement_log as rl
from backend.scanner import inventory_reconcile as ir

DEALER = "d-strike"
URL = "https://d-strike.com"
OLD = "2026-10-20T00:00:00.000000Z"
RUN_START = "2026-10-26T12:00:00+00:00"


def _vin(i: int) -> str:
    return f"1HGBH41JXMN{i:06d}"


def _conn(n: int = 30) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE cars (id INTEGER PRIMARY KEY, dealer_id TEXT, dealer_url TEXT, vin TEXT, condition TEXT, "
        "make TEXT, scraped_at TEXT, listing_active INTEGER, listing_removed_at TEXT)"
    )
    for i in range(n):
        conn.execute(
            "INSERT INTO cars (dealer_id, dealer_url, vin, condition, make, scraped_at, listing_active) "
            "VALUES (?, ?, ?, 'Used', 'Honda', ?, 1)", (DEALER, URL, _vin(i), OLD),
        )
    conn.commit()
    return conn


def _run(conn, seen, **kw):
    return ir.reconcile_dealer_inventory_after_scan(
        DEALER, URL, {_vin(i) for i in seen}, {"deduped_rows": len(seen)},
        run_started_iso=RUN_START, _conn=conn, **kw,
    )


def _active(conn) -> int:
    return conn.execute("SELECT COUNT(*) FROM cars WHERE listing_active = 1").fetchone()[0]


def _count(conn, table) -> int:
    try:
        return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    except sqlite3.OperationalError:
        return 0


def _age_strikes(conn, iso="2026-10-24T00:00:00+00:00") -> None:
    conn.execute("UPDATE listing_miss_strikes SET first_missed_at = ?", (iso,))
    conn.commit()


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    for k in ("SCANNER_RECONCILE", "SCANNER_NO_RETIRE", "SCANNER_RECONCILE_MIN_COVERAGE",
              "SCANNER_RECONCILE_BUCKET_SHARE", "SCANNER_RECONCILE_MIN_ROWS", "SCANNER_RETIRE_STRIKES",
              "SCANNER_STRIKE_GAP_HOURS"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setattr(ir, "ensure_cars_table_columns", lambda cur: None)


def test_first_miss_retires_nothing_and_records_a_strike():
    conn = _conn()
    out = _run(conn, range(27))  # 3 gone
    assert out["ran"] and out["retired"] == 0 and out["struck"] == 3 and out["strikes_needed"] == 2
    assert _active(conn) == 30
    assert _count(conn, "listing_miss_strikes") == 3 and _count(conn, "listing_retirements") == 0


def test_a_rerun_inside_the_gap_does_not_count_as_the_second_strike():
    conn = _conn()
    _run(conn, range(27))
    out = _run(conn, range(27))
    assert out["retired"] == 0 and out["strike_waiting"] == 3 and out.get("struck", 0) == 0
    assert _active(conn) == 30 and _count(conn, "listing_miss_strikes") == 3


def test_second_miss_after_the_gap_retires_with_a_provenance_row():
    conn = _conn()
    _run(conn, range(27))
    _age_strikes(conn)
    out = _run(conn, range(27))
    assert out["retired"] == 3 == out["marked_inactive"] and _active(conn) == 27
    rows = conn.execute("SELECT car_id, vin, dealer_id, writer, reason, strikes FROM listing_retirements ORDER BY car_id").fetchall()
    assert [(r[1], r[2], r[3], r[5]) for r in rows] == [(_vin(i), DEALER, "scanner_reconcile", 2) for i in (27, 28, 29)]
    assert [r[0] for r in rows] == [28, 29, 30]  # ids, rows = UPDATE rowcount
    assert _count(conn, "listing_miss_strikes") == 0  # retired cars drop their strike


def test_a_car_seen_again_loses_its_strike():
    conn = _conn()
    _run(conn, range(27))
    _age_strikes(conn)
    out = _run(conn, list(range(27)) + [29])  # 29 is back; 27 and 28 still gone
    assert out["retired"] == 2 and _active(conn) == 28
    assert conn.execute("SELECT listing_active FROM cars WHERE vin = ?", (_vin(29),)).fetchone()[0] == 1
    assert _count(conn, "listing_miss_strikes") == 0


def test_dry_mode_writes_no_strikes_and_no_retirements(monkeypatch):
    monkeypatch.setenv("SCANNER_RECONCILE", "dry")
    conn = _conn()
    out = _run(conn, range(27))
    assert out["dry_run"] and out["would_strike"] == 3 and out["would_retire"] == 0 and out["retired"] == 0
    assert _count(conn, "listing_miss_strikes") == 0 and _count(conn, "listing_retirements") == 0
    assert _active(conn) == 30


def test_dry_mode_with_aged_strikes_reports_the_would_retire_and_still_writes_nothing(monkeypatch):
    conn = _conn()
    _run(conn, range(27))
    _age_strikes(conn)
    monkeypatch.setenv("SCANNER_RECONCILE", "dry")
    out = _run(conn, range(27))
    assert out["would_retire"] == 3 and out["retired"] == 0 and _active(conn) == 30
    assert _count(conn, "listing_retirements") == 0 and _count(conn, "listing_miss_strikes") == 3


def test_no_retire_switch_writes_no_strikes_either(monkeypatch):
    monkeypatch.setenv("SCANNER_NO_RETIRE", "1")
    conn = _conn()
    out = _run(conn, range(27))
    assert out["skipped_reason"] == "no_retire_switch" and out["would_strike"] == 3
    assert _count(conn, "listing_miss_strikes") == 0 and _active(conn) == 30


def test_single_strike_knob_retires_on_the_first_miss_with_provenance(monkeypatch):
    monkeypatch.setenv("SCANNER_RETIRE_STRIKES", "1")
    conn = _conn()
    out = _run(conn, range(27), retirement_writer="delta_scan")
    assert out["retired"] == 3 and _active(conn) == 27
    assert {r[0] for r in conn.execute("SELECT writer FROM listing_retirements")} == {"delta_scan"}
    assert _count(conn, "listing_miss_strikes") == 0


def test_an_ineligible_run_leaves_strikes_alone():
    conn = _conn()
    _run(conn, range(27))
    out = ir.reconcile_dealer_inventory_after_scan(  # a run error: no evidence either way
        DEALER, URL, {_vin(i) for i in range(27)}, {"deduped_rows": 27, "error": "boom"},
        run_started_iso=RUN_START, _conn=conn,
    )
    assert out["skipped_reason"] == "dealer_error" and _count(conn, "listing_miss_strikes") == 3


def test_a_row_refreshed_after_the_plan_is_not_retired_and_not_logged(monkeypatch):
    from backend.scanner import retirement_policy

    conn = _conn()
    _run(conn, range(27))
    _age_strikes(conn)
    real = retirement_policy.plan_retirement

    def plan_then_upsert(rows, evidence, policy, now):
        plan = real(rows, evidence, policy, now)
        conn.execute("UPDATE cars SET scraped_at = '2026-10-26T13:00:00.000000Z' WHERE id = ?", (plan.retire_ids[0],))
        return plan

    monkeypatch.setattr(retirement_policy, "plan_retirement", plan_then_upsert)
    out = _run(conn, range(27))
    assert out["retired"] == 2 and _count(conn, "listing_retirements") == 2


def test_strict_provenance_failure_rolls_the_retirement_back(monkeypatch):
    monkeypatch.setenv("SCANNER_RETIRE_STRIKES", "1")
    conn = _conn()

    def boom(*a, **k):
        raise sqlite3.OperationalError("disk full")

    monkeypatch.setattr(ir, "record_retirements", boom)
    with pytest.raises(sqlite3.OperationalError):
        _run(conn, range(27))
    conn.rollback()
    assert _active(conn) == 30


def test_best_effort_provenance_never_raises_and_rolls_back_to_the_savepoint():
    conn = sqlite3.connect(":memory:")
    cur = conn.cursor()
    cur.execute("CREATE TABLE listing_retirements (id INTEGER PRIMARY KEY)")  # wrong shape: INSERT fails
    assert rl.record_retirements(cur, [(1, "V", "d")], writer="admin_delete", strict=False) == 0
    cur.execute("INSERT INTO listing_retirements (id) VALUES (9)")  # the connection is still usable
    assert conn.execute("SELECT COUNT(*) FROM listing_retirements").fetchone()[0] == 1


def test_admin_delete_paths_write_admin_delete_rows(tmp_path, monkeypatch):
    from backend.db.repositories import cars_repo

    db = sqlite3.connect(str(tmp_path / "c.db"))
    db.execute("CREATE TABLE cars (id INTEGER PRIMARY KEY, vin TEXT, dealer_id TEXT)")
    db.execute("CREATE TABLE saved_cars (car_id INTEGER)")
    db.execute("INSERT INTO cars (vin, dealer_id) VALUES ('VIN001', 'dx'), ('1HGBH41JXMN100000', 'dx')")
    db.commit()
    db.close()
    monkeypatch.setattr(cars_repo, "get_conn", lambda: sqlite3.connect(str(tmp_path / "c.db")))
    monkeypatch.setattr(cars_repo, "ensure_nhtsa_vpic_cache_table", lambda conn: conn.execute(
        "CREATE TABLE IF NOT EXISTS nhtsa_vpic_cache (vin TEXT)"))
    monkeypatch.setattr("backend.db.incomplete_listings_db.delete_incomplete_record", lambda cid: None)
    out = cars_repo.delete_cars_with_dummy_placeholder_vins()
    assert out["deleted"] == 1
    chk = sqlite3.connect(str(tmp_path / "c.db"))
    assert chk.execute("SELECT car_id, vin, dealer_id, writer FROM listing_retirements").fetchall() == [
        (1, "VIN001", "dx", "admin_delete")]
