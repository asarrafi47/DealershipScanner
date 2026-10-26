"""P5A.4: the scanner reconcile runs the retirement policy, writes through a guarded UPDATE, and the
one no-retire switch (SCANNER_NO_RETIRE) stops every writer: scanner reconcile (full and delta),
rooftop disown, the pipeline's reconcile_dealer (W2 and the lifecycle retry)."""
from __future__ import annotations

import logging
import sqlite3

import pytest

from backend.attribution import disown
from backend.scanner import inventory_reconcile as ir
from backend.scanner import retirement_policy
from backend.scanner.pipeline.reconcile import reconcile_dealer


@pytest.fixture(autouse=True)
def _single_miss(monkeypatch):
    """These tests pin the retirement POLICY (which ids); the two-strike layer (P5B.4) has its own
    file, test_two_strike_retirement.py. One miss retires here."""
    monkeypatch.setenv("SCANNER_RETIRE_STRIKES", "1")


DEALER = "d-matrix"
URL = "https://d-matrix.com"
RUN_START = "2026-10-26T12:00:00+00:00"
OLD = "2026-10-20T00:00:00.000000Z"


def _vin(i: int) -> str:
    return f"1HGBH41JXMN{i:06d}"


def _conn(n_new: int = 30, n_used: int = 30, scraped_at: str | None = OLD) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE cars (id INTEGER PRIMARY KEY, dealer_id TEXT, dealer_url TEXT, vin TEXT, condition TEXT, "
        "make TEXT, scraped_at TEXT, listing_active INTEGER, listing_removed_at TEXT)"
    )
    for i in range(n_new + n_used):
        conn.execute(
            "INSERT INTO cars (dealer_id, dealer_url, vin, condition, make, scraped_at, listing_active) "
            "VALUES (?, ?, ?, ?, 'Honda', ?, 1)",
            (DEALER, URL, _vin(i), "New" if i < n_new else "Used", scraped_at),
        )
    conn.commit()
    return conn


def _active(conn) -> int:
    return conn.execute("SELECT COUNT(*) FROM cars WHERE listing_active = 1").fetchone()[0]


def _run(conn, seen, stats=None, **kw):
    stats = {"deduped_rows": len(seen)} if stats is None else stats
    return ir.reconcile_dealer_inventory_after_scan(
        DEALER, URL, set(seen), stats, run_started_iso=RUN_START, _conn=conn, **kw
    )


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    for k in ("SCANNER_RECONCILE", "SCANNER_NO_RETIRE", "SCANNER_RECONCILE_MIN_COVERAGE",
              "SCANNER_RECONCILE_BUCKET_SHARE", "SCANNER_RECONCILE_MIN_ROWS"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setattr(ir, "ensure_cars_table_columns", lambda cur: None)


def test_both_buckets_complete_retires_the_missing_vins():
    conn = _conn()
    out = _run(conn, [_vin(i) for i in range(55)])  # 5 gone
    assert out["ran"] and out["eligible"] and out["retired"] == 5 == out["marked_inactive"]
    assert _active(conn) == 55
    assert set(out["kept"]) >= {"condition_missing", "bucket_low_coverage", "make_missing"}


def test_row_refreshed_between_plan_and_update_is_not_retired(monkeypatch):
    conn = _conn()
    real = retirement_policy.plan_retirement

    def plan_then_upsert(rows, evidence, policy, now):
        plan = real(rows, evidence, policy, now)
        # a concurrent upsert re-lists two of the planned ids after run start
        for rid in plan.retire_ids[:2]:
            conn.execute("UPDATE cars SET scraped_at = '2026-10-26T13:00:00.000000Z' WHERE id = ?", (rid,))
        return plan

    monkeypatch.setattr(retirement_policy, "plan_retirement", plan_then_upsert)
    out = _run(conn, [_vin(i) for i in range(55)])
    assert out["retired"] == 3 and _active(conn) == 57


def test_dry_mode_plans_and_writes_nothing(monkeypatch):
    monkeypatch.setenv("SCANNER_RECONCILE", "dry")
    conn = _conn()
    out = _run(conn, [_vin(i) for i in range(55)])
    assert out["ran"] is False and out["dry_run"] and out["would_retire"] == 5 and out["retired"] == 0
    assert out["skipped_reason"] == "dry_run" and _active(conn) == 60


def test_off_mode_is_unchanged(monkeypatch):
    monkeypatch.setenv("SCANNER_RECONCILE", "0")
    out = _run(_conn(), [_vin(i) for i in range(55)])
    assert out["skipped_reason"] == "disabled"


def test_incomplete_walk_and_recovery_replace_retire_nothing():
    conn = _conn()
    seen = [_vin(i) for i in range(55)]
    out = _run(conn, seen, replay={"walks": [{"end": "http_403", "complete": False}]})
    assert out["reason"] == "partial:walk_http_403" and out["retired"] == 0
    out = _run(conn, seen, source="recovery:browser")
    assert out["reason"] == "partial:recovery_replaced" and out["retired"] == 0
    assert _active(conn) == 60


def test_protected_vins_stay_listed():
    conn = _conn()
    out = _run(conn, [_vin(i) for i in range(55)], protected_vins={_vin(57)})
    assert out["retired"] == 4 and out["kept"]["protected_refusal"] == 1


def test_run_refreshed_rows_never_count_as_baseline_or_retire():
    # every stored row was re-written by this run (scraped_at after run start): nothing to judge
    conn = _conn(scraped_at="2026-10-26T12:30:00.000000Z")
    out = _run(conn, [_vin(i) for i in range(55)])
    assert out["retired"] == 0 and _active(conn) == 60


def test_deprecated_min_coverage_is_ignored_with_a_warning(monkeypatch, caplog):
    monkeypatch.setenv("SCANNER_RECONCILE_MIN_COVERAGE", "0.9")
    conn = _conn()
    with caplog.at_level(logging.WARNING):
        out = _run(conn, [_vin(i) for i in range(40)] + [_vin(i) for i in range(30, 40)])
    assert "deprecated" in caplog.text
    # 30 new + 10 used of 30 used = 33% used coverage: used stays under the 0.6 share, not 0.9
    assert out["kept"]["bucket_low_coverage"] == 20


def test_no_retire_switch_matrix(monkeypatch):
    monkeypatch.setenv("SCANNER_NO_RETIRE", "1")
    assert ir.no_retire_switch()
    # scanner reconcile (full scan and delta scan both call it)
    conn = _conn()
    out = _run(conn, [_vin(i) for i in range(55)])
    assert out["skipped_reason"] == "no_retire_switch" and out["would_retire"] == 5 and _active(conn) == 60
    # rooftop disown never reaches the database
    assert disown.disown_foreign_rooftop_vins(DEALER, {_vin(1)}) == 0
    # pipeline reconcile_dealer (W2 and the lifecycle retry) turns into a report
    c2 = sqlite3.connect(":memory:")
    c2.execute("CREATE TABLE cars (id INTEGER PRIMARY KEY, dealer_id TEXT, vin TEXT, scraped_at TEXT, listing_active INTEGER DEFAULT 1, listing_removed_at TEXT)")
    c2.executemany("INSERT INTO cars (dealer_id, vin, scraped_at) VALUES ('d', ?, ?)",
                   [(f"V{i:016d}", "2026-07-10T00:00:00Z") for i in range(6)] + [(f"W{i:016d}", "2026-09-26T01:00:00Z") for i in range(10)])
    c2.commit()
    rep = reconcile_dealer(c2, "d", "2026-09-26T00:00:00Z", baseline=12, rows_this_run=25, verdict="ok")
    assert rep["eligible"] and rep["stale"] == 6 and rep["retired"] == 0
    assert c2.execute("SELECT COUNT(*) FROM cars WHERE listing_active = 0").fetchone()[0] == 0


def test_switch_off_leaves_pipeline_report_only(monkeypatch):
    monkeypatch.setenv("SCANNER_NO_RETIRE", "0")
    assert not ir.no_retire_switch()
    c2 = sqlite3.connect(":memory:")
    c2.execute("CREATE TABLE cars (id INTEGER PRIMARY KEY, dealer_id TEXT, vin TEXT, scraped_at TEXT, listing_active INTEGER DEFAULT 1, listing_removed_at TEXT)")
    c2.executemany("INSERT INTO cars (dealer_id, vin, scraped_at) VALUES ('d', ?, ?)",
                   [(f"V{i:016d}", "2026-07-10T00:00:00Z") for i in range(6)] + [(f"W{i:016d}", "2026-09-26T01:00:00Z") for i in range(10)])
    c2.commit()
    # P5A.7: the pipeline never writes; with the switch off it still reports what the guards allow
    out = reconcile_dealer(c2, "d", "2026-09-26T00:00:00Z", baseline=12, rows_this_run=25, verdict="ok")
    assert out["retired"] == 0 and out["retirable"] == 6 and not out["no_retire_switch"]
    assert c2.execute("SELECT COUNT(*) FROM cars WHERE listing_active = 1").fetchone()[0] == 16


def test_retired_ids_leave_the_incomplete_index(monkeypatch, tmp_path):
    path = tmp_path / "cars.db"
    seed = _conn()
    disk = sqlite3.connect(str(path))
    seed.backup(disk)
    disk.close()
    deleted: list[int] = []
    import backend.db.incomplete_listings_db as ild

    monkeypatch.setattr(ir, "get_conn", lambda: sqlite3.connect(str(path)))
    monkeypatch.setattr(ild, "delete_incomplete_records", lambda ids: deleted.extend(ids))
    out = ir.reconcile_dealer_inventory_after_scan(
        DEALER, URL, {_vin(i) for i in range(55)}, {"deduped_rows": 55}, run_started_iso=RUN_START
    )
    assert out["retired"] == 5 and len(deleted) == 5
    # a dry plan deletes nothing
    deleted.clear()
    monkeypatch.setenv("SCANNER_RECONCILE", "dry")
    ir.reconcile_dealer_inventory_after_scan(
        DEALER, URL, {_vin(i) for i in range(50)}, {"deduped_rows": 50}, run_started_iso=RUN_START
    )
    assert deleted == []


def test_delete_incomplete_records_is_idempotent(monkeypatch, tmp_path):
    import backend.db.incomplete_listings_db as ild

    monkeypatch.setattr(ild, "DB_PATH", str(tmp_path / "inc.db"))
    monkeypatch.setattr(ild.inventory_pg, "is_inventory_postgres", lambda: False)
    ild.reset_incomplete_listings_schema_cache()
    conn = ild.get_conn()
    ild._ensure_schema(conn)
    cols = [r[1] for r in conn.execute("PRAGMA table_info(incomplete_listings)")]
    assert "car_id" in cols
    conn.execute("INSERT INTO incomplete_listings (car_id, vin, missing_fields_json, updated_at) VALUES (1, 'A', '[]', 'x')")
    conn.execute("INSERT INTO incomplete_listings (car_id, vin, missing_fields_json, updated_at) VALUES (2, 'B', '[]', 'x')")
    conn.commit()
    conn.close()
    ild.delete_incomplete_records([1, 1])
    ild.delete_incomplete_records([])
    conn = ild.get_conn()
    assert [r[0] for r in conn.execute("SELECT car_id FROM incomplete_listings")] == [2]
    conn.close()
