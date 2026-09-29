"""VIN ownership guard in upsert_vehicles (2026-09-29 Railway fleet incident).

``cars`` is one row per VIN (ON CONFLICT(vin)), so the last store to write a VIN
owned it: the first full fleet run moved 6,961 VINs to the wrong dealer
(mtnviewnissan-com (CA) -> cleveland-nissan-com (TN) 876, ...). A write must not
move a VIN whose stored row is active and scraped within the guard window under
a different dealer_id.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta

import pytest

from backend.scanner import database as sdb
from backend.scanner.database import upsert_vehicles

VIN = "1N4BL4DV0PN300001"


@pytest.fixture()
def db(tmp_path, monkeypatch):
    db_path = tmp_path / "test.db"
    monkeypatch.setenv("INVENTORY_DB_PATH", str(db_path))
    from backend.db import inventory_db as inv

    monkeypatch.setattr(inv, "DB_PATH", str(db_path))
    monkeypatch.setattr(sdb, "_vin_owner_table_ready", False)
    monkeypatch.delenv("SCANNER_VIN_OWNER_GUARD_HOURS", raising=False)
    return str(db_path)


def _car(dealer_id: str, price: int = 30000, vin: str = VIN) -> dict:
    return {"vin": vin, "title": "2023 Nissan Altima", "year": 2023, "make": "Nissan", "model": "Altima",
            "price": price, "dealer_id": dealer_id, "dealer_name": dealer_id, "dealer_url": f"https://{dealer_id}"}


def _row(db_path: str, vin: str = VIN):
    conn = sqlite3.connect(db_path)
    try:
        return conn.execute("SELECT dealer_id, price, listing_active FROM cars WHERE vin=?", (vin,)).fetchone()
    finally:
        conn.close()


def _set(db_path: str, sql: str, params=()):
    conn = sqlite3.connect(db_path)
    conn.execute(sql, params)
    conn.commit()
    conn.close()


def _conflicts(db_path: str):
    conn = sqlite3.connect(db_path)
    try:
        return conn.execute("SELECT vin, owner_dealer_id, claimant_dealer_id FROM vin_owner_conflicts").fetchall()
    finally:
        conn.close()


def test_fresh_active_row_of_another_dealer_is_not_reassigned(db):
    upsert_vehicles([_car("mtnviewnissan-com", 30000)])
    stats: dict = {}
    n = upsert_vehicles([_car("cleveland-nissan-com", 99999), _car("cleveland-nissan-com", 25000, vin="JN8AT3BB0PW000002")], stats)
    assert n == 1  # only the claimant's own new VIN was written
    assert _row(db) == ("mtnviewnissan-com", 30000, 1)  # owner's row untouched (price too)
    assert _row(db, "JN8AT3BB0PW000002")[0] == "cleveland-nissan-com"
    assert stats["vin_owner_conflicts"] == 1
    assert stats["vin_owner_conflict_vins"] == [VIN]
    assert stats["vin_owner_conflict_owners"] == {"mtnviewnissan-com": 1}
    assert _conflicts(db) == [(VIN, "mtnviewnissan-com", "cleveland-nissan-com")]
    # a second sighting updates seen_at, it does not add a row
    upsert_vehicles([_car("cleveland-nissan-com")])
    assert len(_conflicts(db)) == 1


def test_same_dealer_rewrites_normally(db):
    upsert_vehicles([_car("a-com", 30000)])
    stats: dict = {}
    assert upsert_vehicles([_car("a-com", 28000)], stats) == 1
    assert _row(db) == ("a-com", 28000, 1)
    assert stats["vin_owner_conflicts"] == 0


def test_stale_owner_row_can_be_taken(db):
    upsert_vehicles([_car("a-com")])
    old = (datetime.utcnow() - timedelta(hours=49)).isoformat() + "Z"
    _set(db, "UPDATE cars SET scraped_at=? WHERE vin=?", (old, VIN))
    assert upsert_vehicles([_car("b-com")]) == 1
    assert _row(db)[0] == "b-com"


def test_inactive_owner_row_can_be_taken(db):
    upsert_vehicles([_car("a-com")])
    _set(db, "UPDATE cars SET listing_active=0 WHERE vin=?", (VIN,))
    assert upsert_vehicles([_car("b-com")]) == 1
    assert _row(db) == ("b-com", 30000, 1)


def test_window_is_configurable_and_zero_disables(db, monkeypatch):
    upsert_vehicles([_car("a-com")])
    old = (datetime.utcnow() - timedelta(hours=5)).isoformat() + "Z"
    _set(db, "UPDATE cars SET scraped_at=? WHERE vin=?", (old, VIN))
    monkeypatch.setenv("SCANNER_VIN_OWNER_GUARD_HOURS", "4")
    assert upsert_vehicles([_car("b-com")]) == 1  # 5 h old > 4 h window
    assert _row(db)[0] == "b-com"
    monkeypatch.setenv("SCANNER_VIN_OWNER_GUARD_HOURS", "0")
    assert upsert_vehicles([_car("c-com")]) == 1  # guard off: last writer wins
    assert _row(db)[0] == "c-com"


def test_sql_backstop_catches_a_claim_the_prefetch_missed(db, monkeypatch):
    """A concurrent shard may claim the VIN between the prefetch and the write;
    the ON CONFLICT ... WHERE clause still refuses the move."""
    upsert_vehicles([_car("a-com", 30000)])
    monkeypatch.setattr(sdb, "_vin_owned_elsewhere", lambda *a, **k: False)
    stats: dict = {}
    assert upsert_vehicles([_car("b-com", 1)], stats) == 0
    assert _row(db) == ("a-com", 30000, 1)
    assert stats["vin_owner_conflicts"] == 1
    assert stats["vin_owner_conflict_owners"] == {"a-com": 1}


def test_guard_hours_env_parsing(monkeypatch):
    monkeypatch.delenv("SCANNER_VIN_OWNER_GUARD_HOURS", raising=False)
    assert sdb.vin_owner_guard_hours() == 48
    monkeypatch.setenv("SCANNER_VIN_OWNER_GUARD_HOURS", "junk")
    assert sdb.vin_owner_guard_hours() == 48
    monkeypatch.setenv("SCANNER_VIN_OWNER_GUARD_HOURS", "0")
    assert sdb.vin_owner_guard_hours() == 0


def test_scan_summary_and_triage_reason_carry_the_count():
    """scan_runs.summary_json gets vin_owner_conflicts; the pipeline triage names
    it in the reason when > 10."""
    import json

    from backend.scripts import dealer_pipeline as dp

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE cars (id INTEGER PRIMARY KEY, dealer_id TEXT, vin TEXT, condition TEXT, dealer_name TEXT, zip_code TEXT, scraped_at TEXT, listing_active INTEGER)")
    conn.execute("CREATE TABLE scan_runs (id INTEGER PRIMARY KEY, dealer_id TEXT, provider TEXT, finished_at TEXT, duration_seconds REAL, inventory_rows INTEGER, upserted INTEGER, error TEXT, summary_json TEXT)")
    summary = {"capture_coverage": {"n": 0}, "vin_owner_conflicts": 876,
               "vin_owner_conflict_owners": {"mtnviewnissan-com": 876}}
    conn.execute("INSERT INTO scan_runs (dealer_id, provider, finished_at, duration_seconds, inventory_rows, upserted, error, summary_json) VALUES (?,?,?,?,?,?,?,?)",
                 ("cleveland-nissan-com", "dealer_dot_com", "2026-09-29T10:00:00+00:00", 60.0, 0, 0, None, json.dumps(summary)))
    out = dp.assess(conn, "cleveland-nissan-com", "2026-09-29T00:00:00+00:00", known_before=0, recipe_info={"had_recipes": 1})
    assert out["vin_owner_conflicts"] == 876
    assert "VIN owner guard: 876" in out["reason"] and "mtnviewnissan-com 876" in out["reason"]

    conn.execute("UPDATE scan_runs SET summary_json=?", (json.dumps({"capture_coverage": {"n": 0}, "vin_owner_conflicts": 10}),))
    out = dp.assess(conn, "cleveland-nissan-com", "2026-09-29T00:00:00+00:00", known_before=0, recipe_info={"had_recipes": 1})
    assert "VIN owner guard" not in (out.get("reason") or "")


def test_record_scan_outcomes_writes_conflict_count(tmp_path, monkeypatch):
    import json

    from backend.db import inventory_db as inv
    from backend.db.repositories import dealers_repo

    db_path = tmp_path / "t.db"
    monkeypatch.setenv("INVENTORY_DB_PATH", str(db_path))
    monkeypatch.setattr(inv, "DB_PATH", str(db_path))
    dealers_repo.record_scan_outcomes(
        [{"dealer_id": "b-com", "vin_owner_conflicts": 12, "vin_owner_conflict_owners": {"a-com": 12}}],
        finished_at="2026-09-29T00:00:00Z",
    )
    conn = sqlite3.connect(str(db_path))
    s = json.loads(conn.execute("SELECT summary_json FROM scan_runs").fetchone()[0])
    conn.close()
    assert s["vin_owner_conflicts"] == 12 and s["vin_owner_conflict_owners"] == {"a-com": 12}
