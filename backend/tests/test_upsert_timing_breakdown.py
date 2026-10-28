"""P11A.7: ``stats['timing']`` splits one upsert into wall and thread-CPU parts."""
from __future__ import annotations

import pytest

from backend.scanner import database as sdb
from backend.scanner.database import upsert_vehicles
from backend.scanner.upsert import timing as tm

N = 120


@pytest.fixture()
def db(tmp_path, monkeypatch):
    db_path = tmp_path / "test.db"
    monkeypatch.setenv("INVENTORY_DB_PATH", str(db_path))
    from backend.db import inventory_db as inv

    monkeypatch.setattr(inv, "DB_PATH", str(db_path))
    monkeypatch.setattr(sdb, "_vin_owner_table_ready", False)
    monkeypatch.delenv("SCANNER_VIN_OWNER_GUARD_HOURS", raising=False)
    return str(db_path)


def _cars(n: int) -> list[dict]:
    return [
        {"vin": f"1N4BL4DV0PN{i:06d}", "title": "2023 Nissan Altima", "year": 2023, "make": "Nissan",
         "model": "Altima", "price": 30000 + i, "dealer_id": "a-com", "dealer_name": "a-com",
         "dealer_url": "https://a-com"}
        for i in range(n)
    ]


def test_every_key_filled_and_parts_sum_to_wall(db, monkeypatch):
    monkeypatch.setenv("SCANNER_MAX_DEALER_CONCURRENCY", "4")
    stats: dict = {}
    assert upsert_vehicles(_cars(N), stats) == N
    t = stats["timing"]
    for key in ("setup", "prefetch", "write_rows", "commit", "moves"):
        assert t[f"{key}_s"] >= 0 and t[f"{key}_cpu_s"] >= 0, key
    assert t["rows"] == N
    assert t["row_selects"] == N
    assert t["dealer_concurrency"] == 4
    assert t["total_s"] > 0 and t["total_cpu_s"] >= 0
    pw = t["post_write"]
    for key in ("incomplete_sync", "spec_backfill", "model_specs", "catalog_link", "total"):
        assert pw[f"{key}_s"] >= 0 and pw[f"{key}_cpu_s"] >= 0, key
    assert pw["cars"] == N
    assert pw["get_conn_calls"] >= 2  # id lookup in step 1 and in the re-sync
    assert pw["vpic_network_fetches"] >= 0
    parts = sum(t[f"{k}_s"] for k in ("setup", "prefetch", "write_rows", "commit", "moves")) + pw["total_s"]
    assert abs(t["total_s"] - parts) <= 0.10 * t["total_s"]
    assert t["other_s"] <= 0.10 * t["total_s"]


def test_timing_is_reset_on_every_attempt(db):
    stats: dict = {}
    upsert_vehicles(_cars(3), stats)
    first = stats["timing"]
    upsert_vehicles(_cars(3), stats)
    assert stats["timing"] is not first
    assert stats["timing"]["rows"] == 3


def test_no_stats_no_timing_and_same_result(db):
    assert upsert_vehicles(_cars(2)) == 2


def test_lap_accumulates_and_none_is_a_noop():
    d: dict = {}
    with tm.lap(d, "x"):
        pass
    with tm.lap(d, "x"):
        pass
    assert set(d) == {"x_s", "x_cpu_s"}
    with tm.lap(None, "x"):
        pass

