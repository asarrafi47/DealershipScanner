"""P5A.6: the delta scan retires through the same policy (plan_retirement) and evidence as the full scan."""
from __future__ import annotations

import asyncio
import sqlite3

import pytest

import backend.scanner.delta_scan as ds
from backend.scanner import inventory_reconcile as ir


@pytest.fixture(autouse=True)
def _single_miss(monkeypatch):
    """These tests pin the retirement POLICY (which ids); the two-strike layer (P5B.4) has its own
    file, test_two_strike_retirement.py. One miss retires here."""
    monkeypatch.setenv("SCANNER_RETIRE_STRIKES", "1")


DEALER = "d-matrix"
URL = "https://d-matrix.com"
OLD = "2026-10-20T00:00:00.000000Z"
RUN_START = "2026-10-26T12:00:00+00:00"


def _vin(i: int) -> str:
    return f"1HGBH41JXMN{i:06d}"


def _conn(n_new=30, n_used=30, ghosts=0) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.execute(
        "CREATE TABLE cars (id INTEGER PRIMARY KEY, dealer_id TEXT, dealer_url TEXT, vin TEXT, condition TEXT, "
        "make TEXT, scraped_at TEXT, listing_active INTEGER, listing_removed_at TEXT)"
    )
    for i in range(n_new + n_used):
        conn.execute(
            "INSERT INTO cars (dealer_id, dealer_url, vin, condition, make, scraped_at, listing_active) "
            "VALUES (?, ?, ?, ?, 'Honda', ?, 1)",
            (DEALER, URL, _vin(i), "New" if i < n_new else "Used", OLD),
        )
    for g in range(ghosts):  # backlog: active rows with no valid VIN
        conn.execute(
            "INSERT INTO cars (dealer_id, dealer_url, vin, condition, make, scraped_at, listing_active) "
            "VALUES (?, ?, ?, 'Used', 'Honda', ?, 1)", (DEALER, URL, f"UNKNOWN{g}", OLD),
        )
    conn.commit()
    return conn


class _Keep:
    """A connection whose close() is a no-op, so the test reads the DB after reconcile closed it."""
    def __init__(self, conn):
        self._c = conn

    def __getattr__(self, name):
        return (lambda: None) if name == "close" else getattr(self._c, name)


def _active(conn, cond=None) -> int:
    q = "SELECT COUNT(*) FROM cars WHERE listing_active = 1"
    if cond:
        q += f" AND condition = '{cond}'"
    return conn.execute(q).fetchone()[0]


def _dealer():
    return {"dealer_id": DEALER, "url": URL, "name": "D", "provider": "dealer_dot_com"}


def _patch(monkeypatch, conn, feed, *, walks=None, known=None, n_new=30):
    rows = [{"vin": _vin(i), "price": 20000 + i, "condition": "New" if i < n_new else "Used"} for i in feed]

    async def _fetch(dealer_id, provider, base_url, dealer_name, **kw):
        if walks is not None and kw.get("replay_out") is not None:
            kw["replay_out"]["walks"] = list(walks)
        return ([(URL + "/api", {"x": 1})], len(rows))

    monkeypatch.setattr("backend.scanner.recipes.try_fetch_via_recipes", _fetch)
    monkeypatch.setattr("backend.parsers.parse", lambda *a, **k: [dict(r) for r in rows])
    monkeypatch.setattr(ds, "_active_count", lambda d: known if known is not None else _active(conn))
    monkeypatch.setattr(ir, "get_conn", lambda: _Keep(conn))
    monkeypatch.setattr(ir, "ensure_cars_table_columns", lambda cur: None)
    monkeypatch.setattr(ir, "_delete_index_rows", lambda ids: None)
    for k in ("SCANNER_RECONCILE", "SCANNER_NO_RETIRE", "SCANNER_RECONCILE_MIN_COVERAGE",
              "SCANNER_RECONCILE_BUCKET_SHARE", "SCANNER_RECONCILE_MIN_ROWS"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("SCANNER_VDP_DB_MERGE", "0")
    monkeypatch.setenv("SCANNER_POST_LISTING_GAP_FILL", "0")

    class _Coord:
        async def upsert_vehicles(self, vehicles, stats=None):
            return len(vehicles)

    monkeypatch.setattr("backend.scanner.inventory_write.InventoryWriteCoordinator", _Coord)


def test_delta_replay_of_only_new_cars_keeps_the_used_rows(monkeypatch):
    conn = _conn()
    _patch(monkeypatch, conn, range(0, 30))  # all 30 new re-seen, no used in the feed
    out = asyncio.run(ds.delta_scan_dealer(_dealer()))
    assert out["skipped"] is None
    assert _active(conn, "Used") == 30 and _active(conn, "New") == 30
    assert out["reconcile"]["retired"] == 0 and out["reconcile"]["kept"]["bucket_low_coverage"] >= 0


def test_delta_new_only_replay_still_retires_the_gone_new_cars(monkeypatch):
    conn = _conn(n_new=40, n_used=20)
    _patch(monkeypatch, conn, range(0, 38), n_new=40)  # two new gone; used absent
    out = asyncio.run(ds.delta_scan_dealer(_dealer()))
    assert out["reconcile"]["marked_inactive"] == 2
    assert _active(conn, "Used") == 20 and _active(conn, "New") == 38


def test_delta_truncated_walk_retires_nothing(monkeypatch):
    conn = _conn()
    _patch(monkeypatch, conn, range(0, 59), walks=[{"end": "http_403", "complete": False}])
    out = asyncio.run(ds.delta_scan_dealer(_dealer()))
    assert _active(conn) == 60
    assert out["reconcile"]["reason"] == "partial:walk_http_403" and out["reconcile"]["retired"] == 0
    assert out["recipe_replay"]["walks"]


def test_delta_and_full_scan_make_the_same_plan_except_for_the_share(monkeypatch):
    feed = range(0, 57)  # 3 gone, both buckets seen at 90%+
    d_conn, f_conn = _conn(), _conn()
    _patch(monkeypatch, d_conn, feed, walks=[{"end": "total_reached", "complete": True}])
    out = asyncio.run(ds.delta_scan_dealer(_dealer()))
    seen = {ir.normalize_scanner_vin(_vin(i)) for i in feed}
    full = ir.reconcile_dealer_inventory_after_scan(
        DEALER, URL, seen, {"deduped_rows": len(seen)},
        replay={"walks": [{"end": "total_reached", "complete": True}]},
        protected_vins=set(), run_started_iso=ds.datetime.now(ds.timezone.utc).isoformat(),
        _conn=f_conn,
    )
    for k in ("eligible", "reason", "retired", "kept", "buckets"):
        assert out["reconcile"][k] == full[k], k
    assert _active(d_conn) == _active(f_conn) == 57


def test_delta_share_is_stricter_than_the_full_scan_default(monkeypatch):
    # used bucket re-seen at 70%: the full scan's 0.6 retires it, the delta's 0.8 keeps it
    feed = list(range(0, 30)) + list(range(30, 51))
    conn = _conn()
    _patch(monkeypatch, conn, feed)
    out = asyncio.run(ds.delta_scan_dealer(_dealer()))
    assert out["reconcile"]["retired"] == 0 and _active(conn) == 60
    assert out["reconcile"]["kept"]["bucket_low_coverage"] == 9
    full_conn = _conn()
    seen = {ir.normalize_scanner_vin(_vin(i)) for i in feed}
    full = ir.reconcile_dealer_inventory_after_scan(
        DEALER, URL, seen, {"deduped_rows": len(seen)}, run_started_iso=ds.datetime.now(ds.timezone.utc).isoformat(),
        _conn=full_conn,
    )
    assert full["retired"] == 9


def test_delta_backlog_dealer_retires_the_vinless_ghosts_through_a_retirable_bucket(monkeypatch):
    conn = _conn(ghosts=10)
    _patch(monkeypatch, conn, range(0, 60))
    out = asyncio.run(ds.delta_scan_dealer(_dealer()))
    full_conn = _conn(ghosts=10)
    seen = {ir.normalize_scanner_vin(_vin(i)) for i in range(60)}
    full = ir.reconcile_dealer_inventory_after_scan(
        DEALER, URL, seen, {"deduped_rows": len(seen)}, run_started_iso=ds.datetime.now(ds.timezone.utc).isoformat(),
        _conn=full_conn,
    )
    assert out["reconcile"]["retired"] == full["retired"]
    assert _active(conn) == _active(full_conn)


def test_delta_passes_the_run_evidence(monkeypatch):
    conn = _conn()
    seen_kw = {}
    _patch(monkeypatch, conn, range(0, 60), walks=[{"end": "short_page", "complete": True}])
    monkeypatch.setattr(
        "backend.scanner.inventory_reconcile.reconcile_dealer_inventory_after_scan",
        lambda *a, **k: seen_kw.update(k) or {"ran": False},
    )
    asyncio.run(ds.delta_scan_dealer(_dealer()))
    assert seen_kw["bucket_share"] == 0.8 and seen_kw["source"] == "recipe"
    assert seen_kw["replay"]["walks"] and seen_kw["protected_vins"] == set()
    assert seen_kw["run_started_iso"]
