"""P11A.8: connection-class errors are retried with a longer backoff; retry counts are
reported in ``stats['timing']``; a mid-batch deadlock leaves the same rows as a clean run."""
from __future__ import annotations

import asyncio
import json

import pytest

from backend.scanner import database as sdb
from backend.scanner import inventory_write as iw
from backend.scanner.upsert import write as uw


class _Err(Exception):
    def __init__(self, msg: str = "", sqlstate: str | None = None):
        super().__init__(msg)
        self.sqlstate = sqlstate


class OperationalError(Exception):
    """Name-matches psycopg.OperationalError; no sqlstate."""


def _coordinator(monkeypatch, sleeps: list | None = None):
    monkeypatch.setattr(iw, "scanner_parallel_upsert_enabled", lambda: True)
    real_sleep = asyncio.sleep

    async def fake_sleep(d, *_a, **_k):
        if sleeps is not None:
            sleeps.append(d)
        await real_sleep(0)

    monkeypatch.setattr(iw.asyncio, "sleep", fake_sleep)
    return iw.InventoryWriteCoordinator()


@pytest.mark.parametrize("code", ["08006", "08001", "53300", "57P01"])
def test_connection_sqlstates_are_retryable(code):
    assert iw._is_retryable_db_error(_Err("x", code))
    assert iw._is_connection_error(_Err("x", code))


def test_operational_error_without_sqlstate_needs_connection_message():
    assert iw._is_retryable_db_error(OperationalError("connection refused"))
    assert iw._is_retryable_db_error(OperationalError("server closed the connection unexpectedly"))
    assert not iw._is_retryable_db_error(OperationalError("syntax weirdness"))


def test_statement_timeout_not_retried():
    assert not iw._is_retryable_db_error(_Err("canceling statement due to statement timeout", "57014"))


def test_connection_backoff_ladder_is_2_4_8_with_jitter():
    e = _Err("x", "08006")
    for attempt, base in ((1, 2.0), (2, 4.0), (3, 8.0), (4, 8.0)):
        d = iw._retry_delay(e, attempt)
        assert base <= d <= base * 1.25
    d = iw._retry_delay(_Err("x", "40P01"), 2)
    assert 1.0 <= d <= 1.5


def test_connection_loss_retried_and_counted(monkeypatch):
    import backend.scanner.database as db

    calls = {"n": 0}

    def fake_upsert(vehicles, stats=None):
        calls["n"] += 1
        stats["timing"] = {"rows": 0}  # the real upsert resets this on every attempt
        if calls["n"] <= 2:
            raise _Err("connection lost", "08006")
        return len(vehicles)

    monkeypatch.setattr(db, "upsert_vehicles", fake_upsert)
    sleeps: list = []
    co = _coordinator(monkeypatch, sleeps)
    stats: dict = {}
    assert asyncio.run(co.upsert_vehicles([{"vin": "A"}], stats)) == 1
    assert calls["n"] == 3
    assert stats["timing"]["upsert_retries"] == 2
    assert stats["timing"]["upsert_conn_retries"] == 2
    assert 2.0 <= sleeps[0] <= 2.5 and 4.0 <= sleeps[1] <= 5.0


def test_connection_error_gives_up_after_four(monkeypatch):
    import backend.scanner.database as db

    calls = {"n": 0}

    def fake_upsert(vehicles, stats=None):
        calls["n"] += 1
        raise _Err("too many connections", "53300")

    monkeypatch.setattr(db, "upsert_vehicles", fake_upsert)
    co = _coordinator(monkeypatch)
    stats: dict = {}
    with pytest.raises(_Err):
        asyncio.run(co.upsert_vehicles([{"vin": "A"}], stats))
    assert calls["n"] == 4
    assert stats["timing"]["upsert_retries"] == 3


def test_statement_timeout_raises_without_retry(monkeypatch):
    import backend.scanner.database as db

    calls = {"n": 0}

    def fake_upsert(vehicles, stats=None):
        calls["n"] += 1
        raise _Err("canceling statement due to statement timeout", "57014")

    monkeypatch.setattr(db, "upsert_vehicles", fake_upsert)
    co = _coordinator(monkeypatch)
    stats: dict = {}
    with pytest.raises(_Err):
        asyncio.run(co.upsert_vehicles([{"vin": "A"}], stats))
    assert calls["n"] == 1
    assert "upsert_retries" not in stats.get("timing", {})


# --- deadlock after the first 200-row commit, real (Postgres) upsert ------------------------

N = 450


def _cars(price_base: int) -> list[dict]:
    return [
        {"vin": f"1N4BL4DV0PN{i:06d}", "title": "2023 Nissan Altima", "year": 2023, "make": "Nissan",
         "model": "Altima", "price": price_base + i, "dealer_id": "a-com", "dealer_name": "a-com",
         "dealer_url": "https://a-com"}
        for i in range(N)
    ]


@pytest.fixture()
def pg_env(pg_db, monkeypatch):
    """Postgres clone; the network post-write steps stubbed (vPIC backfill, catalog linker)."""
    monkeypatch.setattr("backend.utils.project_env.load_project_dotenv", lambda *a, **k: None)
    monkeypatch.setattr(sdb, "_vin_owner_table_ready", False)
    monkeypatch.delenv("SCANNER_VIN_OWNER_GUARD_HOURS", raising=False)
    monkeypatch.setattr(
        "backend.enrichment.spec_structured_backfill.apply_structured_spec_backfill_for_car",
        lambda *a, **k: {},
    )
    monkeypatch.setattr("backend.catalog.linker.link_cars_by_vins", lambda vins: 0)
    return pg_db


def _reset_cars(env):
    with env.connect() as conn:
        conn.execute("DELETE FROM cars")
        conn.commit()


def _dump():
    con = sdb.get_conn()
    try:
        rows = con.execute("SELECT vin, price, price_provenance_json FROM cars ORDER BY vin").fetchall()
    finally:
        con.close()
    return [(v, p, [h["price"] for h in json.loads(ph or "[]")]) for v, p, ph in rows]


@pytest.mark.pg
def test_deadlock_after_first_batch_commit_matches_clean_run(pg_env, monkeypatch):
    monkeypatch.setattr(iw, "scanner_parallel_upsert_enabled", lambda: False)
    # clean run: seed at 30000+i, then reprice to 31000+i
    sdb.upsert_vehicles(_cars(30000))
    sdb.upsert_vehicles(_cars(31000))
    expected = _dump()

    # faulted run: same seed, then the reprice deadlocks on row 250 (after the row-200 commit)
    _reset_cars(pg_env)
    sdb.upsert_vehicles(_cars(30000))
    real = uw.fetch_prev_price_row
    seen = {"n": 0, "raised": False}

    def flaky(cursor, vin):
        seen["n"] += 1
        if seen["n"] == 250 and not seen["raised"]:
            seen["raised"] = True
            raise _Err("deadlock detected", "40P01")
        return real(cursor, vin)

    monkeypatch.setattr(uw, "fetch_prev_price_row", flaky)
    co = _coordinator(monkeypatch)
    stats: dict = {}
    assert asyncio.run(co.upsert_vehicles(_cars(31000), stats)) == N
    assert seen["raised"] and stats["timing"]["upsert_retries"] == 1
    got = _dump()
    assert got == expected
    # one history entry for the seed and exactly one for the reprice, per VIN
    assert all(len(h) == 2 for _v, _p, h in got)
