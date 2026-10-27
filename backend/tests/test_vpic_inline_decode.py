"""P8B.4: uncached VINs are decoded inline on the scan write path, budgeted, off the
event loop, failing open. Fetch is stubbed; the cache is a throwaway SQLite table."""
from __future__ import annotations

import asyncio
import json
import sqlite3
import threading
import time

import pytest

from backend.enrichment import knowledge_engine as ke
from backend.enrichment import vpic_facts as vf

VIN_A = "1HGCY2F60PA000001"
VIN_B = "1HGCY2F60PA000002"


class _Conn:
    """sqlite3 shim: the production adapter takes '?' placeholders too."""

    def __init__(self):
        self._c = sqlite3.connect(":memory:", check_same_thread=False)
        self._c.execute("CREATE TABLE nhtsa_vpic_cache (vin TEXT PRIMARY KEY, response_json TEXT, fetched_at TEXT)")

    def cursor(self):
        return self._c.cursor()

    def execute(self, sql, params=()):
        return self._c.execute(sql, params)

    def commit(self):
        self._c.commit()

    def close(self):
        pass


def _payload(vin, drive="4WD/4-Wheel Drive/4x4"):
    return {"Count": 1, "Message": "Batch decode",
            "Results": [{"VIN": vin, "DriveType": drive, "ErrorCode": "0"}]}


@pytest.fixture
def cache(monkeypatch):
    conn = _Conn()
    monkeypatch.setattr(ke, "_conn", lambda: conn)
    monkeypatch.setenv("SCANNER_VIN_DECODE_INLINE", "1")
    monkeypatch.setattr(vf, "_PAUSE_S", 0)
    ke.clear_vpic_lookup_cache()
    yield conn
    ke.clear_vpic_lookup_cache()


def test_disabled_by_default_under_test(monkeypatch):
    monkeypatch.setenv("SCANNER_VIN_DECODE_INLINE", "0")
    monkeypatch.setattr(vf, "fetch_vpic_batch", lambda *a, **k: pytest.fail("fetched"))
    assert vf.decode_inline([VIN_A])["requested"] == 0


def test_uncached_decoded_once_and_memo_popped_only_for_decoded(cache, monkeypatch):
    calls = []

    def fetch(vins, timeout=90):
        calls.append((list(vins), timeout))
        return {v: _payload(v) for v in vins}

    monkeypatch.setattr(vf, "fetch_vpic_batch", fetch)
    ke._VPIC_MEMO[VIN_A] = {"stale": 1}
    ke._VPIC_MEMO["OTHERVIN"] = {"keep": 1}
    out = vf.decode_inline([VIN_A, VIN_A, VIN_B])
    assert out["stored"] == 2 and len(calls) == 1
    assert 0 < calls[0][1] <= 15
    assert VIN_A not in ke._VPIC_MEMO and "OTHERVIN" in ke._VPIC_MEMO
    vf.decode_inline([VIN_A])  # cached now: no second fetch
    assert len(calls) == 1


def test_override_vehicles_decodes_then_overrides(cache, monkeypatch):
    monkeypatch.setattr(vf, "fetch_vpic_batch", lambda vins, timeout=90: {v: _payload(v) for v in vins})
    cars = [{"vin": VIN_A, "drivetrain": "RWD"}]
    stats = vf.override_vehicles(cars)
    assert stats["decoded_inline"] == 1 and stats["drivetrain"] == 1
    assert cars[0]["drivetrain"] != "RWD"
    assert json.loads(cars[0]["spec_source_json"])["drivetrain"]["source"] == "nhtsa_vpic"


def test_http_error_fails_open(cache, monkeypatch):
    def boom(vins, timeout=90):
        raise RuntimeError("503")

    monkeypatch.setattr(vf, "fetch_vpic_batch", boom)
    cars = [{"vin": VIN_A, "drivetrain": "RWD"}]
    stats = vf.override_vehicles(cars)
    assert stats["decode_failed"] == 1 and cars[0]["drivetrain"] == "RWD"


def test_budget_exhaustion_skips_remaining_batches(cache, monkeypatch):
    monkeypatch.setenv("SCANNER_VIN_DECODE_INLINE_BUDGET_S", "0.05")
    seen = []

    def slow(vins, timeout=90):
        seen.append(timeout)
        time.sleep(0.08)
        return {v: _payload(v) for v in vins}

    monkeypatch.setattr(vf, "fetch_vpic_batch", slow)
    vins = [f"1HGCY2F60PA{n:06d}" for n in range(120)]  # 3 batches of 50
    out = vf.decode_inline(vins)
    assert len(seen) == 1 and seen[0] <= 0.05
    assert out["stored"] == 50 and out["skipped_budget"] == 70


def test_connection_error_fails_open(monkeypatch):
    monkeypatch.setenv("SCANNER_VIN_DECODE_INLINE", "1")
    monkeypatch.setattr(ke, "_conn", lambda: (_ for _ in ()).throw(RuntimeError("no db")))
    assert vf.decode_inline([VIN_A]).get("error") == 1


def test_other_coroutine_progresses_during_decode(cache, monkeypatch):
    from backend.scanner.phases.dealer_run_steps import enrich

    gate = threading.Event()
    in_decode = threading.Event()

    def blocking(vins, timeout=90):
        in_decode.set()
        gate.wait(5)
        return {v: _payload(v) for v in vins}

    monkeypatch.setattr(vf, "fetch_vpic_batch", blocking)

    class _Run:
        name = "d"
        all_vehicles = [{"vin": VIN_A, "drivetrain": "RWD"}]
        result: dict = {}

    async def main():
        run = _Run()
        run.result = {}
        task = asyncio.create_task(enrich.apply_vin_facts(run))
        ticks = 0
        while not in_decode.is_set():
            await asyncio.sleep(0.01)
        for _ in range(5):  # the loop is free while the decode blocks
            await asyncio.sleep(0.01)
            ticks += 1
        gate.set()
        await task
        return ticks, run

    ticks, run = asyncio.run(main())
    assert ticks == 5 and run.result["vin_facts"]["decoded_inline"] == 1
