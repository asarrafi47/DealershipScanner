"""Runtime state files resolve outside tracked trees; the scan counter is race-safe."""
from __future__ import annotations

import importlib
import json
import multiprocessing as mp
import os
import subprocess
import sys
from pathlib import Path

from backend.utils import runtime_state

REPO = Path(__file__).resolve().parents[2]


def test_state_path_default_is_workspace_state(monkeypatch):
    monkeypatch.delenv("RUNTIME_STATE_DIR", raising=False)
    assert runtime_state.state_path("x.json") == REPO / "workspace" / "state" / "x.json"


def test_state_path_env_override(monkeypatch, tmp_path):
    monkeypatch.setenv("RUNTIME_STATE_DIR", str(tmp_path))
    assert runtime_state.state_path("x.json") == tmp_path / "x.json"


def test_consumers_use_state_path(monkeypatch):
    monkeypatch.delenv("RUNTIME_STATE_DIR", raising=False)
    from backend.cron import scan_run_counter, sync_gas_prices
    from backend.routes import fuel_api

    for p in (scan_run_counter.COUNTER_PATH, sync_gas_prices.OUTPUT_PATH, fuel_api._LIVE_GAS_PRICES_PATH):
        assert "dictionary" not in p.parts
        assert p.parent == REPO / "workspace" / "state"


def test_fuel_lookup_unchanged_with_file_absent(monkeypatch, tmp_path):
    from backend.routes import fuel_api

    monkeypatch.setattr(fuel_api, "_LIVE_GAS_PRICES_PATH", tmp_path / "missing.json")
    monkeypatch.setattr(fuel_api, "_live_gas_prices_cache", None)
    monkeypatch.setattr(fuel_api, "_live_gas_prices_cache_mtime", None)
    assert fuel_api._load_live_gas_prices_payload() is None
    from backend.cron.sync_gas_prices import fallback_payload

    got = {k: v for k, v in fuel_api._fuel_market_payload().items() if k != "synced_at"}
    assert got == {k: v for k, v in fallback_payload().items() if k != "synced_at"}


def test_write_json_atomic_leaves_no_tmp(tmp_path):
    target = tmp_path / "a" / "s.json"
    runtime_state.write_json_atomic(target, {"k": 1})
    assert json.loads(target.read_text()) == {"k": 1}
    assert [p.name for p in target.parent.iterdir()] == ["s.json"]


def _bump(state_dir: str, n: int) -> None:
    os.environ["RUNTIME_STATE_DIR"] = state_dir
    from backend.cron import scan_run_counter

    for _ in range(n):
        scan_run_counter.record_scanner_batch_finished()


def test_two_concurrent_writers_both_count(tmp_path):
    ctx = mp.get_context("spawn")
    procs = [ctx.Process(target=_bump, args=(str(tmp_path), 15)) for _ in range(2)]
    for p in procs:
        p.start()
    for p in procs:
        p.join(120)
        assert p.exitcode == 0
    state = json.loads((tmp_path / "scanner_run_counter.json").read_text())
    assert state["batch_count"] == 30
