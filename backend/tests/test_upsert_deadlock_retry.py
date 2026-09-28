"""Upsert retries a Postgres deadlock victim (Chapman Ford, 2026-09-28)."""
from __future__ import annotations

import asyncio

import pytest

from backend.scanner import inventory_write as iw


class DeadlockDetected(Exception):
    sqlstate = "40P01"


def _coordinator(monkeypatch):
    monkeypatch.setattr(iw, "scanner_parallel_upsert_enabled", lambda: True)
    real_sleep = asyncio.sleep
    monkeypatch.setattr(iw.asyncio, "sleep", lambda *_a, **_k: real_sleep(0))
    return iw.InventoryWriteCoordinator()


def test_deadlock_is_retried_then_succeeds(monkeypatch):
    calls = {"n": 0}

    def fake_upsert(vehicles):
        calls["n"] += 1
        if calls["n"] == 1:
            raise DeadlockDetected("deadlock detected")
        return len(vehicles)

    import backend.scanner.database as db

    monkeypatch.setattr(db, "upsert_vehicles", fake_upsert)
    co = _coordinator(monkeypatch)
    assert asyncio.run(co.upsert_vehicles([{"vin": "A"}, {"vin": "B"}])) == 2
    assert calls["n"] == 2


def test_other_errors_are_not_retried(monkeypatch):
    calls = {"n": 0}

    def fake_upsert(vehicles):
        calls["n"] += 1
        raise ValueError("bad row")

    import backend.scanner.database as db

    monkeypatch.setattr(db, "upsert_vehicles", fake_upsert)
    co = _coordinator(monkeypatch)
    with pytest.raises(ValueError):
        asyncio.run(co.upsert_vehicles([{"vin": "A"}]))
    assert calls["n"] == 1


def test_persistent_deadlock_gives_up_after_three(monkeypatch):
    calls = {"n": 0}

    def fake_upsert(vehicles):
        calls["n"] += 1
        raise DeadlockDetected("deadlock detected")

    import backend.scanner.database as db

    monkeypatch.setattr(db, "upsert_vehicles", fake_upsert)
    co = _coordinator(monkeypatch)
    with pytest.raises(DeadlockDetected):
        asyncio.run(co.upsert_vehicles([{"vin": "A"}]))
    assert calls["n"] == 3
