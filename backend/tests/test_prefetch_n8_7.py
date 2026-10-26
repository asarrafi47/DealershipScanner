"""N8.7 prefetch quick fixes: ownership pre-check, gap-first order, 403/429 counts, exhausted re-check."""
from __future__ import annotations

import asyncio
import sqlite3

import pytest

from backend.scanner import database as sdb
from backend.scanner.database import upsert_vehicles, vins_refused_by_owner_guard
from backend.scanner.vdp import prefetch as pf

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


def _car(dealer_id, vin=VIN):
    return {"vin": vin, "title": "2023 Nissan Altima", "year": 2023, "make": "Nissan", "model": "Altima",
            "price": 30000, "dealer_id": dealer_id, "dealer_name": dealer_id, "dealer_url": f"https://{dealer_id}"}


def test_precheck_reports_refused_vin_and_writes_nothing(db):
    upsert_vehicles([_car("owner-com")])
    def _n():
        c = sqlite3.connect(db)
        try:
            return c.execute("SELECT count(*) FROM vin_owner_conflicts").fetchone()[0]
        except sqlite3.OperationalError:
            return 0  # the table is created on the first recorded conflict
        finally:
            c.close()

    before = _n()
    out = vins_refused_by_owner_guard([_car("claimant-com"), _car("claimant-com", vin="JN8AT3BB0PW000002")])
    assert list(out) == [VIN]
    assert _n() == before == 0


def test_precheck_empty_when_guard_off(db, monkeypatch):
    upsert_vehicles([_car("owner-com")])
    monkeypatch.setenv("SCANNER_VIN_OWNER_GUARD_HOURS", "0")
    assert vins_refused_by_owner_guard([_car("claimant-com")]) == {}


def test_prefetch_details_makes_no_fetch_for_refused_vin(db, monkeypatch):
    from backend.scanner.phases.dealer_run_steps import enrich

    upsert_vehicles([_car("owner-com")])
    seen = []

    async def _fake(vehicles, dealer_id, name):
        seen.extend(v["vin"] for v in vehicles)
        return None

    monkeypatch.setattr(pf, "prefetch_before_vdp", _fake)

    class _Run:
        result: dict = {}
        name = "Claimant"
        dealer_id = "claimant-com"
        all_vehicles = [{**_car("claimant-com"), "dealer_id": ""}, _car("claimant-com", vin="JN8AT3BB0PW000002")]

    run = _Run()
    run.result = {}
    asyncio.run(enrich.prefetch_details(run))
    assert seen == ["JN8AT3BB0PW000002"]
    assert run.result["vdp_prefetch_ownership_skipped"] == 1
    assert len(run.all_vehicles) == 2  # refused rows stay for the persist step


def test_candidates_are_ordered_gap_first(monkeypatch):
    base = {"exterior_color": "Blue", "interior_color": "Black", "price": 20000, "trim": "EX",
            "gallery": [f"https://i.example/{i}.jpg" for i in range(12)], "description": ""}
    cars = [
        {**base, "vin": "PHOTO", "gallery": [], "_detail_url": "https://d.example/4"},
        {**base, "vin": "TRIM", "trim": "", "_detail_url": "https://d.example/3"},
        {**base, "vin": "COLOR", "exterior_color": "", "_detail_url": "https://d.example/2"},
        {**base, "vin": "PRICE", "price": None, "_detail_url": "https://d.example/1"},
    ]
    order = []
    def _fetch(url):
        order.append(url)
        pf._LAST_STATUS[url] = 500  # answered, so the host is not judged a tarpit
        return None

    monkeypatch.setattr(pf, "_fetch_html", _fetch)
    monkeypatch.setattr(pf, "_http_first_max", lambda *a, **k: 100)
    monkeypatch.setattr(pf, "_HTTP_CONCURRENCY", 1)
    monkeypatch.setattr(pf, "_HTTP_POLITE_DELAY_S", 0)
    asyncio.run(pf.http_prefetch_missing_fields(cars))
    assert order == ["https://d.example/1", "https://d.example/2", "https://d.example/3", "https://d.example/4"]


def test_403_and_429_are_counted_per_attempt(monkeypatch):
    def _blocked(url):
        pf._LAST_STATUS[url] = 403
        return None

    monkeypatch.setattr(pf, "_fetch_html", _blocked)
    monkeypatch.setattr(pf, "_HTTP_POLITE_DELAY_S", 0)
    monkeypatch.setattr(pf, "_SLOW_HOST_DELAY_S", 0)
    monkeypatch.setattr(pf, "_SLOW_HOST_COOLDOWN_S", 0)
    monkeypatch.setattr(pf, "_SLOW_HOSTS", set())
    monkeypatch.setattr(pf, "_HOST_COOLDOWN_UNTIL", {})
    v = {"vin": "V1", "_detail_url": "https://blocked.example/car", "price": None}
    stats = asyncio.run(pf.http_prefetch_missing_fields([v]))
    assert stats["statuses"]["403"] >= 1
    assert stats["fetched"] == 0


def test_exhausted_host_is_rechecked_inside_the_lock(monkeypatch):
    calls = []

    def _blocked(url):
        calls.append(url)
        pf._LAST_STATUS[url] = 429
        return None

    monkeypatch.setattr(pf, "_fetch_html", _blocked)
    monkeypatch.setattr(pf, "_SLOW_HOST_DELAY_S", 0)
    monkeypatch.setattr(pf, "_SLOW_HOST_COOLDOWN_S", 0)
    monkeypatch.setattr(pf, "_SLOW_HOST_MAX_FAILS", 1)
    monkeypatch.setattr(pf, "_slow_host_budget", lambda: 1000)
    monkeypatch.setattr(pf, "_SLOW_HOSTS", {"hot.example"})
    monkeypatch.setattr(pf, "_HOST_COOLDOWN_UNTIL", {})
    monkeypatch.setattr(pf, "_HOST_LOCKS", {})
    cars = [{"vin": f"V{i}", "_detail_url": f"https://hot.example/{i}", "price": None} for i in range(6)]
    stats = asyncio.run(pf.http_prefetch_missing_fields(cars))
    assert stats["host_exhausted"] == "hot.example"
    # tasks queued behind the lock when the host was exhausted never hit the network
    assert len(calls) < 6 * 3
    assert stats["slow_host_skipped"] >= 1
