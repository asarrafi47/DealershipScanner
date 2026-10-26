"""Delta scan quality gates: a thin or price-less replay must never touch the DB."""
from __future__ import annotations

import asyncio

import pytest

import backend.scanner.delta_scan as ds


def _dealer():
    return {"dealer_id": "d-com", "url": "https://d.example", "name": "D", "provider": "dealer_dot_com"}


def _rows(n, priced=True):
    return [{"vin": f"1HGBH41JXMN1{i:05d}", "price": 20000 + i if priced else None} for i in range(n)]


def _patch_pipeline(monkeypatch, *, replay_rows, known_active, upserts):
    async def _fake_fetch(dealer_id, provider, base_url, dealer_name, **kw):
        return ([("https://d.example/api", {"x": 1})], len(replay_rows))

    monkeypatch.setattr("backend.scanner.recipes.try_fetch_via_recipes", _fake_fetch)
    monkeypatch.setattr("backend.parsers.parse", lambda *a, **k: list(replay_rows))
    monkeypatch.setattr(ds, "_active_count", lambda dealer_id: known_active)
    monkeypatch.setenv("SCANNER_VDP_DB_MERGE", "0")

    class _FakeCoordinator:
        async def upsert_vehicles(self, vehicles, stats=None):
            upserts.append(list(vehicles))
            return len(vehicles)

    monkeypatch.setattr("backend.scanner.inventory_write.InventoryWriteCoordinator", _FakeCoordinator)


def test_delta_skips_partial_feed(monkeypatch):
    upserts = []
    _patch_pipeline(monkeypatch, replay_rows=_rows(10), known_active=100, upserts=upserts)
    out = asyncio.run(ds.delta_scan_dealer(_dealer()))
    assert out["skipped"] and "partial_feed" in out["skipped"]
    assert upserts == []


def test_delta_skips_priceless_feed(monkeypatch):
    upserts = []
    _patch_pipeline(monkeypatch, replay_rows=_rows(90, priced=False), known_active=100, upserts=upserts)
    out = asyncio.run(ds.delta_scan_dealer(_dealer()))
    assert out["skipped"] and "low_price_coverage" in out["skipped"]
    assert upserts == []


def test_delta_upserts_good_feed_but_gates_reconcile(monkeypatch):
    upserts = []
    reconciles = []
    _patch_pipeline(monkeypatch, replay_rows=_rows(60), known_active=100, upserts=upserts)
    monkeypatch.setattr(
        "backend.scanner.inventory_reconcile.reconcile_dealer_inventory_after_scan",
        lambda *a, **k: reconciles.append(a) or {"ran": True},
    )
    out = asyncio.run(ds.delta_scan_dealer(_dealer()))
    # 60% coverage: good enough to refresh rows, NOT good enough to mark cars sold.
    assert out["skipped"] is None
    assert out["upserted"] == 60
    assert reconciles == []
    assert "vin_coverage" in out["reconcile"]["skipped_reason"]


def test_delta_full_feed_reconciles(monkeypatch):
    upserts = []
    reconciles = []
    _patch_pipeline(monkeypatch, replay_rows=_rows(95), known_active=100, upserts=upserts)
    monkeypatch.setattr(
        "backend.scanner.inventory_reconcile.reconcile_dealer_inventory_after_scan",
        lambda *a, **k: reconciles.append(a) or {"ran": True, "marked_inactive": 2},
    )
    out = asyncio.run(ds.delta_scan_dealer(_dealer()))
    assert out["upserted"] == 95
    assert len(reconciles) == 1


def test_delta_no_recipe_skips(monkeypatch):
    async def _none(*a, **k):
        return None

    monkeypatch.setattr("backend.scanner.recipes.try_fetch_via_recipes", _none)
    out = asyncio.run(ds.delta_scan_dealer(_dealer()))
    assert out["skipped"] == "no_recipe_yield"


def test_delta_carries_the_manifest_registry_id_to_the_gate_and_the_rows(monkeypatch):
    """N8.14: a store on a shared host is scanned with its own registry row (place + stamp)."""
    seen = {}
    upserts = []
    _patch_pipeline(monkeypatch, replay_rows=_rows(60), known_active=60, upserts=upserts)

    async def _fetch(dealer_id, provider, base_url, dealer_name, **kw):
        seen["fetch"] = kw
        return ([("https://d.example/api", {"x": 1})], 60)

    monkeypatch.setattr("backend.scanner.recipes.try_fetch_via_recipes", _fetch)
    real = ds.DealerCtx.for_store
    monkeypatch.setattr(ds.DealerCtx, "for_store", classmethod(lambda cls, *a: seen.setdefault("ctx", a) and real.__func__(cls, *a)))
    monkeypatch.setattr("backend.attribution.gate.store_place", lambda *a: {})
    monkeypatch.setattr("backend.scanner.inventory_reconcile.reconcile_dealer_inventory_after_scan", lambda *a, **k: {"ran": True})
    d = dict(_dealer(), dealership_registry_id=302)
    asyncio.run(ds.delta_scan_dealer(d))
    assert seen["fetch"] == {"union": True, "registry_id": 302, "replay_out": {}}
    assert seen["ctx"][-1] == 302
    assert upserts and all(v["dealership_registry_id"] == 302 for v in upserts[0])
