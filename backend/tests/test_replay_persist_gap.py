"""P5B.7: replay->persist gap telemetry and the parity pin.

terrylabonte kept 324 VINs through the replay and wrote 11, and nothing in the result, the
scan_runs row or triage said so. ``persist`` now names a write that kept under half of what
the replay kept; the pin feeds one unscoped two-store carscommerce feed through
``try_fetch_via_recipes`` and then ``run_dealer`` and compares what was written with what the
replay kept. This shape (store rows stamped with the store's own name, sibling rows with another
rooftop's) passes today, so the pin is a plain test; Terry's own drop (Hendrick address-block
rows, N8.8) is not reproduced by it.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time

import pytest

import backend.scanner.recipes as rec
import backend.tests.test_dealer_run_golden_20261001 as G
import backend.tests.test_rooftop_corpus as C
from backend.scanner.network_observer import CapturedEndpoint
from backend.scanner.phases.dealer_run_steps.persist import persist, replay_persist_gap
from backend.scanner.phases.dealer_run_steps.state import DealerRun, new_result
from backend.scanner.pipeline.assess import assess


def _vin(i: int) -> str:
    return f"1HGBH41JXMN{i:06d}"


# ── the telemetry ────────────────────────────────────────────────────────────

def test_gap_is_named_only_when_under_half_were_written():
    assert replay_persist_gap(324, 11, {"unattributable": 313, "vin_owner_guard": 0}) == {
        "kept": 324, "persisted": 11, "dropped_by": {"unattributable": 313}}
    assert replay_persist_gap(100, 50, {}) is None  # exactly half is not a gap
    assert replay_persist_gap(100, 49, {}) == {"kept": 100, "persisted": 49, "dropped_by": {}}
    assert replay_persist_gap(0, 0, {}) is None


def _run(vehicles, replay) -> DealerRun:
    result = new_result("d-com", "D", "p")
    if replay is not None:
        result["recipe_replay"] = replay
    run = DealerRun(dealer={}, name="D", url="https://d.com", provider="p", dealer_id="d-com",
                    result=result, t0=time.perf_counter())
    run.all_vehicles = vehicles
    return run


class _Coord:
    def __init__(self, written, stats):
        self.written, self.stats = written, stats

    async def upsert_vehicles(self, vehicles, stats=None):
        stats.update(self.stats)
        return self.written


def _persist(run, written, stats=None):
    run.write_coordinator = _Coord(written, stats or {})
    asyncio.run(persist(run))
    return run.result


def test_persist_records_the_gap_logs_a_warning_and_names_the_dropping_step(caplog):
    caplog.set_level(logging.WARNING, logger="scanner")
    rows = [{"vin": _vin(i)} for i in range(40)]
    out = _persist(_run(rows, {"walks": []}), 8, {"unattributable_vins": {_vin(i): "third_rooftop" for i in range(30)},
                                                  "identifier_gate_refused": {"no_vin": 2}})
    assert out["replay_persist_gap"] == {"kept": 40, "persisted": 8,
                                         "dropped_by": {"unattributable": 30, "identifier_gate": 2}}
    assert any("Replay->persist gap" in r.getMessage() and r.levelno == logging.WARNING for r in caplog.records)


def test_persist_is_quiet_for_a_healthy_write_and_for_a_run_without_a_replay():
    rows = [{"vin": _vin(i)} for i in range(40)]
    assert "replay_persist_gap" not in _persist(_run(rows, {"walks": []}), 40)
    assert "replay_persist_gap" not in _persist(_run(rows, None), 3)


def test_the_gap_reaches_scan_runs_and_assess_adds_a_note():
    from backend.db.repositories import dealers_repo

    src = open(dealers_repo.__file__, encoding="utf-8").read()
    assert '"replay_persist_gap": o.get("replay_persist_gap")' in src  # the summary_json whitelist

    import sqlite3
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    from backend.scanner.pipeline import assess as A
    gap = {"kept": 324, "persisted": 11, "dropped_by": {"unattributable": 313}}
    A_assess = A._assess
    try:
        A._assess = lambda *a, **k: {"verdict": "ok", "reason": "complete and verified", "replay_persist_gap": gap}
        out = assess(conn, "d-com", "2026-01-01", 0, {})
    finally:
        A._assess = A_assess
    assert out["verdict"] == "ok"  # a verdict is never changed by the note
    assert "replay->persist gap: 11 written of 324 VINs the replay kept (unattributable 313)" in out["reason"]


# ── the parity pin ───────────────────────────────────────────────────────────

DEALER_ID = "tuttoncdjr-com"
CC_URL = "https://websites-search.api.carscommerce.inc/api/v1/listings/6033780/search"


def _two_store_feed(fx: dict) -> tuple[list[dict], set[str]]:
    own = fx["scenarios"][0]["rows"][0]["stamp"]
    sibling = {"location": "Voyles CDJR of Birmingham", "source_id": "MP22431"}
    rows, own_vins = [], set()
    for i in range(12):
        v = f"2C3CDXBG{i:09d}"
        rows.append(C._cc_listing(v, "New", own, fx["feed"]["account"]))
        own_vins.add(v)
    for i in range(12):
        rows.append(C._cc_listing(f"2C4RC1BG{i:09d}", "Used", sibling, fx["feed"]["account"]))
    return rows, own_vins


def test_unscoped_two_store_feed_persists_exactly_what_the_replay_kept(monkeypatch, caplog, tmp_path):
    monkeypatch.setattr(rec, "RECIPES_DIR", tmp_path / "recipes")
    fx = G._fixture(DEALER_ID)
    rows, _own = _two_store_feed(fx)
    ep = CapturedEndpoint(url=CC_URL, method="POST", content_type="application/json",
                          post_data_sample='{"page":1,"perPage":50}', reason="legacy", sniffed=False,
                          vehicle_rows=len(rows), total_count=len(rows), auth_headers={"authorization": "Bearer k"})
    rec.promote_from_ledger(DEALER_ID, "carscommerce", [ep])

    def fake_req(recipe, body, base_url, url=None):
        page = {"data": {"listings": rows if body.get("page", 1) == 1 else []},
                "meta": {"pagination": {"total": len(rows)}}}
        return 200, page

    monkeypatch.setattr(rec, "_replay_request", fake_req)
    real_fetch, real_load = rec.try_fetch_via_recipes, rec.load_recipes

    h = G.Harness(monkeypatch, caplog)
    h.base_env()
    h.stubs(aliases=fx["roster"].get("aliases") or (), place=C._place_kwargs(fx, {"place": None}), known=len(rows))
    monkeypatch.setattr("backend.scanner.recipes.try_fetch_via_recipes", real_fetch)
    monkeypatch.setattr("backend.scanner.recipes.load_recipes", real_load)
    monkeypatch.setattr("backend.scanner.recipes.recipe_yield_replaces_browser", lambda *a, **k: (True, ""))

    dealer = G._corpus_dealer(fx, "carscommerce")
    # the replay, on its own: which VINs does it keep for this store?
    from backend.attribution import DealerCtx, decide, parse_page
    out: dict = {}
    hit = asyncio.run(real_fetch(DEALER_ID, "carscommerce", dealer["url"], dealer["name"], union=True, replay_out=out))
    ctx = DealerCtx.for_store(DEALER_ID, dealer["name"], dealer["url"], None)
    refused: list = []
    kept = []
    for _u, body in hit[0]:
        kept.extend(parse_page("carscommerce", body, ctx, refused))
    replay_kept = {v["vin"] for v in decide(kept + refused, ctx).kept}
    assert replay_kept  # the pin is meaningless on an empty replay

    result = h.run(dealer)["result"]
    persisted = {v["vin"] for u in h.upserts for v in u["vehicles"]}
    assert persisted == replay_kept
    assert "replay_persist_gap" not in (result or {})
