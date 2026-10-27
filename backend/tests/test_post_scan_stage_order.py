"""P8B.3: post-scan runs the vPIC decode first and the heal last; no fleet-wide model_specs call."""
from __future__ import annotations

import asyncio
import inspect
import re

import pytest

VINS = ["1HGCM82633A004352"]


def _stub_stages(monkeypatch, mod, calls):
    import backend.enrichment.vpic_facts as vf
    import backend.scanner.database as dbm

    monkeypatch.setenv("SCANNER_POST_VPIC", "1")
    monkeypatch.setattr(mod, "post_vpic_env_enabled", lambda: True, raising=False)
    import backend.scanner.scan_efficiency as se

    monkeypatch.setattr(se, "post_vpic_env_enabled", lambda: True)
    monkeypatch.setattr(vf, "post_scan_decode", lambda v: calls.append("decode") or {})
    monkeypatch.setattr(vf, "post_scan_heal", lambda v: calls.append("heal") or {})
    monkeypatch.setattr(dbm, "apply_model_specs_corrections", lambda *a, **k: calls.append("model_specs") or 0)
    monkeypatch.setattr(mod, "run_post_scan", lambda *a, **k: calls.append("post_scan") or {})
    monkeypatch.setattr(mod, "run_listing_gap_fill_stage", lambda v: calls.append("gap_fill") or {})
    monkeypatch.setattr(mod, "post_dict_enrich_env_enabled", lambda: True)
    monkeypatch.setattr(mod, "run_dictionary_enrich_for_vins", lambda v: calls.append("dictionary") or {})
    monkeypatch.setattr(mod, "run_sync_gas_prices", lambda: {})
    monkeypatch.setattr(mod, "run_dealer_google_rating_backfill_if_due", lambda **k: {})
    import backend.db.incomplete_listings_db as ild

    monkeypatch.setattr(ild, "fast_rebuild_incomplete_listings_index", lambda: 0)


def _assert_order(calls):
    assert calls[0] == "decode"
    assert calls[-1] == "heal"
    assert calls.index("decode") < calls.index("post_scan") < calls.index("gap_fill") < calls.index("dictionary") < calls.index("heal")
    assert calls.count("decode") == 1 and calls.count("heal") == 1
    assert "model_specs" not in calls


def test_job_stage_order(monkeypatch):
    from backend.scanner.post_scan import job

    calls: list[str] = []
    _stub_stages(monkeypatch, job, calls)
    asyncio.run(job.run_post_scan_job(VINS, post_listing_gap_fill=True, post_gas_prices=False, post_dealer_ratings=False))
    _assert_order(calls)


def test_orchestrator_stage_order(monkeypatch):
    from backend.scanner import orchestrator as orch

    calls: list[str] = []
    _stub_stages(monkeypatch, orch, calls)
    monkeypatch.setattr(orch, "aggregate_vins_from_dealer_results", lambda o: list(VINS))
    asyncio.run(orch._run_post_scan_tail(
        [], post_repair=True, post_listing_description=False, post_interior_vision=False, post_enrich=False,
        post_enrich_vision_only=False, post_listing_gap_fill=True, post_window_sticker=False, post_gas_prices=False,
        post_dealer_ratings=False, post_gallery_vision=False, enrichment_max_workers=None))
    _assert_order(calls)


def test_no_fleet_wide_model_specs_call_in_post_scan_sources():
    from backend.scanner import orchestrator as orch
    from backend.scanner.post_scan import job

    for mod in (orch, job):
        assert "apply_model_specs_corrections" not in inspect.getsource(mod)


def test_pipeline_vpic_per_batch_plus_backstop():
    from backend.scanner.pipeline import run

    src = inspect.getsource(run._run)
    assert len(re.findall(r"vpic_for_dealers\(", src)) == 2
    assert src.index("vpic_for_dealers(batch)") < src.index("vpic_for_dealers(scannable)")
