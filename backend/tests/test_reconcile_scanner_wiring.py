"""P5A.5: the evidence the full scan hands the reconcile pass (protected VINs, start time, source, walk)."""
from __future__ import annotations

import asyncio

from backend.attribution.disown import protected_refusal_vins
from backend.scanner.database import drop_unattributable_vehicles
from backend.scanner.phases.dealer_run_steps.after_write import reconcile_evidence
from backend.scanner.phases.dealer_run_steps.state import new_result, DealerRun
import time


def _vin(n: int) -> str:
    return f"WA1ANAFY4L20{n:05d}"


def _run(**kw) -> DealerRun:
    return DealerRun(dealer={}, name="d", url="https://d.com", provider="p", dealer_id="d-com",
                     result=new_result("d-com", "d", "p"), t0=time.perf_counter(), **kw)


def test_drop_returns_the_vins_it_dropped_with_reasons():
    kept, n, dropped = drop_unattributable_vehicles([
        {"vin": _vin(1)},
        {"vin": _vin(2).lower(), "_rooftop_reject": "target_rooftop_unidentified"},
        {"vin": _vin(3), "_rooftop_reject": "third_rooftop"},
    ])
    assert [v["vin"] for v in kept] == [_vin(1)] and n == 2
    assert dropped == {_vin(2): "target_rooftop_unidentified", _vin(3): "third_rooftop"}


def test_dropped_vin_is_protected_but_third_rooftop_is_not():
    _, _, dropped = drop_unattributable_vehicles([
        {"vin": _vin(2), "_rooftop_reject": "target_rooftop_unidentified"},
        {"vin": _vin(3), "_rooftop_reject": "third_rooftop"},
        {"vin": _vin(4), "_rooftop_reject": "sibling_rooftop"},
    ])
    assert protected_refusal_vins(dropped=dropped) == {_vin(2)}


def test_refused_rows_follow_the_same_rule():
    refused = [
        {"vin": _vin(5), "_rooftop_reject": "vdp_host_other_store"},
        {"vin": _vin(6), "_rooftop_reject": "third_rooftop"},
    ]
    assert protected_refusal_vins(refused) == {_vin(5)}


def test_evidence_carries_start_walk_and_protected_vins():
    run = _run()
    run.rooftop_refused = [{"vin": _vin(7), "_rooftop_reject": "target_rooftop_unidentified"}]
    run.unattributable_vins = {_vin(8): "unstamped_row_in_group_feed"}
    run.result["recipe_replay"] = {"walks": [{"end": "total_reached"}]}
    ev = reconcile_evidence(run)
    assert ev["protected_vins"] == {_vin(7), _vin(8)}
    assert ev["replay"] == {"walks": [{"end": "total_reached"}]}
    assert ev["source"] == "recipe"
    assert ev["run_started_iso"].endswith("+00:00")


def test_recovery_replaced_run_is_marked_with_its_strategy():
    run = _run()
    run.result["inventory_recovery"] = {"winning_strategy": "html_cards", "replaced": True, "strategies_tried": ["html_cards"]}
    assert reconcile_evidence(run)["source"] == "recovery:html_cards"
    run.result["inventory_recovery"]["replaced"] = False
    assert reconcile_evidence(run)["source"] == "recipe"


def test_after_write_passes_the_evidence_to_reconcile():
    from backend.scanner.phases.dealer_run_steps import after_write as aw

    seen = {}

    def fake(dealer_id, url, scraped, result, **kw):
        seen.update(kw)
        return {"ran": True}

    run = _run()
    run.rooftop_refused = [{"vin": _vin(9), "_rooftop_reject": "target_rooftop_unidentified"}]
    asyncio.run(aw.reconcile(run, set(), set(), fake))
    assert seen["protected_vins"] == {_vin(9)} and seen["source"] == "recipe"
    assert "run_started_iso" in seen and "scraped_conditions" in seen
