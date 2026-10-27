"""P5A.7: the pipeline and lifecycle reconcile are report-only; the gap-aware alarm threshold
(D-N21) and the triage alarm lines. The database cases are in test_pipeline_report_only_pg.py."""
from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from backend.scanner.pipeline import reconcile as rec
from backend.scanner.pipeline.triage import retirement_alarm_lines
from backend.scanner.retirement_policy import ActiveRow, Policy, RunEvidence, plan_retirement

PIPELINE = Path(__file__).resolve().parents[1] / "scanner" / "pipeline"


def test_nothing_under_pipeline_writes_to_cars():
    for path in PIPELINE.glob("*.py"):
        src = path.read_text(encoding="utf-8")
        assert not re.search(r"\bUPDATE\s+cars\b|\bDELETE\s+FROM\s+cars\b|INSERT\s+INTO\s+cars\b", src, re.I), path.name
        assert not re.search(r"listing_active\s*=\s*0", src), path.name


def test_invalid_vin_and_backlog_rows_still_retire_through_policy_rule_8():
    """What the old W2 retired by id, the scanner's policy retires: a stale row with no valid VIN
    (and one far older than the baseline window) goes with a retirable bucket."""
    now = datetime(2026, 10, 20, 12, tzinfo=timezone.utc)
    rows = [ActiveRow(i, f"VIN{i:014d}", "new", "ford", now - timedelta(days=3)) for i in range(40)]
    rows += [ActiveRow(100, None, "new", "ford", now - timedelta(days=3)),    # invalid VIN
             ActiveRow(101, f"VIN{101:014d}", "new", "ford", now - timedelta(days=200))]  # backlog
    ev = RunEvidence(seen_vins={f"VIN{i:014d}" for i in range(30)}, matched_makes={"ford"},
                     run_started_iso=now.isoformat(), rows=30)
    plan = plan_retirement(rows, ev, Policy(), now)
    assert plan.eligible and {100, 101} <= set(plan.retire_ids)
    assert set(plan.retire_ids) == set(range(30, 40)) | {100, 101}


def test_alarm_threshold_nightly_and_gap_aware():
    assert rec.alarm_threshold(0, gap_aware=False) == 0.10
    assert rec.alarm_threshold(35, gap_aware=False) == 0.10
    assert rec.alarm_threshold(0, gap_aware=True) == pytest.approx(0.10)
    assert rec.alarm_threshold(10, gap_aware=True) == pytest.approx(0.25)
    # 35 days: 10% + 52.5% = 62.5%, capped at 60%
    assert rec.alarm_threshold(35, gap_aware=True) == 0.60


def test_triage_alarm_lines():
    results = [{"dealer_id": "a", "reconcile": {"scanner_retired": {"alarm": True, "retired": 12, "prior_active": 100,
                                                                   "share": 0.12, "threshold": 0.10, "days": 1.0}}},
               {"dealer_id": "b", "reconcile": {"scanner_retired": {"alarm": False, "retired": 1}}},
               {"dealer_id": "c", "reconcile": {"error": "x"}}]
    lines = retirement_alarm_lines(results)
    assert len(lines) == 1 and lines[0].startswith("retirement alarm: a retired 12 of 100")


def test_incomplete_alarm_lines():
    from backend.scanner.pipeline.triage import incomplete_alarm_lines, incomplete_open_items

    def res(d, n, inc):
        return {"dealer_id": d, "accuracy": {"rows": n, "incomplete_rows": inc, "missing": {"price": inc}}}

    results = [res("hot-com", 100, 40), res("edge-com", 100, 25), res("tiny-com", 10, 9),
               res("known-com", 100, 90), res("fine-com", 100, 3), {"dealer_id": "noacc-com"}]
    lines = incomplete_alarm_lines(results, {"known-com": "N21.4"})
    assert lines == ["incomplete alarm: hot-com 40 of 100 rows incomplete (40% > 25%), no open item; missing price"]
    # the shipped list excuses the dealers the findings already name, and only those
    shipped = incomplete_open_items()
    assert "gardenahonda-com" in shipped and "hot-com" not in shipped
    assert incomplete_alarm_lines([res("gardenahonda-com", 646, 646)]) == []
