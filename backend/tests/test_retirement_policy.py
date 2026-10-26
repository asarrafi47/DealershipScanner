"""P5A.1: the pure retirement policy. No database, no environment; the whole file runs in well under 2 s."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from backend.scanner.retirement_policy import ActiveRow, Policy, RunEvidence, plan_retirement

NOW = datetime(2026, 10, 20, 12, 0, tzinfo=timezone.utc)
START = NOW.isoformat()
FIX = Path(__file__).resolve().parent / "fixtures" / "cleanroom_shapes" / "inrun_retirements_20261014.json"


def _vin(tag: str, i: int) -> str:
    return f"{tag}{i:0{17 - len(tag)}d}"


def _rows(tag: str, n: int, bucket: str, *, days_ago: float = 1, make: str = "Honda", start: int = 0) -> list[ActiveRow]:
    ts = NOW - timedelta(days=days_ago)
    return [ActiveRow(f"{tag}{i}", _vin(tag, i), bucket, make, ts) for i in range(start, start + n)]


def _seen(rows: list[ActiveRow], upto: int) -> set[str]:
    return {r.vin_norm for r in rows[:upto]}


def _ev(seen: set[str], *, rows: int | None = None, **kw) -> RunEvidence:
    kw.setdefault("matched_makes", {"honda"})
    return RunEvidence(seen_vins=frozenset(seen), rows=len(seen) if rows is None else rows, run_started_iso=START, **kw)


def _plan(rows, ev, **policy):
    return plan_retirement(rows, ev, Policy(**policy), NOW)


def test_policy_module_is_pure():
    src = (Path(__file__).resolve().parents[1] / "scanner" / "retirement_policy.py").read_text()
    for needle in ("backend.db", "os.environ", "getenv", "sqlite3", "psycopg"):
        assert needle not in src.split('"""', 2)[2], needle


def test_both_buckets_complete_retires_the_missing_vins():
    new, used = _rows("N", 100, "new"), _rows("U", 100, "used")
    seen = _seen(new, 95) | _seen(used, 95)
    plan = _plan(new + used, _ev(seen))
    assert plan.eligible and plan.reason == "ok"
    assert set(plan.retire_ids) == {r.id for r in new[95:] + used[95:]}
    assert plan.buckets["new"]["coverage"] == 0.95 and plan.buckets["new"]["retirable"]


def test_parkside_306_new_zero_used_keeps_the_used_rows():
    new, used = _rows("N", 310, "new"), _rows("U", 120, "used")
    plan = _plan(new + used, _ev(_seen(new, 306)))
    assert set(plan.retire_ids) == {r.id for r in new[306:]}
    assert plan.kept["condition_missing"] == 120
    assert not plan.buckets["used"]["retirable"]


def test_lexus_231_new_zero_used_against_237_baseline_retires_no_used():
    new, used = _rows("N", 237, "new", make="Lexus"), _rows("U", 90, "used", make="Lexus")
    plan = _plan(new + used, _ev(_seen(new, 231), matched_makes={"lexus"}))
    assert not set(plan.retire_ids) & {r.id for r in used}
    assert set(plan.retire_ids) == {r.id for r in new[231:]}


def test_page_cap_walk_is_partial():
    new = _rows("N", 100, "new")
    ev = _ev(_seen(new, 100), walks=[{"key": "a", "end": "page_cap", "complete": False, "stale_retry": False}])
    plan = _plan(new + _rows("S", 5, "new", days_ago=2, start=500), ev)
    assert not plan.eligible and plan.reason == "partial:walk_page_cap" and plan.retire_ids == ()
    assert plan.kept["walk_incomplete"] == 5 and plan.walks_summary["capped"] == 1


def test_mid_walk_403_is_partial():
    new = _rows("N", 100, "new")
    ev = _ev(_seen(new, 90), walks=[{"end": "http_403", "complete": False}])
    plan = _plan(new, ev)
    assert plan.reason == "partial:walk_http_403" and plan.retire_ids == ()


def test_stale_retry_failing_again_does_not_block():
    new = _rows("N", 100, "new")
    walks = [{"end": "total_reached", "complete": True}, {"end": "http_403", "complete": False, "stale_retry": True}]
    plan = _plan(new, _ev(_seen(new, 95), walks=walks))
    assert plan.eligible and len(plan.retire_ids) == 5
    assert plan.walks_summary["stale_retries"] == 1


def test_walk_without_complete_flag_uses_the_end_code():
    new = _rows("N", 100, "new")
    assert _plan(new, _ev(_seen(new, 95), walks=[{"end": "short_page"}])).eligible
    assert not _plan(new, _ev(_seen(new, 95), walks=[{"end": "unparsed_mid"}])).eligible


def test_right_toyota_shape_retires_the_july_ghosts():
    # 3,034 active: 1,367 scraped inside the window, the rest July ghosts; the run re-saw 1,273 across both buckets
    new = _rows("N", 700, "new", make="Toyota")
    used = _rows("U", 667, "used", make="Toyota")
    ghosts = _rows("G", 1667, "used", days_ago=100, make="Toyota")
    seen = _seen(new, 650) | _seen(used, 623)
    assert len(seen) == 1273
    plan = _plan(new + used + ghosts, _ev(seen, matched_makes={"toyota"}))
    assert plan.eligible
    assert {r.id for r in ghosts} <= set(plan.retire_ids)
    assert len(plan.retire_ids) == (700 - 650) + (667 - 623) + 1667
    assert round(plan.buckets["used"]["coverage"], 2) == 0.93


def test_gunn_honda_eight_row_run_retires_nothing():
    new = _rows("N", 200, "new")
    plan = _plan(new, _ev(_seen(new, 8)))
    assert not plan.eligible and plan.retire_ids == () and plan.reason.startswith("partial:min_rows")


def test_mb_beverly_hills_two_stamped_rows_retires_nothing():
    used = _rows("U", 150, "used", make="Mercedes-Benz")
    plan = _plan(used, _ev(_seen(used, 2), matched_makes={"mercedes-benz"}))
    assert plan.retire_ids == () and not plan.eligible


def test_one_condition_ok_retires_stray_rows_of_the_absent_bucket():
    used, stray = _rows("U", 200, "used"), _rows("N", 4, "new")
    ev = _ev(_seen(used, 195))
    assert _plan(used + stray, ev).retire_ids == tuple(r.id for r in used[195:])
    ok = _plan(used + stray, ev, one_condition_ok=True)
    assert set(ok.retire_ids) == {r.id for r in used[195:] + stray}


def test_one_condition_ok_partial_capture_retires_nothing():
    used, stray = _rows("U", 200, "used"), _rows("N", 4, "new")
    plan = _plan(used + stray, _ev(_seen(used, 30), rows=30), one_condition_ok=True)
    assert plan.retire_ids == ()


def test_one_condition_ok_does_not_waive_coverage_of_a_present_bucket():
    new, used = _rows("N", 100, "new"), _rows("U", 100, "used")
    plan = _plan(new + used, _ev(_seen(new, 100) | _seen(used, 10)), one_condition_ok=True)
    assert not plan.buckets["used"]["retirable"]
    assert not set(plan.retire_ids) & {r.id for r in used}


def test_protected_vins_are_kept():
    new = _rows("N", 100, "new")
    protected = {new[97].vin_norm, new[98].vin_norm}
    plan = _plan(new, _ev(_seen(new, 95), protected_vins=protected))
    assert set(plan.retire_ids) == {new[95].id, new[96].id, new[99].id}
    assert plan.kept["protected_refusal"] == 2


def test_a_make_returned_as_zero_with_ten_prior_rows_is_kept():
    honda = _rows("H", 100, "new", make="Honda")
    acura = _rows("A", 12, "new", make="Acura", start=0)
    plan = _plan(honda + acura, _ev(_seen(honda, 100), matched_makes={"honda"}))
    assert not set(plan.retire_ids) & {r.id for r in acura}
    assert plan.kept["make_missing"] == 12


def test_a_small_missing_make_is_not_guarded():
    honda, acura = _rows("H", 100, "new"), _rows("A", 9, "new", make="Acura")
    plan = _plan(honda + acura, _ev(_seen(honda, 100)))
    assert {r.id for r in acura} <= set(plan.retire_ids)


def test_five_used_seen_of_300_prior_used_makes_used_non_retirable():
    new, used = _rows("N", 100, "new"), _rows("U", 300, "used")
    plan = _plan(new + used, _ev(_seen(new, 100) | _seen(used, 5)))
    assert not plan.buckets["used"]["retirable"] and plan.kept["bucket_low_coverage"] == 295
    assert plan.retire_ids == ()


def test_300_vins_that_match_nothing_stored_retire_nothing():
    used = _rows("U", 150, "used")
    plan = _plan(used, _ev({_vin("Z", i) for i in range(300)}))
    assert plan.retire_ids == () and not plan.eligible
    assert plan.kept["condition_missing"] == 150


def test_400_stale_rows_plus_a_25_row_complete_run_retire_nothing():
    stale = _rows("S", 400, "used", days_ago=200)
    # the run's own 25 rows are stored by now, stamped at or after the run start
    own = [ActiveRow(f"F{i}", _vin("F", i), "used", "Honda", NOW + timedelta(seconds=i)) for i in range(25)]
    plan = _plan(stale + own, _ev({r.vin_norm for r in own}))
    assert plan.buckets["used"]["prior"] == 400 and plan.buckets["used"]["window"] == "all"
    assert plan.retire_ids == () and not plan.eligible


def test_recovery_replaced_run_retires_nothing():
    new = _rows("N", 100, "new")
    plan = _plan(new, _ev(_seen(new, 60), source="recovery:replaced"))
    assert plan.reason == "partial:recovery_replaced" and plan.retire_ids == ()
    assert plan.kept["recovery_replaced"] == 40
    assert _plan(new, _ev(_seen(new, 60), source="recovery:replaced"), retire_on_recovery_replaced=True).eligible


def test_run_error_retires_nothing():
    new = _rows("N", 100, "new")
    plan = _plan(new, _ev(_seen(new, 100), error="HTTP 403"))
    assert plan.reason.startswith("run_error") and plan.retire_ids == ()


def test_rows_refreshed_by_this_run_are_never_retired_or_counted():
    new = _rows("N", 100, "new")
    refreshed = [ActiveRow("R1", _vin("R", 1), "new", "Honda", NOW + timedelta(minutes=1))]
    plan = _plan(new + refreshed, _ev(_seen(new, 100)))
    assert plan.buckets["new"]["prior"] == 100 and "R1" not in plan.retire_ids


def test_baselines_count_only_valid_vins():
    new = _rows("N", 100, "new")
    vinless = [ActiveRow(f"V{i}", None, "new", "Honda", NOW - timedelta(days=1)) for i in range(50)]
    plan = _plan(new + vinless, _ev(_seen(new, 70)))
    assert plan.buckets["new"]["prior"] == 100 and plan.buckets["new"]["coverage"] == 0.7
    assert {r.id for r in vinless} <= set(plan.retire_ids)  # rule 8: through a retirable bucket


def test_invalid_vin_rows_stay_when_their_bucket_is_not_retirable():
    new = _rows("N", 100, "new")
    vinless = [ActiveRow(f"V{i}", None, "used", "Honda", NOW - timedelta(days=1)) for i in range(5)]
    plan = _plan(new + vinless, _ev(_seen(new, 100)))
    assert plan.retire_ids == () and plan.kept["condition_missing"] == 5


def test_unknown_condition_rows_need_every_known_bucket():
    new, used = _rows("N", 100, "new"), _rows("U", 100, "used")
    blank = [ActiveRow(f"B{i}", _vin("B", i), "unknown", "Honda", NOW - timedelta(days=1)) for i in range(10)]
    seen = _seen(new, 100) | _seen(used, 100) | {b.vin_norm for b in blank[:8]}
    assert set(_plan(new + used + blank, _ev(seen)).retire_ids) == {"B8", "B9"}
    low = _seen(new, 100) | _seen(used, 10) | {b.vin_norm for b in blank[:8]}
    assert _plan(new + used + blank, _ev(low)).retire_ids == ()


# ── the clean-room retry shapes (10-13 first pass, 10-14 retry), policy level ───

def _shape(dealer_id: str) -> tuple[list[ActiveRow], RunEvidence, datetime]:
    d = json.loads(FIX.read_text())["dealers"][dealer_id]
    first_at = datetime.fromisoformat(d["first_pass_at"].replace("Z", "+00:00"))
    retry_at = datetime.fromisoformat(d["retry_at"].replace("Z", "+00:00"))
    make = "Chevrolet"
    rows = [
        ActiveRow(r["vin"], r["vin"], "new" if r["condition"] == "New" else "used", make, first_at)
        for r in d["first_pass"]
    ]
    seen = {r["vin"] for r in d["first_pass"] if r["re_seen_by_retry"]}
    seen |= {f"RETRY{c[0]}{i:011d}" for c, n in d["retry_new_rows"].items() for i in range(n)}
    ev = RunEvidence(
        seen_vins=frozenset(seen), matched_makes=frozenset({"chevrolet"}), rows=sum(d["retry_new_rows"].values()) + len(seen & {r.id for r in rows}),
        run_started_iso=retry_at.isoformat(),
    )
    return rows, ev, retry_at


@pytest.mark.parametrize(
    "dealer_id, expect",
    [
        ("terrylabontechevy-com", 0),
        ("coastlinecdjr-com", 3),
        ("napletonhondamortongrove-com", 1),
        ("southcountylexus-com", 1),
    ],
)
def test_retry_shapes_nominate_only_the_unseen_first_pass_rows(dealer_id, expect):
    """Terry 0/16 re-seen retires none (the 134-row retry never measured the prior rows);
    Coastline 8/11, Napleton 16/17 and South County Lexus 14/15 nominate 3/1/1 (struck, not
    retired, once the strike store of P5B.4 sits on top)."""
    rows, ev, retry_at = _shape(dealer_id)
    plan = plan_retirement(rows, ev, Policy(), retry_at)
    assert len(plan.retire_ids) == expect
    unseen = {r.id for r in rows if r.vin_norm not in ev.seen_vins}
    assert set(plan.retire_ids) == (set() if dealer_id.startswith("terry") else unseen)


# ── a 35-day gap with 30% turnover ──────────────────────────────────────────────

def test_35_day_gap_with_30_percent_turnover_takes_the_90_day_fallback():
    """Every stored row is 35 days old, so the 30-day window is empty. Rule 4 falls back to 90
    days: 70% coverage clears the 60% share, and the 30% that turned over are nominated on
    run 1 and again, unchanged, on run 2 (the strike store retires them on the second miss)."""
    rows = _rows("N", 100, "new", days_ago=35)
    seen = _seen(rows, 70) | {_vin("X", i) for i in range(40)}
    ev = _ev(seen)
    first = _plan(rows, ev)
    assert first.buckets["new"]["window"] == "90d" and first.buckets["new"]["prior"] == 100
    assert first.eligible and set(first.retire_ids) == {r.id for r in rows[70:]}
    second = plan_retirement(rows, _ev(seen), Policy(), NOW + timedelta(hours=21))
    assert second.retire_ids == first.retire_ids


def test_35_day_gap_below_the_share_retires_nothing():
    rows = _rows("N", 100, "new", days_ago=35)
    plan = _plan(rows, _ev(_seen(rows, 50) | {_vin("X", i) for i in range(40)}))
    assert plan.retire_ids == ()
