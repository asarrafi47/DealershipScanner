"""Dealer gates and dealer state (card L8): golden over the three fixture shards, anchors, boundaries, sequences."""

from __future__ import annotations

import copy
import json
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from backend.scanner.loop import gates as G
from backend.scanner.loop.bundle import load_shard, merge_shards
from backend.scanner.loop.gates import GateConfig, evaluate_dealer, evaluate_run, legacy_inputs

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "ops_publish"
SHARD_NAMES = ("scan_2026_11_03_s0", "scan_2026_11_03_s3", "scan_2026_11_03_s6")
GOLDEN = FIXTURES / "expected_dealer_states.json"
SIDE_PATH = FIXTURES / "side_inputs_2026_11_03.json"
REGISTRY_PATH = Path(__file__).resolve().parents[2] / "deploy" / "railway" / "accepted_gaps.json"
NOW = datetime(2026, 11, 13, 12, 0, tzinfo=timezone.utc)
CFG = GateConfig()


# --- helpers ---------------------------------------------------------------------------------------


@pytest.fixture(scope="module")
def dealers() -> list:
    shards = [load_shard(FIXTURES / name) for name in SHARD_NAMES]
    return merge_shards(shards)["dealers"]


@pytest.fixture(scope="module")
def by_id(dealers) -> dict:
    return {d["dealer_id"]: d for d in dealers}


@pytest.fixture(scope="module")
def side() -> dict:
    return json.loads(SIDE_PATH.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def registry() -> list:
    return json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def legacy(dealers, side, registry) -> dict:
    extras = {d["dealer_id"]: legacy_inputs(d, side) for d in dealers}
    return evaluate_run(dealers, extras, {}, registry, CFG, NOW)


def states(result, *ids):
    return {i: result["dealers"][i]["state"] for i in ids}


def entry(**over) -> dict:
    """A clean dealer: 400 stamped rows, both conditions, full coverage, a clean accuracy block."""
    base = {
        "dealer_id": "synthetic-com",
        "verdict": "ok",
        "rows": 400,
        "rows_stamped": 400,
        "rows_new": 200,
        "rows_used": 200,
        "active_after": 400,
        "known_before": 0,
        "vin_owner_conflicts": 0,
        "coverage": {"price": 1.0, "exterior_color": 1.0, "trim": 1.0},
        "accuracy": {
            "rows": 1000,
            "vpic_cached": 1000,
            "incomplete_rows": 0,
            "hard_rows": 0,
            "hard": {},
            "missing": {},
        },
        "http_first": {"wall_clock_hit": False},
        "recipe": {"recipe_status": "ok"},
    }
    base.update(over)
    return base


def extras(**over) -> dict:
    base = {
        "baseline": {"source": "accepted_run", "B": 400, "run_id": 1, "date": "2026-11-06T00:00:00+00:00"},
        "walk": {"pages": 3, "cap": 40, "cap_hit": False, "site_total": 400, "page_size": 150,
                 "pagination": "paged", "raw_vins": 400, "kept_vins": 400, "stop_reason": "end"},
        "recipe_age": {"last_ok_at": "2026-11-10T00:00:00+00:00", "live_recipes": 1},
        "feed_errors": {"403": 0, "429": 0, "challenge": 0, "vins_returned": 400},
    }
    base.update(over)
    return base


def run(e=None, x=None, history=None, registry=None, now=NOW):
    return evaluate_dealer(e if e is not None else entry(), x if x is not None else extras(), history, registry or [], CFG, now)


def acc(**over) -> dict:
    out = entry()["accuracy"]
    out.update(over)
    return out


def gate(result, name):
    return result["gates"][name]["result"]


# --- the golden ------------------------------------------------------------------------------------


def _golden_payload(result) -> dict:
    return {
        "counts": {k: v for k, v in result["counts"].items()},
        "dealers": {
            k: {
                "state": v["state"],
                "watch": v["watch"],
                "failing": v["failing"],
                "cannot_assess": v["cannot_assess"],
                "reasons": v["reasons"],
            }
            for k, v in result["dealers"].items()
        },
    }


def test_golden_states_for_the_172_fixture_dealers(legacy) -> None:
    payload = _golden_payload(legacy)
    text = json.dumps(payload, indent=1, sort_keys=True) + "\n"
    if os.environ.get("UPDATE_GOLDEN") == "1":
        GOLDEN.write_text(text, encoding="utf-8")
    expected = json.loads(GOLDEN.read_text(encoding="utf-8"))
    assert payload == expected
    assert len(payload["dealers"]) == 172


def test_golden_counts_match_the_card(legacy) -> None:
    counts = legacy["counts"]
    assert (counts["ok"], counts["needs_recipe_work"], counts["needs_code_work"], counts["blocked"]) == (106, 18, 48, 0)
    assert counts["unassessed"] == 0


def test_golden_every_dealer_has_a_state_and_gate_results(legacy) -> None:
    for result in legacy["dealers"].values():
        assert result["state"] in G.STATES
        assert list(result["gates"]) == list(G.GATE_NAMES)
        for g in result["gates"].values():
            assert g["result"] in (G.PASS, G.FAIL, G.CANNOT)
            assert "value" in g and "threshold" in g


# --- anchors (GATES.md section 12.4) -----------------------------------------------------------------


def test_robinsford_fails_gf1_and_needs_recipe_work(legacy) -> None:
    r = legacy["dealers"]["robinsford-com"]
    assert r["gates"]["GF1"]["result"] == G.FAIL
    assert r["gates"]["GF1"]["value"] == 24 and r["gates"]["GF1"]["threshold"] == 143.5
    assert r["state"] == "needs_recipe_work"
    assert r["permissions"]["retire"] is False


GF4_TEN = ["parksidekia-com", "davekirk-com", "hileyvwhuntsville-com", "landersmclartysubaru-net", "mbbeverlyhills-com",
           "bentleygmc-com", "crownlexus-com", "lexusescondido-com", "mbhsv-com", "overdrive-usa-com"]


def test_ten_gf4_dealers_are_needs_code_work_and_five_were_ok(legacy, by_id) -> None:
    for dealer in GF4_TEN:
        r = legacy["dealers"][dealer]
        assert "GF4" in r["failing"] and r["state"] == "needs_code_work", dealer
        assert r["permissions"]["moves"] is False and r["permissions"]["retire"] is False
    assert [d for d in GF4_TEN if by_id[d]["verdict"] == "ok"] == GF4_TEN[:5]
    assert sorted(d for d, r in legacy["dealers"].items() if "GF4" in r["failing"]) == sorted(GF4_TEN + ["autosavvy-com"])


def test_overdrive_gf4_outranks_gf1(legacy) -> None:
    r = legacy["dealers"]["overdrive-usa-com"]
    assert r["gates"]["GF1"]["result"] == G.FAIL
    assert r["gates"]["GF4"]["value"] == pytest.approx(142 / 156, abs=1e-4)
    assert r["state"] == "needs_code_work"


def test_mckennasubaru_needs_recipe_work(legacy) -> None:
    assert legacy["dealers"]["mckennasubaru-com"]["state"] == "needs_recipe_work"


def test_autosavvy_with_and_without_the_parser_refused_side_input(dealers, side, registry) -> None:
    row = next(d for d in dealers if d["dealer_id"] == "autosavvy-com")
    with_side = evaluate_dealer(row, legacy_inputs(row, side), None, registry, CFG, NOW)
    without = evaluate_dealer(row, legacy_inputs(row, {k: v for k, v in side.items() if k != "refused_parse_time"}),
                              None, registry, CFG, NOW)
    assert with_side["state"] == "needs_code_work" and with_side["gates"]["GF4"]["value"] == 1.0
    assert without["state"] == "needs_recipe_work"


def test_gf5_fails_for_fourteen_dealers_with_the_registry_and_fifteen_without(dealers, legacy, side, registry) -> None:
    assert sum(1 for r in legacy["dealers"].values() if "GF5" in r["failing"]) == 14
    assert legacy["dealers"]["quantumautosales-com"]["gates"]["GF5"].get("accepted_gap") is True
    extras_map = {d["dealer_id"]: legacy_inputs(d, side) for d in dealers}
    bare = evaluate_run(dealers, extras_map, {}, [], CFG, NOW)
    assert sum(1 for r in bare["dealers"].values() if "GF5" in r["failing"]) == 15


def test_truncated_walks_and_single_page_widget_are_needs_code_work(legacy) -> None:
    for dealer in ("5starford-com", "autonationtoyotacerritos-com", "mountainstatestoyota-com", "premiumautosinc-com",
                   "toyotacarlsbad-com", "mbofstevenscreek-com"):
        r = legacy["dealers"][dealer]
        assert r["gates"]["GF3"]["result"] == G.FAIL and r["state"] == "needs_code_work", dealer
        assert r["permissions"]["retire"] is False
    widget = legacy["dealers"]["dreyerreinboldbmwnorth-com"]
    assert widget["state"] == "needs_code_work" and "hard:single_page_widget" in widget["reasons"]


def test_dealers_that_stay_ok(legacy) -> None:
    ids = ["freemantoyota-com", "keithpiersontoyota-com", "capitaltoyota-com", "sterlingbmw-com", "tustinlexus-com",
           "bmwofsouthaustin-com", "lexusofbellevue-com"]
    assert set(states(legacy, *ids).values()) == {"ok"}


def test_sixteen_comparator_only_dealers_and_two_trim_only_thin_dealers_become_ok(legacy, by_id) -> None:
    inaccurate_ok = [d for d, r in legacy["dealers"].items() if by_id[d]["verdict"] == "inaccurate" and r["state"] == "ok"]
    assert len(inaccurate_ok) == 16
    assert states(legacy, "sterlingbmw-com", "tustinlexus-com") == {"sterlingbmw-com": "ok", "tustinlexus-com": "ok"}


def test_sixteen_dealers_without_accuracy_get_cannot_assess_for_ga2_and_ga3(legacy, by_id) -> None:
    missing = [d for d in by_id if not by_id[d].get("accuracy")]
    assert len(missing) == 16
    for d in missing:
        assert {"GA2", "GA3"} <= set(legacy["dealers"][d]["cannot_assess"])


def test_legacy_mode_stand_in_baseline_evaluates_only_the_fail_level(legacy) -> None:
    for r in legacy["dealers"].values():
        assert r["gates"]["GF2"]["result"] in (G.FAIL, G.CANNOT)
        assert r["gates"]["GF6"]["result"] == G.CANNOT


def test_legacy_discounts_vpic_fuel_and_counts_soft_gates_at_once(by_id, side) -> None:
    row = copy.deepcopy(by_id["bmwofsouthaustin-com"])
    assert row["accuracy"]["hard_rows"] > 0
    r = evaluate_dealer(row, legacy_inputs(row, side), None, [], CFG, NOW)
    assert r["state"] == "ok" and gate(r, "GA3") == G.PASS


def test_legacy_ignores_the_wall_clock(by_id, side) -> None:
    row = by_id["toyotacarlsbad-com"]
    assert row["http_first"]["wall_clock_hit"] is True
    r = evaluate_dealer(row, legacy_inputs(row, side), None, [], CFG, NOW)
    assert gate(r, "GA1") == G.FAIL


def test_legacy_inputs_does_not_mutate_and_is_repeatable(by_id, side) -> None:
    row, s = copy.deepcopy(by_id["autosavvy-com"]), copy.deepcopy(side)
    first = legacy_inputs(row, s)
    assert row == by_id["autosavvy-com"] and s == side
    assert legacy_inputs(row, s) == first
    assert legacy_inputs(None, None) == {"legacy": first["legacy"], "refused": {"parse_time": 0, "owner_guard": 0}}


# --- native mode (no adapter) --------------------------------------------------------------------------


def test_native_mode_gf2_and_gf3_cannot_assess_and_gf1_uses_the_floor(dealers, registry) -> None:
    result = evaluate_run(dealers, {}, {}, registry, CFG, NOW)
    for d in dealers:
        r = result["dealers"][d["dealer_id"]]
        assert r["gates"]["GF2"]["result"] == G.CANNOT and r["gates"]["GF3"]["result"] == G.CANNOT
        stamped = d.get("rows_stamped") or 0
        assert r["gates"]["GF1"]["result"] == (G.FAIL if stamped < 20 else G.CANNOT)


def test_native_baseline_of_287_moves_robinsford_to_needs_recipe_work(by_id, registry) -> None:
    row = by_id["robinsford-com"]
    plain = evaluate_dealer(row, {}, None, registry, CFG, NOW)
    based = evaluate_dealer(row, {"baseline": {"source": "accepted_run", "B": 287, "run_id": 7, "date": "2026-11-06"}},
                            None, registry, CFG, NOW)
    assert plain["state"] == "ok" and based["state"] == "needs_recipe_work"


def test_native_kingfordsales_is_ok_with_watch_then_needs_code_work(by_id, registry) -> None:
    row = by_id["kingfordsales-com"]
    first = evaluate_dealer(row, {}, None, registry, CFG, NOW)
    assert first["state"] == "ok" and first["watch"] is True and gate(first, "GA1") == G.FAIL
    second = evaluate_dealer(row, {}, first["history"], registry, CFG, NOW + timedelta(days=7))
    assert second["state"] == "needs_code_work" and second["watch"] is False


def test_native_toyotacarlsbad_stays_cannot_assess_for_ga1(by_id, registry) -> None:
    r = evaluate_dealer(by_id["toyotacarlsbad-com"], {}, None, registry, CFG, NOW)
    assert gate(r, "GA1") == G.CANNOT and r["gates"]["GA1"]["basis"] == "wall_clock"
    assert r["state"] == "ok"


# --- synthetic: hysteresis and persistence ---------------------------------------------------------


def _hard_share_sequence(shares, legacy_mode=False):
    history, out = None, []
    for i, share in enumerate(shares):
        e = entry(accuracy=acc(hard_rows=round(share * 1000), hard={"vpic_model": round(share * 1000)}))
        r = run(e, extras(), history, now=NOW + timedelta(days=7 * i))
        history = r["history"]
        out.append(gate(r, "GA3"))
    return out


def test_hysteresis_hard_share_6_4_34_4_gives_fail_fail_pass_pass() -> None:
    assert _hard_share_sequence([0.06, 0.04, 0.034, 0.04]) == ["fail", "fail", "pass", "pass"]


def test_hysteresis_49_51_40_34_gives_pass_fail_fail_pass() -> None:
    assert _hard_share_sequence([0.049, 0.051, 0.040, 0.034]) == ["pass", "fail", "fail", "pass"]


def test_hysteresis_exact_boundaries() -> None:
    assert _hard_share_sequence([0.05]) == ["pass"]
    assert _hard_share_sequence([0.051, 0.035]) == ["fail", "pass"]
    assert _hard_share_sequence([0.051, 0.036]) == ["fail", "fail"]


def test_pass_inside_the_band_marks_watch() -> None:
    r = run(entry(accuracy=acc(incomplete_rows=40)))
    assert gate(r, "GA2") == G.PASS and r["state"] == "ok" and r["watch"] is True
    assert run(entry(accuracy=acc(incomplete_rows=30)))["watch"] is False


def test_soft_gate_needs_two_consecutive_failing_scans() -> None:
    bad = entry(accuracy=acc(incomplete_rows=80))
    first = run(bad)
    assert first["state"] == "ok" and first["watch"] is True and first["history"]["gates"]["GA2"]["fails"] == 1
    second = run(bad, history=first["history"], now=NOW + timedelta(days=7))
    assert second["state"] == "needs_code_work" and second["reasons"] == ["soft:GA2"]
    healed = run(entry(), history=first["history"], now=NOW + timedelta(days=7))
    assert healed["history"]["gates"]["GA2"]["fails"] == 0
    third = run(bad, history=healed["history"], now=NOW + timedelta(days=14))
    assert third["state"] == "ok"


def test_cannot_assess_leaves_the_soft_history_unchanged() -> None:
    bad = entry(accuracy=acc(incomplete_rows=80))
    first = run(bad)
    blind = run(entry(accuracy=None), history=first["history"], now=NOW + timedelta(days=7))
    assert gate(blind, "GA2") == G.CANNOT and blind["history"]["gates"]["GA2"] == first["history"]["gates"]["GA2"]


def test_streak_and_since_follow_the_state() -> None:
    a = run()
    b = run(history=a["history"], now=NOW + timedelta(days=7))
    assert (a["streak"], b["streak"], b["since"]) == (1, 2, a["since"])
    c = run(entry(rows=10, rows_stamped=10, rows_new=5, rows_used=5), history=b["history"], now=NOW + timedelta(days=14))
    assert c["state"] == "needs_recipe_work" and c["streak"] == 1 and c["since"] == "2026-11-27"


def test_fail_outcome_leaves_history_unchanged() -> None:
    previous = {"state": "ok", "since": "2026-10-01", "streak": 4, "gates": {}}
    result = evaluate_run([entry()], {"synthetic-com": extras()}, {"synthetic-com": previous}, [], CFG, NOW, outcome="FAIL")
    assert result["history"]["synthetic-com"] == previous
    kept = evaluate_run([entry()], {"synthetic-com": extras()}, {"synthetic-com": previous}, [], CFG, NOW, outcome="PASS")
    assert kept["history"]["synthetic-com"]["streak"] == 5


# --- boundaries, one test per threshold ----------------------------------------------------------------


def test_gf1_floor_without_baseline() -> None:
    assert gate(run(entry(rows_stamped=19), {}), "GF1") == G.FAIL
    assert gate(run(entry(rows_stamped=20), {}), "GF1") == G.CANNOT


def test_gf1_half_of_the_baseline() -> None:
    base = lambda b: extras(baseline={"source": "accepted_run", "B": b, "run_id": 1, "date": "2026-11-06"})
    assert gate(run(entry(rows_stamped=199), base(400)), "GF1") == G.FAIL
    assert gate(run(entry(rows_stamped=200), base(400)), "GF1") == G.PASS
    assert gate(run(entry(rows_stamped=19), base(10)), "GF1") == G.FAIL
    assert gate(run(entry(rows_stamped=20), base(10)), "GF1") == G.PASS


def test_gf1_known_lot_is_the_second_baseline_source() -> None:
    r = run(entry(rows_stamped=100, known_before=300), {})
    assert gate(r, "GF1") == G.FAIL and r["gates"]["GF1"]["basis"] == "known_lot"


def test_gf2_levels_and_the_warn_line() -> None:
    def at(s, days=7):
        date = (NOW - timedelta(days=days)).isoformat()
        return run(entry(rows_stamped=s), extras(baseline={"source": "accepted_run", "B": 1000, "run_id": 1, "date": date}))

    assert gate(at(499), "GF2") == G.FAIL and at(499)["gates"]["GF2"]["level"] == "fail"
    assert gate(at(500), "GF2") == G.FAIL and at(500)["gates"]["GF2"]["level"] == "low"
    assert gate(at(699), "GF2") == G.FAIL
    r = at(700)
    assert gate(r, "GF2") == G.PASS and r["gates"]["GF2"]["warn"] is True and r["watch"] is True
    assert at(940, days=7)["gates"]["GF2"]["warn"] is False  # warn line 0.95 - 0.028 = 0.922
    assert at(921, days=7)["gates"]["GF2"]["warn"] is True
    assert at(699, days=100)["gates"]["GF2"]["result"] == G.FAIL  # the warn line never drops below 0.70


def test_gf2_warn_marks_watch_on_an_ok_dealer() -> None:
    date = (NOW - timedelta(days=7)).isoformat()
    base = {"source": "accepted_run", "B": 450, "run_id": 1, "date": date}
    r = run(entry(), extras(baseline=base))
    assert r["state"] == "ok" and r["watch"] is True and "watch:warn:GF2" in r["reasons"]


def test_gf2_below_070_is_recipe_work_and_switches_retire_off() -> None:
    r = run(entry(rows_stamped=600), extras(baseline={"source": "accepted_run", "B": 1000, "run_id": 1, "date": "2026-11-06"}))
    assert r["state"] == "needs_recipe_work" and "recipe:GF2" in r["reasons"] and r["permissions"]["retire"] is False


def test_gf3_complete_incomplete_and_unknown() -> None:
    def walk(**over):
        base = extras()["walk"]
        base.update(over)
        return run(entry(), extras(walk=base))

    assert gate(walk(), "GF3") == G.PASS
    assert gate(walk(cap_hit=True), "GF3") == G.FAIL
    assert gate(walk(raw_vins=379), "GF3") == G.FAIL and gate(walk(raw_vins=380), "GF3") == G.PASS
    assert gate(walk(site_total=None, cap_hit=False), "GF3") == G.PASS
    assert gate(walk(raw_vins=None), "GF3") == G.CANNOT
    assert gate(run(entry(), extras(walk=None)), "GF3") == G.CANNOT
    assert gate(walk(cap_hit=None, pages=40, cap=40), "GF3") == G.FAIL
    assert gate(walk(cap_hit=None, pages=39, cap=40), "GF3") == G.PASS


def test_gf3_cap_defaults_and_pagination_none() -> None:
    walk = {"pages": 40, "cap": None, "cap_hit": None, "site_total": None, "page_size": 100, "pagination": "paged", "raw_vins": 5}
    assert gate(run(entry(), extras(walk=walk)), "GF3") == G.FAIL  # unknown T: cap 40
    walk.update(site_total=1000, pages=12)  # cap = min(150, ceil(1000 / 100) + 2) = 12
    assert gate(run(entry(), extras(walk=walk)), "GF3") == G.FAIL
    walk.update(pages=11, raw_vins=950)
    assert gate(run(entry(), extras(walk=walk)), "GF3") == G.PASS
    single = {"pages": 1, "cap": None, "cap_hit": True, "site_total": 100, "page_size": 100, "pagination": "none", "raw_vins": 95}
    assert gate(run(entry(), extras(walk=single)), "GF3") == G.PASS  # one page: only the W test
    single["raw_vins"] = 94
    assert gate(run(entry(), extras(walk=single)), "GF3") == G.FAIL


def test_gf4_refused_share_and_stamped_share() -> None:
    assert gate(run(entry(rows=1000, rows_stamped=1000, vin_owner_conflicts=99)), "GF4") == G.PASS
    assert gate(run(entry(rows=1000, rows_stamped=1000, vin_owner_conflicts=100)), "GF4") == G.FAIL
    assert gate(run(entry(rows=100, rows_stamped=89)), "GF4") == G.FAIL
    assert gate(run(entry(rows=100, rows_stamped=90)), "GF4") == G.PASS
    assert gate(run(entry(rows=49, rows_stamped=1)), "GF4") == G.PASS  # under 50 feed rows the second clause is off
    assert gate(run(entry(rows=0, rows_stamped=0)), "GF4") == G.CANNOT


def test_gf4_near_the_limit_flags_watch_without_changing_the_state() -> None:
    def refused(n):
        return run(entry(rows=1000, rows_stamped=1000, vin_owner_conflicts=n))

    assert refused(69)["watch"] is False
    near = refused(70)
    assert near["watch"] is True and near["state"] == "ok" and "watch:near:GF4" in near["reasons"]
    assert refused(99)["watch"] is True and gate(refused(99), "GF4") == G.PASS
    assert refused(100)["watch"] is False and refused(100)["state"] == "needs_code_work"


def test_gf4_counts_parser_refused_rows_in_x_and_f() -> None:
    r = run(entry(rows=900, rows_stamped=900), extras(refused={"parse_time": 100, "owner_guard": 0}))
    assert r["gates"]["GF4"]["value"] == 0.1 and gate(r, "GF4") == G.FAIL


def test_gf5_one_condition() -> None:
    assert gate(run(entry(rows_stamped=20, rows_new=20, rows_used=0)), "GF5") == G.FAIL
    assert gate(run(entry(rows_stamped=19, rows_new=19, rows_used=0)), "GF5") == G.PASS
    assert gate(run(entry(rows_stamped=100, rows_new=99, rows_used=1)), "GF5") == G.PASS
    note = run(entry(rows_stamped=100, rows_new=99, rows_used=1))["gates"]["GF5"]
    assert note.get("note") == "one_condition_info"
    assert "note" not in run(entry(rows_stamped=100, rows_new=98, rows_used=2))["gates"]["GF5"]
    assert gate(run(entry(rows_new=None)), "GF5") == G.CANNOT


def test_gf6_freshness() -> None:
    def age(days, live=1, s=400):
        last = (NOW - timedelta(days=days)).isoformat()
        return run(entry(rows_stamped=s), extras(recipe_age={"last_ok_at": last, "live_recipes": live}))

    assert gate(age(30), "GF6") == G.PASS
    assert gate(age(31, s=359), "GF6") == G.FAIL  # Y = 359 / 400 = 0.8975
    assert gate(age(31, s=360), "GF6") == G.PASS  # Y = 0.90
    assert gate(age(1, live=0), "GF6") == G.FAIL
    assert gate(run(), "GF6") == G.PASS
    assert gate(run(entry(), {}), "GF6") == G.CANNOT


def test_gf7_blocked_by_status_and_by_feed_errors() -> None:
    r = run(entry(recipe={"recipe_status": "blocked:residential:403"}))
    assert r["state"] == "blocked" and r["permissions"] == {"retire": False, "moves": False, "promotion": False}
    assert gate(run(entry(recipe={"recipe_status": "blocked-ish"})), "GF7") == G.PASS

    def feed(count, vins=0, tag="dc-1"):
        return run(entry(recipe={}), extras(feed_errors={"403": count, "429": 0, "challenge": 0, "vins_returned": vins},
                                            code={"egress_tag": tag}))

    assert gate(feed(3), "GF7") == G.FAIL and gate(feed(2), "GF7") == G.PASS
    assert gate(feed(3, vins=1), "GF7") == G.PASS and gate(feed(3, tag=""), "GF7") == G.PASS
    assert gate(run(entry(recipe={}), {}), "GF7") == G.CANNOT


def test_ga1_coverage_thresholds() -> None:
    def cov(**kw):
        c = {"price": 1.0, "exterior_color": 1.0, "trim": 1.0}
        c.update(kw)
        return entry(coverage=c, accuracy=None)

    assert gate(run(cov(price=0.8999)), "GA1") == G.FAIL and gate(run(cov(price=0.90)), "GA1") == G.PASS
    assert gate(run(cov(exterior_color=0.8999)), "GA1") == G.FAIL and gate(run(cov(exterior_color=0.90)), "GA1") == G.PASS
    assert gate(run(cov(used_mileage=0.8999)), "GA1") == G.FAIL and gate(run(cov(used_mileage=0.90)), "GA1") == G.PASS
    assert gate(run(cov(trim=0.7299)), "GA1") == G.FAIL and gate(run(cov(trim=0.73)), "GA1") == G.PASS
    assert gate(run(cov(price=None)), "GA1") == G.CANNOT


def test_ga1_trim_gap_after_the_fill_is_a_share_gate() -> None:
    def trim(missing):
        return run(entry(coverage={"price": 1.0, "exterior_color": 1.0, "trim": 0.1}, accuracy=acc(missing={"trim": missing})))

    assert gate(trim(50), "GA1") == G.PASS and trim(51)["gates"]["GA1"]["failed"] == ["trim"]
    assert trim(40)["watch"] is True


def test_ga1_and_ga2_wall_clock_failures_are_cannot_assess() -> None:
    cut = {"wall_clock_hit": True}
    r = run(entry(coverage={"price": 0.5, "exterior_color": 1.0, "trim": 1.0}, http_first=cut, accuracy=acc(incomplete_rows=100)))
    assert gate(r, "GA1") == G.CANNOT and gate(r, "GA2") == G.CANNOT and r["state"] == "ok"
    ok = run(entry(http_first=cut))
    assert gate(ok, "GA1") == G.PASS


def test_ga2_share() -> None:
    assert gate(run(entry(accuracy=acc(incomplete_rows=50))), "GA2") == G.PASS
    assert gate(run(entry(accuracy=acc(incomplete_rows=51))), "GA2") == G.FAIL


def test_ga3_share_assessable_and_vpic_gated() -> None:
    assert gate(run(entry(accuracy=acc(hard_rows=50))), "GA3") == G.PASS
    assert gate(run(entry(accuracy=acc(hard_rows=51))), "GA3") == G.FAIL
    # 800 assessable of 1000 rows: 51 / 800 = 6.4 %
    r = run(entry(accuracy=acc(hard_rows=41)), extras(vpic_gated=200))
    assert gate(r, "GA3") == G.FAIL and r["gates"]["GA3"]["value"] == pytest.approx(41 / 800, abs=1e-4)
    assert gate(run(entry(accuracy=acc(hard_rows=0)), extras(vpic_gated=250)), "GA3") == G.PASS
    assert gate(run(entry(accuracy=acc(hard_rows=0)), extras(vpic_gated=251)), "GA3") == G.CANNOT


def test_ga3_invariant_breach_is_hard() -> None:
    ok = run(entry(), extras(nhtsa_invariants={"checked": 1000, "drivetrain": 1, "electrification": 1}))
    assert gate(ok, "GA3") == G.PASS
    bad = run(entry(), extras(nhtsa_invariants={"checked": 1000, "drivetrain": 2, "electrification": 1}))
    assert gate(bad, "GA3") == G.FAIL and bad["state"] == "needs_code_work" and bad["reasons"] == ["hard:GA3"]


def test_ga4_identity() -> None:
    assert gate(run(entry(accuracy=acc(rows=100, vpic_cached=100, hard={"vpic_make": 3, "vpic_year": 1}))), "GA4") == G.PASS
    r = run(entry(verdict="inaccurate", accuracy=acc(rows=100, vpic_cached=100, hard={"vpic_make": 3, "vpic_year": 2})))
    assert gate(r, "GA4") == G.FAIL and r["state"] == "needs_code_work" and r["permissions"]["promotion"] is False
    big = acc(rows=1000, hard={"vpic_make": 10})
    assert gate(run(entry(accuracy=big)), "GA4") == G.FAIL
    assert gate(run(entry(accuracy=acc(rows=2000, hard={"vpic_make": 19}))), "GA4") == G.PASS
    assert gate(run(entry(accuracy=acc(rows=2000, hard={"vpic_make": 20}))), "GA4") == G.FAIL


# --- precedence, registry, permissions, verdicts ----------------------------------------------------------


def test_precedence_matrix() -> None:
    blocked = {"recipe": {"recipe_status": "blocked:x:y"}}
    everything = dict(blocked, rows_stamped=10, rows=10, rows_new=10, rows_used=0, vin_owner_conflicts=9,
                      accuracy=acc(hard={"vpic_make": 9}, incomplete_rows=200))
    assert run(entry(**everything))["state"] == "blocked"
    del everything["recipe"]
    assert run(entry(**everything))["state"] == "needs_code_work"  # GF4 and GA4 outrank GF1
    assert run(entry(rows_stamped=10, rows=10, rows_new=10, rows_used=0, accuracy=acc(incomplete_rows=200)))["state"] == "needs_recipe_work"
    soft = entry(accuracy=acc(incomplete_rows=200))
    assert run(soft, history={"state": "ok", "since": "2026-11-01", "streak": 1, "gates": {"GA2": {"result": "fail", "fails": 1}}})["state"] == "needs_code_work"
    assert run(soft)["state"] == "ok"
    single = {"pages": 1, "cap": None, "cap_hit": False, "site_total": None, "page_size": 400, "pagination": "none", "raw_vins": 400}
    assert run(entry(), extras(walk=single))["state"] == "needs_code_work"
    single["page_size"] = 401
    assert run(entry(), extras(walk=single))["state"] == "ok"
    assert run(entry(verdict="no_recipe"))["state"] == "needs_recipe_work"


def test_cannot_assess_in_gf1_to_gf4_keeps_retire_and_moves_off_but_does_not_change_the_state() -> None:
    clean = run()
    assert clean["permissions"] == {"retire": True, "moves": True, "promotion": True}
    for key, patch in (("walk", None), ("baseline", None)):
        blind = run(entry(), extras(**{key: patch}))
        assert blind["state"] == "ok" and blind["permissions"]["retire"] is False and blind["permissions"]["moves"] is False
    blind4 = run(entry(rows=0, rows_stamped=400))
    assert gate(blind4, "GF4") == G.CANNOT and blind4["permissions"]["moves"] is False
    # cannot_assess in a gate outside GF1 to GF4 changes nothing
    assert run(entry(accuracy=None))["permissions"] == clean["permissions"]


def test_promotion_by_verdict() -> None:
    assert run(entry(verdict="thin"))["permissions"]["promotion"] is True
    assert run(entry(verdict="inaccurate"))["permissions"]["promotion"] is True
    assert run(entry(verdict="inaccurate", rows=100, rows_stamped=100, vin_owner_conflicts=20))["permissions"]["promotion"] is False
    for verdict in ("no_rows", "error", "no_recipe"):
        assert run(entry(verdict=verdict))["permissions"]["promotion"] is False


def test_error_verdict_is_unassessed_with_every_gate_cannot_assess() -> None:
    prev = {"state": "ok", "since": "2026-10-01", "streak": 3, "gates": {}}
    r = run(entry(verdict="error", rows_stamped=0), history=prev)
    assert r["state"] == "unassessed" and r["failing"] == [] and r["cannot_assess"] == list(G.GATE_NAMES)
    assert r["history"] == prev and r["permissions"] == {"retire": False, "moves": False, "promotion": False}


def test_registry_exempts_gf5_until_it_expires() -> None:
    row = entry(rows=209, rows_stamped=209, rows_new=0, rows_used=209)
    item = {"dealer_id": "synthetic-com", "gate": "GF5", "evidence": "x", "approved": "2026-11-13", "expires": "2027-02-11"}
    assert gate(run(row, {}, registry=[item]), "GF5") == G.PASS
    assert run(row, {}, registry=[item], now=datetime(2027, 2, 11, 23, tzinfo=timezone.utc))["state"] == "ok"
    expired = run(row, {}, registry=[item], now=datetime(2027, 2, 12, tzinfo=timezone.utc))
    assert gate(expired, "GF5") == G.FAIL and expired["state"] == "needs_recipe_work"
    assert gate(run(row, {}, registry=[dict(item, dealer_id="other-com")]), "GF5") == G.FAIL
    assert gate(run(row, {}, registry=[dict(item, gate="GF1")]), "GF5") == G.FAIL
    assert gate(run(row, {}, registry=[dict(item, expires="garbage")]), "GF5") == G.FAIL
    hard = run(entry(rows=100, rows_stamped=50), registry=[dict(item, gate="GF4")])
    assert gate(hard, "GF4") == G.FAIL  # hard gates cannot be exempted


def test_registry_can_exempt_a_soft_gate() -> None:
    item = {"dealer_id": "synthetic-com", "gate": "GA2", "evidence": "x", "approved": "2026-11-13", "expires": "2027-02-11"}
    r = run(entry(accuracy=acc(incomplete_rows=200)), registry=[item], history={"gates": {"GA2": {"result": "fail", "fails": 5}}})
    assert gate(r, "GA2") == G.PASS and r["state"] == "ok" and r["history"]["gates"]["GA2"]["fails"] == 0


def test_seed_registry_has_the_two_gf5_entries() -> None:
    data = json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))
    assert {(e["dealer_id"], e["gate"]) for e in data} == {("quantumautosales-com", "GF5"), ("honestcardeal-com", "GF5")}
    for e in data:
        assert set(e) == {"dealer_id", "gate", "evidence", "approved", "expires"}
        approved = datetime.fromisoformat(e["approved"])
        assert datetime.fromisoformat(e["expires"]) - approved == timedelta(days=90)


# --- purity -----------------------------------------------------------------------------------------


def test_same_input_twice_gives_equal_output_and_the_input_is_unchanged() -> None:
    e, x, h, reg = entry(accuracy=acc(incomplete_rows=80)), extras(), {"state": "ok", "since": "2026-11-01", "streak": 2,
                                                                      "gates": {"GA2": {"result": "fail", "fails": 1}}}, []
    frozen = copy.deepcopy((e, x, h, reg))
    a = evaluate_dealer(e, x, h, reg, CFG, NOW)
    b = evaluate_dealer(e, x, h, reg, CFG, NOW)
    assert a == b and (e, x, h, reg) == frozen
    a["gates"]["GA2"]["result"] = "tampered"
    assert evaluate_dealer(e, x, h, reg, CFG, NOW)["gates"]["GA2"]["result"] == G.FAIL


def test_run_over_the_fixtures_is_repeatable_and_does_not_mutate(dealers, side, registry) -> None:
    frozen = copy.deepcopy(dealers)
    extras_map = {d["dealer_id"]: legacy_inputs(d, side) for d in dealers}
    first = evaluate_run(dealers, extras_map, {}, registry, CFG, NOW)
    assert dealers == frozen
    assert json.dumps(first, sort_keys=True) == json.dumps(evaluate_run(dealers, extras_map, {}, registry, CFG, NOW), sort_keys=True)


def test_evaluate_run_refuses_duplicate_or_missing_ids() -> None:
    with pytest.raises(ValueError):
        evaluate_run([entry(), entry()], {}, {}, [], CFG, NOW)
    with pytest.raises(ValueError):
        evaluate_run([{"verdict": "ok"}], {}, {}, [], CFG, NOW)


def test_no_threshold_is_read_from_the_environment(monkeypatch, dealers, side, registry) -> None:
    source = Path(G.__file__).read_text(encoding="utf-8")
    assert not re.search(r"\bimport (os|sys)\b|os\.environ|getenv|\bopen\(|\.read_text|time\.time|datetime\.now|utcnow", source)
    baseline = evaluate_run(dealers, {d["dealer_id"]: legacy_inputs(d, side) for d in dealers}, {}, registry, CFG, NOW)
    for name in ("ROW_FLOOR", "MIN_ROWS_UNKNOWN", "GATE_ROW_FLOOR", "GF1_MIN_ROWS", "SHARE_ENTER", "SOFT_FAIL_SCANS"):
        monkeypatch.setenv(name, "1")
    again = evaluate_run(dealers, {d["dealer_id"]: legacy_inputs(d, side) for d in dealers}, {}, registry, CFG, NOW)
    assert again["counts"] == baseline["counts"]


def test_gate_config_is_frozen_and_carries_the_documented_thresholds() -> None:
    with pytest.raises(Exception):
        CFG.min_rows = 1  # type: ignore[misc]
    assert (CFG.min_rows, CFG.row_floor, CFG.yield_low, CFG.share_enter, CFG.share_leave) == (20, 0.5, 0.70, 0.05, 0.035)
    for needle in ("GF1", "GF7", "GA1", "GA4", "0.95 - 0.004 g", "hysteresis"):
        assert needle in G.__doc__


class _Rec(dict):
    """A dict that records the dotted path of every key read through ``get``."""

    def __init__(self, data, reads, prefix):
        super().__init__({k: _wrap(v, reads, prefix + "." + k) for k, v in data.items()})
        self._reads, self._prefix = reads, prefix

    def get(self, key, default=None):
        self._reads.add(self._prefix + "." + key)
        return super().get(key, default)


def _wrap(value, reads, prefix):
    return _Rec(value, reads, prefix) if isinstance(value, dict) else value


def _covered(path, declared) -> bool:
    return any(path == d or path.startswith(d + ".") or d.startswith(path + ".") for d in declared)


def test_input_fields_lists_every_field_the_gates_read(dealers, side, registry) -> None:
    declared = {p for fields in G.INPUT_FIELDS.values() for p in fields}
    reads = set()
    for d in dealers:
        evaluate_dealer(_wrap(d, reads, "entry"), _wrap(legacy_inputs(d, side), reads, "extras"), None, registry, CFG, NOW)
    full = extras(vpic_gated=1, nhtsa_invariants={}, code={"egress_tag": "t"})
    evaluate_dealer(_wrap(entry(), reads, "entry"), _wrap(full, reads, "extras"), None, registry, CFG, NOW)
    uncovered = sorted(p for p in reads if not _covered(p, declared))
    assert uncovered == []
    for fields in G.INPUT_FIELDS.values():
        assert all(p.startswith(("entry.", "extras.")) for p in fields)
