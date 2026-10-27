"""P15A.4: SCAN_POST_STEPS parsing, ordering, validation before any scan, per-step reporting,
and the recover/harvest steps aimed at dealers with open index codes.

``run_step`` and the roster are faked, so nothing spawns dealer_pipeline or touches a DB."""
from __future__ import annotations

import json

import pytest

from backend.scripts import fleet_scan


def test_default_and_switches():
    assert fleet_scan.post_steps("", "railway") == ["index", "market", "cards"]
    assert fleet_scan.post_steps("1", "all") == ["index", "market", "cards"]
    assert fleet_scan.post_steps("0", "railway") == []
    assert fleet_scan.post_steps("", "home") == []  # P15A.1 default on the home lane
    assert fleet_scan.post_steps("1", "home") == ["index", "market", "cards"]


def test_list_runs_in_canonical_order_with_cards_last():
    assert fleet_scan.post_steps("cards, recover ,market", "railway") == ["market", "recover", "cards"]
    assert fleet_scan.post_steps("cards,harvest,recover", "home") == ["recover", "harvest", "cards"]


@pytest.mark.parametrize("raw", ["market,bogus", "rebuild", ",", "0,market"])
def test_unknown_step_is_refused(raw):
    with pytest.raises(ValueError):
        fleet_scan.post_steps(raw, "railway")


def test_harvest_is_home_lane_only():
    with pytest.raises(ValueError, match="home"):
        fleet_scan.post_steps("market,harvest", "railway")
    with pytest.raises(ValueError, match="home"):
        fleet_scan.post_steps("harvest", "all")


@pytest.fixture()
def run(monkeypatch, tmp_path):
    for k in ("SCAN_FLEET", "SCAN_DEALERS", "SCAN_FLEET_WINDOW_UTC", "SCAN_FLEET_LOCK", "SCAN_RESUME_DIR",
              "SCAN_POST_STEPS", "SCAN_LANE", "SCANNER_EGRESS_TAG", "SCAN_RECOVER_MAX_DEALERS",
              "SCAN_IDLE_HOLD_SECONDS", "SCAN_POST_RUN_HOLD_SECONDS"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("SCAN_OUT_ROOT", str(tmp_path))
    monkeypatch.setenv("SCAN_DEALERS", "a-com,b-com")
    monkeypatch.setenv("SCAN_LANE", "all")
    monkeypatch.setattr("sys.argv", ["fleet_scan"])
    st = {"steps": [], "cmds": {}, "envs": {}, "codes": ["a-com"]}

    def fake_step(tag, cmd, env, sink):
        st["steps"].append(tag)
        st["cmds"][tag] = cmd
        st["envs"][tag] = env
        return (7, 1.5) if tag == st.get("fail") else (0, 0.5)

    monkeypatch.setattr(fleet_scan.time, "sleep", lambda s: None)
    monkeypatch.setattr(fleet_scan, "run_step", fake_step)
    monkeypatch.setattr(fleet_scan, "load_roster", lambda: ["a-com", "b-com"])
    monkeypatch.setattr(fleet_scan, "active_rows", lambda ids: {})
    monkeypatch.setattr(fleet_scan, "prune", lambda *a, **k: None)
    monkeypatch.setattr(fleet_scan, "rooftop_candidate_report", lambda since: {})
    monkeypatch.setattr(fleet_scan, "lane_alarms", lambda: {})
    monkeypatch.setattr(fleet_scan, "open_code_dealers", lambda ids: [d for d in st["codes"] if d in ids])
    monkeypatch.setattr(fleet_scan, "ResourceSampler", lambda: type(
        "S", (), {"start": lambda s: None, "stop": type("E", (), {"set": lambda s: None})(),
                  "summary": lambda s: {}})())
    monkeypatch.setattr(fleet_scan, "tally_http", lambda logs: {"by_status": {}})
    monkeypatch.setattr("backend.scanner.pipeline.assess.fleet_accounting",
                        lambda r: {"rows_captured": 0, "rows_refused_vin_owner": 0})
    monkeypatch.setattr("backend.scanner.pipeline.triage.group_feed_unstamped", lambda r: 0)
    st["summary"] = lambda: json.loads(next(tmp_path.glob("fleet_*/fleet_summary.json")).read_text())
    return st


def test_default_run_keeps_todays_steps_and_cards_env(run):
    assert fleet_scan.run_fleet() == 0
    assert run["steps"][-3:] == ["index", "market", "cards"]
    assert run["envs"]["cards"]["FLASK_ENV"] == "production"
    post = run["summary"]()["post"]
    assert list(post) == ["rebuild_incomplete_index", "compute_market_stats", "build_listings_grid_cards"]
    assert all({"rc", "seconds"} <= set(v) for v in post.values())


def test_unknown_step_exits_before_any_scan(run, monkeypatch):
    monkeypatch.setenv("SCAN_POST_STEPS", "market,nope")
    with pytest.raises(SystemExit) as e:
        fleet_scan.run_fleet()
    assert e.value.code == 2 and run["steps"] == []


def test_recover_targets_only_open_code_dealers_and_cards_stay_last(run, monkeypatch):
    monkeypatch.setenv("SCAN_POST_STEPS", "cards,recover")
    assert fleet_scan.run_fleet() == 0
    assert run["steps"][-2:] == ["recover", "cards"]
    cmd = run["cmds"]["recover"]
    assert cmd[1:3] == ["-m", "backend.scripts.heal_from_recipes"]
    assert cmd[3:] == ["--dealer-id", "a-com"]  # b-com has no open codes
    assert run["summary"]()["post"]["heal_from_recipes"]["rc"] == 0


def test_recover_skips_when_no_open_codes(run, monkeypatch):
    run["codes"] = []
    monkeypatch.setenv("SCAN_POST_STEPS", "recover")
    assert fleet_scan.run_fleet() == 0
    assert "recover" not in run["steps"]
    assert run["summary"]()["post"]["heal_from_recipes"]["skipped"] == "no open index codes"


def test_recover_cap_and_step_failure_flips_exit(run, monkeypatch):
    run["codes"] = ["a-com", "b-com"]
    monkeypatch.setenv("SCAN_POST_STEPS", "recover")
    monkeypatch.setenv("SCAN_RECOVER_MAX_DEALERS", "1")
    run["fail"] = "recover"
    assert fleet_scan.run_fleet() == 1
    assert run["cmds"]["recover"].count("--dealer-id") == 1
    assert run["summary"]()["post"]["heal_from_recipes"] == {"rc": 7, "seconds": 1.5}


def test_harvest_runs_on_home_lane_only(run, monkeypatch):
    monkeypatch.setenv("SCAN_LANE", "home")
    monkeypatch.setenv("SCAN_POST_STEPS", "harvest")
    assert fleet_scan.run_fleet() == 0
    assert run["cmds"]["harvest"][1:3] == ["-m", "backend.scripts.harvest_html_jsonld"]
    assert "market" not in run["steps"] and "cards" not in run["steps"]


def test_home_lane_default_is_no_post_steps(run, monkeypatch):
    monkeypatch.setenv("SCAN_LANE", "home")
    assert fleet_scan.run_fleet() == 0
    assert run["summary"]()["post"] == {}
