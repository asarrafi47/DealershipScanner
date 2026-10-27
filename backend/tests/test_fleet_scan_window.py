"""P6A.2: SCAN_FLEET_WINDOW_UTC, the fleet lease (exit 3), the capped idle hold, the post-run hold.

``run_step`` and the roster are faked, so nothing spawns dealer_pipeline."""
from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import datetime, timezone

import pytest

from backend.scanner.scan_leases import FleetLease
from backend.scripts import fleet_scan


def at(h, m=0, s=0):
    return datetime(2026, 10, 26, h, m, s, tzinfo=timezone.utc)


# --- window arithmetic ------------------------------------------------------

def test_parse_window_normal_wrap_and_invalid():
    assert fleet_scan.parse_window("08:50-09:40") == (530, 580)
    assert fleet_scan.parse_window(" 22:00 - 02:30 ") == (1320, 150)
    for bad in ("", "9", "08:50", "25:00-26:00", "08:61-09:00", "09:00-09:00", "a-b"):
        with pytest.raises(ValueError):
            fleet_scan.parse_window(bad)


def test_in_window_normal_and_wrapping():
    w = fleet_scan.parse_window("08:50-09:40")
    assert fleet_scan.in_window(at(8, 50), w) and fleet_scan.in_window(at(9, 39), w)
    assert not fleet_scan.in_window(at(8, 49), w) and not fleet_scan.in_window(at(9, 40), w)
    wrap = fleet_scan.parse_window("22:00-02:00")
    assert fleet_scan.in_window(at(23), wrap) and fleet_scan.in_window(at(1, 59), wrap)
    assert not fleet_scan.in_window(at(2), wrap) and not fleet_scan.in_window(at(12), wrap)


def test_capped_hold_ends_five_minutes_before_the_window_opens():
    w = fleet_scan.parse_window("09:00-09:50")
    # 08:35 + 1500 s = 09:00 would span the fire: cut to 08:55
    assert fleet_scan.capped_hold(1500, at(8, 35), w) == 1200
    assert fleet_scan.capped_hold(1500, at(8, 57), w) == 0
    assert fleet_scan.capped_hold(1500, at(7, 0), w) == 1500  # far enough: unchanged
    assert fleet_scan.capped_hold(1500, at(8, 35), None) == 1500  # no window: today's behaviour
    assert fleet_scan.capped_hold(0, at(8, 35), w) == 0


# --- main() -----------------------------------------------------------------

@pytest.fixture()
def fx(monkeypatch, tmp_path):
    for k in ("SCAN_FLEET", "SCAN_DEALERS", "SCAN_FLEET_WINDOW_UTC", "SCAN_FLEET_LOCK", "SCAN_FLEET_LANE",
              "SCAN_IDLE_HOLD_SECONDS", "SCAN_POST_RUN_HOLD_SECONDS", "SCAN_RESUME_DIR", "SCAN_POST_STEPS"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("SCAN_OUT_ROOT", str(tmp_path))
    monkeypatch.setenv("SCAN_POST_STEPS", "0")
    monkeypatch.setattr("sys.argv", ["fleet_scan"])
    st = {"sleeps": [], "spawned": [], "lock": FleetLease(True), "lanes": [], "now": at(12)}
    monkeypatch.setattr(fleet_scan.time, "sleep", lambda s: st["sleeps"].append(s) if s > 5 else None)
    monkeypatch.setattr(fleet_scan, "_utcnow", lambda: st["now"])

    def fake_step(tag, cmd, env, sink):
        st["spawned"].append(tag)
        return 0, 0.0

    @contextmanager
    def fake_lock(lane):
        st["lanes"].append(lane)
        yield st["lock"]

    monkeypatch.setattr(fleet_scan, "run_step", fake_step)
    monkeypatch.setattr(fleet_scan.scan_leases, "fleet_lock", fake_lock)
    monkeypatch.setattr(fleet_scan, "load_roster", lambda: ["a-com", "b-com"])
    monkeypatch.setattr(fleet_scan, "active_rows", lambda ids: {})
    monkeypatch.setattr(fleet_scan, "prune", lambda *a, **k: None)
    monkeypatch.setattr(fleet_scan, "rooftop_candidate_report", lambda since: {})
    monkeypatch.setattr(fleet_scan, "ResourceSampler", lambda: type(
        "S", (), {"start": lambda s: None, "stop": type("E", (), {"set": lambda s: None})(),
                  "summary": lambda s: {}})())
    monkeypatch.setattr(fleet_scan, "tally_http", lambda logs: {"by_status": {}})
    monkeypatch.setattr("backend.scanner.pipeline.assess.fleet_accounting", lambda r: {"rows_captured": 0, "rows_refused_vin_owner": 0})
    monkeypatch.setattr("backend.scanner.pipeline.triage.group_feed_unstamped", lambda r: 0)
    return st


def _summary(tmp_path):
    (path,) = tmp_path.glob("fleet_*/fleet_summary.json")
    return json.loads(path.read_text())


def test_unset_window_and_lock_keep_todays_behaviour(fx, tmp_path, monkeypatch):
    monkeypatch.setenv("SCAN_FLEET", "1")
    monkeypatch.setenv("SCAN_FLEET_LOCK", "0")
    assert fleet_scan.main() == 0
    assert fx["spawned"] and fx["lanes"] == []
    assert fx["sleeps"] == []
    s = _summary(tmp_path)
    assert s["window"] is None and s["warnings"] == []


def test_fleet_run_outside_window_idles_and_spawns_nothing(fx, capsys, monkeypatch):
    monkeypatch.setenv("SCAN_FLEET", "1")
    monkeypatch.setenv("SCAN_FLEET_WINDOW_UTC", "09:00-09:50")
    monkeypatch.setenv("SCAN_IDLE_HOLD_SECONDS", "1500")
    fx["now"] = at(8, 35)
    assert fleet_scan.main() == 0
    assert "outside run window" in capsys.readouterr().out
    assert fx["spawned"] == [] and fx["lanes"] == []
    assert fx["sleeps"] == [1200]  # capped: ends at 08:55, five minutes before the fire


def test_fleet_run_inside_window_runs_under_the_lane_lock(fx, tmp_path, monkeypatch):
    monkeypatch.setenv("SCAN_FLEET", "1")
    monkeypatch.setenv("SCAN_FLEET_WINDOW_UTC", "09:00-09:50")
    monkeypatch.setenv("SCAN_FLEET_LANE", "nightly")
    fx["now"] = at(9, 5)
    assert fleet_scan.main() == 0
    assert fx["spawned"] and fx["lanes"] == ["nightly"]
    s = _summary(tmp_path)
    assert s["lane"] == "nightly" and s["window"] == "09:00-09:50"
    assert s["lock"]["acquired"] is True and s["reason"] == "SCAN_FLEET"


def test_invalid_window_idles_with_a_warning(fx, capsys, monkeypatch):
    monkeypatch.setenv("SCAN_FLEET", "1")
    monkeypatch.setenv("SCAN_FLEET_WINDOW_UTC", "nine to ten")
    assert fleet_scan.main() == 0
    out = capsys.readouterr().out
    assert "WARN" in out and "invalid" in out
    assert fx["spawned"] == []


def test_scan_dealers_ignores_the_window_but_warns(fx, tmp_path, capsys, monkeypatch):
    monkeypatch.setenv("SCAN_FLEET", "1")
    monkeypatch.setenv("SCAN_DEALERS", "a-com,b-com")
    monkeypatch.setenv("SCAN_FLEET_WINDOW_UTC", "09:00-09:50")
    fx["now"] = at(15)  # well outside
    assert fleet_scan.main() == 0
    assert fx["spawned"] and fx["lanes"] == []  # an explicit list is not a fleet run: no lease
    s = _summary(tmp_path)
    assert any("SCAN_DEALERS" in w for w in s["warnings"])
    assert s["reason"] == "SCAN_DEALERS"


def test_lease_held_exits_3_before_any_shard(fx, capsys, monkeypatch):
    monkeypatch.setenv("SCAN_FLEET", "1")
    fx["lock"] = FleetLease(False, "scanner:fleet:other-host (backend pid 7, connected 2026-10-26)")
    monkeypatch.setattr(fleet_scan, "load_roster", lambda: pytest.fail("roster must not load"))
    assert fleet_scan.main() == fleet_scan.EXIT_FLEET_LOCK_HELD == 3
    out = capsys.readouterr().out
    assert "other-host" in out and fx["spawned"] == []


def test_post_run_hold_is_capped_by_the_next_window(fx, monkeypatch):
    monkeypatch.setenv("SCAN_FLEET", "1")
    monkeypatch.setenv("SCAN_FLEET_WINDOW_UTC", "09:00-09:50")
    monkeypatch.setenv("SCAN_POST_RUN_HOLD_SECONDS", "3600")
    fx["now"] = at(9, 10)
    assert fleet_scan.main() == 0
    assert fx["sleeps"] == [3600]  # next open is tomorrow
    # the cap itself: an hour-long hold at 07:40 stops at 07:55 for an 08:00 window
    assert fleet_scan.capped_hold(3600, at(7, 40), fleet_scan.parse_window("08:00-08:30")) == 900
