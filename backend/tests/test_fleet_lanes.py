"""Egress lanes (P15A.1): home_lane_dealers.txt, the railway/home roster, the alarms."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

import backend.scanner.manifest as m
from backend.scripts import fleet_scan


def _d(did, active=True):
    return {"dealer_id": did, "url": f"https://www.{did}", "name": did, "_has_active_inventory": active}


ROSTER = [_d("a-com"), _d("home1-com"), _d("home2-com"), _d("dns-com"), _d("recipeonly-com", active=False)]
HOME = {"home1-com", "home2-com", "dns-com"}


def _rule(lane):
    return m.apply_scannable_roster_rule(ROSTER, do_not_scan={"dns-com"}, revived=set(), lane=lane, home_lane=HOME)


def test_railway_lane_drops_home_ids_and_reports():
    kept, rep = _rule("railway")
    assert [d["dealer_id"] for d in kept] == ["a-com"]
    assert rep["excluded_home_lane"] == ["home1-com", "home2-com"]
    assert rep["excluded_do_not_scan"] == ["dns-com"]  # do-not-scan wins over the home list
    assert rep["excluded_recipe_only"] == ["recipeonly-com"]


def test_home_lane_is_home_ids_minus_do_not_scan():
    kept, rep = _rule("home")
    assert [d["dealer_id"] for d in kept] == ["home1-com", "home2-com"]
    assert rep["excluded_not_home_lane"] == 2  # a-com and the recipe-only one


def test_no_lane_and_all_lane_are_todays_roster():
    for lane in (None, "all"):
        kept, rep = _rule(lane)
        assert [d["dealer_id"] for d in kept] == ["a-com", "home1-com", "home2-com"]
        assert rep["excluded_home_lane"] == []


def test_unknown_lane_fails_loudly():
    with pytest.raises(ValueError):
        _rule("moon")


def test_scan_lane_defaults(monkeypatch):
    monkeypatch.delenv("SCAN_LANE", raising=False)
    monkeypatch.delenv("SCANNER_EGRESS_TAG", raising=False)
    assert m.scan_lane() == "all"
    monkeypatch.setenv("SCANNER_EGRESS_TAG", "railway")
    assert m.scan_lane() == "railway"
    monkeypatch.setenv("SCAN_LANE", "ALL")  # explicit value beats the tag: the kill switch
    assert m.scan_lane() == "all"
    monkeypatch.setenv("SCAN_LANE", "home")
    assert m.scan_lane() == "home"
    monkeypatch.setenv("SCAN_LANE", "bogus")
    with pytest.raises(ValueError):
        m.scan_lane()


def test_tracked_home_file_parses_and_starts_empty():
    assert m.HOME_LANE_DEALERS_PATH.is_file()
    assert m.load_home_lane_ids() == set()


def _fleet_env(monkeypatch):
    monkeypatch.delenv("SCAN_DEALERS", raising=False)
    monkeypatch.setenv("SCAN_FLEET", "1")
    monkeypatch.setattr(m, "_load_scannable_dealers", lambda: list(ROSTER))
    monkeypatch.setattr(m, "load_do_not_scan_ids", lambda paths=None: {"dns-com"})
    monkeypatch.setattr(m, "load_revived_dealer_ids", lambda path=None: set())
    monkeypatch.setattr(m, "load_home_lane_ids", lambda path=None: set(HOME))


def test_fleet_roster_per_lane(monkeypatch):
    _fleet_env(monkeypatch)
    monkeypatch.delenv("SCANNER_EGRESS_TAG", raising=False)
    monkeypatch.delenv("SCAN_LANE", raising=False)
    assert fleet_scan.load_roster() == ["a-com", "home1-com", "home2-com"]
    monkeypatch.setenv("SCANNER_EGRESS_TAG", "railway")
    assert fleet_scan.load_roster() == ["a-com"]
    assert fleet_scan.ROSTER_REPORT["excluded_home_lane"] == ["home1-com", "home2-com"]
    monkeypatch.setenv("SCAN_LANE", "home")
    assert fleet_scan.load_roster() == ["home1-com", "home2-com"]
    monkeypatch.setenv("SCAN_LANE", "nope")
    with pytest.raises(SystemExit):
        fleet_scan.load_roster()


def test_explicit_dealers_untouched_by_lane(monkeypatch):
    monkeypatch.setenv("SCAN_DEALERS", "home1-com,dns-com")
    monkeypatch.setenv("SCANNER_EGRESS_TAG", "railway")
    assert fleet_scan.load_roster() == ["home1-com", "dns-com"]


def test_load_manifest_scannable_honors_lane(monkeypatch):
    _fleet_env(monkeypatch)
    monkeypatch.setenv("DEALERS_FROM_SCANNABLE", "1")
    monkeypatch.setenv("SCANNER_EGRESS_TAG", "railway")
    monkeypatch.delenv("SCAN_LANE", raising=False)
    assert [d["dealer_id"] for d in m.load_manifest()] == ["a-com"]
    assert [d["dealer_id"] for d in m.load_manifest(explicit_dealer_ids=True)] == [d["dealer_id"] for d in ROSTER]
    monkeypatch.delenv("SCANNER_EGRESS_TAG")  # no tag, no lane: unchanged
    assert [d["dealer_id"] for d in m.load_manifest()] == ["a-com", "home1-com", "home2-com"]


# ---- alarms ----

def test_blocked_railway_ids_reads_both_shapes():
    rows = [
        ("new-com", json.dumps({"recipe_blocked": {"railway": "blocked:railway:403:x"}})),
        ("legacy-com", json.dumps({"recipe_status": "blocked:railway:403:2026-09-29"})),
        ("other-host-com", json.dumps({"recipe_blocked": {"laptop": "x"}})),
        ("plain-com", json.dumps({"recipe_status": "ok"})),
        ("bad-com", "{not json"),
        ("none-com", None),
    ]
    assert fleet_scan.blocked_railway_ids(rows) == {"new-com", "legacy-com"}


def test_blocked_unassigned_excludes_home_and_do_not_scan():
    assert fleet_scan.blocked_unassigned({"a", "b", "c", "d"}, {"b"}, {"c"}) == ["a", "d"]


def test_home_lane_stale_36h():
    now = datetime(2026, 11, 10, 12, 0, tzinfo=timezone.utc)
    newest = {
        "fresh": (now - timedelta(hours=35)).isoformat(),
        "stale": (now - timedelta(hours=37)).isoformat(),
        "naive": (now - timedelta(hours=40)).replace(tzinfo=None).isoformat(),
        "dt": now - timedelta(hours=1),
        "junk": "yesterday-ish",
    }
    home = {"fresh", "stale", "naive", "dt", "junk", "norows"}
    assert fleet_scan.home_lane_stale(home, newest, now) == ["junk", "naive", "norows", "stale"]


class _Cur:
    def __init__(self, hints, newest):
        self.hints, self.newest, self.sql = hints, newest, ""

    def execute(self, sql, *_a):
        self.sql = sql

    def fetchall(self):
        return self.hints if "dealer_recipes" in self.sql else self.newest


class _Conn:
    def __init__(self, cur):
        self.cur = cur

    def cursor(self):
        return self.cur

    def close(self):
        pass


def test_lane_alarms_on_fixtures(monkeypatch):
    import backend.db.inventory_db as inv

    now = datetime(2026, 11, 10, tzinfo=timezone.utc)
    hints = [("x-com", json.dumps({"recipe_blocked": {"railway": "v"}})),
             ("h-com", json.dumps({"recipe_blocked": {"railway": "v"}})),
             ("z-com", json.dumps({"recipe_blocked": {"railway": "v"}}))]
    newest = [("h-com", (now - timedelta(hours=60)).isoformat()), ("h2-com", now.isoformat())]
    monkeypatch.setattr(inv, "get_conn", lambda: _Conn(_Cur(hints, newest)))
    monkeypatch.setattr(m, "load_home_lane_ids", lambda path=None: {"h-com", "h2-com", "h3-com"})
    monkeypatch.setattr(m, "load_do_not_scan_ids", lambda paths=None: {"z-com"})
    out = fleet_scan.lane_alarms(now)
    assert out["blocked_unassigned"] == ["x-com"]
    assert out["home_lane_stale"] == ["h-com", "h3-com"]
    assert "error" not in out


def test_lane_alarms_read_failure_is_reported_not_raised(monkeypatch):
    import backend.db.inventory_db as inv

    def boom():
        raise RuntimeError("db down")

    monkeypatch.setattr(inv, "get_conn", boom)
    monkeypatch.setattr(m, "load_home_lane_ids", lambda path=None: set())
    out = fleet_scan.lane_alarms()
    assert out["blocked_unassigned"] == [] and "db down" in out["error"]


def test_post_steps_default_by_lane(monkeypatch):
    monkeypatch.delenv("SCANNER_EGRESS_TAG", raising=False)
    monkeypatch.delenv("SCAN_LANE", raising=False)
    assert fleet_scan.egress_lane() == "all"
    monkeypatch.setenv("SCAN_LANE", "home")
    assert fleet_scan.egress_lane() == "home"
