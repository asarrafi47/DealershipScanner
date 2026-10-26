"""N9.12: honest fleet accounting, the group_feed_unstamped flag and handover_pending lines (offline)."""
from __future__ import annotations

import glob
import json
import sqlite3
from pathlib import Path

import pytest

from backend.scanner.pipeline.assess import account_row, fleet_accounting, group_feed_flag, handover_pending
from backend.scanner.pipeline.triage import accounting_line, group_feed_lines, group_feed_unstamped, handover_pending_lines

_REL = "workspace/cleanroom_2026_10/runs/fleet_20261013T140256Z"
# the workspace is gitignored: a worktree finds it in the main checkout (.claude/worktrees/<name>/)
RUN_10_13 = next((p / _REL for p in Path(__file__).resolve().parents if (p / _REL).exists()), Path(__file__).parent / "_absent")


def test_flag_needs_25_conflicts_and_20_percent_against_one_sister():
    r = {"dealer_id": "mtnviewnissan-com", "rows": 862, "vin_owner_conflict_owners": {"cleveland-nissan-com": 848}}
    assert group_feed_flag(r)["sister"] == "cleveland-nissan-com"
    assert group_feed_flag({"dealer_id": "a", "rows": 862, "vin_owner_conflict_owners": {"x": 24}}) is None  # < 25
    assert group_feed_flag({"dealer_id": "a", "rows": 1000, "vin_owner_conflict_owners": {"x": 100, "y": 90}}) is None  # < 20% each
    assert group_feed_flag({"dealer_id": "a", "rows": 0, "vin_owner_conflict_owners": {"x": 100}}) is None


def test_flags_are_reported_with_the_pair_and_stamped_on_the_result():
    res = [{"dealer_id": "d1", "rows": 100, "vin_owner_conflict_owners": {"sis": 60}}, {"dealer_id": "d2", "rows": 100}]
    flags = group_feed_unstamped(res)
    assert [f["dealer_id"] for f in flags] == ["d1"]
    assert res[0]["group_feed_unstamped"]["sister"] == "sis" and "group_feed_unstamped" not in res[1]
    line = group_feed_lines(flags)[0]
    assert "d1 <-> sis" in line and "owner OK" in line


def test_account_row_counts_error_dealer_as_zeros_and_keeps_active():
    a = account_row({"dealer_id": "x", "verdict": "error", "rows": None, "active_after": 553})
    assert a == {"rows_captured": 0, "rows_refused_vin_owner": 0, "rows_refused_rooftop": 0, "rows_written": 0, "active": 553, "inactive": 0}


def test_identity_synthetic():
    res = [{"rows": 100, "vin_owner_conflicts": 30, "active_after": 70, "inactive_after": 2},
           {"rows": 50, "filtered_count": 5, "active_after": 45},
           {"verdict": "error", "rows": None, "active_after": 10}]
    a = fleet_accounting(res)
    assert a["rows_captured"] - a["rows_refused_vin_owner"] - a["rows_refused_rooftop"] == a["rows_written"] == 115
    assert (a["active"], a["inactive"]) == (125, 2)
    assert "written 115" in accounting_line(res)


@pytest.mark.skipif(not RUN_10_13.exists(), reason="10-13 clean-room triage not on this checkout")
def test_replay_of_the_10_13_triage():
    res = []
    for f in sorted(glob.glob(str(RUN_10_13 / "s*/triage.json"))):
        res += json.loads(Path(f).read_text())["dealers"]
    a = fleet_accounting(res)
    assert (a["rows_captured"], a["rows_refused_vin_owner"], a["rows_written"]) == (208233, 9610, 198623)
    assert a["active"] == 199457  # includes the 834 rows of the two error dealers
    flagged = {f["dealer_id"] for f in group_feed_unstamped(res)}
    assert {"mtnviewnissan-com", "overdrive-usa-com"} <= flagged


def _conn():
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    c.execute("CREATE TABLE cars (id INTEGER PRIMARY KEY, dealer_id TEXT, vin TEXT, listing_active INTEGER)")
    c.execute("CREATE TABLE vin_owner_conflicts (vin TEXT, owner_dealer_id TEXT, claimant_dealer_id TEXT, seen_at TEXT)")
    return c


def test_handover_pending_fixture_is_reported():
    c = _conn()
    c.executemany("INSERT INTO cars (dealer_id, vin, listing_active) VALUES (?,?,?)",
                  [("old-com", "VIN1", 0), ("old-com", "VIN2", 1), ("old-com", "VIN3", 0)])
    c.executemany("INSERT INTO vin_owner_conflicts VALUES (?,?,?,?)",
                  [("VIN1", "old-com", "new-com", "2026-10-13T15:00:00+00:00"),   # retired + fresh claim: pending
                   ("VIN2", "old-com", "new-com", "2026-10-13T15:00:00+00:00"),   # still active: not a handover
                   ("VIN3", "old-com", "new-com", "2026-10-01T00:00:00+00:00")])  # stale conflict
    hp = handover_pending(c, "old-com", "2026-10-13T14:00:00+00:00")
    assert hp == {"vins": 1, "examples": ["VIN1"], "claimants": {"new-com": 1}}
    assert handover_pending(c, "new-com", "2026-10-13T14:00:00+00:00") is None
    line = handover_pending_lines([{"dealer_id": "old-com", "handover_pending": hp}])[0]
    assert "old-com retired 1 VIN" in line and "new-com" in line


def test_handover_pending_without_the_table_is_none():
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    c.execute("CREATE TABLE cars (id INTEGER PRIMARY KEY, dealer_id TEXT, vin TEXT, listing_active INTEGER)")
    assert handover_pending(c, "x", "2026-10-13") is None
