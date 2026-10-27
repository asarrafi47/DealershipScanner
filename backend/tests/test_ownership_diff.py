"""ownership_diff (N10.7): snapshot diff, move-log cross-reference, conflict census."""
from __future__ import annotations

import csv
import subprocess
from pathlib import Path

from backend.scripts import ownership_diff as od

ROOT = Path(__file__).resolve().parents[2]
HDR = ["id", "vin", "dealer_id", "listing_active", "scraped_at"]


def _w(path, header, rows):
    with open(path, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(header)
        w.writerows(rows)
    return path


def test_diff_pairs_and_move_log(tmp_path):
    before = _w(tmp_path / "b.csv", HDR, [
        (1, "V1", "a", "t", "2026-09-28"), (2, "V2", "a", "t", "2026-09-28"),
        (3, "V3", "b", "t", "2026-09-28"), (4, "V4", "c", "t", "2026-09-28"),
        (5, "V5", "c", "f", "2026-09-28"), (6, "V6", "d", "t", "2026-09-28")])
    after = _w(tmp_path / "a.csv", HDR, [
        (11, "V1", "b", "t", "2026-10-20"), (12, "V2", "b", "t", "2026-10-20"),
        (3, "V3", "b", "t", "2026-10-20"), (4, "V4", "c", "f", "2026-10-20"),
        (5, "V5", "c", "t", "2026-10-20"), (13, "V7", "e", "t", "2026-10-20")])
    moves = _w(tmp_path / "m.csv", ["id", "car_id", "from_dealer_id", "to_dealer_id", "reverted_at"], [
        (1, 11, "a", "b", ""), (2, 12, "a", "b", "2026-10-21")])  # second one reverted: not counted
    d = od.diff_snapshots(od.load_snapshot(before), od.load_snapshot(after), od.load_moves(moves))
    assert d["moved"] == 2 and d["pairs"] == [
        {"from": "a", "to": "b", "vins": 2, "active_after": 2, "logged": 1, "unlogged": 1}]
    assert (d["appeared"], d["vanished"], d["retired"], d["relisted"], d["unchanged"]) == (1, 1, 1, 1, 1)
    assert od.main([str(before), str(after), "--moves", str(moves)]) == 0


def test_one_holder_per_vin_prefers_active(tmp_path):
    p = _w(tmp_path / "s.csv", HDR, [(1, "V1", "old", "f", "2026-10-01"), (2, "V1", "new", "t", "2026-09-01")])
    assert od.load_snapshot(p)["V1"]["dealer_id"] == "new"


def test_census_reproduces_rca_classes(tmp_path):
    snap = od.load_snapshot(_w(tmp_path / "s.csv", HDR, [
        (1, "V1", "cl", "t", ""), (2, "V2", "ow", "t", ""), (3, "V3", "x", "t", ""),
        (4, "V4", "x", "f", ""), (5, "V5", "cl", "f", ""), (6, "V6", "ow", "f", "")]))
    conflicts = [{"vin": f"V{i}", "owner_dealer_id": "ow", "claimant_dealer_id": "cl"} for i in range(1, 8)]
    got = od.census_conflicts(snap, conflicts)
    assert got == {"claimant_active": 1, "owner_active": 1, "other_active": 1, "other_retired": 1,
                   "claimant_retired": 1, "owner_retired": 1, "absent": 1}
    assert od.census_pairs(snap, conflicts)[0]["rows"] == 7


def test_snapshot_script_refuses_bad_args():
    r = subprocess.run(["sh", str(ROOT / "scripts/ownership_snapshot.sh"), "NO_SUCH_VAR_N107", "x"],
                       capture_output=True, text=True, env={"PATH": "/usr/bin:/bin"})
    assert r.returncode == 2 and "not set" in r.stderr
    r = subprocess.run(["sh", str(ROOT / "scripts/ownership_snapshot.sh"), "A", "bad label"],
                       capture_output=True, text=True, env={"PATH": "/usr/bin:/bin"})
    assert r.returncode == 2
