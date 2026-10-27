"""N11.4a: determinism_compare on two SQLite fixtures (offline, read only)."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from backend.scripts.cleanroom import determinism_compare as dc

DDL = """
CREATE TABLE cars (id INTEGER PRIMARY KEY, vin TEXT, dealer_id TEXT, listing_active INTEGER);
CREATE TABLE vin_owner_conflicts (vin TEXT, owner_dealer_id TEXT, claimant_dealer_id TEXT, seen_at TEXT);
"""


def _db(path: Path, rows: list[tuple[str, str, int]], conflicts=()) -> Path:
    c = sqlite3.connect(path)
    c.executescript(DDL)
    c.executemany("INSERT INTO cars (vin, dealer_id, listing_active) VALUES (?,?,?)", rows)
    c.executemany("INSERT INTO vin_owner_conflicts VALUES (?,?,?,'t')", conflicts)
    c.commit()
    c.close()
    return path


def _pair(tmp_path, a_rows, b_rows, a_conf=(), b_conf=()):
    return _db(tmp_path / "a.db", a_rows, a_conf), _db(tmp_path / "b.db", b_rows, b_conf)


def _run(a, b, tmp_path, *extra):
    out = tmp_path / "o.json"
    pools = tmp_path / "pools.json"
    if not pools.exists():
        pools.write_text(json.dumps({"pools": [], "aliases": []}))
    rc = dc.main(["--a", str(a), "--b", str(b), "--pools", str(pools), "--json", str(out), *extra])
    return rc, json.loads(out.read_text()) if out.exists() else None


def test_full_agreement(tmp_path):
    rows = [(f"V{i:03d}", "d1" if i % 2 else "d2", 1) for i in range(10)]
    a, b = _pair(tmp_path, rows, rows)
    rc, res = _run(a, b, tmp_path)
    assert rc == dc.EXIT_OK and res["agreement_pct"] == 100.0 and res["accepted"] and res["pairs"] == []


def test_flagged_difference_is_explained_within_threshold(tmp_path):
    a_rows = [(f"V{i:03d}", "d1", 1) for i in range(200)]
    b_rows = [(v, "d2" if v == "V007" else d, 1) for v, d, _ in a_rows]
    a, b = _pair(tmp_path, a_rows, b_rows, a_conf=[("V007", "d1", "d2")])
    rc, res = _run(a, b, tmp_path)
    assert rc == dc.EXIT_OK and res["agreement_pct"] == 99.5
    assert res["pairs"] == [{"a_dealer": "d1", "b_dealer": "d2", "vins": 1,
                             "group_feed_unstamped": True, "in_group_pools": False}]
    assert res["differences"] == [{"vin": "V007", "a_dealer": "d1", "b_dealer": "d2", "status": "explained"}]


def test_unflagged_difference_fails_even_above_threshold(tmp_path):
    a_rows = [(f"V{i:03d}", "d1", 1) for i in range(200)]
    b_rows = [(v, "d2" if v == "V007" else d, 1) for v, d, _ in a_rows]
    a, b = _pair(tmp_path, a_rows, b_rows)
    rc, res = _run(a, b, tmp_path)
    assert rc == dc.EXIT_FAIL and res["unexplained_vins"] == 1 and not res["accepted"]


def test_pooled_pair_is_unexplained_and_below_threshold_fails(tmp_path):
    a_rows = [(f"V{i:03d}", "d1", 1) for i in range(10)]
    b_rows = [(v, "d2" if i < 3 else d, 1) for i, (v, d, _) in enumerate(a_rows)]
    a, b = _pair(tmp_path, a_rows, b_rows, b_conf=[("V000", "d2", "d1")])
    (tmp_path / "pools.json").write_text(json.dumps({"pools": [{"primary": "d1", "members": ["d1", "d2"]}]}))
    rc, res = _run(a, b, tmp_path)
    assert rc == dc.EXIT_FAIL and res["agreement_pct"] == 70.0
    assert res["pairs"][0]["in_group_pools"] is True and res["pairs"][0]["group_feed_unstamped"] is True
    rc2, res2 = _run(a, b, tmp_path, "--min-agree", "50")
    assert rc2 == dc.EXIT_FAIL and res2["unexplained_vins"] == 3  # in a pool: it should have converged


def test_triage_flag_marks_the_pair_and_inactive_rows_are_ignored(tmp_path):
    a_rows = [("V1", "d1", 1), ("V2", "d1", 1), ("V3", "d1", 0)]
    b_rows = [("V1", "d1", 1), ("V2", "d2", 1), ("V3", "d9", 1), ("V4", "d2", 1)]
    a, b = _pair(tmp_path, a_rows, b_rows)
    run = tmp_path / "runB"
    run.mkdir()
    (run / "triage.json").write_text(json.dumps({"dealers": [
        {"dealer_id": "d2", "group_feed_unstamped": {"sister": "d1"}}]}))
    rc, res = _run(a, b, tmp_path, "--run-dir-b", str(run), "--min-agree", "50")
    assert rc == dc.EXIT_OK and res["vins_both"] == 2 and res["different_dealer"] == 1
    assert res["b_only_vins"] == 2 and res["pairs"][0]["group_feed_unstamped"] is True


def test_a_only_vins_in_b_dealers_are_counted(tmp_path):
    a_rows = [("V1", "d1", 1), ("V2", "d1", 1), ("V3", "d5", 1)]
    b_rows = [("V1", "d1", 1)]
    a, b = _pair(tmp_path, a_rows, b_rows)
    rc, res = _run(a, b, tmp_path)
    assert res["a_only_in_b_dealers"] == 1  # V2 (d1 was scanned in B); V3's dealer was not


def test_refuses_same_db(tmp_path):
    a, _ = _pair(tmp_path, [], [])
    assert dc.main(["--a", str(a), "--b", str(a), "--json", str(tmp_path / "o.json")]) == dc.EXIT_REFUSED
