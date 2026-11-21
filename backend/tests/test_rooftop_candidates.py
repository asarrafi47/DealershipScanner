"""N8.13 unrostered-rooftop detector and queue."""
from __future__ import annotations

import json

import pytest

from backend.attribution import candidates as cand
from backend.parsers import resolve_rooftop_attribution
from backend.scripts import rooftop_candidates as cli

ROSTER = [
    {"id": "mtnviewnissan-com", "name": "mtnviewnissan", "street_key": "100mainstreet", "city_state": "", "host": "mtnviewnissan.com"},
    {"id": "mtnviewnissanofbristol-com", "name": "mtnviewnissanofbristol", "street_key": "", "city_state": "", "host": ""},
]


def _row(vin, name, **extra):
    return {"vin": vin, "_rooftop": {"key": name, "name": name, **extra}, "source_url": "https://mtnviewnissan.com/v/" + vin}


def _gate():
    rows = [_row("V1", "Mtn View Nissan"), _row("V2", "Mtn View Nissan"),
            _row("V3", "Mtn View Nissan of Dalton", address="5 Dalton Pike", city="Dalton", state="GA", zip="30720"),
            _row("V4", "Mtn View Nissan of Dalton"),
            _row("V5", "Mtn View Nissan of Bristol")]
    return resolve_rooftop_attribution(rows, dealer_id="mtnviewnissan-com", dealer_name="Mtn View Nissan",
                                       dealer_url="https://mtnviewnissan.com")


def test_unknown_rooftop_yields_one_candidate_and_nothing_filed_under_sister():
    kept, rejected = _gate()
    assert {r["vin"] for r in kept} == {"V1", "V2"}  # nothing from the unknown store is kept
    found = cand.candidates_from_refused(rejected, ROSTER, "mtnviewnissan-com")
    assert len(found) == 1  # Bristol is on the roster
    c = found[0]
    assert c["name"] == "Mtn View Nissan of Dalton"
    assert c["car_count"] == 2
    assert "5 Dalton Pike" in c["address"] and "GA" in c["address"]
    assert c["host"] == "mtnviewnissan.com"
    assert c["first_seen"] and c["last_seen"] and c["status"] == "new"
    assert c["seen_via"] == ["mtnviewnissan-com"]


def test_street_match_counts_as_rostered():
    rows = [_row("V9", "Some Renamed Store", address="100 Main St")]
    rows[0]["_rooftop_reject"] = "sibling_rooftop"
    assert cand.candidates_from_refused(rows, ROSTER) == []


def test_unstamped_refusals_are_not_candidates():
    rows = [{"vin": "V1", "_rooftop_reject": "unstamped_row_in_group_feed"}]
    assert cand.candidates_from_refused(rows, ROSTER) == []


def test_record_merges_and_keeps_first_seen(tmp_path):
    path = tmp_path / "q.json"
    _, rejected = _gate()
    first = cand.candidates_from_refused(rejected, ROSTER, "a-com")
    cand.record(first, path)
    later = [dict(first[0], last_seen="2099-01-01T00:00:00Z", car_count=7, seen_via=["b-com"])]
    cand.record(later, path)
    items = list(cand.load(path)["candidates"].values())
    assert len(items) == 1
    assert items[0]["first_seen"] == first[0]["first_seen"] and items[0]["last_seen"].startswith("2099")
    assert items[0]["car_count"] == 7 and items[0]["seen_via"] == ["a-com", "b-com"]
    assert len(cand.new_candidates(path)) == 1


def test_note_refusals_writes_only_when_enabled(tmp_path, monkeypatch):
    path = tmp_path / "q.json"
    monkeypatch.setenv("ROOFTOP_CANDIDATES_PATH", str(path))
    _, rejected = _gate()
    monkeypatch.delenv("SCANNER_ROOFTOP_CANDIDATES", raising=False)
    assert len(cand.note_refusals(rejected, "mtnviewnissan-com", ROSTER)) == 1
    assert not path.exists()
    monkeypatch.setenv("SCANNER_ROOFTOP_CANDIDATES", "1")
    cand.note_refusals(rejected, "mtnviewnissan-com", ROSTER)
    assert len(cand.new_candidates(path)) == 1


def test_cli_lists_and_marks(tmp_path, monkeypatch, capsys):
    path = tmp_path / "q.json"
    monkeypatch.setenv("ROOFTOP_CANDIDATES_PATH", str(path))
    _, rejected = _gate()
    found = cand.candidates_from_refused(rejected, ROSTER, "a-com")
    cand.record(found, path)
    assert cli.main(["--json"]) == 0
    assert json.loads(capsys.readouterr().out)[0]["name"] == "Mtn View Nissan of Dalton"
    assert cli.main(["--mark", found[0]["id"], "--status", "profiled"]) == 0
    assert cand.new_candidates(path) == []
    assert cli.main(["--mark", "nope"]) == 1


def test_fleet_summary_report_lists_candidates(tmp_path, monkeypatch):
    from datetime import datetime, timedelta, timezone

    from backend.scripts import fleet_scan

    path = tmp_path / "q.json"
    monkeypatch.setenv("ROOFTOP_CANDIDATES_PATH", str(path))
    _, rejected = _gate()
    cand.record(cand.candidates_from_refused(rejected, ROSTER, "a-com"), path)
    rep = fleet_scan.rooftop_candidate_report(datetime.now(timezone.utc) - timedelta(hours=1))
    assert rep["open"] == 1 and rep["seen_this_run"] == 1
    assert rep["new_this_run"][0]["name"] == "Mtn View Nissan of Dalton"
