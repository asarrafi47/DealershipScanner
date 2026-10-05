"""Scan timing as part of the dealer fingerprint (2026-09-28): flags from a scan_runs
row, the recommended HTTP-first window, the merge into scan_hints, the prefetch
readers, and the backfill script."""
from __future__ import annotations

import json
import math
import sqlite3

import pytest

from backend.scanner import scan_timing as st
from backend.scanner.vdp import prefetch as pf
from backend.scripts import fingerprint_timing as ft

AVONDALE_SUMMARY = {
    "vdp_prefetch": {"http_first": {
        "fetched": 448, "retried": 7, "statuses": {"200": 448, "403": 1}, "candidates": 800, "skipped_cap": 867,
        "slowed_host": "www.avondaletoyota.com", "host_exhausted": "www.avondaletoyota.com",
        "wall_clock_hit": True, "slow_host_skipped": 4,
    }},
    "vdp_phase_timed_out": False,
    "phase_secs": {"upsert": 402.08},
}
AVONDALE_RUN = {"id": 9101, "finished_at": "2026-09-28T08:15:00+00:00", "duration_seconds": 919,
                "error": None, "summary_json": json.dumps(AVONDALE_SUMMARY)}
# The same pass with the host never exhausted: time was the only constraint.
UNTHROTTLED_HF = {k: v for k, v in AVONDALE_SUMMARY["vdp_prefetch"]["http_first"].items()
                  if k not in ("host_exhausted", "slowed_host")}
UNTHROTTLED_RUN = dict(AVONDALE_RUN, summary_json=json.dumps(
    dict(AVONDALE_SUMMARY, vdp_prefetch={"http_first": UNTHROTTLED_HF})))


def _run(rid, finished, secs=100, error=None, summary=None):
    return {"id": rid, "finished_at": finished, "duration_seconds": secs, "error": error,
            "summary_json": json.dumps(summary or {})}


# ---------------------------------------------------------------- timing_entry

def test_avondale_entry_flags_and_prefetch_facts():
    e = st.timing_entry(AVONDALE_RUN)
    assert e["run_id"] == 9101 and e["finished_at"] == "2026-09-28T08:15:00+00:00"
    assert e["duration_s"] == 919 and e["upsert_s"] == 402.1
    assert e["prefetch"] == {
        "candidates": 800, "fetched": 448, "skipped_cap": 867, "retried": 7, "statuses": {"200": 448, "403": 1},
        "wall_clock_hit": True, "wall_clock_sec": None,
        "host_exhausted": "www.avondaletoyota.com", "slowed_host": "www.avondaletoyota.com",
    }
    assert set(e["flags"]) == {"cap_hit", "host_exhausted", "slowed_host", "slow", "upsert_slow"}
    # deterministic order (FLAG_ORDER), not set order
    assert e["flags"] == ["cap_hit", "host_exhausted", "slowed_host", "slow", "upsert_slow"]
    assert e["error"] is None


def test_entry_accepts_parsed_summary_dict():
    e = st.timing_entry(dict(AVONDALE_RUN, summary_json=AVONDALE_SUMMARY))
    assert "cap_hit" in e["flags"]


def test_errored_run_flags_error_and_timeout_text():
    e = st.timing_entry(_run(1, "2026-09-28T01:00:00+00:00", 30, error="Recipe replay failed: connection reset"))
    assert e["flags"] == ["error"] and e["error"].startswith("Recipe replay failed")
    e2 = st.timing_entry(_run(2, "2026-09-28T01:00:00+00:00", 1900, error="Timeout 1800s exceeded"))
    assert e2["flags"] == ["error", "timed_out", "slow", "very_slow"]
    e3 = st.timing_entry(_run(3, "2026-09-28T01:00:00+00:00", 50, summary={"vdp_phase_timed_out": True}))
    assert e3["flags"] == ["timed_out"]


def test_403s_and_cap_hit_without_wall_clock():
    summary = {"vdp_prefetch": {"http_first": {"candidates": 300, "fetched": 120, "skipped_cap": 40,
                                               "statuses": {"200": 120, "403": 5}}}}
    e = st.timing_entry(_run(4, "2026-09-28T01:00:00+00:00", 200, summary=summary))
    assert e["flags"] == ["cap_hit", "403s"]
    # page cap reached but every candidate fetched: not a cap_hit
    summary2 = {"vdp_prefetch": {"http_first": {"candidates": 300, "fetched": 300, "skipped_cap": 40, "statuses": {"403": 4}}}}
    assert st.timing_entry(_run(5, "2026-09-28T01:00:00+00:00", 200, summary=summary2))["flags"] == []


def test_clean_fast_run_has_no_flags_and_empty_prefetch():
    e = st.timing_entry(_run(6, "2026-09-28T01:00:00+00:00", 45))
    assert e["flags"] == [] and e["upsert_s"] is None
    assert e["prefetch"]["candidates"] == 0 and e["prefetch"]["statuses"] == {}


# ---------------------------------------------------------------- recommend_windows

def test_avondale_recommendation_is_scaled_to_the_minute():
    e = st.timing_entry(UNTHROTTLED_RUN)
    rec = st.recommend_windows([e])
    expected = 60 * math.ceil(300 * (800 + 867) / 448 / 60)  # 1116.3 s -> 19 min
    assert expected == 1140
    assert rec == {"vdp_http_first_max_sec": 1140, "pages_needed": 1667}


def _capped(rid, finished, hf):
    return st.timing_entry(_run(rid, finished, 400, summary={"vdp_prefetch": {"http_first": dict(hf, wall_clock_hit=True)}}))


def test_recommendation_scales_off_the_window_the_run_actually_used():
    """A hinted dealer's capped pass ran 1140 s; scaling that off the 300 s default
    recommended 1140 s again at best and less when the pass fetched more, so the
    hint oscillated. The recorded wall_clock_sec is the base now."""
    hf = dict(UNTHROTTLED_HF)
    # no wall_clock_sec recorded (older rows): the default is assumed -> 1140
    assert st.recommend_windows([st.timing_entry(UNTHROTTLED_RUN)])["vdp_http_first_max_sec"] == 1140
    e = _capped(9102, "2026-09-29T08:15:00+00:00", dict(hf, wall_clock_sec=1140))
    assert e["prefetch"]["wall_clock_sec"] == 1140 and st.run_window_sec(e) == 1140
    # 1140 s fetched 448 of 1667 wanted -> 4242 s, clamped to the ceiling
    assert st.recommend_windows([e])["vdp_http_first_max_sec"] == 1800
    # the 1140 s pass fetched 1400 of 1667: 1140 * 1667 / 1400 = 1357 -> 1380, not 300 * ... = 360
    e2 = _capped(9103, "2026-09-30T08:15:00+00:00", {"candidates": 1400, "fetched": 1400, "skipped_cap": 267, "wall_clock_sec": 1140})
    assert st.recommend_windows([e2])["vdp_http_first_max_sec"] == 1380
    # never below the window that just hit the cap, even when every page landed
    e3 = _capped(9104, "2026-10-01T08:15:00+00:00", {"candidates": 100, "fetched": 100, "wall_clock_sec": 600})
    assert st.recommend_windows([e3])["vdp_http_first_max_sec"] == 600
    # an odd env window (420 s) is kept as the floor and the scale rounds to the minute above it
    e4 = _capped(9105, "2026-10-02T08:15:00+00:00", {"candidates": 100, "fetched": 99, "wall_clock_sec": 420})
    assert st.recommend_windows([e4])["vdp_http_first_max_sec"] == 480
    # a zero / missing stat falls back to the default base
    e5 = _capped(9106, "2026-10-03T08:15:00+00:00", {"candidates": 100, "fetched": 99, "wall_clock_sec": 0})
    assert e5["prefetch"]["wall_clock_sec"] is None and st.recommend_windows([e5])["vdp_http_first_max_sec"] == 360
    # merge_timing carries the same number
    assert st.merge_timing({}, e2)["vdp_http_first_max_sec"] == 1380


def test_recommendation_is_none_when_latest_run_was_not_capped_and_clamped_otherwise():
    clean = st.timing_entry(_run(7, "2026-09-29T01:00:00+00:00", 45, summary={"vdp_prefetch": {"http_first": {"candidates": 120, "fetched": 120}}}))
    capped = st.timing_entry(AVONDALE_RUN)
    rec = st.recommend_windows([capped, clean])  # clean is newer
    assert rec["vdp_http_first_max_sec"] is None and rec["pages_needed"] == 1667
    # 2 pages in 300 s wanting 5000 -> way past the ceiling
    huge = st.timing_entry(_run(8, "2026-09-30T01:00:00+00:00", 400, summary={"vdp_prefetch": {"http_first": {
        "candidates": 800, "fetched": 2, "skipped_cap": 4200, "wall_clock_hit": True}}}))
    assert st.recommend_windows([huge])["vdp_http_first_max_sec"] == 1800
    # wall clock hit right as the last page landed: never below the 300 s default
    small = st.timing_entry(_run(9, "2026-09-30T02:00:00+00:00", 400, summary={"vdp_prefetch": {"http_first": {
        "candidates": 100, "fetched": 100, "skipped_cap": 0, "wall_clock_hit": True}}}))
    assert st.recommend_windows([small])["vdp_http_first_max_sec"] == 300
    # one page short scales 300 s to 303 s, which rounds up to the next minute
    short = st.timing_entry(_run(10, "2026-09-30T03:00:00+00:00", 400, summary={"vdp_prefetch": {"http_first": {
        "candidates": 100, "fetched": 99, "skipped_cap": 0, "wall_clock_hit": True}}}))
    assert st.recommend_windows([short])["vdp_http_first_max_sec"] == 360
    assert st.recommend_windows([]) == {"vdp_http_first_max_sec": None, "pages_needed": None}


# ---------------------------------------------------------------- merge_timing

def test_merge_keeps_five_newest_and_dedupes_by_run_id():
    hints: dict = {"needs_http_proxy": True}
    for i in range(1, 8):
        entry = st.timing_entry(_run(i, f"2026-09-{20 + i:02d}T00:00:00+00:00", 60 * i))
        hints = {"needs_http_proxy": True, "timing": st.merge_timing(hints, entry)}
    t = hints["timing"]
    assert [r["run_id"] for r in t["runs"]] == [7, 6, 5, 4, 3]
    assert t["updated_at"] == "2026-09-27T00:00:00+00:00"
    # re-assessing run 7 (now flagged) replaces its record instead of adding one
    again = st.timing_entry(_run(7, "2026-09-27T00:00:00+00:00", 1300))
    t2 = st.merge_timing(hints, again)
    assert [r["run_id"] for r in t2["runs"]] == [7, 6, 5, 4, 3]
    assert t2["runs"][0]["duration_s"] == 1300 and t2["flags"] == ["slow", "very_slow"]
    # an older run folded in later does not become "latest"
    old = st.timing_entry(_run(2, "2026-09-22T00:00:00+00:00", 10))
    t3 = st.merge_timing({"timing": t2}, old)
    assert [r["run_id"] for r in t3["runs"]] == [7, 6, 5, 4, 3] and t3["flags"] == ["slow", "very_slow"]


def test_merge_from_empty_hints_carries_avondale_recommendation():
    t = st.merge_timing({}, st.timing_entry(AVONDALE_RUN))
    assert t["vdp_http_first_max_sec"] is None and t["pages_needed"] == 1667  # throttled: no wider window
    assert t["flags"] == ["cap_hit", "host_exhausted", "slowed_host", "slow", "upsert_slow"]
    assert st.needs_attention(t["flags"]) and not st.needs_attention(["slow", "slowed_host"])
    assert st.minutes(t["runs"][0]) == 15.3


# ---------------------------------------------------------------- prefetch readers

@pytest.fixture()
def hinted(monkeypatch):
    store: dict[str, dict] = {}

    def _get(dealer_id):
        return store.get(dealer_id, {})

    monkeypatch.setattr("backend.scanner.recipe_store.get_scan_hints", _get)
    monkeypatch.delenv("SCANNER_VDP_HTTP_FIRST_MAX_SEC", raising=False)
    monkeypatch.delenv("SCANNER_VDP_HTTP_FIRST_MAX", raising=False)
    return store


def test_wall_clock_prefers_dealer_hint_and_clamps(hinted, caplog):
    hinted["avondaletoyota-com"] = {"timing": {"vdp_http_first_max_sec": 1140, "pages_needed": 1667}}
    hinted["tiny-com"] = {"timing": {"vdp_http_first_max_sec": 5}}
    hinted["huge-com"] = {"timing": {"vdp_http_first_max_sec": 99999}}
    hinted["nohint-com"] = {"timing": {"vdp_http_first_max_sec": None}}
    import logging

    with caplog.at_level(logging.INFO, logger="scanner"):
        assert pf._http_first_wall_clock_sec("avondaletoyota-com") == 1140.0
    assert any("per-dealer wall clock 1140s" in m for m in caplog.messages)
    assert pf._http_first_wall_clock_sec("tiny-com") == 30.0
    assert pf._http_first_wall_clock_sec("huge-com") == 1800.0
    assert pf._http_first_wall_clock_sec("nohint-com") == 300.0
    assert pf._http_first_wall_clock_sec("unknown-com") == 300.0
    assert pf._http_first_wall_clock_sec() == 300.0


def test_page_cap_widens_from_dealer_hint_never_narrows(hinted):
    hinted["avondaletoyota-com"] = {"timing": {"pages_needed": 1667}}
    hinted["huge-com"] = {"timing": {"pages_needed": 99999}}
    hinted["small-com"] = {"timing": {"pages_needed": 100}}
    assert pf._http_first_max("avondaletoyota-com") == 1667
    assert pf._http_first_max("huge-com") == 5000
    assert pf._http_first_max("small-com") == 800
    assert pf._http_first_max("unknown-com") == 800
    assert pf._http_first_max() == 800


def test_http_prefetch_reads_the_timing_block_once_per_pass(monkeypatch):
    """Two window helpers used to each open a synchronous get_scan_hints() read
    inside the async pass; now one to_thread read feeds both."""
    import asyncio

    calls: list[str] = []

    def _get(dealer_id):
        calls.append(dealer_id)
        return {"timing": {"vdp_http_first_max_sec": 600, "pages_needed": 900}}

    monkeypatch.setattr("backend.scanner.recipe_store.get_scan_hints", _get)
    monkeypatch.delenv("SCANNER_VDP_HTTP_FIRST_MAX_SEC", raising=False)
    monkeypatch.delenv("SCANNER_VDP_HTTP_FIRST_MAX", raising=False)
    monkeypatch.setattr(pf, "_fetch_html", lambda url: "")
    v = {"vin": "1HGBH41JXMN109186", "_detail_url": "https://d.example/car", "price": None}
    stats = asyncio.run(pf.http_prefetch_missing_fields([v], "avondaletoyota-com"))
    assert calls == ["avondaletoyota-com"]
    assert stats["wall_clock_sec"] == 600 and stats["candidates"] == 1
    # a pre-read block (or {}) means no read at all; no dealer means no read either
    calls.clear()
    stats = asyncio.run(pf.http_prefetch_missing_fields([dict(v)], "avondaletoyota-com", timing={"vdp_http_first_max_sec": 90}))
    assert calls == [] and stats["wall_clock_sec"] == 90
    assert asyncio.run(pf.http_prefetch_missing_fields([dict(v)]))["wall_clock_sec"] == 300 and calls == []
    # the sync helpers still read on their own when handed no block, and honour one when they are
    assert pf._http_first_max("avondaletoyota-com") == 900 and calls == ["avondaletoyota-com"]
    assert pf._http_first_max("avondaletoyota-com", {"pages_needed": 1200}) == 1200 and len(calls) == 1
    assert pf._http_first_wall_clock_sec("x-com", {}) == 300.0 and len(calls) == 1
    assert pf._dealer_timing_hint("x-com", "pages_needed", {"pages_needed": "abc"}) is None


def test_hint_lookup_failure_falls_back_to_env(monkeypatch):
    def _boom(dealer_id):
        raise RuntimeError("no database")

    monkeypatch.setattr("backend.scanner.recipe_store.get_scan_hints", _boom)
    monkeypatch.setenv("SCANNER_VDP_HTTP_FIRST_MAX_SEC", "420")
    assert pf._http_first_wall_clock_sec("any-com") == 420.0
    assert pf._http_first_max("any-com") == 800


# ---------------------------------------------------------------- backfill script

@pytest.fixture()
def scan_runs_conn():
    c = sqlite3.connect(":memory:")
    c.execute("CREATE TABLE scan_runs (id INTEGER PRIMARY KEY, dealer_id TEXT, finished_at TEXT, duration_seconds REAL, error TEXT, summary_json TEXT)")
    rows = [(9101, "avondaletoyota-com", AVONDALE_RUN["finished_at"], 919, None, AVONDALE_RUN["summary_json"])]
    for i in range(1, 8):  # 7 clean runs for one dealer; only the newest 5 count
        rows.append((100 + i, "quick-com", f"2026-09-{20 + i:02d}T00:00:00+00:00", 40 + i, None, "{}"))
    rows.append((200, "broken-com", "2026-09-27T12:00:00+00:00", 12, "scanner exited 1", "{}"))
    rows.append((201, "ancient-com", "2026-08-01T12:00:00+00:00", 12, None, "{}"))  # outside the window
    c.executemany("INSERT INTO scan_runs VALUES (?,?,?,?,?,?)", rows)
    c.commit()
    return c


def test_build_timing_groups_dealers_and_keeps_last_five(scan_runs_conn):
    blocks = ft.build_timing(scan_runs_conn, "2026-09-20T00:00:00+00:00")
    assert set(blocks) == {"avondaletoyota-com", "quick-com", "broken-com"}
    assert [r["run_id"] for r in blocks["quick-com"]["runs"]] == [107, 106, 105, 104, 103]
    assert blocks["quick-com"]["flags"] == [] and blocks["quick-com"]["vdp_http_first_max_sec"] is None
    assert blocks["broken-com"]["flags"] == ["error"]
    av = blocks["avondaletoyota-com"]
    assert av["vdp_http_first_max_sec"] is None and av["pages_needed"] == 1667 and av["updated_at"] == AVONDALE_RUN["finished_at"]


def test_build_timing_honours_dealer_filter_and_existing_hints(scan_runs_conn):
    older = st.timing_entry(_run(50, "2026-09-10T00:00:00+00:00", 700))
    existing = {"broken-com": {"notes": "x", "timing": st.merge_timing({}, older)}}
    blocks = ft.build_timing(scan_runs_conn, "2026-09-20T00:00:00+00:00", ["broken-com"], existing=lambda d: existing.get(d, {}))
    assert list(blocks) == ["broken-com"]
    assert [r["run_id"] for r in blocks["broken-com"]["runs"]] == [200, 50]
    assert blocks["broken-com"]["flags"] == ["error"]


class _KeepOpen:
    """main() closes the connection it opens; the fixture connection must survive."""

    def __init__(self, conn):
        self._conn = conn

    def execute(self, *a):
        return self._conn.execute(*a)

    def close(self):
        return None


def test_dry_run_prints_table_and_writes_nothing(scan_runs_conn, monkeypatch, capsys):
    writes: list = []
    monkeypatch.setattr(ft, "get_conn", lambda: _KeepOpen(scan_runs_conn))
    monkeypatch.setattr("backend.scanner.recipe_store.get_scan_hints", lambda d: {})
    monkeypatch.setattr("backend.scanner.recipe_store.set_scan_hints", lambda *a, **k: writes.append(a) or True)
    rc = ft.main(["--since", "2026-09-20T00:00:00+00:00", "--dry-run"])
    out = capsys.readouterr().out
    assert rc == 0 and writes == []
    assert "3 dealer(s) (dry run)" in out
    assert "avondaletoyota-com" in out and "1667" in out and "1140s" not in out
    assert "cap_hit host_exhausted slowed_host slow upsert_slow" in out
    assert "broken-com" in out and "error" in out


def test_write_mode_calls_set_scan_hints_per_dealer(scan_runs_conn, monkeypatch, capsys):
    writes: dict = {}
    monkeypatch.setattr(ft, "get_conn", lambda: _KeepOpen(scan_runs_conn))
    monkeypatch.setattr("backend.scanner.recipe_store.get_scan_hints", lambda d: {})
    monkeypatch.setattr("backend.scanner.recipe_store.set_scan_hints", lambda d, h, merge=True: writes.setdefault(d, h) or True)
    rc = ft.main(["--since", "2026-09-20T00:00:00+00:00", "--dealers", "avondaletoyota-com,broken-com"])
    assert rc == 0 and set(writes) == {"avondaletoyota-com", "broken-com"}
    assert writes["avondaletoyota-com"]["timing"]["vdp_http_first_max_sec"] is None
    assert "wrote timing for 2/2" in capsys.readouterr().out


# ---------------------------------------------------------------- pipeline writer

def test_pipeline_record_timing_writes_fingerprint_and_never_raises(monkeypatch):
    from backend.scripts import dealer_pipeline as dp

    store: dict = {"avondaletoyota-com": {"needs_http_proxy": True}}
    monkeypatch.setattr("backend.scanner.recipe_store.get_scan_hints", lambda d: store.get(d, {}))

    def _set(d, hints, merge=True):
        store.setdefault(d, {}).update(hints)
        return True

    monkeypatch.setattr("backend.scanner.recipe_store.set_scan_hints", _set)
    view = dp.record_timing("avondaletoyota-com", AVONDALE_RUN)
    assert view["minutes"] == 15.3 and view["flags"] == ["cap_hit", "host_exhausted", "slowed_host", "slow", "upsert_slow"]
    assert view["vdp_http_first_max_sec"] is None and view["pages_needed"] == 1667 and view["stored"] is True
    assert store["avondaletoyota-com"]["needs_http_proxy"] is True  # merge kept the other keys
    assert store["avondaletoyota-com"]["timing"]["runs"][0]["run_id"] == 9101
    assert dp._timing_text({"timing": view}) == "15.3 min cap_hit host_exhausted slowed_host slow upsert_slow"

    def _boom(d):
        raise RuntimeError("database unreachable")

    monkeypatch.setattr("backend.scanner.recipe_store.get_scan_hints", _boom)
    view2 = dp.record_timing("avondaletoyota-com", AVONDALE_RUN)
    assert view2["minutes"] == 15.3 and view2["flags"][0] == "cap_hit" and "database unreachable" in view2["error"]


def test_pipeline_slow_dealers_file_and_errors_index(monkeypatch, tmp_path):
    from datetime import datetime, timezone

    from backend.scripts import dealer_pipeline as dp
    from backend.tests.pipeline_patch import patch_pipeline

    patch_pipeline(monkeypatch, "LOG_ROOT", tmp_path / "dealer_logs")
    results = [
        {"dealer_id": "avondaletoyota-com", "minutes": 15.3,
         "timing": {"minutes": 15.3, "flags": ["cap_hit", "host_exhausted", "slowed_host", "slow", "upsert_slow"],
                    "vdp_http_first_max_sec": 1140, "pages_needed": 1667}},
        {"dealer_id": "quick-com", "minutes": 1.2, "timing": {"minutes": 1.2, "flags": []}},
        {"dealer_id": "slowish-com", "minutes": 11.0, "timing": {"minutes": 11.0, "flags": ["slow"]}},  # slow alone is not an alarm
        {"dealer_id": "broken-com", "minutes": 0.2, "timing": {"minutes": 0.2, "flags": ["error"]}},
        {"dealer_id": "norun-com"},  # no scan_runs row: no timing at all
    ]
    out_dir = tmp_path / "run"
    out_dir.mkdir()
    started = datetime(2026, 9, 28, 9, 0, tzinfo=timezone.utc)
    assert dp.write_slow_dealers(results, out_dir, started) == ["avondaletoyota-com", "broken-com"]
    slow = (out_dir / "slow_dealers.txt").read_text().splitlines()
    assert slow == ["avondaletoyota-com  cap_hit+host_exhausted+slowed_host+slow+upsert_slow  15.3", "broken-com  error  0.2"]
    idx = (tmp_path / "dealer_logs" / "_learning" / "errors_index.md").read_text().splitlines()
    assert idx[0] == "# errors_index"
    assert idx[-2] == ("- 2026-09-28T09:00:00+00:00 scan_timing_cap_hit+host_exhausted+slowed_host+slow+upsert_slow -> avondaletoyota-com "
                       "(15.3 min; window 1140s/1667 pages; workspace/dealer_logs/avondaletoyota-com/scan_runs.md)")
    assert idx[-1].startswith("- 2026-09-28T09:00:00+00:00 scan_timing_error -> broken-com (0.2 min;")
    # a second run appends without a second header
    dp.write_slow_dealers(results, out_dir, started)
    text = (tmp_path / "dealer_logs" / "_learning" / "errors_index.md").read_text()
    assert text.count("# errors_index") == 1 and text.count("scan_timing_error -> broken-com") == 2


def test_host_exhausted_pass_gets_no_wider_window():
    """A pass stopped by the dealer's host (serialized, still refusing) was not short
    of time; 110 of 160 backfill recommendations on 2026-09-28 came from this."""
    rec = st.recommend_windows([st.timing_entry(AVONDALE_RUN)])
    assert rec["vdp_http_first_max_sec"] is None and rec.get("throttled") is True
    assert rec["pages_needed"] == 1667
