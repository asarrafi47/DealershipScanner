"""N9.13: the batch timeout counts active time, not time the machine slept."""
from __future__ import annotations

import subprocess

import pytest

from backend.scanner.pipeline import runner
from backend.scanner.pipeline.active_time import ActiveTimer


class _Clock:
    def __init__(self) -> None:
        self.t = 1000.0

    def __call__(self) -> float:
        return self.t


def test_active_timer_does_not_charge_a_pause():
    clk = _Clock()
    timer = ActiveTimer(max_step=30, mono=clk, wall=clk)
    clk.t += 10
    assert timer.tick() == 10
    clk.t += 78_000  # Mac asleep (or SIGSTOP): one huge gap between ticks
    assert timer.tick() == 40  # only max_step of it is charged
    assert round(timer.paused) == 78_000 - 30
    clk.t += 5
    assert timer.tick() == 45
    assert timer.stats() == {"wall_seconds": 78_015.0, "active_seconds": 45.0}


def _fake_popen(clk, script):
    """Popen whose wait() advances the fake clock by the next scripted step, then times out
    (or returns rc when the script is exhausted)."""
    class _P:
        killed = False

        def __init__(self, *a, **k):
            self.steps = list(script)

        def wait(self, timeout=None):
            if _P.killed:
                return -9
            if not self.steps:
                return 0
            clk.t += self.steps.pop(0)
            if not self.steps:
                return 0
            raise subprocess.TimeoutExpired("scanner", timeout)

        def kill(self):
            _P.killed = True
    return _P


def _run(monkeypatch, tmp_path, script, timeout_sec):
    clk = _Clock()
    monkeypatch.setattr(runner, "ActiveTimer", lambda: ActiveTimer(mono=clk, wall=clk))
    monkeypatch.setattr(subprocess, "Popen", _fake_popen(clk, script))
    stats: dict = {}
    rc = runner.run_http_only_scan(["a-com"], concurrency=1, log_path=tmp_path / "s.log", timeout_sec=timeout_sec, stats=stats)
    return rc, stats


def test_simulated_pause_longer_than_timeout_is_not_charged(monkeypatch, tmp_path):
    # 1 s polls, then a 21.7 h sleep, then more polls; timeout is 600 s of active time
    rc, stats = _run(monkeypatch, tmp_path, [1, 1, 78_000, 1, 1, 1], timeout_sec=600)
    assert rc == 0
    assert stats["wall_seconds"] > 78_000 and stats["active_seconds"] < 100


def test_real_overrun_still_kills(monkeypatch, tmp_path):
    rc, stats = _run(monkeypatch, tmp_path, [20] * 40, timeout_sec=600)
    assert rc == -9
    assert 600 <= stats["active_seconds"] <= 640


# ---- N9.13: killed dealers are re-queued once, no scan_runs row is retried, upserts get a run row ----
import sqlite3  # noqa: E402

from backend.scanner.pipeline import assess as assess_mod  # noqa: E402
from backend.scanner.pipeline import lifecycle  # noqa: E402

SINCE = "2026-10-01T00:00:00+00:00"


def _db(upserted_dealers=()):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE cars (id INTEGER PRIMARY KEY, dealer_id TEXT, vin TEXT, condition TEXT, dealer_name TEXT, zip_code TEXT, scraped_at TEXT, listing_active INTEGER, listing_removed_at TEXT)")
    conn.execute("CREATE TABLE scan_runs (id INTEGER PRIMARY KEY, dealer_id TEXT, provider TEXT, finished_at TEXT, duration_seconds REAL, inventory_rows INTEGER, upserted INTEGER, error TEXT, summary_json TEXT)")
    for did in upserted_dealers:
        for i in range(3):
            conn.execute("INSERT INTO cars (dealer_id, vin, condition, scraped_at, listing_active) VALUES (?,?,?,?,1)",
                         (did, f"{did[:3].upper()}{i:014d}", "used", "2026-10-01T01:00:00+00:00"))
    conn.commit()
    return conn


def test_assess_backfills_scan_run_for_upserted_dealer():
    conn = _db(["righthonda-com"])
    out = assess_mod.assess(conn, "righthonda-com", SINCE, known_before=3, recipe_info={"had_recipes": 1})
    assert out["scan_run_backfilled"] == 3
    assert out.get("reason") != "no scan_runs row"
    row = conn.execute("SELECT provider FROM scan_runs WHERE dealer_id='righthonda-com'").fetchone()
    assert row["provider"] == "pipeline_backfill"


def test_assess_without_rows_stays_error_and_writes_nothing():
    conn = _db([])
    out = assess_mod.assess(conn, "ghost-com", SINCE, known_before=0, recipe_info={"had_recipes": 1})
    assert (out["verdict"], out["reason"]) == ("error", "no scan_runs row")
    assert conn.execute("SELECT COUNT(*) FROM scan_runs").fetchone()[0] == 0


def test_route_verdict_retries_no_scan_runs_row():
    d = {"dealer_id": "x-com", "url": "https://x.example"}
    r = {"dealer_id": "x-com", "verdict": "error", "reason": "no scan_runs row"}
    assert lifecycle.route_verdict(d, r, recipe_status="")["action"] == "retry"
    r2 = {"dealer_id": "x-com", "verdict": "error", "reason": "boom"}
    assert lifecycle.route_verdict(d, r2, recipe_status="")["action"] == "none"


def test_lifecycle_pass_retry_skips_resynth(monkeypatch, tmp_path):
    scanned = []
    monkeypatch.setattr(lifecycle, "scan_retry_batch", lambda ids, **k: scanned.append(list(ids)) or {"rc": [0], "chromium_leaks": []})
    monkeypatch.setattr(lifecycle, "run_lifecycle", lambda *a, **k: pytest.fail("no re-synth for a missing run row"))
    retry_db = _db(["x-com"])
    retry_db.execute("INSERT INTO scan_runs (dealer_id, finished_at, upserted, inventory_rows) VALUES ('x-com', '2999-01-01T00:00:00+00:00', 3, 3)")
    monkeypatch.setattr(lifecycle, "_assess_conn", lambda: retry_db)
    results = [{"dealer_id": "x-com", "verdict": "error", "reason": "no scan_runs row", "recipe": {"had_recipes": 1}}]
    summary = lifecycle.run_lifecycle_pass(results, {"x-com": {"dealer_id": "x-com", "url": "https://x.example"}}, {"x-com": 3},
                                           out_dir=tmp_path, stamp="t", no_vpic=True, no_reconcile=True)
    assert scanned == [["x-com"]] and summary["retry_no_scan_run"] == 1
    assert results[0]["verdict"] != "error" or results[0]["reason"] != "no scan_runs row"


def test_requeue_killed_reruns_only_dealers_without_a_run_row(monkeypatch, tmp_path):
    conn = _db([])
    conn.execute("INSERT INTO scan_runs (dealer_id, finished_at) VALUES ('done-com', '2026-10-01T02:00:00+00:00')")
    conn.commit()
    monkeypatch.setattr("backend.scanner.pipeline.db._assess_conn", lambda: conn)
    seen = []
    monkeypatch.setattr(runner, "scan_retry_batch", lambda ids, **k: seen.append(list(ids)) or {"rc": [0], "chromium_leaks": []})

    class _Keep:  # the helper closes its connection; keep the shared in-memory one open
        def __init__(self, c): self.c = c
        def execute(self, *a): return self.c.execute(*a)
        def close(self): pass

    monkeypatch.setattr("backend.scanner.pipeline.db._assess_conn", lambda: _Keep(conn))
    out = runner.requeue_killed(["done-com", "lost-com", "lost2-com"], since_iso=SINCE, out_dir=tmp_path, batch=4, scan_timeout=60, lock_wait=1)
    assert out["requeued"] == ["lost-com", "lost2-com"] and out["reason"] == "killed_by_timeout"
    assert seen == [["lost-com", "lost2-com"]]
