"""P1A.1: interim per-condition-bucket retirement guards in all three writers.

The lot-wide share hid one-sided runs. On 2026-09-28 parksidekia-com returned
306 new / 0 used: the scanner kept the used cars (zero-only guard) and the
pipeline's reconcile then retired them anyway; 12-13 one-condition batches
(1,399-1,480 rows) were never restored. A run that returns a handful of one
condition passed both guards too. Every writer now retires a bucket only on
evidence for that bucket:

* pipeline ``reconcile_dealer``: run rows of the bucket > 0 and >= 60% of the
  bucket's pre-run baseline;
* scanner ``reconcile_dealer_inventory_after_scan`` (full scan and delta): the
  run re-saw >= MIN_COVERAGE of the bucket's active rows;
* blank-condition rows retire only when both new and used qualify.
"""
from __future__ import annotations

import ast
import asyncio
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from backend.scanner import inventory_reconcile as ir
from backend.scanner.pipeline import reconcile as rc
from backend.scripts import dealer_pipeline as dp
from backend.tests.pipeline_patch import patch_pipeline

REPO = Path(__file__).resolve().parents[2]

SINCE = "2026-09-26T00:00:00+00:00"           # this run started
BASELINE_SINCE = "2026-08-27T00:00:00+00:00"  # SINCE - 30 days
OLD = "2026-09-20T00:00:00+00:00"             # seen 6 days before the run: inside the baseline
JULY = "2026-07-10T00:00:00+00:00"            # ghosts nobody has seen in 30 days
RUN = "2026-09-26T01:00:00+00:00"             # stamped by this run

COND = {"new": "New", "used": "Used", "blank": ""}


def _vin(i: int) -> str:
    return f"5NPE34AF{i:09d}"


# ── pipeline reconcile_dealer ────────────────────────────────────────────────

def _lot(**spec: int | tuple[int, str]) -> sqlite3.Connection:
    """cars for dealer ``d``: ``_lot(new=300, used=200, blank=10)``; a value may be
    ``(n, scraped_at)`` (default OLD)."""
    c = sqlite3.connect(":memory:")
    c.execute("CREATE TABLE cars (id INTEGER PRIMARY KEY, dealer_id TEXT, vin TEXT, condition TEXT, scraped_at TEXT, "
              "listing_active INTEGER DEFAULT 1, listing_removed_at TEXT)")
    i = 0
    for key, val in spec.items():
        n, ts = val if isinstance(val, tuple) else (val, OLD)
        cond = COND[key.split("_")[0]]
        for _ in range(n):
            c.execute("INSERT INTO cars (dealer_id, vin, condition, scraped_at) VALUES ('d', ?, ?, ?)", (_vin(i), cond, ts))
            i += 1
    c.execute("INSERT INTO cars (dealer_id, vin, condition, scraped_at) VALUES ('other', 'OTHERVIN000000001', 'Used', ?)", (JULY,))
    c.commit()
    return c


def _see(c: sqlite3.Connection, key: str, n: int) -> None:
    """This run re-saw *n* active rows of the bucket (lowest ids first)."""
    c.execute("UPDATE cars SET scraped_at = ? WHERE id IN (SELECT id FROM cars WHERE dealer_id = 'd' AND condition = ? "
              "AND COALESCE(listing_active,1)=1 ORDER BY id LIMIT ?)", (RUN, COND[key], n))
    c.commit()


def _active(c: sqlite3.Connection, key: str | None = None) -> int:
    if key is None:
        return c.execute("SELECT COUNT(*) FROM cars WHERE dealer_id='d' AND listing_active=1").fetchone()[0]
    return c.execute("SELECT COUNT(*) FROM cars WHERE dealer_id='d' AND listing_active=1 AND condition=?", (COND[key],)).fetchone()[0]


def _pipeline_run(c: sqlite3.Connection, seen: dict[str, int], *, verdict: str = "inaccurate", dry_run: bool = False,
                  buckets: bool = True) -> dict:
    """The run.py sequence: per-bucket baseline before the scan, the scan, then
    reconcile with the run's own bucket counts."""
    base = rc.bucket_counts(c, "d", since_iso=BASELINE_SINCE)
    known = sum(base.values())
    for key, n in seen.items():
        _see(c, key, n)
    rows = rc.bucket_counts(c, "d", since_iso=SINCE)
    kw = {"baseline_buckets": base, "rows_buckets": rows} if buckets else {}
    return dp.reconcile_dealer(c, "d", SINCE, known, sum(rows.values()), verdict, dry_run=dry_run, **kw)


def test_bucket_counts_fold_blank_into_unknown():
    c = _lot(new=3, used=2, blank=1)
    c.execute("UPDATE cars SET condition = 'Certified Pre-Owned' WHERE id = 4")
    c.execute("UPDATE cars SET condition = NULL WHERE id = 5")
    c.commit()
    assert rc.bucket_counts(c, "d") == {"new": 3, "used": 1, "unknown": 2}
    assert rc.bucket_counts(c, "d", since_iso=SINCE) == {"new": 0, "used": 0, "unknown": 0}
    assert rc.bucket_counts(c, "other", before_iso=SINCE) == {"new": 0, "used": 1, "unknown": 0}


def test_new_only_rerun_retires_no_used_rows():
    """300 new + 200 used; the run re-sees the 300 new. 300 >= 60% of 500, so the
    lot-wide guard passes and the old code retired all 200 used cars."""
    legacy = _lot(new=300, used=200)
    out = _pipeline_run(legacy, {"new": 300}, buckets=False)
    assert out["eligible"] and out["retired"] == 200, "the hole this unit closes"

    c = _lot(new=300, used=200)
    out = _pipeline_run(c, {"new": 300})
    assert out["eligible"] and out["stale"] == 200
    assert out["retired"] == 0 and out["retirable"] == 0
    assert out["kept_missing_condition"] == 200 and out["kept_low_bucket"] == 0
    assert out["buckets"]["used"] == {"baseline": 200, "rows": 0, "stale": 200, "kept": "missing_condition"}
    assert _active(c, "used") == 200 and _active(c, "new") == 300


def test_handful_of_used_keeps_the_used_rows():
    c = _lot(new=300, used=200)
    out = _pipeline_run(c, {"new": 300, "used": 5})
    assert out["eligible"] and out["stale"] == 195
    assert out["retired"] == 0 and out["kept_low_bucket"] == 195 and out["kept_missing_condition"] == 0
    assert out["buckets"]["used"]["kept"] == "low_bucket"
    assert _active(c, "used") == 200


def test_both_buckets_covered_still_retires_sold_cars():
    c = _lot(new=300, used=200)
    out = _pipeline_run(c, {"new": 300, "used": 180})
    assert out["retired"] == 20 and out["kept_missing_condition"] == 0 and out["kept_low_bucket"] == 0
    assert _active(c, "used") == 180 and _active(c, "new") == 300
    removed = c.execute("SELECT COUNT(*) FROM cars WHERE listing_active=0 AND listing_removed_at IS NOT NULL").fetchone()[0]
    assert removed == 20
    assert c.execute("SELECT listing_active FROM cars WHERE dealer_id='other'").fetchone()[0] == 1


@pytest.mark.parametrize("seen, retired, kept_missing, kept_low, blank_left", [
    ({"new": 320, "used": 190}, 20, 0, 0, 0),     # both qualify: 10 used + 10 blank retire
    ({"new": 320, "used": 10}, 0, 0, 200, 10),    # used too thin: 190 used + 10 blank kept
    ({"new": 320}, 0, 210, 0, 10),                # no used at all: 200 used + 10 blank kept
])
def test_blank_condition_rows_retire_only_when_both_buckets_qualify(seen, retired, kept_missing, kept_low, blank_left):
    # 320 + 200 + 10: every run below clears the lot-wide 60% (318), so only the bucket rule decides
    c = _lot(new=320, used=200, blank=10)
    out = _pipeline_run(c, seen)
    assert out["eligible"]
    assert (out["retired"], out["kept_missing_condition"], out["kept_low_bucket"]) == (retired, kept_missing, kept_low)
    assert _active(c, "blank") == blank_left


def test_new_bucket_missing_keeps_blank_rows():
    c = _lot(new=100, used=300, blank=10)
    out = _pipeline_run(c, {"used": 300})
    assert out["eligible"] and out["retired"] == 0
    assert out["kept_missing_condition"] == 110 and out["buckets"]["unknown"]["kept"] == "missing_condition"


def test_empty_bucket_baseline_falls_back_like_the_lot_baseline():
    """No used car seen in 30 days (July ghosts): a share of a zero baseline passes
    anything, so the bucket falls back to its 90-day window (Gunn Honda rule)."""
    c = _lot(new=300, used=(200, JULY))
    out = _pipeline_run(c, {"new": 300, "used": 5})
    assert out["eligible"] and out["retired"] == 0
    assert out["bucket_baseline_fallback"] == {"used": 195} and out["kept_low_bucket"] == 195
    # a full used run against the same ghosts retires the ones it did not return
    c = _lot(new=300, used=(200, JULY))
    out = _pipeline_run(c, {"new": 300, "used": 150})
    assert out["bucket_baseline_fallback"] == {"used": 50} and out["retired"] == 50


def test_dry_run_reports_what_would_retire():
    c = _lot(new=300, used=200)
    out = _pipeline_run(c, {"new": 300, "used": 180}, dry_run=True)
    assert out["retirable"] == 20 and out["retired"] == 0 and _active(c) == 500


def test_existing_guards_still_refuse_before_buckets():
    c = _lot(new=300, used=200)
    out = _pipeline_run(c, {"new": 300}, verdict="no_rows")
    assert not out["eligible"] and "buckets" not in out and _active(c) == 500
    c = _lot(new=300, used=200)
    out = _pipeline_run(c, {"new": 100, "used": 100})  # 200 < 60% of 500
    assert not out["eligible"] and "60%" in out["reason"] and _active(c) == 500


class _RacingConn:
    """sqlite3 connection whose stale-id SELECT is followed by another process
    re-stamping one of those rows before the UPDATE."""

    def __init__(self, c: sqlite3.Connection, refresh_id: int):
        self._c, self._rid = c, refresh_id

    def execute(self, sql, params=()):
        cur = self._c.execute(sql, params)
        if sql.startswith("SELECT id, condition"):
            rows, desc = cur.fetchall(), cur.description
            self._c.execute("UPDATE cars SET scraped_at = ? WHERE id = ?", (RUN, self._rid))
            return type("Cur", (), {"description": desc, "fetchall": lambda _self: rows})()
        return cur

    def commit(self):
        self._c.commit()


def test_row_refreshed_between_select_and_update_is_not_retired():
    c = _lot(new=300, used=200)
    base = rc.bucket_counts(c, "d", since_iso=BASELINE_SINCE)
    _see(c, "new", 300)
    _see(c, "used", 180)
    rows = rc.bucket_counts(c, "d", since_iso=SINCE)
    racer_id = c.execute("SELECT id FROM cars WHERE dealer_id='d' AND scraped_at < ? ORDER BY id LIMIT 1", (SINCE,)).fetchone()[0]
    out = dp.reconcile_dealer(_RacingConn(c, racer_id), "d", SINCE, 500, 480, "ok", baseline_buckets=base, rows_buckets=rows)
    assert out["retirable"] == 20 and out["retired"] == 19
    assert c.execute("SELECT listing_active FROM cars WHERE id = ?", (racer_id,)).fetchone()[0] == 1


# ── scanner reconcile_dealer_inventory_after_scan ────────────────────────────

def _scanner_lot(new: int, used: int, blank: int = 0) -> sqlite3.Connection:
    c = sqlite3.connect(":memory:")
    c.execute("CREATE TABLE cars (id INTEGER PRIMARY KEY, dealer_id TEXT, dealer_url TEXT, vin TEXT, condition TEXT, "
              "listing_active INTEGER, listing_removed_at TEXT)")
    i = 0
    for cond, n in (("New", new), ("Used", used), ("", blank)):
        for _ in range(n):
            c.execute("INSERT INTO cars (dealer_id, dealer_url, vin, condition, listing_active) VALUES ('d','https://d.example',?,?,1)",
                      (_vin(i), cond))
            i += 1
    c.commit()
    return c


def _scanner_run(c, seen_vins: set[str], conditions: set[str] | None) -> dict:
    return ir.reconcile_dealer_inventory_after_scan("d", "https://d.example", seen_vins, {"deduped_rows": len(seen_vins)},
                                                    scraped_conditions=conditions, _conn=c)


@pytest.fixture()
def _no_ddl(monkeypatch):
    monkeypatch.setattr(ir, "ensure_cars_table_columns", lambda cur: None)
    monkeypatch.delenv("SCANNER_RECONCILE_MIN_COVERAGE", raising=False)
    monkeypatch.setenv("SCANNER_RECONCILE", "1")


def test_scanner_keeps_used_when_run_returns_a_handful(_no_ddl):
    """305 of 500 re-seen clears the 50% lot-wide gate; 5 of 200 used does not."""
    c = _scanner_lot(new=300, used=200)
    out = _scanner_run(c, {_vin(i) for i in range(305)}, {"new", "used"})
    assert out["ran"] is True and out["skipped_reason"] == "ok"
    assert out["marked_inactive"] == 0 and out["kept_low_bucket"] == 195
    assert "kept_missing_condition" not in out
    assert c.execute("SELECT COUNT(*) FROM cars WHERE listing_active = 1").fetchone()[0] == 500


@pytest.mark.parametrize("seen, conditions, marked, kept_missing, kept_low", [
    (490, {"new", "used"}, 30, None, None),        # used 190/200: 10 used + 20 blank retire
    (305, {"new", "used"}, 0, None, 215),          # used 5/200: 195 used + 20 blank kept
    (300, {"new"}, 0, 220, None),                  # no used rows: 200 used + 20 blank kept
])
def test_scanner_blank_rows_retire_only_when_both_buckets_qualify(_no_ddl, seen, conditions, marked, kept_missing, kept_low):
    c = _scanner_lot(new=300, used=200, blank=20)
    out = _scanner_run(c, {_vin(i) for i in range(seen)}, conditions)
    assert out["ran"] is True
    assert out["marked_inactive"] == marked
    assert out.get("kept_missing_condition") == kept_missing and out.get("kept_low_bucket") == kept_low


def test_scanner_run_without_any_condition_retires_nothing(_no_ddl):
    """A run whose rows carry no condition at all is evidence for neither bucket
    (the old truthiness check let an empty set retire everything)."""
    c = _scanner_lot(new=30, used=0)
    out = _scanner_run(c, {_vin(i) for i in range(25)}, set())
    assert out["marked_inactive"] == 0 and out["kept_missing_condition"] == 5


def test_delta_scan_passes_the_run_buckets(monkeypatch):
    import backend.scanner.delta_scan as ds

    rows = [{"vin": f"1HGBH41JXMN1{i:05d}", "price": 20000 + i, "condition": "New" if i < 90 else ""} for i in range(95)]

    async def _fake_fetch(dealer_id, provider, base_url, dealer_name, **kw):
        return ([("https://d.example/api", {"x": 1})], len(rows))

    class _FakeCoordinator:
        async def upsert_vehicles(self, vehicles, stats=None):
            return len(vehicles)

    calls: list[dict] = []
    monkeypatch.setattr("backend.scanner.recipes.try_fetch_via_recipes", _fake_fetch)
    monkeypatch.setattr("backend.parsers.parse", lambda *a, **k: [dict(r) for r in rows])
    monkeypatch.setattr(ds, "_active_count", lambda dealer_id: 100)
    monkeypatch.setattr("backend.scanner.inventory_write.InventoryWriteCoordinator", _FakeCoordinator)
    monkeypatch.setattr("backend.scanner.inventory_reconcile.reconcile_dealer_inventory_after_scan",
                        lambda *a, **k: calls.append(k) or {"ran": True})
    monkeypatch.setenv("SCANNER_VDP_DB_MERGE", "0")
    monkeypatch.setenv("SCANNER_POST_LISTING_GAP_FILL", "0")
    asyncio.run(ds.delta_scan_dealer({"dealer_id": "d-com", "url": "https://d.example", "name": "D", "provider": "dealer_dot_com"}))
    assert calls == [{"scraped_conditions": {"new"}}]


# ── wiring: every writer gets its bucket evidence ────────────────────────────

def _calls(path: Path):
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.Call):
            yield node


def _name(node) -> str:
    return node.id if isinstance(node, ast.Name) else (node.attr if isinstance(node, ast.Attribute) else "")


def test_every_reconcile_call_site_passes_bucket_evidence():
    sources = [p for p in (REPO / "backend").rglob("*.py")
               if "tests" not in p.parts and "reconcile" in p.read_text(encoding="utf-8", errors="ignore")]
    pipeline_sites, scanner_sites = [], []
    for p in sources:
        for call in _calls(p):
            kws = {k.arg for k in call.keywords}
            if _name(call.func) == "reconcile_dealer":
                pipeline_sites.append((p.relative_to(REPO).as_posix(), kws))
            target = call.args[0] if _name(call.func) == "to_thread" and call.args else call.func
            if _name(target) in ("reconcile_dealer_inventory_after_scan", "reconcile_fn"):
                scanner_sites.append((p.relative_to(REPO).as_posix(), kws))
    assert sorted(f for f, _ in pipeline_sites) == ["backend/scanner/pipeline/lifecycle.py", "backend/scanner/pipeline/run.py"]
    for f, kws in pipeline_sites:
        assert {"baseline_buckets", "rows_buckets"} <= kws, f
    assert sorted(f for f, _ in scanner_sites) == ["backend/scanner/delta_scan.py", "backend/scanner/phases/dealer_run_steps/after_write.py"]
    for f, kws in scanner_sites:
        assert "scraped_conditions" in kws, f


# ── run.py and the lifecycle retry, end to end on SQLite ─────────────────────

D = "bucket-dealer-com"


@pytest.fixture()
def log_root(tmp_path, monkeypatch):
    root = tmp_path / "dealer_logs"
    patch_pipeline(monkeypatch, "LOG_ROOT", root)
    monkeypatch.setenv("DEALER_LOGS_ROOT", str(root))
    return root


def _seed_lot(db: Path, n_new: int, n_used: int) -> None:
    seen = (datetime.now(timezone.utc) - timedelta(days=5)).replace(microsecond=0).isoformat()
    c = sqlite3.connect(str(db))
    try:
        c.executemany("INSERT INTO cars (dealer_id, vin, scraped_at, listing_active, condition, dealer_name, zip_code) VALUES (?,?,?,?,?,?,?)",
                      [(D, _vin(i), seen, 1, "New" if i < n_new else "Used", "Bucket Store", "78701") for i in range(n_new + n_used)])
        c.commit()
    finally:
        c.close()


def _fake_scan_new_only(db: Path, n_new: int):
    """A replay that answers the new-car section only: re-stamps the new rows and
    writes the scan_runs row assess reads."""
    def _scan(dealer_ids, **kw):
        now = (datetime.now(timezone.utc) + timedelta(seconds=1)).replace(microsecond=0).isoformat()
        c = sqlite3.connect(str(db))
        try:
            c.execute("UPDATE cars SET scraped_at = ? WHERE dealer_id = ? AND condition = 'New'", (now, D))
            summary = {"capture_coverage": {"n": n_new, "price": 1.0, "trim": 1.0, "exterior_color": 1.0}, "recipe_fetch": "hit"}
            c.execute("INSERT INTO scan_runs (dealer_id, finished_at, duration_seconds, upserted, inventory_rows, error, provider, summary_json) "
                      "VALUES (?,?,?,?,?,?,?,?)", (D, now, 30, n_new, n_new, None, "carscommerce", json.dumps(summary)))
            c.commit()
        finally:
            c.close()
        return 0 if "concurrency" in kw else {"rc": [0], "chromium_leaks": []}
    return _scan


def _common_stubs(monkeypatch):
    patch_pipeline(monkeypatch, "wait_for_db", lambda *a, **k: True)
    patch_pipeline(monkeypatch, "wait_for_scanner_lock", lambda *a, **k: True)
    patch_pipeline(monkeypatch, "chromium_process_count", lambda: 0)
    patch_pipeline(monkeypatch, "record_timing", lambda did, run: {"minutes": 0.5, "flags": []})
    patch_pipeline(monkeypatch, "verify_accuracy", lambda conn, did, since: {"rows": 300, "incomplete_rows": 0, "missing": {}, "hard": {}, "hard_rows": 0})
    monkeypatch.setenv("SCANNER_RECONCILE", "1")


def _used_active(db: Path) -> int:
    c = sqlite3.connect(str(db))
    try:
        return c.execute("SELECT COUNT(*) FROM cars WHERE dealer_id = ? AND condition = 'Used' AND listing_active = 1", (D,)).fetchone()[0]
    finally:
        c.close()


def test_pipeline_main_pass_retires_no_used_rows(monkeypatch, tmp_path, log_root, sqlite_inventory):
    _seed_lot(sqlite_inventory.path, 300, 200)
    _common_stubs(monkeypatch)
    patch_pipeline(monkeypatch, "ensure_recipe", lambda dealer, force=False: {"had_recipes": 1, "synth": None})
    patch_pipeline(monkeypatch, "run_http_only_scan", _fake_scan_new_only(sqlite_inventory.path, 300))
    manifest = tmp_path / "dealers.json"
    manifest.write_text(json.dumps({"dealers": [{"dealer_id": D, "url": f"https://www.{D}.com", "name": "Bucket Store"}]}), encoding="utf-8")
    out = tmp_path / "out"
    monkeypatch.setattr("sys.argv", ["dealer_pipeline", "--dealers", D, "--manifest", str(manifest), "--out", str(out),
                                     "--no-vpic", "--no-lifecycle"])
    assert dp.main() == 0
    (r,) = json.loads((out / "triage.json").read_text(encoding="utf-8"))["dealers"]
    assert r["verdict"] == "inaccurate" and r["known_before"] == 500
    rec = r["reconcile"]
    assert rec["eligible"] and rec["retired"] == 0 and rec["kept_missing_condition"] == 200
    assert rec["buckets"]["new"]["baseline"] == 300 and rec["buckets"]["new"]["rows"] == 300
    assert rec["buckets"]["used"] == {"baseline": 200, "rows": 0, "stale": 200, "kept": "missing_condition"}
    assert _used_active(sqlite_inventory.path) == 200


def test_lifecycle_retry_uses_its_own_bucket_counts(monkeypatch, tmp_path, log_root, sqlite_inventory):
    _seed_lot(sqlite_inventory.path, 300, 200)
    _common_stubs(monkeypatch)
    patch_pipeline(monkeypatch, "_scan_hints", lambda did: {})
    patch_pipeline(monkeypatch, "run_lifecycle", lambda d, r, **k: {"lifecycle": "resynth_ok", "steps": [], "recipe": {"had_recipes": 1, "synth": "saved_1"}})
    patch_pipeline(monkeypatch, "scan_retry_batch", _fake_scan_new_only(sqlite_inventory.path, 300))
    results = [{"dealer_id": D, "verdict": "no_rows", "reason": "recipe replay yielded 0 rows", "recipe": {"had_recipes": 1}, "rows": 0}]
    dealers = {D: {"dealer_id": D, "url": f"https://www.{D}.com", "name": "Bucket Store"}}
    summary = dp.run_lifecycle_pass(results, dealers, {D: 500}, out_dir=tmp_path / "out", stamp="2026-10-07 12:00 UTC",
                                    no_vpic=True, known_buckets={D: {"new": 300, "used": 200, "unknown": 0}})
    assert summary["retried"] == {D: "retried: no_rows → inaccurate"}
    rec = results[0]["reconcile"]
    assert rec["eligible"] and rec["retired"] == 0 and rec["kept_missing_condition"] == 200
    assert rec["buckets"]["new"] == {"baseline": 300, "rows": 300, "stale": 0, "kept": None}
    assert _used_active(sqlite_inventory.path) == 200
