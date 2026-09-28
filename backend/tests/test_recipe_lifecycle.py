"""The stale-recipe lifecycle end to end (docs/HTTP_ONLY_SCANS_PLAN.md Phase 3).

recipes.try_fetch_via_recipes: a 401/403 before any VIN marks the recipe stale
AND writes scan_hints.recipe_status = "stale:<status>:<iso>"; a stale recipe is
still tried once (keys rotate back) and a replay that answers again un-stales it
and clears the status. dealer_pipeline: route_verdict decides who enters the
lifecycle, run_lifecycle does force re-synth -> discovery capture (stubbed; no
browser, no HTTP here), run_lifecycle_pass rescans only the dealers whose recipe
validated, once per dealer per UTC day, and the triage grows a lifecycle column.
Every replay / subprocess / scan is a stub; logs go to a tmp DEALER_LOGS_ROOT.
"""
from __future__ import annotations

import asyncio
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from backend.scanner.network_observer import CapturedEndpoint
from backend.scanner.recipes import load_recipes, mark_stale, promote_from_ledger
from backend.scripts import dealer_pipeline as dp

D = "lifecycle-dealer-com"
URL = "https://www.lifecycle-dealer.com"
DEALER = {"dealer_id": D, "url": URL, "name": "Lifecycle Dealer"}


# ── fixtures: tmp recipe dir, in-memory scan hints, tmp log root ───────────────

@pytest.fixture(autouse=True)
def _tmp_recipes_dir(tmp_path, monkeypatch):
    import backend.scanner.recipes as rec

    monkeypatch.setattr(rec, "RECIPES_DIR", tmp_path / "recipes")


@pytest.fixture()
def hints(monkeypatch):
    """scan_hints without a database: the module functions recipes / dealer_pipeline
    import lazily are replaced by a dict-backed pair."""
    from backend.scanner import recipe_store

    store: dict[str, dict] = {}

    def _get(dealer_id):
        return dict(store.get(dealer_id) or {})

    def _set(dealer_id, hints, *, merge=True):
        base = dict(store.get(dealer_id) or {}) if merge else {}
        for k, v in hints.items():
            if v is None:
                base.pop(k, None)
            else:
                base[k] = v
        store[dealer_id] = base
        return True

    monkeypatch.setattr(recipe_store, "get_scan_hints", _get)
    monkeypatch.setattr(recipe_store, "set_scan_hints", _set)
    monkeypatch.setattr(recipe_store, "db_load_recipes", lambda *a, **k: [])
    monkeypatch.setattr(recipe_store, "db_save_recipes", lambda *a, **k: None)
    return store


@pytest.fixture()
def log_root(tmp_path, monkeypatch):
    root = tmp_path / "dealer_logs"
    monkeypatch.setattr(dp, "LOG_ROOT", root)
    monkeypatch.setenv("DEALER_LOGS_ROOT", str(root))
    return root


def _ep(url, method="POST", rows=20, total=45, post='{"page":1,"perPage":20}', auth=None):
    return CapturedEndpoint(url=url, method=method, content_type="application/json", post_data_sample=post,
                            reason="legacy", sniffed=False, vehicle_rows=rows, total_count=total, auth_headers=auth or {})


def _vehicle(i):
    return {"vin": f"1HGBH41JXMN10{i:04d}", "year": 2024, "make": "Honda", "model": "Civic", "price": 30000 + i}


def _recipe(dealer_id=D):
    ep = _ep("https://websites-search.api.carscommerce.inc/api/v1/listings/1/search", auth={"authorization": "Bearer k"})
    promote_from_ledger(dealer_id, "dealer_dot_com", [ep])
    return load_recipes(dealer_id)[0]


def _replay(dealer_id=D):
    import backend.scanner.recipes as rec

    return asyncio.run(rec.try_fetch_via_recipes(dealer_id, "dealer_dot_com", "https://dealer.example", "Dealer"))


# ── stale marking + clearing in the replay ─────────────────────────────────────

def test_403_marks_stale_and_records_recipe_status(monkeypatch, hints):
    import backend.scanner.recipes as rec

    _recipe()
    monkeypatch.setattr(rec, "_replay_request", lambda *a: (403, None))
    assert _replay() is None
    (r,) = load_recipes(D)
    assert r.stale and r.stale_reason == "http_403"
    status = hints[D]["recipe_status"]
    assert status.startswith("stale:403:")
    datetime.fromisoformat(status.split(":", 2)[2])  # the third field is an ISO timestamp


def test_stale_recipe_is_tried_once_and_unstaled_on_success(monkeypatch, hints):
    import backend.scanner.recipes as rec

    r = _recipe()
    mark_stale(D, r, "http_401")
    hints[D] = {"recipe_status": "stale:401:2026-09-27T00:00:00+00:00"}
    calls = []

    def dead(recipe, body, base_url, url=None):
        calls.append(body["page"])
        return 401, None

    monkeypatch.setattr(rec, "_replay_request", dead)
    assert _replay() is None
    assert calls == [1], "a stale recipe costs exactly one page-1 request while its key is still dead"
    assert load_recipes(D)[0].stale and hints[D]["recipe_status"].startswith("stale:401:")

    pages = {1: [_vehicle(i) for i in range(20)], 2: [_vehicle(20 + i) for i in range(20)], 3: [_vehicle(40 + i) for i in range(5)]}
    monkeypatch.setattr(rec, "_replay_request", lambda recipe, body, base_url, url=None: (200, {"inventory": pages.get(body["page"], [])}))
    hit = _replay()
    assert hit is not None and hit[1] == 45
    (r2,) = load_recipes(D)
    assert not r2.stale and r2.stale_reason == "" and r2.last_ok_at > 0
    assert hints[D]["recipe_status"] == "ok"


def test_success_clears_only_a_stale_status(monkeypatch, hints):
    """A validator verdict (rejected:/uncertain:) is not the replay's to overwrite."""
    import backend.scanner.recipes as rec

    _recipe()
    hints[D] = {"recipe_status": "uncertain:site_total_unknown"}
    pages = {1: [_vehicle(i) for i in range(20)], 2: [_vehicle(20 + i) for i in range(20)], 3: [_vehicle(40 + i) for i in range(5)]}
    monkeypatch.setattr(rec, "_replay_request", lambda recipe, body, base_url, url=None: (200, {"inventory": pages.get(body["page"], [])}))
    assert _replay() is not None
    assert hints[D]["recipe_status"] == "uncertain:site_total_unknown"


def test_hint_store_failure_never_reaches_the_scan(monkeypatch, hints):
    import backend.scanner.recipes as rec
    from backend.scanner import recipe_store

    _recipe()

    def boom(*a, **k):
        raise RuntimeError("db down")

    monkeypatch.setattr(recipe_store, "set_scan_hints", boom)
    monkeypatch.setattr(rec, "_replay_request", lambda *a: (403, None))
    assert _replay() is None
    assert load_recipes(D)[0].stale


# ── route_verdict ──────────────────────────────────────────────────────────────

def _res(verdict, reason="", synth="saved_1", had=1):
    return {"dealer_id": D, "verdict": verdict, "reason": reason, "recipe": {"had_recipes": had, "synth": synth}}


@pytest.mark.parametrize("result,status,expect", [
    (_res("ok", "complete and verified"), "", "none"),
    (_res("thin", "key fields below 90%: price=40%"), "ok", "none"),
    (_res("inaccurate", "only one condition captured"), "", "none"),
    (_res("no_rows", "recipe replay yielded 0 rows"), "", "lifecycle"),
    (_res("no_recipe", "no_template_for_wordpress", synth="no_template_for_wordpress", had=0), "", "lifecycle"),
    (_res("no_recipe", "validated_zero", synth="validated_zero", had=0), "", "lifecycle"),
    (_res("error", "recipe replay HTTP 403 (auth rotated?)"), "", "lifecycle"),
    (_res("error", "no scan_runs row"), "", "none"),
    (_res("ok", "complete and verified"), "stale:403:2026-09-28T10:00:00+00:00", "lifecycle"),
    (_res("no_recipe", "rejected:one_condition", synth="rejected:one_condition", had=0), "rejected:one_condition", "lifecycle"),
    (_res("ok"), "uncertain:site_total_unknown", "none"),
])
def test_route_verdict_decisions(result, status, expect):
    route = dp.route_verdict(DEALER, result, recipe_status=status)
    assert route["action"] == expect, route


def test_route_verdict_needs_a_url():
    r = _res("no_recipe", "not_in_manifest_or_db", synth="not_in_manifest_or_db", had=0)
    assert dp.route_verdict({"dealer_id": D, "url": ""}, r, recipe_status="")["action"] == "none"
    assert dp.route_verdict(DEALER, r, recipe_status="")["action"] == "none"


def test_route_verdict_reads_recipe_status_from_scan_hints(hints):
    hints[D] = {"recipe_status": "stale:401:2026-09-28T01:00:00+00:00"}
    assert dp.route_verdict(DEALER, _res("ok"))["action"] == "lifecycle"
    hints[D] = {"recipe_status": "ok"}
    assert dp.route_verdict(DEALER, _res("ok"))["action"] == "none"


# ── once-per-day guard ─────────────────────────────────────────────────────────

def test_lifecycle_attempted_today_guard():
    now = datetime(2026, 9, 28, 15, 0, tzinfo=timezone.utc)
    assert dp.lifecycle_attempted_today(D, {"lifecycle_last_attempt": "2026-09-28T01:00:00+00:00"}, now)
    assert dp.lifecycle_attempted_today(D, {"lifecycle_last_attempt": "2026-09-28T01:00:00Z"}, now)
    assert not dp.lifecycle_attempted_today(D, {"lifecycle_last_attempt": "2026-09-27T23:59:00+00:00"}, now)
    assert not dp.lifecycle_attempted_today(D, {}, now)
    assert not dp.lifecycle_attempted_today(D, {"lifecycle_last_attempt": "garbage"}, now)


def test_run_lifecycle_skips_a_dealer_already_attempted_today(monkeypatch, hints, log_root):
    hints[D] = {"lifecycle_last_attempt": datetime.now(timezone.utc).isoformat()}

    def never(*a, **k):
        raise AssertionError("ensure_recipe must not run twice in one day")

    monkeypatch.setattr(dp, "ensure_recipe", never)
    monkeypatch.setattr(dp, "run_discovery_capture", never)
    out = dp.run_lifecycle(DEALER, _res("no_rows", "recipe replay yielded 0 rows"), trigger="no_rows")
    assert out["lifecycle"] == "failed:attempted_today"
    text = (log_root / D / "discovery.md").read_text(encoding="utf-8")
    assert "lifecycle skipped (trigger: no_rows)" in text and "one lifecycle attempt per dealer per UTC day" in text


# ── run_lifecycle steps ────────────────────────────────────────────────────────

def test_run_lifecycle_step1_resynth_ok(monkeypatch, hints, log_root):
    monkeypatch.setattr(dp, "ensure_recipe", lambda dealer, force=False: {
        "had_recipes": 0, "synth": "saved_2", "platform": "carscommerce", "synth_vins": 120, "recipe_status": "ok",
        "validation": {"verdict": "ok", "vins_total": 120, "site_total": 121}, "providers": ["carscommerce"]} if force else {"had_recipes": 0, "synth": None})
    monkeypatch.setattr(dp, "run_discovery_capture", lambda *a, **k: pytest.fail("capture must not run when re-synth saved"))
    out = dp.run_lifecycle(DEALER, _res("no_rows", "recipe replay yielded 0 rows", had=1), trigger="no_rows", stamp="2026-09-28 12:00 UTC")
    assert out["lifecycle"] == "resynth_ok" and out["recipe"]["synth"] == "saved_2"
    assert hints[D]["lifecycle_last_attempt"]
    text = (log_root / D / "discovery.md").read_text(encoding="utf-8")
    assert "## 2026-09-28 12:00 UTC lifecycle step 1 — force re-synth (trigger: no_rows)" in text
    assert "synthesis: saved_2, 120 VINs validated" in text and "queued for this run's retry batch" in text
    assert not (log_root / "_learning" / "errors_index.md").exists()


def test_run_lifecycle_step2_capture_ok(monkeypatch, hints, log_root):
    monkeypatch.setattr(dp, "ensure_recipe", lambda dealer, force=False: {"had_recipes": 0, "synth": "no_template_for_unknown", "platform": None})
    captured = []

    def capture(dealer_id, timeout_sec=900):
        captured.append(dealer_id)
        return {"seconds": 88, "rc": 0, "records": 40, "recipes_before": 0, "recipes_after": 1, "endpoints": 1,
                "validation": {"verdict": "ok", "status": "ok"}}

    monkeypatch.setattr(dp, "run_discovery_capture", capture)

    class Rep:
        verdict, reasons, vins_total, site_total, status = "ok", [], 97, 97, "ok"

        def summary(self):
            return {"verdict": "ok", "vins_total": 97}

    monkeypatch.setattr(dp, "validate_live_recipes", lambda dealer, context="lifecycle capture": Rep())
    out = dp.run_lifecycle(DEALER, _res("no_recipe", "no_template_for_unknown", synth="no_template_for_unknown", had=0), trigger="no_recipe", stamp="2026-09-28 12:00 UTC")
    assert out["lifecycle"] == "capture_ok" and captured == [D]
    assert [s["step"] for s in out["steps"]] == ["resynth", "capture"]
    text = (log_root / D / "discovery.md").read_text(encoding="utf-8")
    assert "lifecycle step 1 — force re-synth" in text and "failed (no_template_for_unknown); next: discovery capture" in text
    assert "lifecycle step 2 — discovery capture" in text and "validation: ok" in text
    idx = (log_root / "_learning" / "errors_index.md").read_text(encoding="utf-8")
    assert "lifecycle_resynth_failed:no_template_for_unknown -> lifecycle-dealer-com" in idx
    assert "lifecycle_capture_failed" not in idx


def test_run_lifecycle_capture_rejected_or_skipped_fails(monkeypatch, hints, log_root):
    monkeypatch.setattr(dp, "ensure_recipe", lambda dealer, force=False: {"had_recipes": 0, "synth": "validated_zero", "platform": "dealer_dot_com"})
    monkeypatch.setattr(dp, "run_discovery_capture", lambda *a, **k: {"skipped": "already captured today", "recipes_after": 0})
    out = dp.run_lifecycle(DEALER, _res("no_recipe", "validated_zero", synth="validated_zero", had=0), trigger="validated_zero")
    assert out["lifecycle"] == "failed:capture_skipped_today"

    hints.clear()  # a fresh day for the next scenario
    monkeypatch.setattr(dp, "run_discovery_capture", lambda *a, **k: {"recipes_after": 2, "records": 10})

    class Reject:
        verdict, reasons, vins_total, site_total, status = "reject", ["one_condition: only new rows"], 50, 90, "rejected:one_condition"

        def summary(self):
            return {"verdict": "reject"}

    monkeypatch.setattr(dp, "validate_live_recipes", lambda dealer, context="lifecycle capture": Reject())
    out = dp.run_lifecycle(DEALER, _res("no_rows"), trigger="no_rows")
    assert out["lifecycle"] == "failed:capture_validation_rejected:one_condition"
    idx = (log_root / "_learning" / "errors_index.md").read_text(encoding="utf-8")
    assert "lifecycle_capture_failed:capture_skipped_today" in idx
    assert "lifecycle_capture_failed:capture_validation_rejected:one_condition" in idx


def test_run_lifecycle_no_discover_stops_after_step1(monkeypatch, hints, log_root):
    monkeypatch.setattr(dp, "ensure_recipe", lambda dealer, force=False: {"had_recipes": 0, "synth": "homepage_unreachable_503"})
    monkeypatch.setattr(dp, "run_discovery_capture", lambda *a, **k: pytest.fail("--no-discover forbids the capture"))
    out = dp.run_lifecycle(DEALER, _res("no_recipe", had=0, synth="homepage_unreachable_503"), trigger="no_recipe", no_discover=True)
    assert out["lifecycle"] == "failed:homepage_unreachable_503"
    assert "discovery capture disabled (--no-discover)" in (log_root / D / "discovery.md").read_text(encoding="utf-8")


# ── run_lifecycle_pass: retry batch only from validated dealers ─────────────────

def _seed_retry_rows(db_path: Path, dealer_id: str, n_new: int, n_used: int) -> None:
    now = datetime.now(timezone.utc).replace(microsecond=0)
    c = sqlite3.connect(str(db_path))
    try:
        rows = []
        for i in range(n_new + n_used):
            cond = "New" if i < n_new else "Used"
            rows.append((dealer_id, f"RT{dealer_id[:3].upper()}{i:011d}", (now + timedelta(seconds=1)).isoformat(), 1, cond, "Retry Store", "78701"))
        c.executemany("INSERT INTO cars (dealer_id, vin, scraped_at, listing_active, condition, dealer_name, zip_code) VALUES (?,?,?,?,?,?,?)", rows)
        summary = {"capture_coverage": {"n": n_new + n_used, "price": 1.0, "trim": 1.0, "exterior_color": 1.0}, "recipe_fetch": "hit"}
        c.execute("INSERT INTO scan_runs (dealer_id, finished_at, duration_seconds, upserted, inventory_rows, error, provider, summary_json) VALUES (?,?,?,?,?,?,?,?)",
                  (dealer_id, (now + timedelta(seconds=1)).isoformat(), 30, n_new + n_used, n_new + n_used, None, "carscommerce", json.dumps(summary)))
        c.commit()
    finally:
        c.close()


def test_lifecycle_pass_rescans_only_validated_dealers(monkeypatch, hints, log_root, sqlite_inventory, tmp_path):
    A, B, C = "alpha-ok-com", "bravo-norows-com", "charlie-norecipe-com"
    dealers = {x: {"dealer_id": x, "url": f"https://www.{x}.com", "name": x} for x in (A, B, C)}
    results = [
        {"dealer_id": A, "verdict": "ok", "reason": "complete and verified", "recipe": {"had_recipes": 1, "synth": None}, "rows": 100},
        {"dealer_id": B, "verdict": "no_rows", "reason": "recipe replay yielded 0 rows", "recipe": {"had_recipes": 1, "synth": None}, "rows": 0},
        {"dealer_id": C, "verdict": "no_recipe", "reason": "no_template_for_wix", "recipe": {"had_recipes": 0, "synth": "no_template_for_wix"}},
    ]

    def ensure(dealer, force=False):
        assert force
        if dealer["dealer_id"] == B:
            return {"had_recipes": 1, "synth": "saved_1", "platform": "carscommerce", "synth_vins": 30, "providers": ["carscommerce"]}
        return {"had_recipes": 0, "synth": "no_template_for_wix", "platform": "wix"}

    monkeypatch.setattr(dp, "ensure_recipe", ensure)
    monkeypatch.setattr(dp, "run_discovery_capture", lambda did, timeout_sec=900: {"error": "capture timed out after 900s", "recipes_after": 0})
    scanned: list[list[str]] = []

    def fake_scan(dealer_ids, *, concurrency, log_path, timeout_sec):
        scanned.append(list(dealer_ids))
        for did in dealer_ids:
            _seed_retry_rows(sqlite_inventory.path, did, 18, 12)
        return 0

    monkeypatch.setattr(dp, "run_http_only_scan", fake_scan)
    monkeypatch.setattr(dp, "wait_for_db", lambda *a, **k: True)
    monkeypatch.setattr(dp, "wait_for_scanner_lock", lambda *a, **k: True)
    monkeypatch.setattr(dp, "chromium_process_count", lambda: 0)
    monkeypatch.setattr(dp, "vpic_for_dealers", lambda ids: pytest.fail("--no-vpic"))
    monkeypatch.setattr(dp, "verify_accuracy", lambda conn, did, since: {"rows": 30, "vpic_cached": 30, "incomplete_rows": 0, "missing": {}, "hard": {}, "hard_rows": 0, "examples": {}})

    summary = dp.run_lifecycle_pass(results, dealers, {A: 100, B: 0, C: 0}, out_dir=tmp_path / "out", stamp="2026-09-28 12:00 UTC",
                                    batch=4, no_vpic=True, no_reconcile=True)
    (tmp_path / "out").mkdir(exist_ok=True)

    assert scanned == [[B]], "the retry batch holds only the dealer whose recipe validated"
    by = {r["dealer_id"]: r for r in results}
    assert by[A]["lifecycle"] == "none" and "retried" not in by[A]
    assert by[B]["lifecycle"] == "resynth_ok" and by[B]["verdict"] == "ok" and by[B]["rows"] == 30
    assert by[B]["retried"] == "retried: no_rows → ok" and by[B]["previous_verdict"] == "no_rows"
    assert by[C]["lifecycle"] == "failed:capture_error" and by[C]["verdict"] == "no_recipe"
    assert summary["routed"] == 2 and summary["resynth_ok"] == 1 and summary["failed"] == 1
    assert summary["retried"] == {B: "retried: no_rows → ok"}
    assert hints[B]["lifecycle_last_attempt"] and hints[C]["lifecycle_last_attempt"] and A not in hints

    # logs: step blocks for B and C, retry block for B, errors index for C, scan_runs for the retry
    b_disc = (log_root / B / "discovery.md").read_text(encoding="utf-8")
    assert "lifecycle step 1 — force re-synth (trigger: no_rows)" in b_disc and "lifecycle step 3 — retry scan" in b_disc
    assert "retried: no_rows → ok" in b_disc
    assert "(lifecycle retry)" in (log_root / B / "scan_runs.md").read_text(encoding="utf-8")
    c_disc = (log_root / C / "discovery.md").read_text(encoding="utf-8")
    assert "lifecycle step 2 — discovery capture (trigger: no_recipe)" in c_disc and "failed (capture_error)" in c_disc
    idx = (log_root / "_learning" / "errors_index.md").read_text(encoding="utf-8")
    assert f"lifecycle_resynth_failed:no_template_for_wix -> {C}" in idx and f"lifecycle_capture_failed:capture_error -> {C}" in idx
    assert B not in idx, "a dealer whose lifecycle succeeded has no failure class"

    out_dir = tmp_path / "out"
    lines = dp.write_needs_discovery(results, out_dir)
    assert len(lines) == 1 and lines[0].startswith(f"{C}  no_recipe  failed:capture_error")
    assert (out_dir / "needs_discovery.txt").read_text(encoding="utf-8").startswith(C)

    table = dp.triage_table(results)
    assert "| lifecycle |" in table[0]
    row_b = next(ln for ln in table if ln.startswith(f"| {B} "))
    assert "| resynth_ok (retried: no_rows → ok) |" in row_b
    row_c = next(ln for ln in table if ln.startswith(f"| {C} "))
    assert "| failed:capture_error |" in row_c


def test_lifecycle_pass_with_nothing_to_route_does_not_scan(monkeypatch, hints, log_root, tmp_path):
    results = [{"dealer_id": D, "verdict": "thin", "reason": "key fields below 90%: price=10%", "recipe": {"had_recipes": 1}}]
    monkeypatch.setattr(dp, "scan_retry_batch", lambda *a, **k: pytest.fail("no retry batch without a lifecycle success"))
    monkeypatch.setattr(dp, "ensure_recipe", lambda *a, **k: pytest.fail("thin is not a recipe problem"))
    summary = dp.run_lifecycle_pass(results, {D: DEALER}, {D: 50}, out_dir=tmp_path, stamp="s")
    assert summary["routed"] == 0 and results[0]["lifecycle"] == "none"


# ── --no-lifecycle through main() ──────────────────────────────────────────────

def _main_env(monkeypatch, tmp_path, hints, log_root, flag: bool):
    manifest = tmp_path / "dealers.json"
    manifest.write_text(json.dumps({"dealers": [dict(DEALER, provider="unknown")]}), encoding="utf-8")
    out = tmp_path / ("out_flag" if flag else "out_noflag")
    monkeypatch.setattr(dp, "ensure_recipe", lambda dealer, force=False: {"had_recipes": 0, "synth": "no_template_for_unknown", "platform": None})
    monkeypatch.setattr(dp, "run_discovery_capture", lambda did, timeout_sec=900: {"skipped": "already captured today", "recipes_after": 0})
    from backend.scripts import discovery_probe

    monkeypatch.setattr(discovery_probe, "probe_dealer", lambda did, url, paths=True: {"classification": "unknown_platform"})
    monkeypatch.setattr(dp, "run_http_only_scan", lambda *a, **k: pytest.fail("no recipe, nothing to scan"))
    calls: list[str] = []

    def spy(results, dealers, known, **kw):
        calls.append("lifecycle")
        for r in results:
            r["lifecycle"] = "failed:capture_skipped_today"
        return {"routed": 1, "resynth_ok": 0, "capture_ok": 0, "failed": 1, "retried": {}, "chromium_leaks": []}

    monkeypatch.setattr(dp, "run_lifecycle_pass", spy)
    argv = ["dealer_pipeline", "--dealers", D, "--manifest", str(manifest), "--out", str(out), "--no-vpic", "--no-reconcile"]
    if flag:
        argv.append("--no-lifecycle")
    monkeypatch.setattr("sys.argv", argv)
    return out, calls


def test_no_lifecycle_flag_disables_the_pass(monkeypatch, tmp_path, hints, log_root, sqlite_inventory):
    out, calls = _main_env(monkeypatch, tmp_path, hints, log_root, flag=True)
    assert dp.main() == 0
    assert calls == []
    triage = json.loads((out / "triage.json").read_text(encoding="utf-8"))
    assert triage["lifecycle"]["enabled"] is False
    assert triage["lifecycle"]["by_dealer"] == {D: "none"}
    md = (out / "triage.md").read_text(encoding="utf-8")
    assert "| lifecycle |" in md and f"| {D} | no_recipe |" in md and "| none |" in md
    # still failing -> the discovery workflow's input
    assert (out / "needs_discovery.txt").read_text(encoding="utf-8").startswith(f"{D}  no_recipe  none")


def test_lifecycle_runs_by_default_after_the_batches(monkeypatch, tmp_path, hints, log_root, sqlite_inventory):
    out, calls = _main_env(monkeypatch, tmp_path, hints, log_root, flag=False)
    assert dp.main() == 0
    assert calls == ["lifecycle"]
    triage = json.loads((out / "triage.json").read_text(encoding="utf-8"))
    assert triage["lifecycle"]["enabled"] is True and triage["lifecycle"]["failed"] == 1
    assert triage["lifecycle"]["by_dealer"] == {D: "failed:capture_skipped_today"}
    assert "| failed:capture_skipped_today |" in (out / "triage.md").read_text(encoding="utf-8")
