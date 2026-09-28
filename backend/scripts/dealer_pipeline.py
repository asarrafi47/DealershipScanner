#!/usr/bin/env python3
"""Mass HTTP-only dealer pipeline: recipe -> scan -> NHTSA heal -> assess -> triage.

One deterministic pass per dealer, no browser, no LLM. Its output (triage JSON)
is what the agentic discovery workflow consumes for the dealers that came back
empty, thin or broken.

Per dealer:
  1. recipe   ensure a non-stale recipe exists; synthesize from the homepage when
              missing (recipe_synth platform templates), record platform + totals
  2. scan     `scanner.py --dealer-id … --scan-only` with SCANNER_HTTP_ONLY=1
              (batched: N dealers per scanner process, dealer_concurrency=N)
  3. vpic     decode new VINs with NHTSA vPIC, store drivetrain / electrification
  4. assess   scan_runs.summary_json (capture_coverage, recipe_coverage,
              vdp_prefetch, error) + row counts -> verdict:
                ok        rows >= 50% of last listed count (or >= 20 when unknown)
                          and price/trim/colour >= 90%
                thin      rows fine, listed field(s) below 90%
                no_rows   recipe replay produced nothing (needs discovery)
                error     scanner error for the dealer
                no_recipe nothing to replay and synthesis failed (needs discovery)

Usage:
  python -m backend.scripts.dealer_pipeline --dealers a,b,c --out workspace/pipeline/run1
  python -m backend.scripts.dealer_pipeline --manifest dealers.json --limit 40 --shard-index 0 --shard-count 4 --out …
  python -m backend.scripts.dealer_pipeline --dealers a --out … --skip-scan   # assess an existing scan only
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from backend.utils.project_env import load_project_dotenv

load_project_dotenv()

from backend.db.inventory_db import get_conn  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
LOG_ROOT = ROOT / "workspace" / "dealer_logs"
PROCESS_DOC = "docs/NETWORK_SCAN_PROCESS.md"
INCOMPLETE_FLOOR = 0.05   # share of rows with a missing spec-sheet field after the NHTSA fill
DISCREPANCY_FLOOR = 0.05  # share of rows with a hard dictionary-vs-dealer discrepancy
FIELD_FLOOR = 0.90
BASELINE_DAYS = 30            # "listed before" = rows seen this many days before the run
RECONCILE_MIN_SHARE = 0.60    # a run that returned fewer rows than this share of the baseline is partial: never retire on it
ROW_FLOOR = 0.50
MIN_ROWS_UNKNOWN = 20
KEY_FIELDS = ("price", "trim", "exterior_color")
SECONDARY_FIELDS = ("interior_color", "engine_description", "transmission", "drivetrain", "fuel_type",
                    "body_style", "description", "stock_number", "msrp", "gallery_8plus")

# Scans are HTTP-only by policy (backend/scanner/browser_gate.py): no cap juggling
# needed any more — without SCANNER_ALLOW_BROWSER the scanner never launches
# Chromium, opens no page and the browser VDP pool does not exist.
HTTP_ONLY_ENV = {"SCANNER_HTTP_ONLY": "1", "SCANNER_VDP_DB_MERGE": "0"}


def chromium_process_count() -> int:
    """Headless Chromium processes alive on this machine (the fleet's browser-free
    contract; deploy/nightly_http_refresh.sh keeps the same counter)."""
    try:
        out = subprocess.run(["pgrep", "-f", "ms-playwright|headless_shell|chrome-headless"], capture_output=True, text=True, timeout=10)
        return len([ln for ln in out.stdout.splitlines() if ln.strip()])
    except Exception:  # noqa: BLE001
        return -1


def wait_for_db(max_wait: int, poll: int = 30) -> bool:
    """Block until the inventory database answers, up to *max_wait* seconds.

    The mini reaches the MBP's Postgres through an SSH reverse tunnel; when it
    dropped mid-fleet on 2026-09-27 the pipeline burned through 55 batches in
    seconds (each scanner exited 1 on connect) and both shards reported DONE.
    Waiting keeps the roster intact for when the tunnel comes back."""
    deadline = time.time() + max(0, max_wait)
    warned = False
    while True:
        try:
            conn = get_conn()
            try:
                conn.execute("SELECT 1").fetchone()
            finally:
                try:
                    conn.close()
                except Exception:  # noqa: BLE001
                    pass
            if warned:
                print("scan    database reachable again", flush=True)
            return True
        except Exception as exc:  # noqa: BLE001
            if time.time() >= deadline:
                return False
            if not warned:
                print(f"scan    database unreachable ({str(exc).splitlines()[0][:120]}); waiting", flush=True)
                warned = True
            time.sleep(poll)


def _rows(conn, sql: str, params: tuple = ()) -> list[dict[str, Any]]:
    cur = conn.execute(sql, params)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


# --------------------------------------------------------------------------
# dealers
# --------------------------------------------------------------------------

def load_manifest_dealers(path: str) -> list[dict[str, Any]]:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    items = raw.get("dealers") if isinstance(raw, dict) else raw
    out = []
    for d in items or []:
        if isinstance(d, dict) and d.get("dealer_id") and d.get("url"):
            out.append(d)
    return out


def dealers_from_db(dealer_ids: list[str]) -> dict[str, dict[str, Any]]:
    """{dealer_id: {dealer_id, url, name}} from the cars table for ids the manifest lacks."""
    if not dealer_ids:
        return {}
    conn = get_conn()
    try:
        rows = _rows(
            conn,
            "SELECT dealer_id, MAX(dealer_url) AS url, MAX(dealer_name) AS name FROM cars "
            "WHERE dealer_id IN (" + ",".join("?" * len(dealer_ids)) + ") AND COALESCE(listing_active,1)=1 GROUP BY dealer_id",
            tuple(dealer_ids),
        )
    finally:
        conn.close()
    return {r["dealer_id"]: {"dealer_id": r["dealer_id"], "url": str(r["url"] or ""), "name": str(r["name"] or r["dealer_id"]), "provider": "unknown"}
            for r in rows if r.get("url")}


def dealer_from_manifest(dealer_id: str, manifest: list[dict[str, Any]]) -> dict[str, Any] | None:
    for d in manifest:
        if d.get("dealer_id") == dealer_id:
            return d
    return None


# --------------------------------------------------------------------------
# 1. recipe
# --------------------------------------------------------------------------

def ensure_recipe(dealer: dict[str, Any], *, force: bool = False) -> dict[str, Any]:
    from backend.scanner.recipes import load_recipes, save_recipes
    from backend.scanner.recipe_synth import fetch_dealer_html, fingerprint_platform, synthesize_recipes, validate_recipe

    did = dealer["dealer_id"]
    live = [r for r in load_recipes(did) if not r.stale]
    info: dict[str, Any] = {
        "had_recipes": len(live),
        "providers": sorted({r.provider_hint for r in live if r.provider_hint}),
        "synth": None,
    }
    # A cosmos store with a single recipe predates the per-section synth (used and new
    # have separate pageIds): re-synthesize so both conditions are collected.
    from backend.scanner.recipes import recipe_is_section_scoped

    # A captured carscommerce recipe pinned to one SRP section (type_slug / make /
    # model_slug facets) replays a fraction of the lot: 16 no_rows verdicts on
    # 2026-09-24. The synth builds the whole-lot body (+ store filter), so rebuild.
    section_scoped = bool(live) and all(recipe_is_section_scoped(r) for r in live)
    if live and not force and not (len(live) == 1 and live[0].provider_hint == "dealer_on_cosmos") and not section_scoped:
        return info
    if live and not force:
        info["resynth"] = "cc_section_scoped" if section_scoped else "cosmos_single_section"
    html = fetch_dealer_html(dealer["url"])
    if not html:
        status = None
        try:
            from curl_cffi import requests as cr

            status = cr.get(dealer["url"], impersonate="chrome", timeout=20).status_code
        except Exception:  # noqa: BLE001
            pass
        info["synth"] = f"homepage_unreachable_{status or 'exc'}"
        return info
    platform = fingerprint_platform(html, dealer["url"])
    info["platform"] = platform
    try:
        from backend.scanner.dealer_place import learn_place, place_kwargs

        place_full = learn_place(did, dealer["url"], html)
        place = place_kwargs(place_full)
        if place:
            info["place"] = f"{place.get('dealer_city')}, {place.get('dealer_state')} ({place_full.get('place_source')})"
    except Exception:  # noqa: BLE001
        place = {}
    cands = synthesize_recipes(did, dealer["url"], html, platform)
    if not cands:
        info["synth"] = f"no_template_for_{platform or 'unknown'}"
        return info
    kept = []
    for c in cands:
        try:
            n = validate_recipe(c, dealer["url"], did, dealer.get("name") or did, place=place or None)
        except Exception as exc:  # noqa: BLE001
            info["synth"] = f"validate_error:{str(exc)[:80]}"
            continue
        if n and n >= 5:
            c.vehicle_rows = int(n)
            c.last_ok_at = time.time()
            kept.append(c)
    if not kept:
        info["synth"] = info.get("synth") or "validated_zero"
        return info
    save_recipes(did, kept)
    info["synth"] = f"saved_{len(kept)}"
    info["providers"] = sorted({r.provider_hint for r in kept if r.provider_hint})
    info["synth_vins"] = sum(r.vehicle_rows for r in kept)
    return info


# --------------------------------------------------------------------------
# 2. scan
# --------------------------------------------------------------------------

from backend.scanner.scan_lock import default_lock_path as _default_lock_path

# Per-shard lock (SCANNER_LOCK_PATH); the scanner subprocesses inherit the env,
# so this pipeline and the scanners it spawns agree on the file.
LOCK_FILE = _default_lock_path()


def _lock_holder_alive() -> int | None:
    """PID holding the scanner lock, or None when free / stale."""
    try:
        txt = LOCK_FILE.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return None
    import re

    m = re.search(r"\d+", txt)
    if not m:
        return None
    pid = int(m.group(0))
    try:
        os.kill(pid, 0)
    except OSError:
        return None
    return pid


def run_discovery_capture(dealer_id: str, timeout_sec: int = 900) -> dict[str, Any]:
    """Run ``discovery_probe --browser-capture`` for one dealer in a separate process
    (so SCANNER_ALLOW_BROWSER never enters this one). Bounded to one capture per
    dealer per UTC day via a marker file; returns a small summary for the triage."""
    marker_dir = ROOT / "workspace" / "dealer_logs" / dealer_id
    marker_dir.mkdir(parents=True, exist_ok=True)
    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    marker = marker_dir / f".capture_{today}"
    if marker.exists():
        return {"skipped": "already captured today", "recipes_after": 0}
    marker.write_text(datetime.now(timezone.utc).isoformat())
    py = str(ROOT / ".venv" / "bin" / "python") if (ROOT / ".venv" / "bin" / "python").exists() else sys.executable
    env = dict(os.environ)
    env.pop("SCANNER_ALLOW_BROWSER", None)  # the probe sets it for itself
    t0 = time.time()
    try:
        proc = subprocess.run([py, "-m", "backend.scripts.discovery_probe", "--no-paths", "--browser-capture", "--dealers", dealer_id],
                              cwd=str(ROOT), env=env, capture_output=True, text=True, timeout=timeout_sec)
        tail = (proc.stdout or "").strip().splitlines()[-2:]
    except subprocess.TimeoutExpired:
        return {"error": f"capture timed out after {timeout_sec}s", "recipes_after": 0, "seconds": round(time.time() - t0)}
    out: dict[str, Any] = {"seconds": round(time.time() - t0), "rc": proc.returncode, "stdout_tail": tail, "recipes_after": 0}
    caps = sorted(marker_dir.glob("capture_*.json"))
    if caps:
        try:
            cap = json.loads(caps[-1].read_text())
            out.update({k: cap.get(k) for k in ("records", "recipes_before", "recipes_after", "profile", "errors") if k in cap})
            out["endpoints"] = len(cap.get("endpoints") or [])
        except Exception as exc:  # noqa: BLE001
            out["error"] = f"capture report unreadable: {str(exc)[:80]}"
    print(f"discover {dealer_id:36s} {json.dumps(out)[:220]}", flush=True)
    return out


def wait_for_scanner_lock(max_wait_sec: int, poll_sec: int = 20) -> bool:
    """The scanner refuses to start while another run holds workspace/scanner.lock
    (one process per machine). Wait for it instead of failing the batch."""
    waited = 0
    while waited < max_wait_sec:
        pid = _lock_holder_alive()
        if pid is None:
            return True
        if waited == 0:
            print(f"scan    waiting for scanner lock held by pid {pid}", flush=True)
        time.sleep(poll_sec)
        waited += poll_sec
    return _lock_holder_alive() is None


def run_http_only_scan(dealer_ids: list[str], *, concurrency: int, log_path: Path, timeout_sec: int) -> int:
    env = dict(os.environ)
    env.update(HTTP_ONLY_ENV)
    # scanner.py rosters from dealers.json by default and exits 1 on an id it
    # does not hold ("No dealer(s) matching …"): 69 of the 72 not-in-manifest
    # dealers failed that way on 2026-09-26 AFTER their recipes were synthesized.
    # Roster from active inventory ∪ stored recipes instead — every dealer this
    # pipeline can scan is in one of those.
    env.setdefault("DEALERS_FROM_SCANNABLE", "1")
    py = str(ROOT / ".venv" / "bin" / "python") if (ROOT / ".venv" / "bin" / "python").exists() else sys.executable
    cmd = [py, "scanner.py", "--dealer-id", ",".join(dealer_ids), "--dealer-concurrency", str(concurrency), "--scan-only"]
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("ab") as fh:
        try:
            proc = subprocess.run(cmd, cwd=str(ROOT), env=env, stdout=fh, stderr=subprocess.STDOUT, timeout=timeout_sec)
            return proc.returncode
        except subprocess.TimeoutExpired:
            return -9


# --------------------------------------------------------------------------
# 3. vpic
# --------------------------------------------------------------------------

def vpic_for_dealers(dealer_ids: list[str]) -> dict[str, Any]:
    from backend.enrichment.vpic_facts import decode_missing_vins, heal_rows

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT vin FROM cars WHERE COALESCE(listing_active,1)=1 AND length(vin)=17 AND dealer_id IN ("
            + ",".join("?" * len(dealer_ids)) + ")",
            tuple(dealer_ids),
        )
        vins = [r[0] for r in cur.fetchall()]
        dec = decode_missing_vins(conn, vins)
        heal = heal_rows(conn, dealers=dealer_ids)
        heal.pop("examples", None)
        return {"decode": dec, "heal": heal}
    finally:
        conn.close()


# --------------------------------------------------------------------------
# logs (docs/NETWORK_SCAN_PROCESS.md: every dealer keeps discovery / scan_runs /
# location / summary / scan_instructions under workspace/dealer_logs/<dealer_id>/)
# --------------------------------------------------------------------------

def _log_append(dealer_id: str, name: str, block: str) -> None:
    d = LOG_ROOT / dealer_id
    d.mkdir(parents=True, exist_ok=True)
    p = d / name
    head = "" if p.exists() else f"# {dealer_id} — {name[:-3]}\n\nProcess: {PROCESS_DOC}\n\n"
    with p.open("a", encoding="utf-8") as fh:
        fh.write(head + block.rstrip() + "\n\n")


def _log_write_once(dealer_id: str, name: str, text: str) -> bool:
    d = LOG_ROOT / dealer_id
    d.mkdir(parents=True, exist_ok=True)
    p = d / name
    if p.exists():
        return False
    p.write_text(text, encoding="utf-8")
    return True


def log_discovery(dealer: dict[str, Any], info: dict[str, Any], stamp: str) -> None:
    lines = [f"## {stamp} discovery",
             f"- url: {dealer.get('url')}",
             f"- recipes on file: {info.get('had_recipes')} ({', '.join(info.get('providers') or []) or 'none'})"]
    if info.get("platform"):
        lines.append(f"- fingerprint: {info['platform']}")
    if info.get("synth"):
        lines.append(f"- synthesis: {info['synth']}" + (f", {info.get('synth_vins')} VINs validated" if info.get("synth_vins") else ""))
    if info.get("place"):
        lines.append(f"- store location for rooftop attribution: {info['place']}")
    if info.get("resynth"):
        lines.append(f"- re-synthesized because: {info['resynth']}")
    if info.get("traceback"):
        lines.append("- traceback:\n```\n" + info["traceback"] + "\n```")
    verdict = "recipe available, proceed to scan" if (info.get("had_recipes") or str(info.get("synth", "")).startswith("saved")) else "NO RECIPE: needs discovery (probe how the site presents data, then rerun)"
    lines.append(f"- verdict: {verdict}")
    _log_append(dealer["dealer_id"], "discovery.md", "\n".join(lines))


def _pct(n: Any, d: Any) -> str:
    try:
        return f"{100.0 * float(n) / float(d):.1f}%" if d else "-"
    except (TypeError, ValueError):
        return "-"


def _one_condition_ok() -> set[str]:
    """Dealer ids whose lot legitimately carries a single condition (used-only
    independents, new-only fleet/RV sellers). One id per line, `#` comments."""
    try:
        text = (ROOT / "workspace" / "pipeline" / "one_condition_ok.txt").read_text()
    except OSError:
        return set()
    return {ln.split("#", 1)[0].strip() for ln in text.splitlines() if ln.split("#", 1)[0].strip()}


def reconcile_dealer(conn, dealer_id: str, since_iso: str, baseline: int, rows_this_run: int, verdict: str,
                     *, dry_run: bool = False) -> dict[str, Any]:
    """Retire the dealer's active rows this run did not return.

    The scanner has never done this (``listing_removed_at`` was NULL on all
    194,811 active rows on 2026-09-26; 45,265 of them last seen before
    September), so sold cars stay listed and every "listed before" count is
    inflated. Guarded: only after a run the assess step accepted (ok / thin /
    inaccurate — not no_rows / error) that returned at least
    ``RECONCILE_MIN_SHARE`` of the rows seen in the baseline window, so a
    section-scoped or truncated replay can never un-list a lot. Rows are
    retired, never deleted (price history / attribution key off ``cars.id``).
    """
    out = {"eligible": False, "stale": 0, "retired": 0, "reason": ""}
    if verdict not in ("ok", "thin", "inaccurate"):
        out["reason"] = f"verdict {verdict}"
        return out
    # The 30-day baseline is 0 for a dealer nobody scanned lately, and a
    # share-of-zero guard passes anything: on 2026-09-26 an 8-row run retired
    # 382 of Gunn Honda's cars. Fall back to the rows seen in the last 90 days,
    # then to every active row, and always demand an absolute floor.
    if not baseline:
        wider = _rows(conn, "SELECT COUNT(*) AS n FROM cars WHERE dealer_id = ? AND COALESCE(listing_active,1)=1 AND scraped_at < ? AND scraped_at >= ?",
                      (dealer_id, since_iso, (datetime.fromisoformat(since_iso.replace("Z", "+00:00")) - timedelta(days=90)).isoformat()))[0]["n"]
        baseline = int(wider or 0) or int(_rows(conn, "SELECT COUNT(*) AS n FROM cars WHERE dealer_id = ? AND COALESCE(listing_active,1)=1 AND scraped_at < ?",
                                               (dealer_id, since_iso))[0]["n"] or 0)
        out["baseline_fallback"] = baseline
    if rows_this_run < MIN_ROWS_UNKNOWN:
        out["reason"] = f"{rows_this_run} rows < absolute floor {MIN_ROWS_UNKNOWN}"
        return out
    if baseline and rows_this_run < baseline * RECONCILE_MIN_SHARE:
        out["reason"] = f"{rows_this_run} rows < {RECONCILE_MIN_SHARE:.0%} of baseline {baseline}"
        return out
    out["eligible"] = True
    stale = _rows(conn, "SELECT COUNT(*) AS n, MIN(scraped_at) AS oldest FROM cars WHERE dealer_id = ? AND COALESCE(listing_active,1)=1 AND scraped_at < ?",
                  (dealer_id, since_iso))[0]
    out["stale"] = int(stale["n"] or 0)
    out["oldest"] = stale.get("oldest")
    if out["stale"] and not dry_run:
        now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        conn.execute("UPDATE cars SET listing_active = 0, listing_removed_at = ? WHERE dealer_id = ? AND COALESCE(listing_active,1)=1 AND scraped_at < ?",
                     (now, dealer_id, since_iso))
        conn.commit()
        out["retired"] = out["stale"]
    return out


def log_scan_run(r: dict[str, Any], stamp: str) -> None:
    did = r["dealer_id"]
    acc = r.get("accuracy") or {}
    cov = r.get("coverage") or {}
    L = [f"## {stamp} scan — verdict **{r.get('verdict')}** ({r.get('reason')})",
         f"- rows written: {r.get('rows', '-')} (new {r.get('rows_new', '-')}, used {r.get('rows_used', '-')}); listed before: {r.get('known_before')}; active after: {r.get('active_after')}",
         f"- recipe: {r.get('recipe_fetch')} coverage {json.dumps(r.get('recipe_coverage'))}; provider {r.get('provider')}; {r.get('minutes', '-')} min",
         f"- location: {r.get('filtered_count', '-')} row(s) refused as other rooftops; dealer names on rows: {r.get('dealer_names')}; zip codes: {r.get('zip_codes')}",
         "- coverage before upsert: " + ", ".join(f"{k} {_pct(v, 1)}" for k, v in cov.items()),
         f"- http_first: {json.dumps(r.get('http_first'))[:300]}",
         f"- NHTSA: decode cached for {acc.get('vpic_cached', '-')}/{acc.get('rows', '-')} rows; heal this batch: {json.dumps(r.get('vpic_heal'))[:200]}",
         f"- incomplete after fill: {acc.get('incomplete_rows', '-')}/{acc.get('rows', '-')} ({_pct(acc.get('incomplete_rows'), acc.get('rows'))}); fields: {json.dumps(acc.get('missing'))}",
         f"- hard discrepancies: {acc.get('hard_rows', '-')} rows ({_pct(acc.get('hard_rows'), acc.get('rows'))}); by code: {json.dumps(acc.get('hard'))}",
         f"- reconcile: {json.dumps(r.get('reconcile'))}"]
    for code, exs in (acc.get("examples") or {}).items():
        L.append(f"  - `{code}`: " + "; ".join(f"{e.get('car')} `{e.get('vin')}` {e.get('why', '')}"[:160] for e in exs[:3]))
    if r.get("error"):
        L.append(f"- error: {r['error']}")
    _log_append(did, "scan_runs.md", "\n".join(L))


def write_instructions_if_first_success(r: dict[str, Any], stamp: str) -> None:
    if r.get("verdict") != "ok":
        return
    did = r["dealer_id"]
    try:
        from backend.scanner.recipes import load_recipes

        recs = [x for x in load_recipes(did) if not x.stale]
    except Exception:  # noqa: BLE001
        recs = []
    lines = [f"# {did} — scan instructions", "", f"Process: {PROCESS_DOC}. Written after the first verified HTTP-only run ({stamp}).", "",
             f"- provider: {r.get('provider')}", "- recipes (replayed in order, union of VINs):"]
    for x in recs:
        lines.append(f"  - {x.method} {x.url}  pagination={x.pagination} rows={x.vehicle_rows} total={x.total_count} coverage={json.dumps(x.field_coverage)}")
    lines += ["- run: `python -m backend.scripts.dealer_pipeline --dealers " + did + " --out workspace/pipeline/<run> --batch 1` (HTTP-only, no browser)",
              f"- expected rows: about {r.get('rows')} (new {r.get('rows_new')}, used {r.get('rows_used')})",
              f"- feed carries: " + ", ".join(k for k, v in (r.get('coverage') or {}).items() if float(v or 0) >= 0.9),
              f"- detail page adds (HTTP-first): {json.dumps({k: v for k, v in (r.get('http_first') or {}).items() if k in ('fetched', 'candidates', 'descriptions_filled', 'galleries_extended', 'statuses')})}",
              "- quirks: see summary.md and discovery.md", ""]
    if _log_write_once(did, "scan_instructions.md", "\n".join(lines)):
        _log_write_once(did, "summary.md", f"# {did} — summary\n\nProcess: {PROCESS_DOC}\n\n## Issues faced and how they were fixed\n\n(first verified run {stamp}: none recorded yet; append each issue with the fix in detail)\n")


# --------------------------------------------------------------------------
# 4. assess
# --------------------------------------------------------------------------

def assess(conn, dealer_id: str, since_iso: str, known_before: int, recipe_info: dict[str, Any]) -> dict[str, Any]:
    runs = _rows(
        conn,
        "SELECT id, provider, finished_at, duration_seconds, inventory_rows, upserted, error, summary_json "
        "FROM scan_runs WHERE dealer_id = ? AND finished_at >= ? ORDER BY id DESC LIMIT 1",
        (dealer_id, since_iso),
    )
    active = _rows(conn, "SELECT COUNT(*) AS n FROM cars WHERE dealer_id = ? AND COALESCE(listing_active,1)=1", (dealer_id,))[0]["n"]
    out: dict[str, Any] = {
        "dealer_id": dealer_id, "recipe": recipe_info, "known_before": known_before, "active_after": int(active),
    }
    mix = _rows(
        conn,
        "SELECT COUNT(*) FILTER (WHERE lower(COALESCE(condition,'')) LIKE 'new%') AS n_new, "
        "COUNT(*) FILTER (WHERE lower(COALESCE(condition,'')) NOT LIKE 'new%') AS n_used, "
        "COUNT(DISTINCT dealer_name) AS names, COUNT(DISTINCT zip_code) AS zips "
        "FROM cars WHERE dealer_id = ? AND COALESCE(listing_active,1)=1 AND scraped_at >= ?",
        (dealer_id, since_iso),
    )[0]
    out.update({"rows_new": int(mix["n_new"] or 0), "rows_used": int(mix["n_used"] or 0),
                "dealer_names": int(mix["names"] or 0), "zip_codes": int(mix["zips"] or 0)})
    if not runs:
        out["verdict"] = "no_recipe" if recipe_info.get("synth") and not str(recipe_info.get("synth")).startswith("saved") and not recipe_info.get("had_recipes") else "error"
        out["reason"] = "no scan_runs row" if out["verdict"] == "error" else recipe_info.get("synth")
        return out
    r = runs[0]
    try:
        s = json.loads(r.get("summary_json") or "{}")
    except ValueError:
        s = {}
    cc = s.get("capture_coverage") or {}
    n = int(cc.get("n") or r.get("upserted") or 0)
    out.update({
        "run_id": r["id"], "provider": r.get("provider"), "minutes": round(float(r.get("duration_seconds") or 0) / 60, 1),
        "rows": n, "recipe_fetch": s.get("recipe_fetch"), "recipe_coverage": s.get("recipe_coverage"),
        "http_first": (s.get("vdp_prefetch") or {}).get("http_first"), "vdp_recipes": (s.get("vdp_prefetch") or {}).get("vdp_recipes"),
        "error": (r.get("error") or None),
        "coverage": {k: cc.get(k) for k in KEY_FIELDS + SECONDARY_FIELDS if k in cc},
        "filtered_count": s.get("filtered_count"), "vin_facts": s.get("vin_facts"),
    })
    if r.get("error"):
        out["verdict"], out["reason"] = "error", str(r["error"])[:200]
        return out
    if n == 0:
        out["verdict"], out["reason"] = "no_rows", "recipe replay yielded 0 rows"
        return out
    floor = known_before * ROW_FLOOR if known_before else MIN_ROWS_UNKNOWN
    if n < floor:
        out["verdict"], out["reason"] = "no_rows", f"{n} rows vs {known_before} listed before (floor {floor:.0f})"
        return out
    weak = [k for k in KEY_FIELDS if float(cc.get(k) or 0) < FIELD_FLOOR]
    weak2 = [k for k in SECONDARY_FIELDS if k in cc and float(cc.get(k) or 0) < FIELD_FLOOR]
    out["weak_key_fields"], out["weak_fields"] = weak, weak2
    if weak:
        out["verdict"], out["reason"] = "thin", "key fields below 90%: " + ", ".join(f"{k}={float(cc.get(k) or 0):.0%}" for k in weak)
        return out
    # Verification (the process doc): incomplete fields after the NHTSA fill and
    # dictionary-vs-dealer discrepancies, row by row, with examples.
    acc = verify_accuracy(conn, dealer_id, since_iso)
    out["accuracy"] = acc
    problems = []
    if acc.get("rows"):
        if acc["incomplete_rows"] / acc["rows"] > INCOMPLETE_FLOOR:
            problems.append(f"incomplete {acc['incomplete_rows']}/{acc['rows']}: {', '.join(list(acc['missing'])[:4])}")
        if acc["hard_rows"] / acc["rows"] > DISCREPANCY_FLOOR:
            problems.append(f"discrepancies {acc['hard_rows']}/{acc['rows']}: {', '.join(list(acc['hard'])[:4])}")
    if out["rows_new"] == 0 or out["rows_used"] == 0:
        if dealer_id in _one_condition_ok():
            # independent used-only (or new-only) lots: one condition IS the lot
            # (quantumautosales-com 209/0 used, 2026-09-26); listed in
            # workspace/pipeline/one_condition_ok.txt after a human check of the site
            weak2 = list(weak2) + [f"one condition by design (new {out['rows_new']}, used {out['rows_used']})"]
        else:
            problems.append(f"only one condition captured (new {out['rows_new']}, used {out['rows_used']})")
    if problems:
        out["verdict"], out["reason"] = "inaccurate", "; ".join(problems)
    else:
        out["verdict"], out["reason"] = "ok", ("secondary gaps: " + ", ".join(weak2)) if weak2 else "complete and verified"
    return out


def verify_accuracy(conn, dealer_id: str, since_iso: str) -> dict[str, Any]:
    """Row-by-row verification from scan_lab_report: missing spec-sheet fields and
    dictionary-vs-dealer discrepancies (VIN decode, catalog link, engine text)."""
    try:
        from backend.scripts.scan_lab_report import INFORMATIONAL, tally_dealer

        t = tally_dealer(conn, dealer_id, since_iso)
    except Exception as exc:  # noqa: BLE001 - verification must not crash the triage
        return {"error": str(exc)[:200]}
    hard = {c: n for c, n in (t.get("discrepancies") or {}).items() if c not in INFORMATIONAL}
    hard_rows = 0
    for c in t.get("car_issues") or []:
        if any(k not in INFORMATIONAL for k in (c.get("disc") or {})):
            hard_rows += 1
    return {
        "rows": t.get("rows", 0), "vpic_cached": t.get("vpic_cached", 0),
        "incomplete_rows": t.get("incomplete_rows", 0), "missing": t.get("missing") or {},
        "hard": hard, "hard_rows": hard_rows,
        "examples": {c: (t.get("discrepancy_examples") or {}).get(c, [])[:3] for c in list(hard)[:5]},
    }


# --------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dealers", default="", help="comma-separated dealer ids")
    ap.add_argument("--manifest", default=str(ROOT / "dealers.json"))
    ap.add_argument("--limit", type=int, default=0, help="first N manifest dealers (after sharding)")
    ap.add_argument("--shard-index", type=int, default=0)
    ap.add_argument("--shard-count", type=int, default=1)
    ap.add_argument("--out", required=True, help="output directory")
    ap.add_argument("--batch", type=int, default=4, help="dealers per scanner process (= dealer concurrency)")
    ap.add_argument("--scan-timeout", type=int, default=3600, help="seconds per scanner batch")
    ap.add_argument("--lock-wait", type=int, default=5400, help="seconds to wait for another scanner run to finish")
    ap.add_argument("--skip-scan", action="store_true", help="assess the latest run since --since instead of scanning")
    ap.add_argument("--since", default="", help="with --skip-scan: ISO timestamp of the run to assess")
    ap.add_argument("--force-synth", action="store_true", help="re-synthesize recipes even when one exists")
    ap.add_argument("--no-vpic", action="store_true")
    ap.add_argument("--no-reconcile", action="store_true", help="report but do not retire rows the run did not return")
    ap.add_argument("--no-discover", action="store_true",
                    help="do not run the discovery browser capture for dealers whose recipe synthesis failed (default: run it once per dealer per day, in its own process)")
    args = ap.parse_args()

    manifest = load_manifest_dealers(args.manifest)
    if args.dealers:
        ids = [d.strip() for d in args.dealers.split(",") if d.strip()]
    else:
        ids = [d["dealer_id"] for i, d in enumerate(manifest) if i % max(1, args.shard_count) == args.shard_index]
        if args.limit:
            ids = ids[: args.limit]
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    started = datetime.now(timezone.utc).replace(microsecond=0)
    baseline_since = (started - timedelta(days=BASELINE_DAYS)).isoformat()
    since_iso = args.since or started.isoformat()

    conn = get_conn()
    known: dict[str, int] = {}
    for did in ids:
        # Rows seen in the 30 days before this run. Every active row would count
        # cars nobody has seen since July (Right Toyota: 3,034 "active", 1,367 seen
        # in September) and the floor then calls a full 1,273-car replay no_rows.
        known[did] = int(_rows(conn, "SELECT COUNT(*) AS n FROM cars WHERE dealer_id = ? AND COALESCE(listing_active,1)=1 AND scraped_at >= ?",
                                (did, baseline_since))[0]["n"])
    conn.close()

    recipe_info: dict[str, dict[str, Any]] = {}
    db_dealers = dealers_from_db([did for did in ids if not dealer_from_manifest(did, manifest)])
    discovered: dict[str, dict[str, Any]] = {}
    for did in ids:
        # 72 of the 222 stale active dealers (2026-09-26) are not in dealers.json
        # at all; their url and name live on their own rows. The manifest is a
        # convenience, never the source of truth for a dealer we already sell.
        d = dealer_from_manifest(did, manifest) or db_dealers.get(did) or {"dealer_id": did, "url": ""}
        if not d.get("url"):
            recipe_info[did] = {"had_recipes": 0, "synth": "not_in_manifest_or_db"}
            continue
        try:
            recipe_info[did] = ensure_recipe(d, force=args.force_synth)
            if (not args.no_discover and not recipe_info[did].get("had_recipes")
                    and not str(recipe_info[did].get("synth", "")).startswith("saved")):
                # Phase 1 of docs/HTTP_ONLY_SCANS_PLAN.md: the ONE sanctioned browser
                # use, in its own process, only for a dealer no HTTP template could
                # describe. It writes recipes + discovery.md, never car rows.
                discovered[did] = run_discovery_capture(did)
                if discovered[did].get("recipes_after", 0) > 0:
                    recipe_info[did]["synth"] = f"browser_capture_{discovered[did]['recipes_after']}"
                    recipe_info[did]["had_recipes"] = discovered[did]["recipes_after"]
                recipe_info[did]["discovery"] = discovered[did]
        except Exception as exc:  # noqa: BLE001
            import traceback as _tb

            recipe_info[did] = {"had_recipes": 0, "synth": f"error:{str(exc)[:100]}", "traceback": _tb.format_exc()[-1200:]}
        print(f"recipe  {did:36s} {json.dumps(recipe_info[did])[:140]}", flush=True)
        # Logs now, not at the end: the discovery entry for every dealer, and the
        # verbose probe (everything the site answered) for every synthesis failure.
        try:
            log_discovery(d, recipe_info[did], started.strftime("%Y-%m-%d %H:%M UTC"))
            synth = str(recipe_info[did].get("synth") or "")
            if d.get("url") and not recipe_info[did].get("had_recipes") and synth and not synth.startswith("saved"):
                from backend.scripts.discovery_probe import probe_dealer

                rep = probe_dealer(did, d["url"], paths=True)
                recipe_info[did]["probe"] = rep.get("classification")
                print(f"probe   {did:36s} {rep.get('classification')}", flush=True)
        except Exception as exc:  # noqa: BLE001
            print(f"log     discovery {did}: {str(exc)[:100]}", flush=True)

    if not args.skip_scan:
        scannable = [did for did in ids if recipe_info[did].get("had_recipes") or str(recipe_info[did].get("synth", "")).startswith("saved")]
        skipped = [did for did in ids if did not in scannable]
        if skipped:
            print(f"scan    skipping {len(skipped)} dealer(s) with no recipe: {', '.join(skipped)[:200]}", flush=True)
        chromium_leaks: list[dict[str, Any]] = []
        for i in range(0, len(scannable), args.batch):
            batch = scannable[i:i + args.batch]
            t0 = time.time()
            if not wait_for_db(args.lock_wait):
                print(f"scan    database unreachable for {args.lock_wait}s; stopping before batch {i // args.batch + 1}", flush=True)
                break
            # Another pipeline's shard frees the lock between ITS batches and grabs
            # it again a second later; on 2026-09-24 the mini's Stevenson batch lost
            # that race ("Scanner already running (lock held by pid …)"), the scanner
            # exited 0 and both dealers were logged as "no scan_runs row". Re-wait and
            # relaunch whenever the log says the lock was taken first.
            rc = -1
            for attempt in range(1, 7):
                if not wait_for_scanner_lock(args.lock_wait):
                    print("scan    scanner lock still held; aborting remaining batches", flush=True)
                    break
                log_path = out_dir / "scanner.log"
                mark = log_path.stat().st_size if log_path.exists() else 0
                chrome_before = chromium_process_count()
                rc = run_http_only_scan(batch, concurrency=len(batch), log_path=log_path, timeout_sec=args.scan_timeout)
                chrome_after = chromium_process_count()
                if chrome_after > max(chrome_before, 0):
                    print(f"scan    BROWSER LEAK: chromium processes {chrome_before} -> {chrome_after} during batch {', '.join(batch)[:80]}", flush=True)
                    chromium_leaks.append({"batch": batch, "before": chrome_before, "after": chrome_after})
                try:
                    with log_path.open("rb") as fh:
                        fh.seek(mark)
                        tail = fh.read().decode("utf-8", "replace")
                except OSError:
                    tail = ""
                if "Scanner already running" in tail and "refusing to start" in tail:
                    print(f"scan    lost the lock race (attempt {attempt}); re-waiting", flush=True)
                    time.sleep(5)
                    continue
                break
            else:
                print("scan    gave up after 6 lock races", flush=True)
            if rc == -1:
                break
            print(f"scan    batch {i // args.batch + 1}: {', '.join(batch)[:120]} rc={rc} in {time.time() - t0:.0f}s", flush=True)
        if not args.no_vpic and scannable:
            try:
                vp = vpic_for_dealers(scannable)
                print(f"vpic    {json.dumps(vp)[:200]}", flush=True)
            except Exception as exc:  # noqa: BLE001
                print(f"vpic    failed: {str(exc)[:120]}", flush=True)

    stamp = started.strftime("%Y-%m-%d %H:%M UTC")
    conn = get_conn()
    results = []
    try:
        for did in ids:
            r = assess(conn, did, since_iso, known[did], recipe_info[did])
            try:
                r["reconcile"] = reconcile_dealer(conn, did, since_iso, known[did], int(r.get("rows") or 0), str(r.get("verdict")),
                                                  dry_run=args.no_reconcile)
            except Exception as exc:  # noqa: BLE001
                r["reconcile"] = {"error": str(exc)[:120]}
            results.append(r)
            try:
                log_scan_run(r, stamp)
                write_instructions_if_first_success(r, stamp)
            except Exception as exc:  # noqa: BLE001
                print(f"log     scan_runs {did}: {str(exc)[:80]}", flush=True)
    finally:
        conn.close()

    live_recipes = sum(1 for r in results if (r.get("recipe") or {}).get("had_recipes") or str((r.get("recipe") or {}).get("synth", "")).startswith(("saved", "browser_capture")))
    triage = {
        "started": started.isoformat(), "dealers": results,
        "chromium_leaks": locals().get("chromium_leaks", []),
        "recipe_coverage": {"dealers": len(results), "with_recipe": live_recipes},
        "counts": {v: sum(1 for r in results if r.get("verdict") == v) for v in ("ok", "thin", "inaccurate", "no_rows", "error", "no_recipe")},
        "needs_discovery": [r["dealer_id"] for r in results if r.get("verdict") in ("no_rows", "error", "no_recipe")],
        "thin": [r["dealer_id"] for r in results if r.get("verdict") == "thin"],
        "inaccurate": [r["dealer_id"] for r in results if r.get("verdict") == "inaccurate"],
    }
    (out_dir / "triage.json").write_text(json.dumps(triage, indent=1, default=str), encoding="utf-8")
    lines = ["| dealer | verdict | rows new/used | before | platform | incomplete | discrepant | reason |", "|---|---|---:|---:|---|---:|---:|---|"]
    for r in results:
        acc = r.get("accuracy") or {}
        lines.append(f"| {r['dealer_id']} | {r.get('verdict')} | {r.get('rows', '-')} ({r.get('rows_new', '-')}/{r.get('rows_used', '-')}) | {r.get('known_before')} | "
                     f"{r.get('provider') or ','.join((r.get('recipe') or {}).get('providers') or []) or '-'} | {_pct(acc.get('incomplete_rows'), acc.get('rows'))} | "
                     f"{_pct(acc.get('hard_rows'), acc.get('rows'))} | {str(r.get('reason') or '')[:90]} |")
    (out_dir / "triage.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines))
    print(json.dumps(triage["counts"]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
