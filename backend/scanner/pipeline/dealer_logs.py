"""Per-dealer markdown logs (workspace/dealer_logs/<dealer_id>/: discovery.md, location.md,
scan_runs.md, scan_instructions.md, summary.md). CLAUDE.md: a scan without its logs is not finished. Moved verbatim from backend/scripts/dealer_pipeline.py (audit F11)."""
from __future__ import annotations

import json
from typing import Any

from backend.scanner.pipeline.constants import LOG_ROOT, PROCESS_DOC


# --------------------------------------------------------------------------
# logs (docs/NETWORK_SCAN_PROCESS.md: every dealer keeps discovery / scan_runs /
# location / summary / scan_instructions under workspace/dealer_logs/<dealer_id>/)
# --------------------------------------------------------------------------

def _log_append(dealer_id: str, name: str, block: str) -> None:
    d = LOG_ROOT / dealer_id
    d.mkdir(parents=True, exist_ok=True)
    p = d / name
    head = "" if p.exists() else f"# {dealer_id} — {name[:-3]}\n\nProcess: {PROCESS_DOC}\n\n"
    if name == "discovery.md":
        from backend.scanner.recipe_validation import strip_url_queries

        block = strip_url_queries(block)  # a recipe URL's query can carry an API key
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


def log_scan_run(r: dict[str, Any], stamp: str) -> None:
    did = r["dealer_id"]
    acc = r.get("accuracy") or {}
    cov = r.get("coverage") or {}
    L = [f"## {stamp} scan — verdict **{r.get('verdict')}** ({r.get('reason')})",
         f"- rows: captured {r.get('rows_captured', r.get('rows', '-'))}, refused by the VIN-owner guard {r.get('rows_refused_vin_owner', r.get('vin_owner_conflicts', '-'))}, "
         f"refused as other rooftops {r.get('rows_refused_rooftop', r.get('filtered_count', '-'))}, written {r.get('rows_written', '-')} "
         f"(new {r.get('rows_new', '-')}, used {r.get('rows_used', '-')}); listed before: {r.get('known_before')}; active after: {r.get('active_after')}; "
         f"inactive: {r.get('inactive_after', '-')}; run finished {r.get('run_finished_at') or '-'}, logged {stamp}",
         f"- recipe: {r.get('recipe_fetch')} coverage {json.dumps(r.get('recipe_coverage'))}; provider {r.get('provider')}; {r.get('minutes', '-')} min",
         f"- location: {r.get('rows_refused_rooftop', r.get('filtered_count', '-'))} row(s) refused as other rooftops, "
         f"{r.get('rows_refused_vin_owner', r.get('vin_owner_conflicts', '-'))} by the VIN-owner guard; dealer names on rows: {r.get('dealer_names')}; zip codes: {r.get('zip_codes')}",
         "- coverage before upsert: " + ", ".join(f"{k} {_pct(v, 1)}" for k, v in cov.items()),
         f"- http_first: {json.dumps(r.get('http_first'))}",
         f"- NHTSA: decode cached for {acc.get('vpic_cached', '-')}/{acc.get('rows', '-')} rows; heal this batch: {json.dumps(r.get('vpic_heal'))}",
         f"- incomplete after fill: {acc.get('incomplete_rows', '-')}/{acc.get('rows', '-')} ({_pct(acc.get('incomplete_rows'), acc.get('rows'))}); fields: {json.dumps(acc.get('missing'))}",
         f"- hard discrepancies: {acc.get('hard_rows', '-')} rows ({_pct(acc.get('hard_rows'), acc.get('rows'))}); by code: {json.dumps(acc.get('hard'))}",
         f"- reconcile: {json.dumps(r.get('reconcile'))}",
         f"- timing: {_timing_text(r)}; window for next run: vdp_http_first_max_sec={(r.get('timing') or {}).get('vdp_http_first_max_sec')} "
         f"pages_needed={(r.get('timing') or {}).get('pages_needed')} (fingerprint scan_hints.timing)"]
    for code, exs in (acc.get("examples") or {}).items():
        L.append(f"  - `{code}`: " + "; ".join(f"{e.get('car')} `{e.get('vin')}` {e.get('why', '')}"[:160] for e in exs[:3]))
    if r.get("scan_run_backfilled"):
        L.append(f"- scan_runs row backfilled by the pipeline: {r['scan_run_backfilled']} row(s) were upserted but the scanner wrote no run row (killed or paused)")
    if r.get("requeued"):
        L.append(f"- requeued once: {r['requeued']}")
    if r.get("handover_pending"):
        hp = r["handover_pending"]
        L.append(f"- handover_pending: {hp.get('vins')} retired VIN(s) freshly claimed by {json.dumps(hp.get('claimants'))}; e.g. {', '.join(hp.get('examples') or [])}")
    if r.get("new_arrival_incomplete"):
        na = r["new_arrival_incomplete"]
        L.append(f"- new arrivals: {na.get('incomplete')}/{na.get('rows')} incomplete ({na.get('rate', 0):.1%})")
    if r.get("error"):
        L.append(f"- error: {r['error']}")
    _log_append(did, "scan_runs.md", "\n".join(L))


def log_location(r: dict[str, Any], stamp: str) -> None:
    """location.md: what the rooftop gate and the VIN-owner guard did to this run's feed."""
    owners = r.get("vin_owner_conflict_owners") or {}
    L = [f"## {stamp} location — verdict {r.get('verdict')}",
         f"- feed rows captured: {r.get('rows_captured', r.get('rows', '-'))}; stamped to this store: {r.get('rows_stamped', '-')}",
         f"- refused as other rooftops: {r.get('rows_refused_rooftop', r.get('filtered_count', '-'))}",
         f"- refused by the VIN-owner guard: {r.get('rows_refused_vin_owner', r.get('vin_owner_conflicts', '-'))}"
         + (f" (owners: {json.dumps(owners)})" if owners else ""),
         f"- dealer names on rows: {r.get('dealer_names')}; zip codes: {r.get('zip_codes')}"]
    from backend.scanner.pipeline.assess import group_feed_flag

    flag = r.get("group_feed_unstamped") or group_feed_flag(r)
    if flag:
        L.append(f"- group_feed_unstamped: {json.dumps(flag)} (dealer_feed_scope stays as is until the owner approves)")
    _log_append(r["dealer_id"], "location.md", "\n".join(L))


def log_run_files(r: dict[str, Any], stamp: str) -> None:
    """Every run, whatever the verdict, leaves all five files (CLAUDE.md: a scan without its logs
    is not finished): scan_runs.md and location.md are appended; scan_instructions.md and
    summary.md exist after the first run, provisional until a verified (ok) run replaces them."""
    did = r["dealer_id"]
    log_scan_run(r, stamp)
    log_location(r, stamp)
    write_instructions_if_first_success(r, stamp)
    if not (LOG_ROOT / did / "discovery.md").exists():
        _log_append(did, "discovery.md", f"## {stamp} discovery\n- no discovery entry before this scan; recipes on file: {(r.get('recipe') or {}).get('had_recipes')}")
    _log_write_once(did, "summary.md", f"# {did} — summary\n\nProcess: {PROCESS_DOC}\n\n## Issues faced and how they were fixed\n\n")
    if r.get("verdict") != "ok":
        _log_write_once(did, "scan_instructions.md",
                        f"# {did} — scan instructions\n\nProcess: {PROCESS_DOC}\n\n{PROVISIONAL_MARK}: no verified run yet. "
                        f"First run {stamp}: verdict {r.get('verdict')} ({r.get('reason')}). Rewritten after the first ok run.\n")
        _log_append(did, "summary.md", f"- {stamp}: verdict {r.get('verdict')} — {r.get('reason')} (see scan_runs.md; record the fix here when found)")


PROVISIONAL_MARK = "PROVISIONAL"


def write_instructions_if_first_success(r: dict[str, Any], stamp: str) -> None:
    if r.get("verdict") != "ok":
        return
    did = r["dealer_id"]
    _prov = LOG_ROOT / did / "scan_instructions.md"
    try:
        if _prov.exists() and PROVISIONAL_MARK in _prov.read_text(encoding="utf-8")[:400]:
            _prov.unlink()  # a thin / inaccurate first run left a placeholder; this ok run writes the real one
    except OSError:
        pass
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


def _timing_text(r: dict[str, Any]) -> str:
    """``12.3 min cap_hit slow`` — the triage table's time column."""
    t = r.get("timing") or {}
    mins = t.get("minutes", r.get("minutes"))
    flags = " ".join(t.get("flags") or [])
    return f"{mins if mins is not None else '-'} min" + (f" {flags}" if flags else "")
