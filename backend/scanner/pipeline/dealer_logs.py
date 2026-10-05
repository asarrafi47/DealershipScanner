"""Per-dealer markdown logs (workspace/dealer_logs/<dealer_id>/: discovery.md, scan_runs.md,
scan_instructions.md, summary.md). CLAUDE.md: a scan without its logs is not finished. Moved verbatim from backend/scripts/dealer_pipeline.py (audit F11)."""
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
         f"- rows written: {r.get('rows', '-')} (new {r.get('rows_new', '-')}, used {r.get('rows_used', '-')}); listed before: {r.get('known_before')}; active after: {r.get('active_after')}",
         f"- recipe: {r.get('recipe_fetch')} coverage {json.dumps(r.get('recipe_coverage'))}; provider {r.get('provider')}; {r.get('minutes', '-')} min",
         f"- location: {r.get('filtered_count', '-')} row(s) refused as other rooftops; dealer names on rows: {r.get('dealer_names')}; zip codes: {r.get('zip_codes')}",
         "- coverage before upsert: " + ", ".join(f"{k} {_pct(v, 1)}" for k, v in cov.items()),
         f"- http_first: {json.dumps(r.get('http_first'))[:300]}",
         f"- NHTSA: decode cached for {acc.get('vpic_cached', '-')}/{acc.get('rows', '-')} rows; heal this batch: {json.dumps(r.get('vpic_heal'))[:200]}",
         f"- incomplete after fill: {acc.get('incomplete_rows', '-')}/{acc.get('rows', '-')} ({_pct(acc.get('incomplete_rows'), acc.get('rows'))}); fields: {json.dumps(acc.get('missing'))}",
         f"- hard discrepancies: {acc.get('hard_rows', '-')} rows ({_pct(acc.get('hard_rows'), acc.get('rows'))}); by code: {json.dumps(acc.get('hard'))}",
         f"- reconcile: {json.dumps(r.get('reconcile'))}",
         f"- timing: {_timing_text(r)}; window for next run: vdp_http_first_max_sec={(r.get('timing') or {}).get('vdp_http_first_max_sec')} "
         f"pages_needed={(r.get('timing') or {}).get('pages_needed')} (fingerprint scan_hints.timing)"]
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


def _timing_text(r: dict[str, Any]) -> str:
    """``12.3 min cap_hit slow`` — the triage table's time column."""
    t = r.get("timing") or {}
    mins = t.get("minutes", r.get("minutes"))
    flags = " ".join(t.get("flags") or [])
    return f"{mins if mins is not None else '-'} min" + (f" {flags}" if flags else "")
