"""End-of-run outputs: triage table, needs_discovery.txt, slow_dealers.txt, platform clusters. Moved verbatim from backend/scripts/dealer_pipeline.py (audit F11)."""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from backend.scanner.pipeline.constants import LOG_ROOT
from backend.scanner.pipeline.dealer_logs import _pct, _timing_text


def triage_table(results: list[dict[str, Any]]) -> list[str]:
    lines = ["| dealer | verdict | rows new/used | before | platform | incomplete | discrepant | time | lifecycle | reason |",
             "|---|---|---:|---:|---|---:|---:|---|---|---|"]
    for r in results:
        acc = r.get("accuracy") or {}
        lc = str(r.get("lifecycle") or "none") + (f" ({r['retried']})" if r.get("retried") else "")
        lines.append(f"| {r['dealer_id']} | {r.get('verdict')} | {r.get('rows', '-')} ({r.get('rows_new', '-')}/{r.get('rows_used', '-')}) | {r.get('known_before')} | "
                     f"{r.get('provider') or ','.join((r.get('recipe') or {}).get('providers') or []) or '-'} | {_pct(acc.get('incomplete_rows'), acc.get('rows'))} | "
                     f"{_pct(acc.get('hard_rows'), acc.get('rows'))} | {_timing_text(r)} | {lc} | {str(r.get('reason') or '')[:90]} |")
    return lines


def platform_cluster_lines(needs_discovery: list[str]) -> list[str]:
    """End-of-run platform clustering (backend/scripts/platform_candidates.py):
    fingerprints every dealer under LOG_ROOT without a live recipe, rewrites
    ``_learning/platform_candidates.md`` and returns one line per cluster that
    holds two or more of this run's ``needs_discovery`` dealers —
    ``discovery: N dealers share unknown platform <signature> (a, b, c) → …``.
    Never raises; an empty list when nothing clusters."""
    if not needs_discovery:
        return []
    try:
        from backend.scripts.platform_candidates import report_for_run

        return list(report_for_run(list(needs_discovery), root=LOG_ROOT))
    except Exception as exc:  # noqa: BLE001 - the triage must still be written
        return [f"discovery: platform clustering skipped: {str(exc)[:160]}"]


def write_needs_discovery(results: list[dict[str, Any]], out_dir: Path) -> list[str]:
    """``<out>/needs_discovery.txt``: one line per dealer still failing at the end
    of the run (``dealer_id  <verdict>  <lifecycle>  <last reason>``), the input
    of the dealer-discovery workflow and of ``discovery_probe --from-file``."""
    lines = []
    for r in results:
        verdict = str(r.get("verdict") or "")
        lc = str(r.get("lifecycle") or "none")
        if verdict in ("no_rows", "error", "no_recipe") or lc.startswith("failed:"):
            lines.append(f"{r['dealer_id']}  {verdict}  {lc}  {str(r.get('reason') or '')[:160]}")
    if lines:
        with (out_dir / "needs_discovery.txt").open("a", encoding="utf-8") as fh:
            fh.write("\n".join(lines) + "\n")
    return lines


def write_slow_dealers(results: list[dict[str, Any]], out_dir: Path, started: datetime) -> list[str]:
    """``<out>/slow_dealers.txt`` (one ``dealer_id  flags  minutes`` line per dealer
    that errored, timed out, hit a cap, exhausted its host or ran very long) and
    the same dealers as lines in ``_learning/errors_index.md``. Returns the ids."""
    from backend.scanner.scan_timing import needs_attention

    flagged = [r for r in results if needs_attention((r.get("timing") or {}).get("flags"))]
    lines = [f"{r['dealer_id']}  {'+'.join((r.get('timing') or {}).get('flags') or [])}  {(r.get('timing') or {}).get('minutes', r.get('minutes', '-'))}"
             for r in flagged]
    (out_dir / "slow_dealers.txt").write_text(("\n".join(lines) + "\n") if lines else "", encoding="utf-8")
    if flagged:
        idx = LOG_ROOT / "_learning" / "errors_index.md"
        idx.parent.mkdir(parents=True, exist_ok=True)
        stamp = started.isoformat()
        with idx.open("a", encoding="utf-8") as fh:
            if idx.stat().st_size == 0:
                fh.write("# errors_index\n\n(one entry per lesson; say which dealer log shows the evidence)\n")
            for r in flagged:
                t = r.get("timing") or {}
                fh.write(f"- {stamp} scan_timing_{'+'.join(t.get('flags') or [])} -> {r['dealer_id']} "
                         f"({t.get('minutes', r.get('minutes', '-'))} min; window {t.get('vdp_http_first_max_sec')}s/{t.get('pages_needed')} pages; "
                         f"workspace/dealer_logs/{r['dealer_id']}/scan_runs.md)\n")
    return [r["dealer_id"] for r in flagged]
