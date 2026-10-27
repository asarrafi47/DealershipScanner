"""End-of-run outputs: triage table, needs_discovery.txt, slow_dealers.txt, platform clusters. Moved verbatim from backend/scripts/dealer_pipeline.py (audit F11)."""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from backend.scanner.pipeline.constants import INCOMPLETE_ALARM_MIN_ROWS, INCOMPLETE_ALARM_SHARE, LOG_ROOT, ROOT
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


def retirement_alarm_lines(results: list[dict[str, Any]]) -> list[str]:
    """One ``retirement alarm`` line per dealer whose scanner retirements this run exceeded the
    D-N21 threshold of its prior active rows (``reconcile.scanner_retirements``: 10% nightly,
    ``min(60%, 10% + 1.5% x days)`` under ``PIPELINE_RETIRE_ALARM_GAP_AWARE``). The pipeline
    itself retires nothing (P5A.7); this reads what the scanner wrote to ``listing_retirements``."""
    lines = []
    for r in results:
        sr = (r.get("reconcile") or {}).get("scanner_retired") or {}
        if sr.get("alarm"):
            lines.append(
                f"retirement alarm: {r['dealer_id']} retired {sr['retired']} of {sr['prior_active']} prior active rows "
                f"({sr['share']:.1%} > {sr['threshold']:.1%}; {sr['days']} days since the retired rows were last scraped"
                f"{', gap-aware' if sr.get('gap_aware') else ''})")
    return lines


OPEN_ITEMS_PATH = ROOT / "backend" / "config" / "incomplete_open_items.json"


def incomplete_open_items(path: Path | None = None) -> dict[str, str]:
    """Dealers whose high incomplete rate already has an open item (N21.5): ``{dealer_id: where}``.
    A missing or unreadable file means no dealer is excused."""
    try:
        data = json.loads((path or OPEN_ITEMS_PATH).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return {k: str(v) for k, v in data.items() if not k.startswith("_")} if isinstance(data, dict) else {}


def incomplete_alarm_lines(results: list[dict[str, Any]], open_items: dict[str, str] | None = None) -> list[str]:
    """N21.5 / IL-17: one ``incomplete alarm`` line per dealer above ``INCOMPLETE_ALARM_SHARE``
    incomplete rows (after the NHTSA fill, ``accuracy``) that no open item explains
    (``backend/config/incomplete_open_items.json``). Tiny runs (under
    ``INCOMPLETE_ALARM_MIN_ROWS`` rows) are skipped. Reported only."""
    excused = incomplete_open_items() if open_items is None else open_items
    lines = []
    for r in results:
        acc = r.get("accuracy") or {}
        n, inc = int(acc.get("rows") or 0), int(acc.get("incomplete_rows") or 0)
        if n < INCOMPLETE_ALARM_MIN_ROWS or inc / n <= INCOMPLETE_ALARM_SHARE or r["dealer_id"] in excused:
            continue
        missing = ", ".join(list(acc.get("missing") or [])[:4])
        lines.append(f"incomplete alarm: {r['dealer_id']} {inc} of {n} rows incomplete ({inc / n:.0%} > "
                     f"{INCOMPLETE_ALARM_SHARE:.0%}), no open item{f'; missing {missing}' if missing else ''}")
    return lines


def group_feed_unstamped(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """WI-3 / D-N8: one ``{dealer_id, sister, conflicts, share, captured}`` per dealer whose feed is
    mostly one sister's VINs (``assess.group_feed_flag``). Reported, never acted on:
    ``dealer_feed_scope='group'`` is set only with the owner's OK."""
    from backend.scanner.pipeline.assess import group_feed_flag

    flags = []
    for r in results:
        f = group_feed_flag(r)
        if f:
            r["group_feed_unstamped"] = f
            flags.append({"dealer_id": r["dealer_id"], **f})
    return flags


def group_feed_lines(flags: list[dict[str, Any]]) -> list[str]:
    return [f"group_feed_unstamped: {f['dealer_id']} <-> {f['sister']}: {f['conflicts']} of {f['captured']} feed VINs "
            f"({f['share']:.0%}) belong to the sister; dealer_feed_scope unchanged (owner OK needed)" for f in flags]


def handover_pending_lines(results: list[dict[str, Any]]) -> list[str]:
    """B4: dealers that retired a VIN another store has since claimed (fresh conflict naming them owner)."""
    return [f"handover_pending: {r['dealer_id']} retired {hp['vins']} VIN(s) now claimed by "
            f"{', '.join(f'{k} {v}' for k, v in (hp.get('claimants') or {}).items())} (e.g. {', '.join(hp.get('examples') or [])})"
            for r in results for hp in [r.get("handover_pending")] if hp]


def accounting_line(results: list[dict[str, Any]]) -> str:
    from backend.scanner.pipeline.assess import fleet_accounting

    a = fleet_accounting(results)
    return (f"accounting: captured {a['rows_captured']}, refused by the VIN-owner guard {a['rows_refused_vin_owner']}, "
            f"refused as other rooftops {a['rows_refused_rooftop']}, written {a['rows_written']}; active {a['active']}, inactive {a['inactive']}")


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
