#!/usr/bin/env python3
"""Backfill the timing block of every dealer's fingerprint from ``scan_runs``.

The pipeline's assess step writes ``scan_hints["timing"]`` after each run
(backend/scanner/scan_timing.py); this script does the same for runs that
happened before that existed, or after a period when the recipe store was
unreachable. For each dealer with scan_runs rows since ``--since`` it takes the
last 5 runs, folds them onto whatever timing block is already stored, writes
the result with ``set_scan_hints(merge=True)`` and prints a table.

Usage:
  python -m backend.scripts.fingerprint_timing                         # last 7 days, write
  python -m backend.scripts.fingerprint_timing --since 2026-09-20T00:00:00+00:00 --dry-run
  python -m backend.scripts.fingerprint_timing --dealers avondaletoyota-com,mblaguna-com
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from backend.utils.project_env import load_project_dotenv

load_project_dotenv()

from backend.db.inventory_db import get_conn  # noqa: E402
from backend.scanner.scan_timing import merge_timing, minutes, timing_entry  # noqa: E402

KEEP_RUNS = 5


def load_runs(conn, since_iso: str, dealers: list[str] | None = None, keep: int = KEEP_RUNS) -> dict[str, list[dict[str, Any]]]:
    """{dealer_id: [scan_runs rows, newest first, at most ``keep``]} since ``since_iso``."""
    sql = ("SELECT id, dealer_id, finished_at, duration_seconds, error, summary_json FROM scan_runs "
           "WHERE finished_at >= ?")
    params: list[Any] = [since_iso]
    if dealers:
        sql += " AND dealer_id IN (" + ",".join("?" * len(dealers)) + ")"
        params.extend(dealers)
    sql += " ORDER BY dealer_id, finished_at DESC, id DESC"
    cur = conn.execute(sql, tuple(params))
    cols = [d[0] for d in cur.description]
    out: dict[str, list[dict[str, Any]]] = {}
    for raw in cur.fetchall():
        row = dict(zip(cols, raw))
        bucket = out.setdefault(str(row["dealer_id"]), [])
        if len(bucket) < keep:
            bucket.append(row)
    return out


def build_timing(conn, since_iso: str, dealers: list[str] | None = None, *,
                 existing: Callable[[str], dict[str, Any]] | None = None, keep: int = KEEP_RUNS) -> dict[str, dict[str, Any]]:
    """{dealer_id: merged timing block} for every dealer with runs since ``since_iso``.

    ``existing`` returns the dealer's current scan_hints (default: none), so runs
    already stored but older than the window survive the fold."""
    runs_by_dealer = load_runs(conn, since_iso, dealers, keep)
    out: dict[str, dict[str, Any]] = {}
    for did, rows in sorted(runs_by_dealer.items()):
        hints: dict[str, Any] = dict(existing(did) or {}) if existing else {}
        for row in reversed(rows):  # oldest first so the newest ends on top
            hints = {"timing": merge_timing(hints, timing_entry(row), keep=keep)}
        out[did] = hints["timing"]
    return out


def format_table(blocks: dict[str, dict[str, Any]]) -> str:
    head = f"{'dealer':40s} {'last run':25s} {'min':>6s}  {'window':>7s} {'pages':>6s}  flags"
    lines = [head, "-" * len(head)]
    for did, t in blocks.items():
        latest = (t.get("runs") or [{}])[0]
        win = t.get("vdp_http_first_max_sec")
        lines.append(f"{did[:40]:40s} {str(latest.get('finished_at') or '')[:25]:25s} {minutes(latest):6.1f}  "
                     f"{(str(win) + 's') if win else '-':>7s} {str(t.get('pages_needed') or '-'):>6s}  {' '.join(t.get('flags') or []) or '-'}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--since", default="", help="ISO timestamp; default: 7 days ago")
    ap.add_argument("--dealers", default="", help="comma-separated dealer ids (default: every dealer with runs)")
    ap.add_argument("--dry-run", action="store_true", help="print the table, write nothing")
    args = ap.parse_args(argv)

    since = args.since or (datetime.now(timezone.utc) - timedelta(days=7)).replace(microsecond=0).isoformat()
    dealers = [d.strip() for d in args.dealers.split(",") if d.strip()] or None

    from backend.scanner.recipe_store import get_scan_hints, set_scan_hints

    conn = get_conn()
    try:
        blocks = build_timing(conn, since, dealers, existing=get_scan_hints)
    finally:
        conn.close()
    print(f"scan_runs since {since}: {len(blocks)} dealer(s)" + (" (dry run)" if args.dry_run else ""))
    print(format_table(blocks))
    if args.dry_run:
        return 0
    written = sum(1 for did, t in blocks.items() if set_scan_hints(did, {"timing": t}, merge=True))
    print(f"wrote timing for {written}/{len(blocks)} dealer(s)")
    return 0 if written == len(blocks) else 1


if __name__ == "__main__":
    sys.exit(main())
