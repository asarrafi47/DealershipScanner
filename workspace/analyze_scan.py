#!/usr/bin/env python3
"""
Analyze a scanner log — either a JSONL scan log or a plain text scanner log.

Usage:
    # JSONL scan log (new format, written to workspace/scanlogs/)
    python workspace/analyze_scan.py workspace/scanlogs/scan_<timestamp>.jsonl

    # Plain text scanner log (tee'd output, older scans)
    python workspace/analyze_scan.py workspace/scan_chattanooga_50mi_20260619.log

    # Per-vehicle detail
    python workspace/analyze_scan.py <log> --cars

    # Filter to one dealer
    python workspace/analyze_scan.py <log> --dealer bmwofchattanooga-com
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from pathlib import Path


def load_jsonl(path: Path) -> tuple[list[dict], list[dict]]:
    summaries, vehicles = [], []
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            t = rec.get("type")
            if t == "dealer_summary":
                summaries.append(rec)
            elif t == "vehicle":
                vehicles.append(rec)
    return summaries, vehicles


_SUMMARY_RE = re.compile(r"dealer_run_summary (\{.*\})")


def load_text_log(path: Path) -> tuple[list[dict], list[dict]]:
    """Parse plain-text scanner log, extracting dealer_run_summary JSON lines."""
    summaries = []
    with path.open(encoding="utf-8", errors="replace") as fh:
        for line in fh:
            m = _SUMMARY_RE.search(line)
            if m:
                try:
                    rec = json.loads(m.group(1))
                    rec.setdefault("type", "dealer_summary")
                    summaries.append(rec)
                except json.JSONDecodeError:
                    pass
    return summaries, []  # text log has no per-vehicle lines


def fmt(v, width=0):
    s = "" if v is None else str(v)
    return s.rjust(width) if width else s


def dealer_table(summaries: list[dict]) -> None:
    cols = ["dealer_name", "provider", "upserted", "inventory_rows", "deduped_rows", "vdps_visited", "seconds", "error"]
    header = f"{'Dealer':<40} {'Provider':<20} {'Upserted':>8} {'InvRows':>7} {'Deduped':>7} {'VDPs':>5} {'Secs':>6}  Error"
    print(header)
    print("-" * len(header))
    total_upserted = 0
    for s in sorted(summaries, key=lambda x: x.get("dealer_name") or ""):
        name = (s.get("dealer_name") or s.get("dealer_id") or "")[:39]
        prov = (s.get("provider") or "unknown")[:19]
        upserted = s.get("upserted") or 0
        inv = s.get("inventory_rows") or 0
        deduped = s.get("deduped_rows") or 0
        vdps = s.get("vdps_visited") or 0
        secs = s.get("seconds") or 0.0
        err = (s.get("error") or "")[:60]
        total_upserted += upserted
        print(f"{name:<40} {prov:<20} {upserted:>8} {inv:>7} {deduped:>7} {vdps:>5} {secs:>6.1f}  {err}")
    print("-" * len(header))
    print(f"{'TOTAL':<40} {'':<20} {total_upserted:>8}")


def car_table(vehicles: list[dict], dealer_filter: str | None = None) -> None:
    if dealer_filter:
        vehicles = [v for v in vehicles if dealer_filter.lower() in (v.get("dealer_id") or "").lower()
                    or dealer_filter.lower() in (v.get("dealer_name") or "").lower()]
    print(f"\n{'VIN':<18} {'Year':>4} {'Make':<12} {'Model':<20} {'Trim':<18} {'Price':>8} {'Miles':>7} {'Color':<20} Dealer")
    print("-" * 130)
    for v in vehicles:
        vin = (v.get("vin") or "")[:17]
        year = v.get("year") or ""
        make = (v.get("make") or "")[:11]
        model = (v.get("model") or "")[:19]
        trim = (v.get("trim") or "")[:17]
        price = v.get("price")
        price_s = f"${price:,.0f}" if price else ""
        miles = v.get("mileage")
        miles_s = f"{miles:,}" if miles else ""
        color = (v.get("exterior_color") or "")[:19]
        dealer = (v.get("dealer_name") or v.get("dealer_id") or "")[:25]
        print(f"{vin:<18} {str(year):>4} {make:<12} {model:<20} {trim:<18} {price_s:>8} {miles_s:>7} {color:<20} {dealer}")
    print(f"\n{len(vehicles)} vehicle(s) shown")


def provider_breakdown(vehicles: list[dict], summaries: list[dict]) -> None:
    by_prov: dict[str, dict] = defaultdict(lambda: {"dealers": 0, "vehicles": 0})
    for s in summaries:
        p = s.get("provider") or "unknown"
        by_prov[p]["dealers"] += 1
    for v in vehicles:
        p = v.get("provider") or "unknown"
        by_prov[p]["vehicles"] += 1
    print(f"\n{'Provider':<25} {'Dealers':>7} {'Vehicles':>9}")
    print("-" * 45)
    for prov, counts in sorted(by_prov.items()):
        print(f"{prov:<25} {counts['dealers']:>7} {counts['vehicles']:>9}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Analyze a scanner JSONL scan log.")
    ap.add_argument("log", help="Path to the .jsonl scan log file")
    ap.add_argument("--cars", action="store_true", help="Print per-vehicle table")
    ap.add_argument("--dealer", metavar="NAME_OR_ID", default=None, help="Filter --cars to one dealer")
    args = ap.parse_args()

    path = Path(args.log)
    if not path.is_file():
        print(f"File not found: {path}", file=sys.stderr)
        sys.exit(1)

    if path.suffix == ".jsonl":
        summaries, vehicles = load_jsonl(path)
    else:
        summaries, vehicles = load_text_log(path)

    print(f"\n=== Scan log: {path.name} ===")
    print(f"Dealers scanned: {len(summaries)}   Vehicles captured: {len(vehicles)}\n")

    print("=== Per-Dealer Summary ===")
    dealer_table(summaries)

    print("\n=== Provider Breakdown ===")
    provider_breakdown(vehicles, summaries)

    if args.cars or args.dealer:
        print("\n=== Per-Vehicle Detail ===")
        car_table(vehicles, dealer_filter=args.dealer)


if __name__ == "__main__":
    main()
