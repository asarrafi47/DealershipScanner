#!/usr/bin/env python3
"""Run the dealer gates, the fleet gate and the improvement queue over the triage files of a run.

Usage:
  python -m backend.scripts.ops_gates --run DIR [DIR ...] [--prev DIR] [--attempts DIR] \\
      [--registry PATH] [--side-inputs FILE] [--now ISO] --out DIR

``--run`` takes one or more shard directories (or a bundle directory) holding ``triage.json`` or
``triage.json.gz``; a ``fleet_summary.json`` in one of them feeds the fleet gate. ``--prev`` is the
previous run's output directory (its ``gate_report.json`` carries the dealer histories and the fleet
metrics). ``--attempts`` is a directory of ``fix_cycles/<dealer>.json`` ledgers. ``--registry`` is the
accepted-gap list (``deploy/railway/accepted_gaps.json``). ``--side-inputs`` is the side-input file of
GATES.md section 12.2. Rows that carry no ``extras`` go through ``legacy_inputs``.

The clock is never read: ``--now`` dates the report and defaults to the ``started`` time of the run.
Writes ``gate_report.json`` and ``queue.json`` into ``--out`` (deterministic bytes) and prints one line.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from backend.scanner.loop.bundle import load_shard, merge_shards
from backend.scanner.loop.config import DEALER_ID_RE, DEFAULTS
from backend.scanner.loop.gates import DEFAULT_CONFIG, evaluate_fleet, evaluate_run, legacy_inputs
from backend.scanner.loop.queue import fix_units, score_dealers, select_cycle
from backend.scanner.loop.scrub import clean_line

SCHEMA = 1
MAX_JSON_BYTES = 1 << 20  # one input file
TEXT_CAP = 200


def _read_json(path: Path):
    if path.stat().st_size > MAX_JSON_BYTES:
        raise ValueError(path.name + ": file too large")
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, RecursionError) as exc:
        raise ValueError(path.name + ": unreadable JSON (" + type(exc).__name__ + ")") from None


def _sanitize(node):
    """Copy of ``node`` with every string and key cut and cleaned (dealer-derived text is untrusted)."""
    if isinstance(node, str):
        return clean_line(node, TEXT_CAP)
    if isinstance(node, dict):
        return {clean_line(str(k), TEXT_CAP): _sanitize(v) for k, v in node.items()}
    if isinstance(node, (list, tuple)):
        return [_sanitize(v) for v in node]
    return node


def _dump(obj) -> bytes:
    return (json.dumps(_sanitize(obj), separators=(",", ":"), sort_keys=True) + "\n").encode("utf-8")


def _load_attempts(directory: Path) -> dict:
    """``{dealer_id: ledger}`` from ``<dealer>.json`` files; names that are not dealer ids are ignored."""
    out = {}
    for path in sorted(directory.glob("*.json")):
        if not DEALER_ID_RE.match(path.stem):
            continue
        record = _read_json(path)
        if isinstance(record, dict):
            out[path.stem] = record
    return out


def _load_prev(directory: Path):
    """``(histories, prior_runs)`` from the previous output directory (empty without a report)."""
    path = directory / "gate_report.json"
    if not path.is_file():
        return {}, []
    report = _read_json(path)
    dealers = report.get("dealers") if isinstance(report, dict) and isinstance(report.get("dealers"), dict) else {}
    histories = {k: v["history"] for k, v in dealers.items() if isinstance(v, dict) and isinstance(v.get("history"), dict)}
    fleet = report.get("fleet") if isinstance(report, dict) and isinstance(report.get("fleet"), dict) else {}
    prior = [{"outcome": fleet.get("outcome"), "metrics": fleet.get("metrics")}] if fleet else []
    return histories, prior


def build(run_dirs, prev_dir, attempts_dir, registry_path, side_path, out_dir, now_arg):
    shards = [load_shard(d) for d in run_dirs]
    merged = merge_shards(shards)
    entries = merged["dealers"]
    now = now_arg or merged.get("started")
    if not now:
        raise ValueError("the triage files carry no start time: pass --now")
    registry = _read_json(registry_path) if registry_path else []
    side = _read_json(side_path) if side_path else None
    attempts = _load_attempts(attempts_dir) if attempts_dir else {}
    histories, prior_runs = _load_prev(prev_dir) if prev_dir else ({}, [])
    summary = None
    for directory in run_dirs:
        if (directory / "fleet_summary.json").is_file():
            summary = _read_json(directory / "fleet_summary.json")
            break

    extras = {}
    for entry in entries:
        given = entry.get("extras")
        extras[entry["dealer_id"]] = given if isinstance(given, dict) else legacy_inputs(entry, side)

    results = evaluate_run(entries, extras, histories, registry, DEFAULT_CONFIG, now)
    fleet = evaluate_fleet({"entries": entries, "results": results, "extras": extras, "started": merged.get("started")},
                           prior_runs, summary, DEFAULT_CONFIG)
    if fleet["outcome"] == "FAIL":  # a FAIL run leaves the histories unchanged
        results = evaluate_run(entries, extras, histories, registry, DEFAULT_CONFIG, now, outcome="FAIL")

    scored = score_dealers(entries, results, extras)
    units = fix_units(scored["ranked"])
    cycle = select_cycle(scored["ranked"], attempts, (), DEFAULTS)
    report = {
        "schema": SCHEMA,
        "now": now,
        "run": {"started": merged.get("started"), "dealers": len(entries)},
        "fleet": fleet,
        "counts": results["counts"],
        "active": results["active"],
        "dealers": {
            dealer_id: dict(result, history=results["history"][dealer_id])
            for dealer_id, result in results["dealers"].items()
        },
    }
    queue = {
        "schema": SCHEMA,
        "now": now,
        "ranked": scored["ranked"],
        "fix_units": units,
        "cycle": cycle["cycle"],
        "pending": cycle["pending"],
        "owner_list": sorted(scored["excluded"] + cycle["owner_list"], key=lambda x: (x["why"], x["dealer_id"])),
        "limits": {"dealers_per_cycle": DEFAULTS.dealers_per_cycle, "attempts_per_dealer": DEFAULTS.attempts_per_dealer},
    }
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "gate_report.json").write_bytes(_dump(report))
    (out_dir / "queue.json").write_bytes(_dump(queue))
    counts = results["counts"]
    return (
        "gates: dealers=" + str(len(entries))
        + " ok=" + str(counts["ok"])
        + " needs_recipe_work=" + str(counts["needs_recipe_work"])
        + " needs_code_work=" + str(counts["needs_code_work"])
        + " blocked=" + str(counts["blocked"])
        + " unassessed=" + str(counts["unassessed"])
        + " fleet=" + fleet["outcome"]
        + " queued=" + str(len(scored["ranked"]))
        + " units=" + str(len(units))
        + " cycle=" + str(len(cycle["cycle"]))
        + " owner=" + str(len(queue["owner_list"]))
    )


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Dealer gates, fleet gate and improvement queue for one run.")
    parser.add_argument("--run", nargs="+", required=True, type=Path, metavar="DIR")
    parser.add_argument("--prev", type=Path, metavar="DIR")
    parser.add_argument("--attempts", type=Path, metavar="DIR")
    parser.add_argument("--registry", type=Path, metavar="PATH")
    parser.add_argument("--side-inputs", type=Path, metavar="FILE", dest="side")
    parser.add_argument("--now", metavar="ISO")
    parser.add_argument("--out", required=True, type=Path, metavar="DIR")
    args = parser.parse_args(argv)
    try:
        line = build(args.run, args.prev, args.attempts, args.registry, args.side, args.out, args.now)
    except (OSError, ValueError) as exc:
        print("ops_gates: " + clean_line(str(exc), 300), file=sys.stderr)
        return 2
    print(line)
    return 0


if __name__ == "__main__":
    sys.exit(main())
