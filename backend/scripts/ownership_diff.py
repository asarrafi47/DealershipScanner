#!/usr/bin/env python3
"""Compare two ownership snapshots; census conflicts against one (N10.7, CF-14 tooling).

Snapshots come from ``scripts/ownership_snapshot.sh`` (CSV columns ``id, vin, dealer_id,
listing_active, scraped_at``). Everything here reads files; there is no database access.

  ownership_diff.py BEFORE.csv AFTER.csv [--moves MOVES.csv] [--json]
      Moves per (from, to) pair, plus VINs that appeared, vanished, were retired or were
      re-listed. With ``--moves`` (a ``car_move_log`` snapshot, ``*_moves.csv``) every moved
      pair is cross-referenced: how many of its VINs have a non-reverted log row for the
      same from/to, and how many have none (a move nobody logged).

  ownership_diff.py --census SNAPSHOT.csv --conflicts CONFLICTS.csv
      Where each ``vin_owner_conflicts`` row's VIN sat in SNAPSHOT: held by the claimant or
      the owner (active or retired), held by a third dealer, or absent. This is the split in
      CLEANROOM_RCA_2026_10 section 4.2 (claimant 3,941 + 59, owner 1,344 + 26, third dealer
      931 + 289, absent 3,048 on the 09-28 ``cars``).

A VIN with several rows in one snapshot is held by its active row (newest ``scraped_at``
first), else by its newest retired row.
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

CENSUS_CLASSES = (
    "claimant_active", "owner_active", "other_active",
    "other_retired", "claimant_retired", "owner_retired", "absent",
)


def _truthy(v: Any) -> bool:
    return str(v).strip().lower() in ("1", "t", "true", "y", "yes")


def load_snapshot(path: str | Path) -> dict[str, dict[str, Any]]:
    """vin -> {'dealer_id', 'active', 'car_id', 'scraped_at'} (one holder per VIN)."""
    held: dict[str, dict[str, Any]] = {}
    with open(path, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            vin = (row.get("vin") or "").strip().upper()
            if not vin:
                continue
            cand = {
                "dealer_id": (row.get("dealer_id") or "").strip(),
                "active": _truthy(row.get("listing_active")),
                "car_id": (row.get("id") or "").strip(),
                "scraped_at": row.get("scraped_at") or "",
            }
            cur = held.get(vin)
            if cur is None or (cand["active"], cand["scraped_at"]) > (cur["active"], cur["scraped_at"]):
                held[vin] = cand
    return held


def load_moves(path: str | Path) -> list[dict[str, str]]:
    with open(path, newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def diff_snapshots(before: dict[str, dict[str, Any]], after: dict[str, dict[str, Any]],
                   moves: list[dict[str, str]] | None = None) -> dict[str, Any]:
    pairs: dict[tuple[str, str], list[str]] = defaultdict(list)
    appeared: Counter = Counter()
    vanished: Counter = Counter()
    retired: Counter = Counter()
    relisted: Counter = Counter()
    unchanged = 0
    for vin, b in before.items():
        a = after.get(vin)
        if a is None:
            vanished[b["dealer_id"]] += 1
        elif a["dealer_id"] != b["dealer_id"]:
            pairs[(b["dealer_id"], a["dealer_id"])].append(vin)
        elif b["active"] and not a["active"]:
            retired[a["dealer_id"]] += 1
        elif a["active"] and not b["active"]:
            relisted[a["dealer_id"]] += 1
        else:
            unchanged += 1
    for vin, a in after.items():
        if vin not in before:
            appeared[a["dealer_id"]] += 1

    logged: dict[tuple[str, str, str], int] = {}
    if moves is not None:
        # car_move_log keys on car_id; a snapshot maps car ids back to VINs
        vin_by_car = {h["car_id"]: v for snap in (before, after) for v, h in snap.items() if h["car_id"]}
        for m in moves:
            if (m.get("reverted_at") or "").strip():
                continue
            vin = vin_by_car.get((m.get("car_id") or "").strip())
            if vin:
                logged[(vin, (m.get("from_dealer_id") or "").strip(), (m.get("to_dealer_id") or "").strip())] = 1

    out_pairs = []
    for (frm, to), vins in sorted(pairs.items(), key=lambda kv: (-len(kv[1]), kv[0])):
        row: dict[str, Any] = {
            "from": frm, "to": to, "vins": len(vins),
            "active_after": sum(1 for v in vins if after[v]["active"]),
        }
        if moves is not None:
            n = sum(1 for v in vins if (v, frm, to) in logged)
            row["logged"], row["unlogged"] = n, len(vins) - n
        out_pairs.append(row)
    total_moved = sum(len(v) for v in pairs.values())
    return {
        "before_vins": len(before), "after_vins": len(after), "moved": total_moved,
        "unchanged": unchanged, "pairs": out_pairs,
        "appeared": sum(appeared.values()), "vanished": sum(vanished.values()),
        "retired": sum(retired.values()), "relisted": sum(relisted.values()),
        "vanished_by_dealer": dict(vanished.most_common()),
        "appeared_by_dealer": dict(appeared.most_common()),
    }


def census_conflicts(snapshot: dict[str, dict[str, Any]], conflicts: list[dict[str, str]]) -> dict[str, int]:
    """Classify each (vin, owner, claimant) conflict row by who held the VIN in ``snapshot``."""
    out = {k: 0 for k in CENSUS_CLASSES}
    for c in conflicts:
        vin = (c.get("vin") or "").strip().upper()
        owner = (c.get("owner_dealer_id") or "").strip()
        claimant = (c.get("claimant_dealer_id") or "").strip()
        h = snapshot.get(vin)
        if h is None:
            out["absent"] += 1
            continue
        who = "claimant" if h["dealer_id"] == claimant else "owner" if h["dealer_id"] == owner else "other"
        out[f"{who}_{'active' if h['active'] else 'retired'}"] += 1
    return out


def census_pairs(snapshot: dict[str, dict[str, Any]], conflicts: list[dict[str, str]]) -> list[dict[str, Any]]:
    """The census split per (owner, claimant) pair, largest pair first."""
    by_pair: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    for c in conflicts:
        by_pair[((c.get("owner_dealer_id") or "").strip(), (c.get("claimant_dealer_id") or "").strip())].append(c)
    rows = []
    for (owner, claimant), cs in by_pair.items():
        rows.append({"owner": owner, "claimant": claimant, "rows": len(cs), **census_conflicts(snapshot, cs)})
    rows.sort(key=lambda r: (-r["rows"], r["owner"], r["claimant"]))
    return rows


def _print_diff(d: dict[str, Any], top: int) -> None:
    print(f"VINs before {d['before_vins']}, after {d['after_vins']}: moved {d['moved']}, unchanged {d['unchanged']}, "
          f"appeared {d['appeared']}, vanished {d['vanished']}, retired in place {d['retired']}, re-listed {d['relisted']}")
    has_log = d["pairs"] and "logged" in d["pairs"][0]
    print("from -> to (VINs, active after" + (", logged, unlogged" if has_log else "") + ")")
    for p in d["pairs"][:top]:
        extra = f"  logged {p['logged']}  unlogged {p['unlogged']}" if has_log else ""
        print(f"  {p['from']} -> {p['to']}: {p['vins']}  active {p['active_after']}{extra}")
    if len(d["pairs"]) > top:
        print(f"  ... {len(d['pairs']) - top} more pair(s); use --json for all")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("files", nargs="*", help="BEFORE.csv AFTER.csv")
    ap.add_argument("--moves", help="car_move_log snapshot (*_moves.csv) to cross-reference")
    ap.add_argument("--census", metavar="SNAPSHOT", help="classify --conflicts against this snapshot")
    ap.add_argument("--conflicts", help="vin_owner_conflicts snapshot (*_conflicts.csv)")
    ap.add_argument("--json", action="store_true", help="print JSON instead of text")
    ap.add_argument("--top", type=int, default=25, help="pairs to print in text mode")
    args = ap.parse_args(argv)

    if args.census:
        if not args.conflicts:
            ap.error("--census needs --conflicts")
        snap = load_snapshot(args.census)
        conflicts = load_moves(args.conflicts)
        total = census_conflicts(snap, conflicts)
        if args.json:
            print(json.dumps({"total": total, "pairs": census_pairs(snap, conflicts)}, indent=2))
        else:
            print(f"{len(conflicts)} conflict row(s) against {args.census}")
            for k in CENSUS_CLASSES:
                print(f"  {k}: {total[k]}")
            for r in census_pairs(snap, conflicts)[: args.top]:
                print(f"  {r['owner']} / {r['claimant']}: {r['rows']} " +
                      " ".join(f"{k}={r[k]}" for k in CENSUS_CLASSES if r[k]))
        return 0

    if len(args.files) != 2:
        ap.error("give BEFORE.csv and AFTER.csv (or --census)")
    d = diff_snapshots(load_snapshot(args.files[0]), load_snapshot(args.files[1]),
                       load_moves(args.moves) if args.moves else None)
    if args.json:
        print(json.dumps(d, indent=2))
    else:
        _print_diff(d, args.top)
    return 0


if __name__ == "__main__":
    sys.exit(main())
