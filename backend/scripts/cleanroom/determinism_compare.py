#!/usr/bin/env python3
"""N11.4a: same-commit determinism check between two clean-room DBs (read only).

    backend/scripts/cleanroom/determinism_compare.py --a cars_cr_rc --b cars_cr_rc_b \\
        [--run-dir-a <pass A run dir>] [--run-dir-b <pass B run dir>] \\
        [--pools backend/config/group_pools.json] [--min-agree 99] --json <out>

Pass A (forward shard order) and pass B (``SCAN_SHARD_ORDER=reverse``, the 109 conflict-group
dealers) ran on the same ``RC_SHA``. For every VIN active in both DBs this joins VIN to
``dealer_id`` and reports the agreement percentage. Each VIN filed under a different dealer is
listed with its pair and three facts: whether the pair is flagged ``group_feed_unstamped``
(a ``vin_owner_conflicts`` row between the two dealers in either DB, or a triage flag in
either run dir), and whether ``group_pools.json`` already covers the pair (one pool's members,
or an alias set). N11.4 accepts >= 99% agreement and only flagged, not-yet-pooled differences.

Both DBs are opened through the acceptance tool's read-only guard (db.open_target); nothing
is written except ``--json``. Exit codes: 0 accepted, 1 below ``--min-agree`` or an
unexplained difference, 2 refused (not read-only, bad arguments).
"""

from __future__ import annotations

import argparse
import collections
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.scripts.cleanroom import run_artifacts  # noqa: E402
from backend.scripts.cleanroom.db import PG_HOST_DEFAULT, PG_PORT_DEFAULT, ReadOnlyRequired, Target, open_target  # noqa: E402

EXIT_OK, EXIT_FAIL, EXIT_REFUSED = 0, 1, 2
DEFAULT_POOLS = ROOT / "backend" / "config" / "group_pools.json"


def active_owners(t: Target) -> dict[str, str]:
    """VIN -> dealer_id for the active rows (one VIN has at most one active owner)."""
    out: dict[str, str] = {}
    for vin, dealer in t.rows("SELECT vin, dealer_id FROM cars WHERE listing_active IS NOT NULL "
                              "AND listing_active <> 0 AND vin IS NOT NULL AND dealer_id IS NOT NULL ORDER BY id"):
        out[str(vin)] = str(dealer)
    return out


def conflict_pairs(t: Target) -> set[frozenset[str]]:
    if not t.has_table("vin_owner_conflicts"):
        return set()
    return {frozenset((o, c)) for o, c in t.rows(
        "SELECT DISTINCT owner_dealer_id, claimant_dealer_id FROM vin_owner_conflicts") if o != c}


def triage_pairs(run_dir: Path | None) -> set[frozenset[str]]:
    if run_dir is None:
        return set()
    out: set[frozenset[str]] = set()
    for d in run_artifacts.load_triage(run_dir):
        sister = (d.get("group_feed_unstamped") or {}).get("sister")
        if d.get("dealer_id") and sister and sister != d["dealer_id"]:
            out.add(frozenset((d["dealer_id"], sister)))
    return out


def load_pool_sets(path: Path) -> list[set[str]]:
    """Each pool's members and each alias set, as sets of dealer_ids."""
    if not path.is_file():
        return []
    data = json.loads(path.read_text())
    sets = [set(p.get("members") or []) | ({p["primary"]} if p.get("primary") else set())
            for p in data.get("pools") or []]
    sets += [set(a) for a in data.get("aliases") or []]
    return [s for s in sets if s]


def in_pools(pair: frozenset[str], pools: list[set[str]]) -> bool:
    return any(pair <= s for s in pools)


def compare(a: dict[str, str], b: dict[str, str], flagged: set[frozenset[str]], pools: list[set[str]],
            list_cap: int = 500) -> dict[str, Any]:
    both = sorted(set(a) & set(b))
    diffs = [(v, a[v], b[v]) for v in both if a[v] != b[v]]
    pair_n: collections.Counter[tuple[str, str]] = collections.Counter((x, y) for _, x, y in diffs)
    pairs = []
    for (x, y), n in sorted(pair_n.items(), key=lambda kv: (-kv[1], kv[0])):
        key = frozenset((x, y))
        pairs.append({"a_dealer": x, "b_dealer": y, "vins": n, "group_feed_unstamped": key in flagged,
                      "in_group_pools": in_pools(key, pools)})
    status = {(p["a_dealer"], p["b_dealer"]): ("explained" if p["group_feed_unstamped"] and not p["in_group_pools"]
                                                 else "unexplained") for p in pairs}
    b_dealers = set(b.values())
    unexplained = sum(p["vins"] for p in pairs if status[(p["a_dealer"], p["b_dealer"])] == "unexplained")
    return {
        "vins_a": len(a), "vins_b": len(b), "vins_both": len(both),
        "same_dealer": len(both) - len(diffs), "different_dealer": len(diffs),
        "agreement_pct": round(100.0 * (len(both) - len(diffs)) / len(both), 4) if both else None,
        "b_only_vins": len(set(b) - set(a)),
        "a_only_in_b_dealers": sum(1 for v in set(a) - set(b) if a[v] in b_dealers),
        "unexplained_vins": unexplained,
        "pairs": pairs,
        "differences": [{"vin": v, "a_dealer": x, "b_dealer": y,
                         "status": status[(x, y)]} for v, x, y in diffs[:list_cap]],
        "differences_truncated": max(0, len(diffs) - list_cap),
    }


def main(argv: list[str] | None = None, connector=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--a", required=True, help="pass A DB (name or SQLite path)")
    ap.add_argument("--b", required=True, help="pass B DB (name or SQLite path)")
    ap.add_argument("--run-dir-a")
    ap.add_argument("--run-dir-b")
    ap.add_argument("--pools", default=str(DEFAULT_POOLS))
    ap.add_argument("--min-agree", type=float, default=99.0, help="percent (plan: 99)")
    ap.add_argument("--pg-host", default=PG_HOST_DEFAULT)
    ap.add_argument("--pg-port", type=int, default=PG_PORT_DEFAULT)
    ap.add_argument("--json", required=True)
    a = ap.parse_args(argv)
    if a.a == a.b:
        print("REFUSED: --a and --b are the same DB", file=sys.stderr)
        return EXIT_REFUSED
    ta = tb = None
    try:
        ta = open_target(a.a, host=a.pg_host, port=a.pg_port, connector=connector)
        tb = open_target(a.b, host=a.pg_host, port=a.pg_port, connector=connector)
        flagged = conflict_pairs(ta) | conflict_pairs(tb)
        flagged |= triage_pairs(Path(a.run_dir_a) if a.run_dir_a else None)
        flagged |= triage_pairs(Path(a.run_dir_b) if a.run_dir_b else None)
        res = compare(active_owners(ta), active_owners(tb), flagged, load_pool_sets(Path(a.pools)))
    except ReadOnlyRequired as e:
        print(f"REFUSED: {e}", file=sys.stderr)
        return EXIT_REFUSED
    finally:
        for t in (ta, tb):
            if t is not None:
                t.close()
    pct = res["agreement_pct"]
    ok = pct is not None and pct >= a.min_agree and res["unexplained_vins"] == 0
    res = {"a": a.a, "b": a.b, "min_agree": a.min_agree, "accepted": ok, **res}
    Path(a.json).parent.mkdir(parents=True, exist_ok=True)
    Path(a.json).write_text(json.dumps(res, indent=1) + "\n")
    print(f"agreement {pct}% of {res['vins_both']} shared VINs; {res['different_dealer']} differ, "
          f"{res['unexplained_vins']} unexplained", file=sys.stderr)
    print("DETERMINISM OK" if ok else "DETERMINISM FAILED", file=sys.stderr)
    return EXIT_OK if ok else EXIT_FAIL


if __name__ == "__main__":
    sys.exit(main())
