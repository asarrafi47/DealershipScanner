#!/usr/bin/env python3
"""N15.5b: repair stored ``fuel_type`` values that name an injection system
("Direct Injection", "Port/Direct Injection", "GDI", "MPI"...) instead of a fuel.

F13 stopped new junk at the write path; 381 stored values remain in ``cars`` and
prod. Each one is re-decided from the cached vPIC decode through
``apply_fact_proposals`` (N15.5: plain Gasoline fills a not-a-fuel value, and
electrification / diesel replace it). A row the decode cannot answer keeps its
value unless ``--clear-undecodable`` is given, which sets it to NULL (the junk
was never a fuel, so clearing it is not a fact write).

DRY RUN by default: nothing is written without ``--apply``. No network: VINs
missing from ``nhtsa_vpic_cache`` are reported, never fetched (run
``heal_from_vpic.py --decode`` for those first).

  PYTHONPATH=. .venv/bin/python backend/scripts/repair_injection_fuel.py
  PYTHONPATH=. .venv/bin/python backend/scripts/repair_injection_fuel.py --apply --clear-undecodable
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Callable

_REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT))

from backend.utils.project_env import load_project_dotenv  # noqa: E402

load_project_dotenv()

# Matches the injection / delivery descriptors field_clean treats as not-a-fuel.
INJECTION_VALUES = ("direct injection", "port/direct injection", "port injection", "sequential mpi", "mpi", "gdi")


def repair_injection_fuel(conn, *, apply: bool = False, clear_undecodable: bool = False,
                          lookup: Callable[[str], dict[str, Any]] | None = None) -> dict[str, Any]:
    """Returns counters; writes only when *apply*. *lookup* is the cached-decode lookup."""
    from backend.enrichment import vpic_facts as vf
    from backend.vehicle_facts.apply import apply_fact_proposals

    if lookup is None:
        from backend.enrichment.knowledge_engine import lookup_vpic_from_cache as lookup
    cur = conn.cursor()
    cur.execute(
        "SELECT id, vin, fuel_type, engine_description, spec_source_json FROM cars "
        "WHERE lower(trim(fuel_type)) IN (" + ",".join("?" * len(INJECTION_VALUES)) + ") ORDER BY id",
        INJECTION_VALUES)
    rows = [dict(zip(("id", "vin", "fuel_type", "engine_description", "spec_source_json"), r)) for r in cur.fetchall()]
    stats: dict[str, Any] = {"rows": len(rows), "healed": 0, "cleared": 0, "left": 0, "no_decode": 0,
                             "by_value": {}, "examples": [], "dry_run": not apply}
    for r in rows:
        vin = str(r["vin"] or "").strip().upper()
        vp = (lookup(vin) if len(vin) == 17 else None) or {}
        if not vp:
            stats["no_decode"] += 1
        decision = vf._vin_decisions(r, vp).get("fuel_type")
        if decision is not None:
            res = apply_fact_proposals(conn, r, [("fuel_type", decision.value, "nhtsa_vpic")], vpic=vp,
                                       dry_run=not apply)
            if res.applied.get("fuel_type"):
                new = res.applied["fuel_type"]
                stats["healed"] += 1
                stats["by_value"][new] = stats["by_value"].get(new, 0) + 1
                if len(stats["examples"]) < 10:
                    stats["examples"].append({"id": r["id"], "vin": vin, "fuel_type": f"{r['fuel_type']} -> {new}"})
                continue
        if clear_undecodable:
            stats["cleared"] += 1
            if apply:
                cur.execute("UPDATE cars SET fuel_type = NULL WHERE id = ?", (r["id"],))
        else:
            stats["left"] += 1
    if apply:
        conn.commit()
    return stats


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true", help="write the repairs (default: dry run)")
    ap.add_argument("--clear-undecodable", action="store_true",
                    help="set the value to NULL where the cached decode cannot answer")
    args = ap.parse_args()
    from backend.db.inventory_db import get_conn

    conn = get_conn()
    try:
        stats = repair_injection_fuel(conn, apply=args.apply, clear_undecodable=args.clear_undecodable)
        ex = stats.pop("examples")
        print(("APPLIED " if args.apply else "DRY RUN ") + json.dumps(stats))
        for e in ex:
            print("  ", e)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
