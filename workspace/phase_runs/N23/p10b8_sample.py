#!/usr/bin/env python3
"""P10B.8 helper: stratified before/after sample for the legacy re-resolve. READ-ONLY.

Picks (seeded) 100 ACTIVE cars whose link would move (spread over the old row's atv class:
Hybrid, FFV, Diesel, FCV, then the rest), 50 INACTIVE moved cars, and 50 UNLINKED cars whose
LIMIT-1 trim lookup would change. For each it renders ``serialize_car_for_api`` and the
generated spec sheet with the CURRENT link (before) and with the link the re-resolve dry run
would write (after; unlinked cars: the trim lookup with the source excluded). It then asserts
that engine, drivetrain, transmission and electrification fields did not change (vPIC and the
dealer text outrank the catalog) and writes everything to ``--out`` for the owner's sign-off.
Acceptance: >=98% of the sample correct by VIN decode (read the "diffs" and "unchanged_guard"
parts of the JSON; the VIN-decode check itself is a manual read of the vPIC columns).

Nothing is written to the database: the dry run of ``reresolve_legacy_catalog_links`` supplies
the new links, the "after" render overrides ``epa_master_id`` in memory. Refuses the frozen
``cars_cleanroom`` / ``cars_cr_*`` databases. Lives outside the product code on purpose.

  PYTHONPATH=. .venv/bin/python workspace/phase_runs/N23/p10b8_sample.py --out workspace/phase_runs/N23/sample.json
  PYTHONPATH=. .venv/bin/python workspace/phase_runs/N23/p10b8_sample.py --source epa_vehicles_csv --makes toyota
"""
from __future__ import annotations

import argparse
import json
import os
import random
import re
import sys
from collections import defaultdict
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_REPO_ROOT))

from backend.db.inventory_db import get_conn  # noqa: E402
from backend.scripts import reresolve_legacy_catalog_links as rr  # noqa: E402

ATV_STRATA = ("Hybrid", "FFV", "Diesel", "FCV")
# fields the catalog must never move: dealer text and the VIN decode decide them
GUARDED = ("engine_display", "drivetrain_display", "transmission_display", "fuel_type", "cylinders")
GUARDED_RE = re.compile(r"electrif|hybrid|phev|bev|^vpic_", re.I)
FROZEN = re.compile(r"cars_cleanroom|cars_cr_")


def refuse_frozen() -> None:
    url = os.environ.get("INVENTORY_DATABASE_URL", "")
    if FROZEN.search(url):
        sys.exit(f"refusing: {url!r} is a frozen baseline")


def _car_rows(cur, ids: list[int]) -> dict[int, dict]:
    out: dict[int, dict] = {}
    for i in range(0, len(ids), 500):
        part = ids[i:i + 500]
        cur.execute("SELECT * FROM cars WHERE id IN (%s)" % ",".join("?" * len(part)), tuple(part))
        names = [d[0] for d in cur.description]
        for r in cur.fetchall():
            d = dict(zip(names, r))
            out[int(d["id"])] = d
    return out


def stratify(pool: list[dict], n: int, rng: random.Random) -> list[dict]:
    """n cars, round-robin over the atv strata (old row's class), then 'other'."""
    buckets: dict[str, list[dict]] = defaultdict(list)
    for c in pool:
        a = c.get("old_atv") or ""
        buckets[a if a in ATV_STRATA else "other"].append(c)
    for b in buckets.values():
        rng.shuffle(b)
    order = [k for k in (*ATV_STRATA, "other") if buckets.get(k)]
    picked: list[dict] = []
    while len(picked) < n and any(buckets[k] for k in order):
        for k in order:
            if buckets[k] and len(picked) < n:
                picked.append(buckets[k].pop())
    return picked


def render(car: dict) -> dict:
    """serialize_car_for_api + the generated spec sheet for this car dict."""
    from backend.enrichment.generated_spec_sheet import build_generated_spec_sheet
    from backend.enrichment.knowledge_engine import merge_verified_specs
    from backend.utils.car_serialize.serialize import serialize_car_for_api

    vs = merge_verified_specs(car)
    api = serialize_car_for_api(car, verified_specs=vs)
    sheet = build_generated_spec_sheet(car, vs)
    return {"api": api, "sheet": sheet}


def clear_caches() -> None:
    from backend.enrichment import knowledge_engine as ke

    for name in ("clear_epa_trim_lookup_cache", "clear_epa_extended_specs_lookup_cache",
                 "clear_epa_aggregate_lookup_cache", "clear_engine_specs_lookup_cache"):
        getattr(ke, name)()
    for fn in (ke._lookup_epa_master_by_id_cached, ke._lookup_extended_by_master_id_cached):
        fn.cache_clear()


def guarded_diff(before: dict, after: dict) -> dict:
    keys = set(GUARDED) | {k for k in before if GUARDED_RE.search(k)} | {k for k in after if GUARDED_RE.search(k)}
    return {k: [before.get(k), after.get(k)] for k in sorted(keys) if before.get(k) != after.get(k)}


def all_diff(before: dict, after: dict) -> dict:
    return {k: [before.get(k), after.get(k)] for k in sorted(set(before) | set(after))
            if before.get(k) != after.get(k)}


def moved_cars(cur, source: str, makes: list[str]) -> list[dict]:
    """Cars the dry run would move, with old/new link and the old row's atv class."""
    out: list[dict] = []
    for mk in makes:
        plan = rr.plan_make(cur, source, mk)
        atv = rr._atv(cur, {i for c in plan["changes"] for i in (c["old"][0], c["new"][0]) if i is not None})
        for c in plan["changes"]:
            out.append({"id": c["id"], "old": c["old"][0], "new": c["new"][0],
                        "old_atv": atv.get(c["old"][0]), "new_atv": atv.get(c["new"][0])})
    return out


def unlinked_cars(cur, source: str, makes: list[str], limit: int) -> list[dict]:
    """Unlinked cars in (year, make, model, trim) groups whose LIMIT-1 trim lookup changes."""
    from backend.enrichment.knowledge_engine import lookup_epa_by_trim

    out: list[dict] = []
    for mk in makes:
        cur.execute(
            "SELECT year, make, model, \"trim\", min(id) FROM cars WHERE epa_master_id IS NULL AND lower(make)=? "
            "AND \"trim\" IS NOT NULL AND \"trim\" <> '' AND EXISTS (SELECT 1 FROM epa_master m WHERE m.source=? "
            "AND m.year=cars.year AND lower(m.make)=?) GROUP BY year, make, model, \"trim\"", (mk, source, mk))
        for year, make, model, trim, cid in cur.fetchall():
            if lookup_epa_by_trim(year, make, model, trim) != lookup_epa_by_trim(
                    year, make, model, trim, exclude_sources=(source,)):
                out.append({"id": int(cid), "old": None, "new": None, "old_atv": None, "new_atv": None})
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--source", help="epa_master.source being retired (default: auto-detect legacy label)")
    ap.add_argument("--makes", default="", help="comma-separated makes (default: all with such rows)")
    ap.add_argument("--active", type=int, default=100)
    ap.add_argument("--inactive", type=int, default=50)
    ap.add_argument("--unlinked", type=int, default=50)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--out", default="workspace/phase_runs/N23/sample.json")
    args = ap.parse_args(argv)
    refuse_frozen()

    rng = random.Random(args.seed)
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY")
        source = rr.detect_source(cur, args.source)
        wanted = [m.strip().lower() for m in args.makes.split(",") if m.strip()]
        makes = [m for m in rr.source_makes(cur, source) if not wanted or m in wanted]
        moved = moved_cars(cur, source, makes)
        rows = _car_rows(cur, [m["id"] for m in moved])
        active = [m for m in moved if (rows[m["id"]].get("listing_active") in (None, 1, True))]
        inactive = [m for m in moved if m not in active]
        picks = {"active_moved": stratify(active, args.active, rng),
                 "inactive_moved": stratify(inactive, args.inactive, rng)}
        unl = unlinked_cars(cur, source, makes, args.unlinked)
        rng.shuffle(unl)
        picks["unlinked"] = unl[:args.unlinked]
        rows.update(_car_rows(cur, [m["id"] for m in picks["unlinked"]]))

        from backend.enrichment import knowledge_engine as ke

        real_lookup = ke.lookup_epa_by_trim
        report: dict = {"source": source, "seed": args.seed, "strata": {}, "violations": 0, "cars": 0}
        for stratum, sample in picks.items():
            entries = []
            for m in sample:
                car = rows[m["id"]]
                clear_caches()
                before = render(car)
                clear_caches()
                after_car = dict(car, epa_master_id=m["new"]) if stratum != "unlinked" else dict(car)
                if m["new"] is None and stratum != "unlinked":
                    after_car.update(epa_match_confidence=None, epa_match_method=None)
                if stratum == "unlinked":  # the legacy rows are gone: the read-time fallback skips them
                    ke.lookup_epa_by_trim = lambda y, mk, md, tr, exclude_sources=(), _f=real_lookup: _f(
                        y, mk, md, tr, exclude_sources=(source,))
                try:
                    after = render(after_car)
                finally:
                    ke.lookup_epa_by_trim = real_lookup
                guard = guarded_diff(before["api"], after["api"])
                report["violations"] += bool(guard)
                report["cars"] += 1
                entries.append({"car_id": m["id"], "vin": car.get("vin"), "old_row": m["old"], "new_row": m["new"],
                                "old_atv": m["old_atv"], "new_atv": m["new_atv"],
                                "unchanged_guard": not guard, "guard_diff": guard,
                                "api_diff": all_diff(before["api"], after["api"]),
                                "sheet_changed": before["sheet"] != after["sheet"],
                                "before": before, "after": after})
            report["strata"][stratum] = {"requested": getattr(args, stratum.split("_")[0]),
                                         "picked": len(entries), "cars": entries}
        report["guard_ok_pct"] = round(100 * (1 - report["violations"] / report["cars"]), 2) if report["cars"] else None
        Path(args.out).write_text(json.dumps(report, indent=1, default=str))
        print(json.dumps({k: report[k] for k in ("source", "cars", "violations", "guard_ok_pct")}
                         | {s: v["picked"] for s, v in report["strata"].items()}, indent=2))
        print(f"wrote {args.out} (read-only run; nothing written to the database)")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
