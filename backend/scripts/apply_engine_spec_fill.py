#!/usr/bin/env python3
"""Load engine-specific truck specs into ``ai_engine_specs`` (keyed by the car's exact
engine, so a diesel is not served a gas engine's numbers).

Consulted by ``knowledge_engine.lookup_engine_specs`` and applied in the serializer:
for the engine-critical fields (hp, torque, tow, 0-60) the engine-matched value WINS
over the model-level spec; fuel_tank/curb only fill when the model-level value is null.

Reversible: ``DROP TABLE ai_engine_specs``. Never mutates any other table.
Defaults to --dry-run; --apply creates the table (IF NOT EXISTS) and upserts.
"""
from __future__ import annotations
import argparse, json, sys
from backend.utils.project_env import load_project_dotenv
from backend.db.inventory_db import get_conn

INT_F = ("horsepower", "torque_lb_ft", "tow_capacity_lb", "curb_weight_lb")
FLOAT_F = ("zero_to_60_sec", "fuel_tank_gal")
ALL_F = ("horsepower", "torque_lb_ft", "tow_capacity_lb", "zero_to_60_sec", "fuel_tank_gal", "curb_weight_lb")

DDL = """
CREATE TABLE IF NOT EXISTS ai_engine_specs (
    year INTEGER NOT NULL, make TEXT NOT NULL, model TEXT NOT NULL,
    engine_description TEXT NOT NULL DEFAULT '', cylinders INTEGER, fuel_type TEXT NOT NULL DEFAULT '',
    horsepower INTEGER, torque_lb_ft INTEGER, tow_capacity_lb INTEGER,
    zero_to_60_sec DOUBLE PRECISION, fuel_tank_gal DOUBLE PRECISION, curb_weight_lb INTEGER,
    specs_json JSONB, source_host TEXT DEFAULT 'ai-engine-research', created_at TIMESTAMPTZ DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_ai_engine_specs
    ON ai_engine_specs (year, lower(make), lower(model), lower(btrim(engine_description)),
                        COALESCE(cylinders,-1), lower(btrim(fuel_type)));
"""


def _vals(fields: dict) -> dict:
    out = {c: None for c in ALL_F}
    for f in INT_F:
        if f in fields and fields[f].get("value") is not None:
            out[f] = int(round(fields[f]["value"]))
    for f in FLOAT_F:
        if f in fields and fields[f].get("value") is not None:
            out[f] = round(float(fields[f]["value"]), 1)
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--proposals", required=True)
    ap.add_argument("--dry-run", action="store_true", default=True)
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args(argv)
    load_project_dotenv()
    props = json.load(open(args.proposals))["configs"]
    rows, counts = [], {c: 0 for c in ALL_F}
    for g in props:
        v = _vals(g.get("fields") or {})
        if not any(x is not None for x in v.values()):
            continue
        for c in ALL_F:
            if v[c] is not None:
                counts[c] += 1
        prov = {f: {"status": m.get("status"), "source_url": m.get("source_url"), "note": m.get("note")}
                for f, m in (g.get("fields") or {}).items()}
        rows.append((g["year"], g["make"], g["model"], g.get("engine_description") or "",
                     g.get("cylinders"), g.get("fuel_type") or "", v, prov))
    print(f"{len(props)} configs -> {len(rows)} with data")
    for c in ALL_F:
        print(f"  {c}: {counts[c]}")
    if not args.apply:
        print("\nDRY RUN. Re-run with --apply.")
        return 0
    conn = get_conn(); cur = conn.cursor()
    for stmt in DDL.strip().split(";"):
        if stmt.strip():
            cur.execute(stmt)
    cols = ["year", "make", "model", "engine_description", "cylinders", "fuel_type", *ALL_F, "specs_json"]
    conflict = ("year, lower(make), lower(model), lower(btrim(engine_description)), "
                "COALESCE(cylinders,-1), lower(btrim(fuel_type))")
    updates = ", ".join(f"{c}=EXCLUDED.{c}" for c in [*ALL_F, "specs_json"])
    for year, make, model, eng, cyl, fuel, v, prov in rows:
        cur.execute(
            f"INSERT INTO ai_engine_specs ({', '.join(cols)}) VALUES ({', '.join(['%s']*len(cols))}) "
            f"ON CONFLICT ({conflict}) DO UPDATE SET {updates}",
            (year, make, model, eng, cyl, fuel, *[v[c] for c in ALL_F], json.dumps(prov)),
        )
    conn.commit()
    cur.execute("SELECT COUNT(*) FROM ai_engine_specs")
    print(f"\nAPPLIED: {len(rows)} upserted; ai_engine_specs holds {cur.fetchone()[0]} rows.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
