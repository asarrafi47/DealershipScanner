#!/usr/bin/env python3
"""Apply AI-researched vehicle specs into a dedicated ``ai_model_specs`` table.

Why a separate table: ``epa_extended_specs`` is keyed 1:1 to ``epa_master`` by a
PRIMARY KEY / FOREIGN KEY (``epa_master_id``), so AI rows cannot live there. This
table is keyed by (year, make, model), carries full per-field provenance in
``specs_json``, and is consulted by ``knowledge_engine.lookup_epa_extended_specs``
ONLY to fill fields still NULL after the real (scraped/EPA) sources resolve.

Fully reversible: ``DROP TABLE ai_model_specs`` (or ``DELETE FROM ai_model_specs``)
removes every AI-sourced value. It never mutates any existing row.

Safety: defaults to --dry-run. --apply creates the table (IF NOT EXISTS) and
upserts one row per model-group (ON CONFLICT DO UPDATE — idempotent re-runs).

Usage:
  python backend/scripts/apply_ai_spec_fill.py --proposals <path>            # dry-run
  python backend/scripts/apply_ai_spec_fill.py --proposals <path> --apply
"""
from __future__ import annotations

import argparse
import json
import sys

from backend.utils.project_env import load_project_dotenv
from backend.db.inventory_db import get_conn

# Numeric fields AI provides; torque_nm / curb_weight_kg are derived here.
CORE_INT = ("horsepower", "torque_lb_ft", "tow_capacity_lb", "curb_weight_lb")
CORE_FLOAT = ("zero_to_60_sec", "fuel_tank_gal")
ALL_COLS = ("horsepower", "torque_lb_ft", "torque_nm", "curb_weight_lb",
            "curb_weight_kg", "zero_to_60_sec", "fuel_tank_gal", "tow_capacity_lb")

DDL = """
CREATE TABLE IF NOT EXISTS ai_model_specs (
    year            INTEGER NOT NULL,
    make            TEXT    NOT NULL,
    model           TEXT    NOT NULL,
    horsepower      INTEGER,
    torque_lb_ft    INTEGER,
    torque_nm       INTEGER,
    curb_weight_lb  INTEGER,
    curb_weight_kg  INTEGER,
    zero_to_60_sec  DOUBLE PRECISION,
    fuel_tank_gal   DOUBLE PRECISION,
    tow_capacity_lb INTEGER,
    specs_json      JSONB,
    source_host     TEXT DEFAULT 'ai-research',
    created_at      TIMESTAMPTZ DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_ai_model_specs_ymm
    ON ai_model_specs (year, lower(make), lower(model));
"""


def _derive(fields: dict) -> dict:
    """Project validated per-field dicts into column values + unit conversions."""
    out = {c: None for c in ALL_COLS}
    for f in CORE_INT:
        if f in fields:
            out[f] = int(round(fields[f]["value"]))
    for f in CORE_FLOAT:
        if f in fields:
            out[f] = round(float(fields[f]["value"]), 1)
    if out["torque_lb_ft"] is not None:
        out["torque_nm"] = int(round(out["torque_lb_ft"] * 1.35582))
    if out["curb_weight_lb"] is not None:
        out["curb_weight_kg"] = int(round(out["curb_weight_lb"] / 2.20462))
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--proposals", required=True, help="validated proposals.json")
    ap.add_argument("--dry-run", action="store_true", default=True)
    ap.add_argument("--apply", action="store_true", help="create table + upsert rows")
    args = ap.parse_args(argv)
    apply = args.apply

    load_project_dotenv()
    proposals = json.load(open(args.proposals))
    groups = proposals.get("groups", [])
    rows = []
    field_counts = {c: 0 for c in ALL_COLS}
    for g in groups:
        vals = _derive(g.get("fields") or {})
        if not any(v is not None for v in vals.values()):
            continue
        for c in ALL_COLS:
            if vals[c] is not None:
                field_counts[c] += 1
        prov = {f: {"status": m.get("status"), "source_url": m.get("source_url"), "note": m.get("note")}
                for f, m in (g.get("fields") or {}).items()}
        rows.append((g["year"], g["make"], g["model"], vals, prov))

    print(f"proposals: {len(groups)} groups -> {len(rows)} with >=1 value")
    print("per-column values to write:")
    for c in ALL_COLS:
        print(f"  {c}: {field_counts[c]}")

    if not apply:
        print("\nDRY RUN — no changes. Re-run with --apply to create table + upsert.")
        return 0

    conn = get_conn()
    cur = conn.cursor()
    for stmt in DDL.strip().split(";"):
        if stmt.strip():
            cur.execute(stmt)
    inserted = 0
    for year, make, model, vals, prov in rows:
        cols = ["year", "make", "model", *ALL_COLS, "specs_json", "source_host"]
        placeholders = ", ".join(["%s"] * len(cols))
        updates = ", ".join(f"{c}=EXCLUDED.{c}" for c in [*ALL_COLS, "specs_json"])
        cur.execute(
            f"INSERT INTO ai_model_specs ({', '.join(cols)}) VALUES ({placeholders}) "
            f"ON CONFLICT (year, lower(make), lower(model)) DO UPDATE SET {updates}",
            (year, make, model, *[vals[c] for c in ALL_COLS], json.dumps(prov), "ai-research"),
        )
        inserted += 1
    conn.commit()
    cur.execute("SELECT COUNT(*) FROM ai_model_specs")
    total = cur.fetchone()[0]
    print(f"\nAPPLIED: upserted {inserted} rows; ai_model_specs now holds {total} rows.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
