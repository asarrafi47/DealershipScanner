#!/usr/bin/env python3
"""
Report (and optionally NULL out) physically impossible values in ``epa_extended_specs``.

The table was populated by scraping MODEL-level review pages, so one variant's
numbers landed on every trim/year row of a nameplate. The fingerprints:

* ``curb_weight_lb`` over the light-duty ceiling on a vehicle that is not a
  heavy-duty pickup / chassis cab / full-size van / heavy BEV truck. This is
  what catches a towing figure parsed into the weight column — every Ram 1500
  row 2013-2026 reads 11,580 lb for both curb and tow.
* a ``zero_to_60_sec`` that is the nameplate-wide constant traced to one
  variant (all 158 Ram 1500 rows carry the 702 hp TRX's 4.9 s).
* a sub-4-second ``zero_to_60_sec`` on a car with no performance badge, no
  quick-from-the-factory nameplate and no battery-electric drivetrain.
* a sub-5-second ``zero_to_60_sec`` on a naturally aspirated pickup / van / SUV.

Note that ``curb_weight_lb == tow_capacity_lb`` is NOT itself a rule. The
collision says the row is a model-level scrape, not which column is wrong, and
acting on it wholesale erased a believable 4,400 lb curb weight AND a correct
5.9 s from every Audi Q7 (and from the 3 Series, Yukon, Ranger and Mustang).

The rules are NOT reimplemented here: this script calls the same
``implausible_extended_spec_fields`` the car page uses at read time, so the
database and the rendered page always agree on what counts as impossible.
Read-time suppression already keeps these values off pages; this pass stops them
propagating into anything that reads the table directly (backfills, exports,
sibling-trim propagation in ``backfill_extended_specs.py``).

Safety: dry-run by default. ``--apply`` first copies every affected row into a
timestamped ``epa_extended_specs_bak_<ts>`` table, then NULLs ONLY the offending
columns. Idempotent — a second run finds nothing because the values are gone.

Usage:
  python backend/scripts/fix_extended_spec_outliers.py            # dry-run report
  python backend/scripts/fix_extended_spec_outliers.py --limit 20 # + sample rows
  python backend/scripts/fix_extended_spec_outliers.py --apply
"""
from __future__ import annotations

import argparse
import datetime as _dt
import sys
from collections import Counter
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

#: Columns this script is allowed to NULL. curb_weight_kg is not checked
#: independently — it is the same number in other units, so it follows the lb
#: column or the row would be self-contradictory.
NULLABLE_COLUMNS = ("curb_weight_lb", "curb_weight_kg", "zero_to_60_sec", "tow_capacity_lb")

SELECT_SQL = """
    SELECT e.epa_master_id, e.year, e.make, e.model, e.trim,
           e.horsepower, e.torque_lb_ft, e.curb_weight_lb, e.curb_weight_kg,
           e.zero_to_60_sec, e.tow_capacity_lb, e.body_style_detail,
           m.fuel_type, m.atv_type, m.engine_description, m.cylinders, m.body_style
    FROM epa_extended_specs e
    LEFT JOIN epa_master m ON m.id = e.epa_master_id
"""

ROW_KEYS = (
    "epa_master_id", "year", "make", "model", "trim",
    "horsepower", "torque_lb_ft", "curb_weight_lb", "curb_weight_kg",
    "zero_to_60_sec", "tow_capacity_lb", "body_style_detail",
    "fuel_type", "atv_type", "engine_description", "cylinders", "body_style",
)


def car_view(row: dict[str, Any]) -> dict[str, Any]:
    """
    The vehicle-identity half of a row, shaped like a ``cars`` row.

    The guard needs make/model/trim/engine text to recognise a heavy-duty truck
    or a performance badge; ``epa_master`` carries the fuel type and engine
    description the extended-specs row itself lacks.
    """
    fuel = row.get("fuel_type")
    if not fuel and str(row.get("atv_type") or "").strip().upper() in ("EV", "FCV"):
        fuel = "Electric"
    return {
        "year": row.get("year"),
        "make": row.get("make"),
        "model": row.get("model"),
        "trim": row.get("trim"),
        "title": " ".join(
            str(row.get(k) or "") for k in ("year", "make", "model", "trim")
        ).strip(),
        "engine_description": row.get("engine_description"),
        "body_style": row.get("body_style") or row.get("body_style_detail"),
        "fuel_type": fuel,
        "cylinders": row.get("cylinders"),
    }


def planned_nulls(row: dict[str, Any]) -> dict[str, None]:
    """Columns to NULL for this row, per the read-time plausibility guard."""
    from backend.enrichment.knowledge_engine_specs import implausible_extended_spec_fields

    bad = implausible_extended_spec_fields(car_view(row), row)
    out: dict[str, None] = {}
    for col in bad:
        if col in NULLABLE_COLUMNS:
            out[col] = None
    # A curb weight in lb and kg is one fact; never leave half of it behind.
    if "curb_weight_lb" in out and row.get("curb_weight_kg") is not None:
        out["curb_weight_kg"] = None
    return out


def rule_labels(row: dict[str, Any], nulls: dict[str, None]) -> list[str]:
    """Which fingerprint(s) this row matched — reporting only."""
    labels: list[str] = []
    if "curb_weight_lb" in nulls:
        curb, tow = row.get("curb_weight_lb"), row.get("tow_capacity_lb")
        collided = curb is not None and tow is not None and float(curb) == float(tow)
        labels.append(
            "curb weight over the light-duty ceiling"
            + (" (and equal to tow — a towing figure in the weight column)"
               if collided else "")
        )
    if "zero_to_60_sec" in nulls:
        from backend.enrichment.knowledge_engine_specs import _zero_to_60_is_misattributed

        z = float(row["zero_to_60_sec"])
        if _zero_to_60_is_misattributed(car_view(row), z):
            labels.append("0-60 is the nameplate-wide figure traced to one variant")
        elif z < 4.0:
            labels.append("sub-4s 0-60 with no badge, marque or battery to justify it")
        else:
            labels.append("sub-5s 0-60 on a naturally aspirated pickup / van / SUV")
    return labels


def _load_rows(conn) -> list[dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(SELECT_SQL)
    return [dict(zip(ROW_KEYS, r)) for r in cur.fetchall()]


def _backup_table(conn, ids: list[Any]) -> str:
    ts = _dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    table = f"epa_extended_specs_bak_{ts}"
    cur = conn.cursor()
    cur.execute(
        f"CREATE TABLE {table} AS "
        "SELECT * FROM epa_extended_specs WHERE epa_master_id = ANY(%s)",
        (ids,),
    )
    return table


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true",
                        help="write the NULLs (default is a dry-run report)")
    parser.add_argument("--limit", type=int, default=10,
                        help="how many sample rows to print (default 10)")
    args = parser.parse_args(argv)

    from backend.utils.project_env import load_project_dotenv

    load_project_dotenv()

    from backend.db.inventory_db import get_conn
    from backend.db.inventory_pg import is_inventory_postgres

    if not is_inventory_postgres():
        raise SystemExit("epa_extended_specs lives in Postgres; set INVENTORY_DATABASE_URL")

    conn = get_conn()
    try:
        rows = _load_rows(conn)
        print(f"scanned {len(rows)} epa_extended_specs rows\n")

        fixes: dict[Any, dict[str, None]] = {}
        per_column: Counter[str] = Counter()
        per_rule: Counter[str] = Counter()
        per_model: Counter[str] = Counter()
        samples: list[str] = []

        for row in rows:
            nulls = planned_nulls(row)
            if not nulls:
                continue
            fixes[row["epa_master_id"]] = nulls
            per_column.update(nulls.keys())
            per_rule.update(rule_labels(row, nulls))
            per_model[f"{row.get('make')} {row.get('model')}"] += 1
            if len(samples) < max(0, args.limit):
                samples.append(
                    f"  {row.get('year')} {row.get('make')} {row.get('model')} "
                    f"{row.get('trim') or ''} | curb={row.get('curb_weight_lb')} "
                    f"tow={row.get('tow_capacity_lb')} 0-60={row.get('zero_to_60_sec')} "
                    f"hp={row.get('horsepower')} -> NULL {', '.join(sorted(nulls))}"
                )

        print("rows matching each fingerprint:")
        for label, n in per_rule.most_common():
            print(f"  {label}: {n}")
        print("\nvalues that would be NULLed:")
        for col in NULLABLE_COLUMNS:
            if per_column.get(col):
                print(f"  {col}: {per_column[col]}")
        print(f"\ntotal: {sum(per_column.values())} value(s) across {len(fixes)} row(s) "
              f"({len(fixes) * 100.0 / max(len(rows), 1):.1f}% of the table)")

        print("\nworst-affected nameplates:")
        for name, n in per_model.most_common(10):
            print(f"  {name}: {n} row(s)")

        if samples:
            print("\nsample rows:")
            for line in samples:
                print(line)

        if not fixes:
            print("\nnothing to do.")
            return 0
        if not args.apply:
            print("\nDRY RUN — nothing written. Re-run with --apply to NULL these values.")
            return 0

        ids = list(fixes.keys())
        table = _backup_table(conn, ids)
        print(f"\nbacked up {len(ids)} row(s) into {table}")
        cur = conn.cursor()
        for mid, nulls in fixes.items():
            sets = ", ".join(f"{col}=NULL" for col in sorted(nulls))
            cur.execute(
                f"UPDATE epa_extended_specs SET {sets} WHERE epa_master_id=%s", (mid,)
            )
        conn.commit()
        print(f"applied: {sum(len(v) for v in fixes.values())} value(s) NULLed")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
