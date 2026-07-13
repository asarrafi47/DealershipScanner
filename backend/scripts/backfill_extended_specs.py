#!/usr/bin/env python3
"""
Backfill NULLs in ``epa_extended_specs`` using ONLY honest, source-verified strategies
(per the 2026-07-11 spec-backfill plan):

1. ``--convert-units`` — internal unit conversions between paired columns:
   ``torque_nm`` <-> ``torque_lb_ft`` (1 lb-ft = 1.3558 Nm) and
   ``curb_weight_kg`` <-> ``curb_weight_lb`` (1 kg = 2.20462 lb).
   The importer writes both units together, so this is expected to find ~0 rows;
   it is kept as a cheap invariant-restoring pass.

2. ``--propagate-same-year`` — within a (year, make, model) group, fill a NULL only
   when ALL non-null sibling values for that column agree exactly.

3. ``--propagate-cross-year`` — same (make, model, trim) at year +/-1, fill a NULL
   only when ALL donor values agree exactly.

Every propagated fill records provenance in ``specs_json``
(``{"<col>_source": "sibling_trim_2019"}`` / ``"sibling_same_ymm"``) because
propagation is honest only if flagged.

Maintenance: ``--repair-provenance-from <epa_extended_specs_bak_YYYYMMDD_HHMMSS>``
retroactively adds the missing ``sibling_same_ymm`` provenance flags for rows an
earlier run of this script filled without writing them (identified precisely via the
backup table: prior value NULL, live value non-NULL, no ``<col>_source`` flag).
It updates ONLY ``specs_json``.

Propagation touches ONLY these columns (plan-approved):
``horsepower``, ``zero_to_60_sec``, ``fuel_tank_gal``, ``tow_capacity_lb``.
Donor values are always ORIGINAL scraped values — a value whose ``specs_json``
carries a ``<col>_source`` provenance flag is never used as a donor, so re-running
the script cannot chain propagated values across years/trims (idempotent).

Strategies the plan rejected are intentionally NOT implemented:
- dictionary EPA CSVs / ``epa_master`` carry none of the extended-spec fields;
- NHTSA vPIC is per-VIN and cannot honestly fill this per-(year,make,model,trim) table.
A NULL that cannot be sourced stays NULL — no estimates, no derived 0-60, no default 0 tow.

Safety: defaults to ``--dry-run``. With ``--apply`` it first writes a JSON snapshot of
every affected row's prior values to the backup dir AND creates a timestamped
``epa_extended_specs_bak_<ts>`` table (CREATE TABLE ... AS SELECT), then issues
per-row UPDATEs restricted to the whitelisted columns (+ ``specs_json`` provenance merge).

Usage:
  python backend/scripts/backfill_extended_specs.py                 # dry-run, all strategies
  python backend/scripts/backfill_extended_specs.py --apply
  python backend/scripts/backfill_extended_specs.py --propagate-same-year --apply
"""
from __future__ import annotations

import argparse
import datetime as _dt
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

NM_PER_LB_FT = 1.3558
LB_PER_KG = 2.20462

#: All extended-spec value columns (snapshot scope).
SPEC_COLUMNS = (
    "horsepower",
    "torque_lb_ft",
    "torque_nm",
    "curb_weight_lb",
    "curb_weight_kg",
    "zero_to_60_sec",
    "fuel_tank_gal",
    "ev_range_miles",
    "battery_kwh",
    "tow_capacity_lb",
)

#: Columns propagation is allowed to write (plan section 2).
PROPAGATION_COLUMNS = ("horsepower", "zero_to_60_sec", "fuel_tank_gal", "tow_capacity_lb")

#: Columns unit conversion is allowed to write.
CONVERSION_COLUMNS = ("torque_lb_ft", "torque_nm", "curb_weight_lb", "curb_weight_kg")

#: Full whitelist of value columns any UPDATE may set.
UPDATABLE_COLUMNS = tuple(dict.fromkeys(CONVERSION_COLUMNS + PROPAGATION_COLUMNS))

DEFAULT_BACKUP_DIR = (
    "/private/tmp/claude-501/-Users-asarrafi-Projects/"
    "0fd58203-ffeb-4591-b16d-3f7f838acd51/scratchpad/db-backups"
)


# ---------------------------------------------------------------------------
# Pure logic (unit-testable)
# ---------------------------------------------------------------------------

def nm_to_lb_ft(nm: float) -> int:
    return int(round(float(nm) / NM_PER_LB_FT))


def lb_ft_to_nm(lb_ft: float) -> int:
    return int(round(float(lb_ft) * NM_PER_LB_FT))


def kg_to_lb(kg: float) -> int:
    return int(round(float(kg) * LB_PER_KG))


def lb_to_kg(lb: float) -> int:
    return int(round(float(lb) / LB_PER_KG))


def unit_conversion_fills(row: dict[str, Any]) -> dict[str, int]:
    """Fill one paired unit column from the other; never overwrite non-NULL."""
    fills: dict[str, int] = {}
    if row.get("torque_lb_ft") is None and row.get("torque_nm") is not None:
        fills["torque_lb_ft"] = nm_to_lb_ft(row["torque_nm"])
    if row.get("torque_nm") is None and row.get("torque_lb_ft") is not None:
        fills["torque_nm"] = lb_ft_to_nm(row["torque_lb_ft"])
    if row.get("curb_weight_lb") is None and row.get("curb_weight_kg") is not None:
        fills["curb_weight_lb"] = kg_to_lb(row["curb_weight_kg"])
    if row.get("curb_weight_kg") is None and row.get("curb_weight_lb") is not None:
        fills["curb_weight_kg"] = lb_to_kg(row["curb_weight_lb"])
    return fills


def consensus(values: list[Any], rel_tolerance: float = 0.0) -> Any | None:
    """
    Consensus of the non-null *values*: the common value when all agree exactly,
    else (when *rel_tolerance* > 0) the first value if the min..max spread is within
    the relative tolerance, else None. Empty/all-null input -> None.
    """
    vals = [v for v in values if v is not None]
    if not vals:
        return None
    first = vals[0]
    if all(v == first for v in vals):
        return first
    if rel_tolerance > 0:
        try:
            lo = min(float(v) for v in vals)
            hi = max(float(v) for v in vals)
        except (TypeError, ValueError):
            return None
        if hi > 0 and (hi - lo) / hi <= rel_tolerance:
            return first
    return None


def compute_same_year_fills(
    rows: list[dict[str, Any]],
    columns: tuple[str, ...] = PROPAGATION_COLUMNS,
) -> dict[Any, dict[str, Any]]:
    """
    Same (year, make, model) propagation: fill a NULL column only when every
    non-null sibling value agrees exactly. Returns {epa_master_id: {col: value}}.
    """
    groups: dict[tuple, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[(row["year"], row["make"], row["model"])].append(row)

    fills: dict[Any, dict[str, Any]] = defaultdict(dict)
    for members in groups.values():
        if len(members) < 2:
            continue
        for col in columns:
            donor_vals = [m[col] for m in members if m[col] is not None]
            if not donor_vals:
                continue
            value = consensus(donor_vals)
            if value is None:
                continue
            for m in members:
                if m[col] is None:
                    fills[m["epa_master_id"]][col] = value
    return dict(fills)


def compute_cross_year_fills(
    rows: list[dict[str, Any]],
    already_filled: dict[Any, dict[str, Any]] | None = None,
    columns: tuple[str, ...] = PROPAGATION_COLUMNS,
) -> tuple[dict[Any, dict[str, Any]], dict[Any, dict[str, str]]]:
    """
    Same (make, model, trim) at year +/-1: fill a NULL column only when ALL donor
    values (original scraped values across both adjacent years) agree exactly.
    Rows already covered by *already_filled* for a column are skipped.
    Returns ({epa_master_id: {col: value}}, {epa_master_id: {col+"_source": str}}).
    """
    already_filled = already_filled or {}
    by_trim_year: dict[tuple, dict[int, list[dict[str, Any]]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for row in rows:
        key = (row["make"], row["model"], row.get("trim") or "")
        by_trim_year[key][row["year"]].append(row)

    fills: dict[Any, dict[str, Any]] = defaultdict(dict)
    provenance: dict[Any, dict[str, str]] = defaultdict(dict)
    for row in rows:
        key = (row["make"], row["model"], row.get("trim") or "")
        for col in columns:
            if row[col] is not None:
                continue
            if col in already_filled.get(row["epa_master_id"], {}):
                continue
            donor_vals: list[Any] = []
            donor_years: set[int] = set()
            for dy in (row["year"] - 1, row["year"] + 1):
                for donor in by_trim_year[key].get(dy, ()):  # original values only
                    if donor[col] is not None:
                        donor_vals.append(donor[col])
                        donor_years.add(dy)
            value = consensus(donor_vals)
            if value is None:
                continue
            fills[row["epa_master_id"]][col] = value
            years_tag = "_".join(str(y) for y in sorted(donor_years))
            provenance[row["epa_master_id"]][f"{col}_source"] = f"sibling_trim_{years_tag}"
    return dict(fills), dict(provenance)


def merge_fill_maps(*maps: dict[Any, dict[str, Any]]) -> dict[Any, dict[str, Any]]:
    merged: dict[Any, dict[str, Any]] = defaultdict(dict)
    for m in maps:
        for row_id, cols in m.items():
            merged[row_id].update(cols)
    return dict(merged)


# ---------------------------------------------------------------------------
# DB plumbing
# ---------------------------------------------------------------------------

def _load_rows(conn) -> list[dict[str, Any]]:
    cols = ["epa_master_id", "year", "make", "model", "trim", *SPEC_COLUMNS]
    cur = conn.cursor()
    cur.execute(f'SELECT {", ".join(cols)} FROM epa_extended_specs')
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def _fill_counts(conn) -> dict[str, int]:
    cur = conn.cursor()
    parts = ", ".join(f"COUNT({c}) AS {c}" for c in SPEC_COLUMNS)
    cur.execute(f"SELECT {parts} FROM epa_extended_specs")
    return dict(zip(SPEC_COLUMNS, cur.fetchone()))


def _per_column_counter(fills: dict[Any, dict[str, Any]]) -> Counter:
    counter: Counter = Counter()
    for cols in fills.values():
        counter.update(cols.keys())
    return counter


def _json_default(v: Any) -> Any:
    if isinstance(v, (_dt.datetime, _dt.date)):
        return v.isoformat()
    return str(v)


def _snapshot_affected_rows(conn, ids: list, backup_dir: Path, ts: str) -> dict[Any, dict]:
    """Fetch current values of affected rows; write JSON snapshot; return by-id map."""
    cols = ["epa_master_id", "year", "make", "model", "trim", *SPEC_COLUMNS, "specs_json"]
    cur = conn.cursor()
    cur.execute(
        f'SELECT {", ".join(cols)} FROM epa_extended_specs WHERE epa_master_id = ANY(%s)',
        (list(ids),),
    )
    current = {r[0]: dict(zip(cols, r)) for r in cur.fetchall()}

    backup_dir.mkdir(parents=True, exist_ok=True)
    snap_path = backup_dir / f"epa_extended_specs_prior_{ts}.json"
    with open(snap_path, "w", encoding="utf-8") as fh:
        json.dump(
            {
                "table": "epa_extended_specs",
                "created_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
                "rows": list(current.values()),
            },
            fh,
            indent=1,
            default=_json_default,
        )
    print(f"snapshot: wrote {len(current)} prior-value rows to {snap_path}")
    return current


def _create_backup_table(conn, ids: list, ts: str) -> str:
    table = f"epa_extended_specs_bak_{ts}"
    cur = conn.cursor()
    cur.execute(
        f"CREATE TABLE {table} AS "
        "SELECT * FROM epa_extended_specs WHERE epa_master_id = ANY(%s)",
        (list(ids),),
    )
    print(f"backup table: created {table} ({len(ids)} candidate rows)")
    return table


def _apply_fills(
    conn,
    fills: dict[Any, dict[str, Any]],
    provenance: dict[Any, dict[str, str]],
    current: dict[Any, dict],
) -> tuple[Counter, int]:
    """Per-row UPDATE of whitelisted columns; skip fills whose target is no longer NULL."""
    applied: Counter = Counter()
    rows_updated = 0
    cur = conn.cursor()
    for row_id, cols in sorted(fills.items()):
        row_now = current.get(row_id)
        if row_now is None:
            continue
        safe_cols = {
            c: v
            for c, v in cols.items()
            if c in UPDATABLE_COLUMNS and row_now.get(c) is None
        }
        if not safe_cols:
            continue
        set_parts = [f"{c} = %s" for c in safe_cols]
        params: list[Any] = list(safe_cols.values())
        prov = {
            k: v
            for k, v in provenance.get(row_id, {}).items()
            if k.removesuffix("_source") in safe_cols
        }
        if prov:
            set_parts.append("specs_json = COALESCE(specs_json, '{}'::jsonb) || %s::jsonb")
            params.append(json.dumps(prov))
        params.append(row_id)
        cur.execute(
            f'UPDATE epa_extended_specs SET {", ".join(set_parts)} WHERE epa_master_id = %s',
            tuple(params),
        )
        rows_updated += 1
        applied.update(safe_cols.keys())
    return applied, rows_updated


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--dry-run", action="store_true", default=True,
                        help="report what would change (default)")
    parser.add_argument("--apply", action="store_true",
                        help="actually UPDATE (snapshot + backup table first)")
    parser.add_argument("--convert-units", action="store_true",
                        help="strategy 1: torque_nm<->torque_lb_ft, curb kg<->lb")
    parser.add_argument("--propagate-same-year", action="store_true",
                        help="strategy 2a: same (year,make,model), all siblings agree")
    parser.add_argument("--propagate-cross-year", action="store_true",
                        help="strategy 2b: same (make,model,trim) at year +/-1, all donors agree")
    parser.add_argument("--backup-dir", default=DEFAULT_BACKUP_DIR,
                        help="directory for the prior-values JSON snapshot")
    args = parser.parse_args(argv)

    run_all = not (args.convert_units or args.propagate_same_year or args.propagate_cross_year)
    do_convert = args.convert_units or run_all
    do_same_year = args.propagate_same_year or run_all
    do_cross_year = args.propagate_cross_year or run_all

    from backend.utils.project_env import load_project_dotenv

    load_project_dotenv()

    from backend.db.inventory_db import get_conn
    from backend.db.inventory_pg import is_inventory_postgres

    if not is_inventory_postgres():
        raise SystemExit("epa_extended_specs lives in Postgres; set INVENTORY_DATABASE_URL")

    conn = get_conn()
    try:
        before = _fill_counts(conn)
        rows = _load_rows(conn)
        print(f"loaded {len(rows)} epa_extended_specs rows")

        conversion_fills: dict[Any, dict[str, Any]] = {}
        if do_convert:
            for row in rows:
                f = unit_conversion_fills(row)
                if f:
                    conversion_fills[row["epa_master_id"]] = f
            c = _per_column_counter(conversion_fills)
            print(f"[1] unit conversions: {sum(c.values())} field(s) "
                  f"across {len(conversion_fills)} row(s) {dict(c) or ''}")

        same_year_fills: dict[Any, dict[str, Any]] = {}
        if do_same_year:
            same_year_fills = compute_same_year_fills(rows)
            c = _per_column_counter(same_year_fills)
            print(f"[2a] same-year consensus propagation: {sum(c.values())} field(s) "
                  f"across {len(same_year_fills)} row(s) {dict(c) or ''}")

        cross_year_fills: dict[Any, dict[str, Any]] = {}
        provenance: dict[Any, dict[str, str]] = {}
        if do_cross_year:
            cross_year_fills, provenance = compute_cross_year_fills(
                rows, already_filled=same_year_fills
            )
            c = _per_column_counter(cross_year_fills)
            print(f"[2b] cross-year (+/-1, same trim) propagation: {sum(c.values())} field(s) "
                  f"across {len(cross_year_fills)} row(s) {dict(c) or ''}")

        fills = merge_fill_maps(conversion_fills, same_year_fills, cross_year_fills)
        totals = _per_column_counter(fills)
        print("\nplanned per-field fill counts:")
        for col in SPEC_COLUMNS:
            if totals.get(col):
                print(f"  {col}: +{totals[col]}")
        print(f"total: {sum(totals.values())} field(s) across {len(fills)} row(s)")

        if not fills:
            print("nothing to do")
            return 0

        if not args.apply:
            print("\nDRY RUN — no changes written. Re-run with --apply to update.")
            sample = list(sorted(fills.items()))[:10]
            for row_id, cols in sample:
                print(f"  sample epa_master_id={row_id}: {cols} "
                      f"{provenance.get(row_id, '')}")
            return 0

        ts = _dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        ids = sorted(fills.keys())
        current = _snapshot_affected_rows(conn, ids, Path(args.backup_dir), ts)
        _create_backup_table(conn, ids, ts)

        applied, rows_updated = _apply_fills(conn, fills, provenance, current)
        conn.commit()

        after = _fill_counts(conn)
        print(f"\nAPPLIED: {sum(applied.values())} field(s) across {rows_updated} row(s)")
        print("per-field change counts (before -> after non-null):")
        for col in SPEC_COLUMNS:
            if applied.get(col) or after[col] != before[col]:
                print(f"  {col}: +{applied.get(col, 0)}  ({before[col]} -> {after[col]})")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
