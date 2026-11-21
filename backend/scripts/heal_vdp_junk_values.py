#!/usr/bin/env python3
"""
Heal parser-artifact junk in ``cars.trim`` / ``exterior_color`` / ``interior_color`` (N6.5).

The dealer.com VDP parser stored ``str()`` of a localized object
(``{'_type': 's', 'en_US': ...}``) as the trim and ``[[...]]`` template tokens as
colours; those counted as filled. ``_clean()`` and the upsert now refuse and
overwrite them; this script clears what is already stored.

Targets (active rows; one category per column value):
  trim_dict / color_dict   value starts with ``{``          (dict repr)
  trim_token / color_dbl   ``[[...]]``                      (double-bracket token)
  color_single             ``[extra_cost_color]``-style     (single-bracket token)
  trim_null_text / ...     ``null`` / ``none`` / ``undefined``
  color_option_code        colour of <= 3 chars with a digit (``X9``, ``21``)
The selection is ``vdp_extras_parse.junk_text_value`` itself, so the parser, the
upsert, the completeness invariant and this heal cannot drift apart. The double
bracket, single bracket and option-code populations are reported separately.

Each changed row is snapshotted to ``cars_junk_value_quarantine`` first, the
column is set to NULL (an honest gap, refilled by the next rescan), and a
provenance note (source ``vdp_junk_heal``) is merged into ``spec_source_json``.

Dry run by default. ``--apply`` needs owner OK locally; the prod apply is N12.10.
  PYTHONPATH=. python backend/scripts/heal_vdp_junk_values.py            # dry run
  PYTHONPATH=. python backend/scripts/heal_vdp_junk_values.py --apply
"""
from __future__ import annotations

import argparse
import sys
from collections import Counter
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT))

from backend.utils.project_env import load_project_dotenv  # noqa: E402

load_project_dotenv()

from backend.scanner.utils.vdp_extras_parse import junk_text_value  # noqa: E402
from backend.utils.spec_provenance import merge_spec_source_json, utc_now_iso  # noqa: E402

HEAL_SOURCE = "vdp_junk_heal"
#: column -> is a colour column (option codes apply)
COLUMNS = {"trim": False, "exterior_color": True, "interior_color": True}
#: --apply ceiling: measured 1,228 trim + 913 colour + 227 option-code on 2026-10; far above means a bad selection.
MAX_APPLY_ROWS = 4000


def junk_category(column: str, value: Any) -> str | None:
    """Category label for a junk stored value, None when the value is fine."""
    color = COLUMNS[column]
    if not junk_text_value(value, color=color):
        return None
    s = str(value).strip()
    kind = "color" if color else "trim"
    if s.startswith("{"):
        return f"{kind}_dict"
    if s.startswith("[["):
        return "color_dbl" if color else "trim_token"
    if s.startswith("["):
        return "color_single" if color else "trim_token"
    if s.lower() in ("null", "none", "undefined"):
        return f"{kind}_null_text"
    return "color_option_code"


def junk_columns(row: dict[str, Any]) -> dict[str, str]:
    """{column: category} for every junk column of *row*."""
    out: dict[str, str] = {}
    for col in COLUMNS:
        cat = junk_category(col, row.get(col))
        if cat:
            out[col] = cat
    return out


_QUARANTINE_DDL = """
    CREATE TABLE IF NOT EXISTS cars_junk_value_quarantine (
        car_id INTEGER, vin TEXT, dealer_id TEXT, column_name TEXT,
        old_value TEXT, category TEXT, quarantined_at TEXT
    )
"""

_COLS = ("id", "vin", "dealer_id", "trim", "exterior_color", "interior_color", "spec_source_json")


def _select_rows(cur, limit: int | None) -> list[dict[str, Any]]:
    # Cheap prefilter; junk_text_value makes the real decision.
    cond = " OR ".join(
        f"({c} IS NOT NULL AND (SUBSTR(BTRIM({c}), 1, 1) IN ('{{', '[') "
        f"OR LOWER(BTRIM({c})) IN ('null', 'none', 'undefined')"
        + (f" OR LENGTH(BTRIM({c})) <= 3" if COLUMNS[c] else "")
        + "))"
        for c in COLUMNS
    )
    sql = f"SELECT {', '.join(_COLS)} FROM cars WHERE COALESCE(listing_active, 1) = 1 AND ({cond})"
    if limit:
        sql += f" LIMIT {int(limit)}"
    cur.execute(sql)
    return [dict(zip(_COLS, r)) for r in cur.fetchall()]


def _heal_row(cur, row: dict[str, Any], cols: dict[str, str]) -> None:
    now = utc_now_iso()
    patch = {}
    for col, cat in cols.items():
        cur.execute(
            "INSERT INTO cars_junk_value_quarantine "
            "(car_id, vin, dealer_id, column_name, old_value, category, quarantined_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (row["id"], row["vin"], row.get("dealer_id"), col, row.get(col), cat, now),
        )
        patch[col] = {"source": HEAL_SOURCE, "detail": f"was {row.get(col)!r} ({cat}); parser artifact"}
    spec = merge_spec_source_json(
        row["spec_source_json"] if isinstance(row.get("spec_source_json"), str) else None, patch
    )
    sets = ", ".join(f"{c} = NULL" for c in cols)
    cur.execute(f"UPDATE cars SET {sets}, spec_source_json = ? WHERE id = ?", (spec, row["id"]))


def heal_vdp_junk_values(*, apply: bool, limit: int | None = None, conn=None) -> dict[str, Any]:
    if conn is None:
        from backend.db.inventory_db import get_conn

        conn = get_conn()
    cur = conn.cursor()
    rows = _select_rows(cur, limit)
    targets = [(r, c) for r in rows if (c := junk_columns(r))]
    cats: Counter[str] = Counter(cat for _, c in targets for cat in c.values())
    dealers: dict[str, set] = {}
    for r, c in targets:
        for cat in c.values():
            dealers.setdefault(cat, set()).add(r.get("dealer_id"))
    stats = {"rows": len(targets), "categories": dict(cats), "healed": 0}
    print(f"junk rows: {len(targets)}", flush=True)
    for cat, n in sorted(cats.items()):
        print(f"  {cat}: {n} values, {len(dealers[cat])} dealers", flush=True)
    for r, c in targets[:15]:
        print(f"  car {r['id']} vin={r['vin']} {c} {[r[k] for k in c]}", flush=True)
    if not apply:
        return stats
    if len(targets) > MAX_APPLY_ROWS:
        raise SystemExit(f"REFUSING --apply: {len(targets)} rows exceeds {MAX_APPLY_ROWS}; check the selection.")
    cur.execute(_QUARANTINE_DDL)
    for r, c in targets:
        _heal_row(cur, r, c)
        stats["healed"] += 1
    conn.commit()
    return stats


def main() -> None:
    ap = argparse.ArgumentParser(description="NULL parser-artifact trim/colour values (N6.5)")
    ap.add_argument("--apply", action="store_true", help="write (default: dry run)")
    ap.add_argument("--dry-run", action="store_true", help="report only (the default)")
    ap.add_argument("--limit", type=int, default=None)
    a = ap.parse_args()
    heal_vdp_junk_values(apply=a.apply and not a.dry_run, limit=a.limit)


if __name__ == "__main__":
    main()
