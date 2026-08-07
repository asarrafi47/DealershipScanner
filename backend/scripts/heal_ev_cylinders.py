#!/usr/bin/env python3
"""
Heal junk ``cylinders`` on true battery-electric rows (evidence-backed one-shot).

Feeds stored the gas sibling's cylinder count on BEVs (Toyota C-HR BEV "4"
×~362, BMW i4 "4") and GM feeds use 99 as an electric sentinel (~123 rows).
There was no write-time guard in the upsert path, so the junk accumulated;
the guard now exists (``fuel_label_plausibility`` in ``upsert_vehicles``) and
this script clears the backlog:

Phase A — EV cylinders: active rows whose fuel label is bare electric and whose
cylinders are non-zero are set to 0, but ONLY with evidence: the nameplate is a
known BEV or the engine text confirms electric, AND the engine text carries no
combustion evidence. Rows WITH combustion evidence (the gas-marked-"Electric"
GX 550 cluster) are deliberately left alone — their labels are corrected at
read time and at the next scan, never rewritten in bulk here (writing labels
without provenance is how past messes started).

Phase B — sentinel counts: any remaining active row with cylinders > 16 (no
production engine has more) is set to NULL regardless of fuel label.

Provenance is merged into ``spec_source_json`` (source ``ev_cylinders_heal``),
mirroring ``heal_cylinders_from_vpic.py``.

Usage::

  PYTHONPATH=. python backend/scripts/heal_ev_cylinders.py            # dry run
  PYTHONPATH=. python backend/scripts/heal_ev_cylinders.py --apply    # write
  PYTHONPATH=. python backend/scripts/heal_ev_cylinders.py --limit 500
"""
from __future__ import annotations

import argparse
import sys
from collections import Counter
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT))

from backend.utils.project_env import load_project_dotenv

load_project_dotenv()

from backend.db.inventory_db import get_conn  # noqa: E402
from backend.utils.fuel_label_plausibility import (  # noqa: E402
    MAX_REAL_CYLINDERS,
    PLAUSIBLE,
    assess_electric_claim,
    combustion_evidence_from_text,
    electric_evidence_from_text,
    engine_text_for_car,
    is_known_bev_nameplate,
)
from backend.utils.spec_provenance import merge_spec_source_json  # noqa: E402

_ELECTRIC_LABELS = ("electric", "ev", "bev", "electricity", "battery electric")

_ROW_COLS = (
    "id", "vin", "year", "make", "model", "trim", "title",
    "cylinders", "fuel_type", "engine_description", "spec_source_json",
)


def _select_rows(cur, where: str, limit: int | None) -> list[dict]:
    sql = f"SELECT {', '.join(_ROW_COLS)} FROM cars WHERE {where}"
    if limit:
        sql += f" LIMIT {int(limit)}"
    cur.execute(sql)
    return [dict(zip(_ROW_COLS, r)) for r in cur.fetchall()]


def _write_cylinders(cur, car: dict, value, detail: str) -> None:
    new_src = merge_spec_source_json(
        car.get("spec_source_json") if isinstance(car.get("spec_source_json"), str) else None,
        {"cylinders": {"source": "ev_cylinders_heal", "detail": detail}},
    )
    cur.execute(
        "UPDATE cars SET cylinders = ?, spec_source_json = ? WHERE id = ?",
        (value, new_src, car["id"]),
    )


def heal_ev_cylinders(*, apply: bool, limit: int | None) -> dict[str, int]:
    """Phase A: zero junk cylinder counts on evidence-confirmed BEV rows."""
    conn = get_conn()
    cur = conn.cursor()
    labels = ", ".join(f"'{v}'" for v in _ELECTRIC_LABELS)
    rows = _select_rows(
        cur,
        "COALESCE(listing_active, 1) = 1 "
        f"AND LOWER(TRIM(fuel_type)) IN ({labels}) "
        "AND cylinders IS NOT NULL AND cylinders != 0",
        limit,
    )
    stats = {
        "rows": len(rows),
        "zeroed": 0,
        "kept_combustion_evidence": 0,
        "kept_label_not_plausible": 0,
        "skipped_no_ev_evidence": 0,
    }
    zeroed_by_model: Counter[tuple] = Counter()
    kept_by_model: Counter[tuple] = Counter()
    skipped_by_model: Counter[tuple] = Counter()

    for car in rows:
        key = (str(car.get("make") or "?"), str(car.get("model") or "?"))
        engine_blob = engine_text_for_car(car)
        combustion = combustion_evidence_from_text(engine_blob)
        if combustion:
            # The gas-marked-"Electric" cluster: cylinders are the evidence that
            # disproves the label. Do NOT touch — read-time handles the display.
            stats["kept_combustion_evidence"] += 1
            kept_by_model[key] += 1
            continue
        if assess_electric_claim(car).verdict != PLAUSIBLE:
            stats["kept_label_not_plausible"] += 1
            kept_by_model[key] += 1
            continue
        if not (is_known_bev_nameplate(car) or electric_evidence_from_text(engine_blob)):
            # Bare "Electric" with no confirming evidence either way: abstain.
            stats["skipped_no_ev_evidence"] += 1
            skipped_by_model[key] += 1
            continue
        stats["zeroed"] += 1
        zeroed_by_model[key] += 1
        if apply:
            _write_cylinders(
                cur, car, 0,
                f"BEV junk count {car.get('cylinders')} -> 0 (fleet heal)",
            )

    if apply:
        conn.commit()
    conn.close()

    def _dump(label: str, counter: Counter) -> None:
        if not counter:
            return
        print(f"  {label}:", flush=True)
        for (mk, md), n in counter.most_common():
            print(f"    {mk} {md}: {n}", flush=True)

    _dump("would zero" if not apply else "zeroed", zeroed_by_model)
    _dump("kept (evidence contradicts the electric label)", kept_by_model)
    _dump("skipped (no confirming EV evidence)", skipped_by_model)
    return stats


def heal_sentinel_cylinders(*, apply: bool, limit: int | None) -> dict[str, int]:
    """Phase B: NULL any remaining impossible counts (feed sentinel 99 etc.)."""
    conn = get_conn()
    cur = conn.cursor()
    rows = _select_rows(
        cur,
        f"COALESCE(listing_active, 1) = 1 AND cylinders > {MAX_REAL_CYLINDERS}",
        limit,
    )
    stats = {"rows": len(rows), "nulled": 0}
    by_model: Counter[tuple] = Counter()
    for car in rows:
        stats["nulled"] += 1
        by_model[(str(car.get("make") or "?"), str(car.get("model") or "?"))] += 1
        if apply:
            _write_cylinders(
                cur, car, None,
                f"sentinel count {car.get('cylinders')} -> NULL (fleet heal)",
            )
    if apply:
        conn.commit()
    conn.close()
    if by_model:
        print(f"  {'would null' if not apply else 'nulled'} (sentinel):", flush=True)
        for (mk, md), n in by_model.most_common():
            print(f"    {mk} {md}: {n}", flush=True)
    return stats


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Zero junk cylinder counts on true BEVs; NULL sentinel counts"
    )
    ap.add_argument("--apply", action="store_true", help="Write changes (default: dry run)")
    ap.add_argument("--limit", type=int, default=None, help="Only process the first N rows (testing)")
    args = ap.parse_args()
    mode = "APPLY" if args.apply else "DRY RUN"

    print(f"Phase A — junk cylinders on electric-labelled rows ({mode})", flush=True)
    a = heal_ev_cylinders(apply=args.apply, limit=args.limit)
    print(f"Phase A done: {a}", flush=True)

    print(f"Phase B — sentinel counts > {MAX_REAL_CYLINDERS} on any row ({mode})", flush=True)
    b = heal_sentinel_cylinders(apply=args.apply, limit=args.limit)
    print(f"Phase B done: {b}", flush=True)


if __name__ == "__main__":
    main()
