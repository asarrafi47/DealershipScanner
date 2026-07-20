#!/usr/bin/env python3
"""
Heal ``cylinders`` (and stale ``forced_induction``) against NHTSA vPIC.

Phase A — cylinders: batch-decode every active 17-char VIN via the vPIC
DecodeVinValuesBatch API (50 VINs/request, authoritative — the engine is
encoded in the VIN) and correct rows whose stored cylinders disagree.
Provenance is merged into ``spec_source_json`` (source ``nhtsa_vpic_heal``).

Phase B — forced induction: re-run the (year-guarded) classifier for rows
with a stored label and year < 2016; blanket make-era guesses are cleared,
explicit-marker labels (turbo in trim/description) survive.

Usage::

  PYTHONPATH=. python backend/scripts/heal_cylinders_from_vpic.py --dry-run
  PYTHONPATH=. python backend/scripts/heal_cylinders_from_vpic.py
  PYTHONPATH=. python backend/scripts/heal_cylinders_from_vpic.py --limit 500
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT))

from backend.utils.project_env import load_project_dotenv

load_project_dotenv()

import requests  # noqa: E402

from backend.db.inventory_db import get_conn  # noqa: E402
from backend.utils.forced_induction import classify_forced_induction_from_car_row  # noqa: E402
from backend.utils.spec_provenance import merge_spec_source_json  # noqa: E402

_VPIC_BATCH_URL = "https://vpic.nhtsa.dot.gov/api/vehicles/DecodeVinValuesBatch/"
_BATCH_SIZE = 50


def _decode_batch(vins: list[str]) -> dict[str, int]:
    """VIN → cylinders for VINs vPIC can decode to a sane count."""
    out: dict[str, int] = {}
    resp = requests.post(
        _VPIC_BATCH_URL,
        data={"format": "json", "data": ";".join(vins)},
        timeout=90,
    )
    resp.raise_for_status()
    for res in resp.json().get("Results", []):
        vin = (res.get("VIN") or "").strip().upper()
        raw = (res.get("EngineCylinders") or "").strip()
        if vin and raw.isdigit():
            n = int(raw)
            if 2 <= n <= 16:
                out[vin] = n
    return out


def heal_cylinders(*, dry_run: bool, limit: int | None) -> dict[str, int]:
    conn = get_conn()
    cur = conn.cursor()
    sql = (
        "SELECT id, vin, cylinders, spec_source_json FROM cars "
        "WHERE COALESCE(listing_active,1)=1 AND vin IS NOT NULL AND length(vin)=17"
    )
    if limit:
        sql += f" LIMIT {int(limit)}"
    cur.execute(sql)
    rows = cur.fetchall()
    stats = {"rows": len(rows), "decoded": 0, "already_correct": 0, "fixed": 0, "filled": 0, "undecodable": 0}

    for i in range(0, len(rows), _BATCH_SIZE):
        chunk = rows[i : i + _BATCH_SIZE]
        try:
            truth = _decode_batch([r[1] for r in chunk])
        except Exception as exc:  # noqa: BLE001 — keep healing on batch failure
            print(f"  batch {i // _BATCH_SIZE}: vPIC request failed ({exc}); skipping", flush=True)
            time.sleep(2)
            continue
        for cid, vin, cyl, spec_src in chunk:
            t = truth.get((vin or "").upper())
            if t is None:
                stats["undecodable"] += 1
                continue
            stats["decoded"] += 1
            cur_cyl = None
            try:
                cur_cyl = int(cyl) if cyl is not None else None
            except (TypeError, ValueError):
                pass
            if cur_cyl == t:
                stats["already_correct"] += 1
                continue
            stats["fixed" if cur_cyl else "filled"] += 1
            if dry_run:
                continue
            new_src = merge_spec_source_json(
                spec_src if isinstance(spec_src, str) else None,
                {"cylinders": {"source": "nhtsa_vpic_heal", "detail": "DecodeVinValuesBatch fleet heal"}},
            )
            cur.execute(
                "UPDATE cars SET cylinders = ?, spec_source_json = ? WHERE id = ?",
                (t, new_src, cid),
            )
        if not dry_run:
            conn.commit()
        done = min(i + _BATCH_SIZE, len(rows))
        if (i // _BATCH_SIZE) % 20 == 0 or done == len(rows):
            print(f"  cylinders: {done}/{len(rows)} rows · fixed={stats['fixed']} filled={stats['filled']}", flush=True)
        time.sleep(0.35)

    conn.close()
    return stats


def heal_forced_induction(*, dry_run: bool) -> dict[str, int]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, vin, make, model, trim, year, cylinders, engine_l, engine_description, "
        "fuel_type, description, title, forced_induction FROM cars "
        "WHERE COALESCE(listing_active,1)=1 AND forced_induction IS NOT NULL "
        "AND forced_induction <> '' AND year IS NOT NULL AND year < 2016"
    )
    cols = (
        "id", "vin", "make", "model", "trim", "year", "cylinders", "engine_l",
        "engine_description", "fuel_type", "description", "title", "forced_induction",
    )
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    stats = {"rows": len(rows), "kept": 0, "cleared": 0, "changed": 0}

    for car in rows:
        new_fi = classify_forced_induction_from_car_row(car)
        old_fi = (car.get("forced_induction") or "").strip()
        if (new_fi or "") == old_fi:
            stats["kept"] += 1
            continue
        stats["cleared" if not new_fi else "changed"] += 1
        if dry_run:
            continue
        cur.execute("UPDATE cars SET forced_induction = ? WHERE id = ?", (new_fi, car["id"]))
    if not dry_run:
        conn.commit()
    conn.close()
    return stats


def main() -> None:
    ap = argparse.ArgumentParser(description="Heal cylinders + forced_induction against NHTSA vPIC")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=None, help="Only process the first N rows (testing)")
    ap.add_argument("--skip-fi", action="store_true", help="Skip the forced-induction recompute phase")
    args = ap.parse_args()

    print(f"Phase A — cylinders vs vPIC ({'DRY RUN' if args.dry_run else 'live'})", flush=True)
    a = heal_cylinders(dry_run=args.dry_run, limit=args.limit)
    print(f"Phase A done: {a}", flush=True)

    if not args.skip_fi:
        print(f"Phase B — forced_induction recompute, year<2016 ({'DRY RUN' if args.dry_run else 'live'})", flush=True)
        b = heal_forced_induction(dry_run=args.dry_run)
        print(f"Phase B done: {b}", flush=True)


if __name__ == "__main__":
    main()
