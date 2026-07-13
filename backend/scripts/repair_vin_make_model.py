#!/usr/bin/env python3
"""Repair cars whose make/model is empty or mis-split, using the VIN (NHTSA vPIC).

Fixes the scraper artifacts where the model was dropped (empty) or a two-word make
was split (``Land Rover`` -> make ``Land`` / model ``Rover``) or truncated
(``Grand Cherokee`` -> ``Grand``). Every such row still carries a valid 17-char VIN,
which decodes to the correct make + model.

Safe: --dry-run default; --apply snapshots to cars_backup_vinrepair_<ts> + JSON first,
then UPDATEs only make/model on the matched rows. Reusable as a periodic safety net.
"""
from __future__ import annotations
import argparse, json, os, sys, time
from backend.utils.project_env import load_project_dotenv
from backend.db.inventory_db import get_conn
from backend.enrichment.nhtsa_vpic import (
    fetch_decode_vin_values_extended, _pretty_make_model, _vpic_scalar_empty,
)

# Rows to repair: empty model, or make is a known two-word-make fragment, or a known truncation.
SELECT_SQL = r"""
SELECT id, make, model, vin FROM cars
WHERE year>=2000 AND vin IS NOT NULL AND length(vin)=17 AND (
    btrim(COALESCE(model,''))='' OR make IN ('Land','Alfa','Aston','Rolls','Mini')
    OR model IN ('Grand','Rover'))
"""


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", default=True)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args(argv)
    load_project_dotenv()
    conn = get_conn(); cur = conn.cursor()
    cur.execute(SELECT_SQL + (f" LIMIT {int(args.limit)}" if args.limit else ""))
    rows = [dict(zip([d[0] for d in cur.description], r)) for r in cur.fetchall()]
    print(f"candidate rows: {len(rows)}")

    plans, unresolved = [], 0
    for r in rows:
        body, flat, err = fetch_decode_vin_values_extended(r["vin"])
        if not flat:
            unresolved += 1
            continue
        mk, md = flat.get("Make"), flat.get("Model")
        new_make = _pretty_make_model(mk) if not _vpic_scalar_empty(mk) else None
        new_model = _pretty_make_model(md) if not _vpic_scalar_empty(md) else None
        chg = {}
        if new_make and new_make != (r["make"] or ""):
            chg["make"] = new_make
        if new_model and new_model != (r["model"] or ""):
            chg["model"] = new_model
        if chg:
            plans.append((r["id"], r["make"], r["model"], chg))
        time.sleep(0.15)  # polite to vPIC

    print(f"resolvable: {len(plans)}, unresolved (vPIC miss): {unresolved}")
    for pid, om, omd, chg in plans[:15]:
        print(f"  #{pid}: {om!r}/{omd!r} -> {chg}")

    if not args.apply:
        print("\nDRY RUN — no changes. Re-run with --apply.")
        return 0
    if not plans:
        print("nothing to apply."); return 0

    ids = [p[0] for p in plans]
    cur.execute("CREATE TABLE IF NOT EXISTS cars_backup_vinrepair_20260713 AS SELECT id, make, model, vin FROM cars WHERE id = ANY(%s)", (ids,))
    bkp = "/private/tmp/claude-501/-Users-asarrafi-Projects/0fd58203-ffeb-4591-b16d-3f7f838acd51/scratchpad/db-backups"
    os.makedirs(bkp, exist_ok=True)
    json.dump([{"id": p[0], "old_make": p[1], "old_model": p[2], "new": p[3]} for p in plans],
              open(f"{bkp}/vinrepair_20260713.json", "w"), indent=0)
    for pid, _om, _omd, chg in plans:
        sets = ", ".join(f"{k}=%s" for k in chg)
        cur.execute(f"UPDATE cars SET {sets} WHERE id=%s", (*chg.values(), pid))
    conn.commit()
    print(f"\nAPPLIED: repaired {len(plans)} rows (backup cars_backup_vinrepair_20260713).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
