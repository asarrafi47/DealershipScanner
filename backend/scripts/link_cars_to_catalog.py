#!/usr/bin/env python3
"""
Link every active listing to its canonical ``epa_master`` catalog row.

Adds ``cars.epa_master_id`` / ``epa_match_confidence`` / ``epa_match_method``
(idempotent), seeds ``model_generations``, then runs the single catalog
resolver over the fleet. Low-confidence cars stay unlinked — they render with
dealer-observed data only.

Usage::

  PYTHONPATH=. python backend/scripts/link_cars_to_catalog.py --dry-run
  PYTHONPATH=. python backend/scripts/link_cars_to_catalog.py
  PYTHONPATH=. python backend/scripts/link_cars_to_catalog.py --only-missing
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

from backend.catalog.generations import seed_model_generations  # noqa: E402
from backend.catalog.resolver import candidates_for, resolve_from_candidates  # noqa: E402
from backend.db.inventory_db import get_conn  # noqa: E402

_LINK_COLUMNS = (
    ("epa_master_id", "BIGINT"),
    ("epa_match_confidence", "DOUBLE PRECISION"),
    ("epa_match_method", "TEXT"),
)


def ensure_link_columns() -> None:
    conn = get_conn()
    cur = conn.cursor()
    for col, ctype in _LINK_COLUMNS:
        try:
            cur.execute(f"ALTER TABLE cars ADD COLUMN IF NOT EXISTS {col} {ctype}")
        except Exception:
            # SQLite: no IF NOT EXISTS — probe and add
            try:
                cur.execute(f"SELECT {col} FROM cars LIMIT 1")
            except Exception:
                sq_type = "REAL" if ctype == "DOUBLE PRECISION" else ("INTEGER" if ctype == "BIGINT" else ctype)
                cur.execute(f"ALTER TABLE cars ADD COLUMN {col} {sq_type}")
    conn.commit()
    conn.close()


def link_fleet(*, dry_run: bool, only_missing: bool) -> dict:
    conn = get_conn()
    cur = conn.cursor()
    where = "COALESCE(listing_active,1)=1"
    if only_missing:
        where += " AND epa_master_id IS NULL"
    cur.execute(
        "SELECT id, year, make, model, trim, cylinders, engine_l, engine_description, "
        f"drivetrain, fuel_type FROM cars WHERE {where}"
    )
    cols = ("id", "year", "make", "model", "trim", "cylinders", "engine_l",
            "engine_description", "drivetrain", "fuel_type")
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]

    cand_cache: dict[tuple, list] = {}
    stats = Counter()
    conf_buckets = Counter()
    updates: list[tuple] = []

    for car in rows:
        stats["examined"] += 1
        try:
            y = int(car.get("year"))
        except (TypeError, ValueError):
            stats["no_year"] += 1
            continue
        mk = (car.get("make") or "").strip()
        md = (car.get("model") or "").strip()
        if not mk or not md:
            stats["no_make_model"] += 1
            continue
        key = (y, mk.lower(), md.lower())
        if key not in cand_cache:
            cand_cache[key] = candidates_for(cur, y, mk, md)
        match = resolve_from_candidates(car, cand_cache[key])
        if match is None:
            stats["unlinked"] += 1
            continue
        stats["linked"] += 1
        conf_buckets[f"{int(match.confidence * 10) / 10:.1f}"] += 1
        updates.append((match.epa_master_id, match.confidence, match.method, car["id"]))

    if not dry_run and updates:
        for i in range(0, len(updates), 1000):
            for u in updates[i : i + 1000]:
                cur.execute(
                    "UPDATE cars SET epa_master_id=?, epa_match_confidence=?, epa_match_method=? WHERE id=?",
                    u,
                )
            conn.commit()
            print(f"  wrote {min(i + 1000, len(updates))}/{len(updates)} links", flush=True)
    conn.close()
    return {"stats": dict(stats), "confidence_hist": dict(sorted(conf_buckets.items()))}


def main() -> None:
    ap = argparse.ArgumentParser(description="Link cars to the epa_master catalog")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--only-missing", action="store_true", help="Skip cars already linked")
    args = ap.parse_args()

    if not args.dry_run:
        ensure_link_columns()
        n = seed_model_generations()
        print(f"model_generations seeded (+{n} rows)", flush=True)

    out = link_fleet(dry_run=args.dry_run, only_missing=args.only_missing)
    print(f"link stats: {out['stats']}", flush=True)
    print(f"confidence histogram: {out['confidence_hist']}", flush=True)


if __name__ == "__main__":
    main()
