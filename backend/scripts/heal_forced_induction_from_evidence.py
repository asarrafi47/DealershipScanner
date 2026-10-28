#!/usr/bin/env python3
"""
Re-resolve ``cars.forced_induction`` from evidence (P10C.9). Dry run by default.

Every active row goes through the same resolver the linker uses
(``resolve_car_forced_induction``: dealer text, vPIC Turbo flag, family-unanimous
catalog flags, description markers, make/model heuristic) and the result is
written only where ``precedence.decide`` allows it, so a value backed by the
dealer's own engine text or the VIN decode is never overwritten.

Dry run writes ``workspace/data_quality/forced_induction_heal_<date>.csv``
(id, vin, ymm, engine_description, old, new, source) and prints a transition
matrix and counts per source and per kind (NA write / boost fill / boost->NA
flip / other). Those are the D-ET2 numbers.

``--sample N`` also writes N rows where the dealer text or VIN decode disagrees
with the unanimous catalog family (mostly catalog mislinks) for owner review.

``--apply`` refuses until a backup CSV (id, vin, forced_induction,
spec_source_json) is written, then updates through a temp table in one
transaction. ``--restore <backup.csv>`` puts the old values back.

Usage::

  PYTHONPATH=. python backend/scripts/heal_forced_induction_from_evidence.py            # dry run
  PYTHONPATH=. python backend/scripts/heal_forced_induction_from_evidence.py --sample 200
  PYTHONPATH=. python backend/scripts/heal_forced_induction_from_evidence.py --apply --backup <csv>
  PYTHONPATH=. python backend/scripts/heal_forced_induction_from_evidence.py --restore <csv>

Writing to any database is owner-gated: never run ``--apply`` without an OK.
"""
from __future__ import annotations

import argparse
import csv
import sys
from collections import Counter
from datetime import date
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT))

from backend.utils.project_env import load_project_dotenv

load_project_dotenv()

NA = "Naturally Aspirated"
_FI_COLS = (
    "id", "vin", "year", "make", "model", "trim", "title", "cylinders", "engine_l",
    "engine_description", "fuel_type", "description", "forced_induction", "spec_source_json",
)
_REPORT_COLS = ("id", "vin", "ymm", "engine_description", "old", "new", "source")
_BACKUP_COLS = ("id", "vin", "forced_induction", "spec_source_json")
_STRONG = ("dealer_feed", "nhtsa_vpic")
_BATCH = 500


class BackupRequired(RuntimeError):
    """``--apply`` without a written backup."""


def _bucket(label: str | None) -> str:
    if not label or not str(label).strip():
        return "(blank)"
    return NA if label == NA else "boosted"


def _kind(old: str | None, new: str) -> str:
    o, n = _bucket(old), _bucket(new)
    if n == NA:
        return "na_write" if o == "(blank)" else "boost_to_na"
    if o == "(blank)":
        return "boost_fill"
    if o == NA:
        return "na_to_boost"
    return "boost_relabel"


def _resolve_rows(conn, *, limit: int | None):
    """Yield (car, label, source, family_labels, decision_write) for every active car."""
    from backend.catalog.linker import _family_labels
    from backend.catalog.resolver import candidates_for
    from backend.enrichment.knowledge_engine import lookup_vpic_from_cache, prime_vpic_cache
    from backend.utils.forced_induction import (
        TWIN_TURBO, classify_forced_induction_from_car_row, resolve_car_forced_induction,
    )
    from backend.vehicle_facts import precedence as prec

    cur = conn.cursor()
    sql = (
        f"SELECT {', '.join(_FI_COLS)} FROM cars WHERE COALESCE(listing_active, 1) = 1 "
        "AND vin IS NOT NULL ORDER BY id"
    )
    if limit:
        sql += f" LIMIT {int(limit)}"
    cur.execute(sql)
    cars = [dict(zip(_FI_COLS, r)) for r in cur.fetchall()]
    cand_cache: dict[tuple, list] = {}
    for i in range(0, len(cars), _BATCH):
        chunk = cars[i:i + _BATCH]
        try:
            prime_vpic_cache([c["vin"] for c in chunk])
        except Exception:  # noqa: BLE001 - an unprimed memo only costs speed
            pass
        for car in chunk:
            try:
                y = int(car.get("year"))
            except (TypeError, ValueError):
                continue
            mk = (car.get("make") or "").strip()
            md = (car.get("model") or "").strip()
            if not mk or not md:
                continue
            key = (y, mk.lower(), md.lower())
            if key not in cand_cache:
                cand_cache[key] = candidates_for(cur, y, mk, md)
            twin = (classify_forced_induction_from_car_row(car) or "") == TWIN_TURBO
            try:
                boosted = (lookup_vpic_from_cache(car.get("vin")) or {}).get("boosted")
            except Exception:  # noqa: BLE001 - no decode means vPIC did not say
                boosted = None
            family = _family_labels(car, cand_cache[key], twin)
            label, src = resolve_car_forced_induction(car, vpic_boosted=boosted, family_flags=family)
            if not label:
                continue
            decision = prec.decide(
                "forced_induction", car.get("forced_induction"), car.get("spec_source_json"),
                label, src,
            )
            yield car, label, src, family, bool(decision.write)


def _write_csv(path: Path, cols, rows) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(cols)
        w.writerows(rows)


def _update_via_temp_table(conn, changes: list[tuple]) -> None:
    """changes: (id, forced_induction, spec_source_json). One transaction."""
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS _fi_heal")
    cur.execute(
        "CREATE TEMP TABLE _fi_heal (id BIGINT PRIMARY KEY, forced_induction TEXT, spec_source_json TEXT)"
    )
    cur.executemany("INSERT INTO _fi_heal (id, forced_induction, spec_source_json) VALUES (?, ?, ?)", changes)
    cur.execute(
        "UPDATE cars SET forced_induction = _fi_heal.forced_induction, "
        "spec_source_json = _fi_heal.spec_source_json "
        "FROM _fi_heal WHERE cars.id = _fi_heal.id"
    )
    cur.execute("DROP TABLE _fi_heal")
    conn.commit()


def heal(conn, *, apply: bool = False, backup_path: Path | None = None,
         out_dir: Path | None = None, limit: int | None = None,
         sample: int = 0, today: str | None = None) -> dict:
    """Re-resolve forced_induction. Dry run unless *apply*; never writes the DB otherwise."""
    from backend.utils.spec_provenance import merge_spec_source_json

    if apply and backup_path is None:
        raise BackupRequired("--apply needs a backup CSV path; nothing was written")
    out_dir = Path(out_dir or _REPO_ROOT / "workspace" / "data_quality")
    stamp = today or date.today().isoformat()

    report, changes, backup = [], [], []
    matrix: Counter = Counter()
    by_source: Counter = Counter()
    by_kind: Counter = Counter()
    conflicts = []
    scanned = unchanged = blocked = 0
    for car, label, src, family, write in _resolve_rows(conn, limit=limit):
        scanned += 1
        old = car.get("forced_induction")
        fam = {str(x).strip() for x in family if x and str(x).strip()}
        if src in _STRONG and len(fam) == 1 and label not in fam:
            conflicts.append((car, label, src, next(iter(fam))))
        if not write:
            blocked += 1
            continue
        if (old or "").strip() == label:
            unchanged += 1
            continue
        matrix[(_bucket(old), _bucket(label))] += 1
        by_source[src] += 1
        by_kind[_kind(old, label)] += 1
        ymm = f"{car.get('year')} {car.get('make')} {car.get('model')}"
        report.append((car["id"], car["vin"], ymm, car.get("engine_description"), old or "", label, src))
        prov = merge_spec_source_json(
            car.get("spec_source_json") if isinstance(car.get("spec_source_json"), str) else None,
            {"forced_induction": {"source": src, "value": label}},
        )
        changes.append((car["id"], label, prov))
        backup.append((car["id"], car["vin"], old or "", car.get("spec_source_json") or ""))

    report_path = out_dir / f"forced_induction_heal_{stamp}.csv"
    _write_csv(report_path, _REPORT_COLS, report)
    sample_path = None
    if sample and conflicts:
        step = max(1, len(conflicts) // sample)
        picked = conflicts[::step][:sample]
        sample_path = out_dir / f"forced_induction_conflicts_{stamp}.csv"
        _write_csv(
            sample_path,
            ("id", "vin", "ymm", "engine_description", "stored", "resolved", "source", "family"),
            [(c["id"], c["vin"], f"{c.get('year')} {c.get('make')} {c.get('model')}",
              c.get("engine_description"), c.get("forced_induction") or "", lab, src, fam)
             for c, lab, src, fam in picked],
        )

    if apply and changes:
        backup_path = Path(backup_path)
        _write_csv(backup_path, _BACKUP_COLS, backup)
        if not backup_path.exists():
            raise BackupRequired(f"backup {backup_path} was not written; nothing updated")
        try:
            _update_via_temp_table(conn, changes)
        except Exception:
            conn.rollback()
            raise

    return {
        "mode": "apply" if apply else "dry_run",
        "scanned": scanned, "changes": len(changes), "unchanged": unchanged,
        "blocked_by_precedence": blocked, "conflicts": len(conflicts),
        "by_kind": dict(by_kind), "by_source": dict(by_source),
        "matrix": {f"{a} -> {b}": n for (a, b), n in sorted(matrix.items())},
        "report_path": str(report_path), "sample_path": str(sample_path) if sample_path else None,
        "backup_path": str(backup_path) if apply and changes else None,
    }


def restore(conn, backup_path: Path) -> int:
    """Put the backed-up forced_induction (and provenance) back. Returns rows restored."""
    with Path(backup_path).open(newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    if rows and not {"id", "forced_induction"} <= set(rows[0]):
        raise ValueError("backup CSV needs id and forced_induction columns")
    changes = [
        (int(r["id"]), r["forced_induction"] or None,
         (r.get("spec_source_json") or None) if "spec_source_json" in r else None)
        for r in rows
    ]
    if not changes:
        return 0
    if "spec_source_json" not in rows[0]:
        cur = conn.cursor()
        cur.executemany("UPDATE cars SET forced_induction = ? WHERE id = ?",
                        [(fi, i) for i, fi, _ in changes])
        conn.commit()
        return len(changes)
    try:
        _update_via_temp_table(conn, changes)
    except Exception:
        conn.rollback()
        raise
    return len(changes)


def _print_summary(res: dict) -> None:
    print(f"{res['mode'].upper()}: scanned {res['scanned']}, changes {res['changes']}, "
          f"unchanged {res['unchanged']}, blocked by precedence {res['blocked_by_precedence']}, "
          f"text/VIN-vs-family conflicts {res['conflicts']}", flush=True)
    print("by kind:", res["by_kind"], flush=True)
    print("by source:", res["by_source"], flush=True)
    print("transition matrix (old -> new):", flush=True)
    for k, n in res["matrix"].items():
        print(f"  {k}: {n}", flush=True)
    print("report:", res["report_path"], flush=True)
    if res["sample_path"]:
        print("conflict sample:", res["sample_path"], flush=True)


def main() -> None:
    ap = argparse.ArgumentParser(description="Re-resolve forced_induction from evidence (dry run by default)")
    ap.add_argument("--apply", action="store_true", help="Write changes (owner-gated; needs a backup)")
    ap.add_argument("--backup", type=Path, default=None, help="Backup CSV path for --apply")
    ap.add_argument("--restore", type=Path, default=None, help="Restore from a backup CSV and exit")
    ap.add_argument("--sample", type=int, default=0, help="Write N text/VIN-vs-family conflicts for review")
    ap.add_argument("--limit", type=int, default=None, help="Only the first N rows (testing)")
    ap.add_argument("--out-dir", type=Path, default=None)
    args = ap.parse_args()

    from backend.db.inventory_db import get_conn

    conn = get_conn()
    try:
        if args.restore:
            print(f"restored {restore(conn, args.restore)} rows", flush=True)
            return
        backup = args.backup
        if args.apply and backup is None:
            backup = (_REPO_ROOT / "workspace" / "backups"
                      / f"forced_induction_backup_{date.today().isoformat()}.csv")
        _print_summary(heal(conn, apply=args.apply, backup_path=backup, out_dir=args.out_dir,
                            limit=args.limit, sample=args.sample))
    finally:
        conn.close()


if __name__ == "__main__":
    main()
