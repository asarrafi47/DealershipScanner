#!/usr/bin/env python3
"""
Link every active listing to its canonical ``epa_master`` catalog row.

Adds ``cars.epa_master_id`` / ``epa_match_confidence`` / ``epa_match_method``
(idempotent), seeds ``model_generations``, then runs the single catalog
resolver over the fleet. Low-confidence cars stay unlinked — they render with
dealer-observed data only.

A catalog read error (``CatalogUnavailableError``) stops the run before any
UPDATE, with exit status 2: a half-read catalog must never decide links.

Usage::

  PYTHONPATH=. python backend/scripts/link_cars_to_catalog.py --dry-run
  PYTHONPATH=. python backend/scripts/link_cars_to_catalog.py
  PYTHONPATH=. python backend/scripts/link_cars_to_catalog.py --only-missing
  PYTHONPATH=. python backend/scripts/link_cars_to_catalog.py --dangling --exclude-source dictionary_legacy_root
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
from backend.catalog.resolver import (  # noqa: E402
    CatalogUnavailableError,
    candidates_for,
    resolve_from_candidates,
)
from backend.db.inventory_db import get_conn  # noqa: E402

_LINK_COLUMNS = (
    ("epa_master_id", "BIGINT"),
    ("epa_match_confidence", "DOUBLE PRECISION"),
    ("epa_match_method", "TEXT"),
)


def ensure_link_columns() -> None:
    """Add the link columns once. Probe first: ``ALTER TABLE ... IF NOT EXISTS`` still
    takes an ACCESS EXCLUSIVE lock even when the column exists, and on 2026-09-23 it
    queued behind a 4-hour idle-in-transaction session from the nightly invariants
    job and parked every web query behind it."""
    conn = get_conn()
    cur = conn.cursor()
    for col, ctype in _LINK_COLUMNS:
        try:
            cur.execute(f"SELECT {col} FROM cars LIMIT 1")
            cur.fetchall()
            continue  # present: no DDL, no lock
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
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


_CAR_COLS = ("id", "year", "make", "model", "trim", "cylinders", "engine_l",
             "engine_description", "drivetrain", "fuel_type", "title", "vin")
_ROW_COLS = _CAR_COLS + ("epa_master_id", "epa_match_confidence", "epa_match_method")
_CLEAR_SQL = "UPDATE cars SET epa_master_id=NULL, epa_match_confidence=NULL, epa_match_method=NULL WHERE id=?"
_SET_SQL = "UPDATE cars SET epa_master_id=?, epa_match_confidence=?, epa_match_method=? WHERE id=?"
_DANGLING_SQL = (
    "(epa_master_id IS NOT NULL AND NOT EXISTS "
    "(SELECT 1 FROM epa_master m WHERE m.id = cars.epa_master_id))"
)
_CHUNK = 1000


def read_vins_file(path: str) -> tuple[str, ...]:
    """One VIN per line; blank lines and ``#`` comments are skipped."""
    out: list[str] = []
    for line in Path(path).read_text().splitlines():
        v = line.split("#", 1)[0].strip().upper()
        if v:
            out.append(v)
    return tuple(dict.fromkeys(out))


def _excluded_catalog_ids(cur, exclude_sources: tuple[str, ...]) -> set[int]:
    """Catalog ids whose ``epa_master.source`` is excluded. Rows with a NULL source
    stay eligible: only a recorded provenance can exclude a row."""
    if not exclude_sources:
        return set()
    cur.execute(
        f"SELECT id FROM epa_master WHERE source IN ({','.join('?' * len(exclude_sources))})",
        tuple(exclude_sources),
    )
    return {int(r[0]) for r in cur.fetchall()}


def _queries(*, only_missing, dealers, years, dangling, include_inactive, vins, car_ids):
    """The (WHERE, params) pairs that together name the candidate cars."""
    scope: list[str] = []
    if dealers:
        scope.append("dealer_id IN (" + ",".join("?" * len(dealers)) + ")")
    if years:
        scope.append("year IN (" + ",".join("?" * len(years)) + ")")
    scope_params: tuple = tuple(dealers) + tuple(years)
    base = ([] if include_inactive else ["COALESCE(listing_active,1)=1"])
    base += (["epa_master_id IS NULL"] if only_missing else []) + scope
    params = scope_params
    if dangling:
        # Dangling links are repaired on active and inactive cars alike. Alone, the mode
        # selects just those cars; with --only-missing it adds them to the missing ones.
        dang = " AND ".join(scope + [_DANGLING_SQL])
        if only_missing:
            where, params = f"({' AND '.join(base)}) OR {dang}", scope_params + scope_params
        else:
            where = dang
    else:
        where = " AND ".join(base) or "1=1"
    if vins:
        return [(f"({where}) AND UPPER(vin) IN ({','.join('?' * len(c))})", params + tuple(c))
                for c in (vins[i:i + _CHUNK] for i in range(0, len(vins), _CHUNK))]
    if car_ids is not None:
        return [(f"({where}) AND id IN ({','.join('?' * len(c))})", params + tuple(c))
                for c in (car_ids[i:i + _CHUNK] for i in range(0, len(car_ids), _CHUNK))]
    return [(where, params)]


def link_fleet(
    *,
    dry_run: bool,
    only_missing: bool,
    dealers: tuple[str, ...] = (),
    years: tuple[int, ...] = (),
    dangling: bool = False,
    include_inactive: bool = False,
    clear_unresolved: bool = False,
    vins: tuple[str, ...] = (),
    exclude_sources: tuple[str, ...] = (),
    car_ids: tuple[int, ...] | None = None,
    on_catalog_error: str = "raise",
) -> dict:
    """Resolve and write catalog links.

    Default (no mode flags): every active car is re-resolved and a car that no longer
    resolves keeps its old link, as before. The modes:

    * ``dangling`` - cars whose ``epa_master_id`` has no catalog row, active or not; they
      are cleared when nothing resolves.
    * ``include_inactive`` - drop the active-listing filter.
    * ``clear_unresolved`` - a linked car that no longer resolves loses its link.
    * ``vins`` / ``car_ids`` - restrict to those cars.
    * ``exclude_sources`` - catalog rows with these ``epa_master.source`` values are never
      candidates, so no car can be linked to one.

    ``on_catalog_error="raise"`` stops the run before any UPDATE (CLI behaviour);
    ``"skip"`` leaves the affected cars untouched and counts ``skipped_catalog_error``.
    Returns ``stats``, ``confidence_hist`` and ``changes`` (one dict per changed car, with
    the old and new link, so a caller can undo)."""
    conn = get_conn()
    cur = conn.cursor()
    rows: list[dict] = []
    for where, qparams in _queries(
        only_missing=only_missing, dealers=dealers, years=years, dangling=dangling,
        include_inactive=include_inactive, vins=vins, car_ids=car_ids,
    ):
        cur.execute(f"SELECT {', '.join(_ROW_COLS)} FROM cars WHERE {where}", qparams)
        rows.extend(dict(zip(_ROW_COLS, r)) for r in cur.fetchall())
    excluded = _excluded_catalog_ids(cur, tuple(exclude_sources))
    dangling_ids: set[int] = set()
    if dangling and rows:
        cur.execute(f"SELECT id FROM cars WHERE {_DANGLING_SQL}")
        dangling_ids = {int(r[0]) for r in cur.fetchall()}
    try:
        from backend.enrichment.knowledge_engine import prime_vpic_cache

        prime_vpic_cache([r.get("vin") for r in rows])
    except Exception:  # noqa: BLE001
        pass

    cand_cache: dict[tuple, list | None] = {}
    stats = Counter({"changed": 0, "cleared": 0, "unchanged": 0, "skipped_catalog_error": 0})
    conf_buckets = Counter()
    writes: list[tuple[str, tuple]] = []
    changes: list[dict] = []

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
            try:
                cand_cache[key] = candidates_for(cur, y, mk, md)
            except CatalogUnavailableError:
                if on_catalog_error == "skip":
                    cand_cache[key] = None
                else:
                    # Every UPDATE runs after this loop, so nothing has been written.
                    for step in (conn.rollback, conn.close):
                        try:
                            step()
                        except Exception:  # noqa: BLE001 - re-raising the catalog error
                            pass
                    raise
        cands = cand_cache[key]
        if cands is None:
            stats["skipped_catalog_error"] += 1
            continue
        if excluded:
            cands = [c for c in cands if int(c["id"]) not in excluded]
        old = (car.get("epa_master_id"), car.get("epa_match_confidence"), car.get("epa_match_method"))
        match = resolve_from_candidates(car, cands)
        if match is None:
            stats["unlinked"] += 1
            if old[0] is not None and (clear_unresolved or car["id"] in dangling_ids):
                stats["cleared"] += 1
                writes.append((_CLEAR_SQL, (car["id"],)))
                changes.append({"id": car["id"], "old": old, "new": (None, None, None)})
            continue
        stats["linked"] += 1
        conf_buckets[f"{int(match.confidence * 10) / 10:.1f}"] += 1
        new = (match.epa_master_id, match.confidence, match.method)
        if new == old:
            stats["unchanged"] += 1
            continue
        stats["changed"] += 1
        writes.append((_SET_SQL, new + (car["id"],)))
        changes.append({"id": car["id"], "old": old, "new": new})

    if not dry_run and writes:
        for i in range(0, len(writes), _CHUNK):
            for sql, params in writes[i : i + _CHUNK]:
                cur.execute(sql, params)
            conn.commit()
            print(f"  wrote {min(i + _CHUNK, len(writes))}/{len(writes)} link changes", flush=True)
    conn.close()
    return {"stats": dict(stats), "confidence_hist": dict(sorted(conf_buckets.items())), "changes": changes}


def relink_cars(
    ids,
    exclude_sources: tuple[str, ...] = (),
    *,
    dry_run: bool = False,
    clear_unresolved: bool = True,
    on_catalog_error: str = "skip",
) -> dict:
    """Library entry (P10B.6): re-resolve exactly these car ids, active or not.

    A car that no longer resolves loses its link (matches ``linker.py``); a catalog read
    error leaves the affected cars alone. Returns the ``link_fleet`` result."""
    ids = tuple(int(i) for i in ids)
    if not ids:
        return {"stats": {}, "confidence_hist": {}, "changes": []}
    return link_fleet(
        dry_run=dry_run, only_missing=False, include_inactive=True,
        clear_unresolved=clear_unresolved, exclude_sources=tuple(exclude_sources),
        car_ids=ids, on_catalog_error=on_catalog_error,
    )


def main() -> None:
    ap = argparse.ArgumentParser(description="Link cars to the epa_master catalog")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--only-missing", action="store_true", help="Skip cars already linked")
    ap.add_argument("--dealers", default="", help="comma-separated dealer_ids to relink (default: whole fleet)")
    ap.add_argument("--years", default="", help="comma-separated model years to relink (default: all)")
    ap.add_argument("--dangling", action="store_true",
                    help="re-resolve cars (active or not) whose epa_master_id has no catalog row; clear if unresolved")
    ap.add_argument("--include-inactive", action="store_true", help="also relink inactive listings")
    ap.add_argument("--clear-unresolved", action="store_true",
                    help="clear links of cars that no longer resolve (default: keep them)")
    ap.add_argument("--vins-file", default="", help="file with one VIN per line; restrict to those cars")
    ap.add_argument("--exclude-source", action="append", default=[], metavar="NAME",
                    help="never link to catalog rows with this epa_master.source (repeatable)")
    args = ap.parse_args()

    if not args.dry_run:
        ensure_link_columns()
        n = seed_model_generations()
        print(f"model_generations seeded (+{n} rows)", flush=True)

    try:
        out = link_fleet(
            dry_run=args.dry_run, only_missing=args.only_missing,
            dealers=tuple(d.strip() for d in args.dealers.split(",") if d.strip()),
            years=tuple(int(y) for y in args.years.split(",") if y.strip().isdigit()),
            dangling=args.dangling, include_inactive=args.include_inactive,
            clear_unresolved=args.clear_unresolved,
            vins=read_vins_file(args.vins_file) if args.vins_file else (),
            exclude_sources=tuple(args.exclude_source),
        )
    except CatalogUnavailableError as exc:
        print(f"catalog unavailable, no links written: {exc}", file=sys.stderr, flush=True)
        raise SystemExit(2) from exc
    print(f"link stats: {out['stats']}", flush=True)
    print(f"confidence histogram: {out['confidence_hist']}", flush=True)


if __name__ == "__main__":
    main()
