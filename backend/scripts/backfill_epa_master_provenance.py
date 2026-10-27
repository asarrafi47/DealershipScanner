#!/usr/bin/env python3
"""P10A.6: stamp every ``epa_master`` row with its provenance (``source``) and its
permanent catalog key (``natural_key``, P10A.2).

Classification, in order. A row that matches no class is never defaulted:

1. ``id`` in ``epa_master_dump_id_map.live_id``       -> ``dictionary_epa``
   (the rows the owner's dump import placed; keyed ``vid:<id>`` when the row has an
   ``epa_vehicle_id``, else base key + ordinal).
2. ``epa_vehicle_id`` present, not in the map         -> ``epa_vehicles_csv``, ``vid:<id>``.
3. ``id`` <= 20006 AND its base key appears in a re-read of the root ``DICTIONARY/``
   CSVs                                               -> ``legacy`` (a POSITIVE match, so an
   interim insert can never be mislabelled legacy). The re-read must happen before
   P10D.8 deletes that folder.

The run refuses (exit 2, nothing written) when any row matches no class, when two rows
would share (source, natural_key) (the first 20 are listed), when the root ``DICTIONARY/``
folder is missing or thin, or when another session is active or idle in transaction on
this database (``pg_stat_activity``: mini shards write this Postgres over the tunnel and
lock files are host-local, so a lock-file check would miss them).

DRY RUN by default. ``--apply`` writes in ONE transaction, by id, only where
``source IS NULL`` unless ``--recompute`` is given (an existing natural_key is always
kept: ordinals are minted once). It never touches ``cars`` or ``epa_extended_specs``,
and asserts afterwards that the per-source counts are as classified and that the
dump-carried columns were not nulled: vid (49,933 rows), atv_type (6,939), city_e (1,947)
in the 2026-10 dump rows. No builder may ever null them.

  PYTHONPATH=. .venv/bin/python backend/scripts/backfill_epa_master_provenance.py
  PYTHONPATH=. .venv/bin/python backend/scripts/backfill_epa_master_provenance.py --apply
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Mapping, Sequence

_REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT))

EXIT_REFUSED = 2
SRC_DICTIONARY_EPA = "dictionary_epa"
SRC_EPA_VEHICLES_CSV = "epa_vehicles_csv"
SRC_LEGACY = "legacy"
LEGACY_MAX_ID = 20006
DEFAULT_MIN_FILES = 7_000  # the root DICTIONARY/ tree holds 8,000 *_EPA.csv
# columns the dump rows carry and no builder may null (counts in the dump, 2026-10)
PROTECTED_COLUMNS = ("epa_vehicle_id", "atv_type", "city_e")

_SELECT = (
    "SELECT id, epa_vehicle_id, year, make, model, \"trim\", engine_description, trany, drive, "
    "displacement, cylinders, fuel_type, source, natural_key FROM public.epa_master ORDER BY id"
)
_FIELDS = ("id", "epa_vehicle_id", "year", "make", "model", "trim", "engine_description", "trany",
           "drive", "displacement", "cylinders", "fuel_type", "source", "natural_key")


class Refused(RuntimeError):
    """The backfill refused to run; ``main`` turns it into exit code 2."""


def dictionary_base_keys(directory: Path | str, *, min_files: int = DEFAULT_MIN_FILES) -> set[str]:
    """Base keys of every row in the root ``DICTIONARY/*_EPA.csv`` files (old-schema headers)."""
    from backend.catalog.epa_key import base_key

    root = Path(directory)
    if not root.is_dir():
        raise Refused(f"DICTIONARY folder not found: {root}")
    files = sorted(root.rglob("*_EPA.csv"))
    if len(files) < min_files:
        raise Refused(
            f"found {len(files)} *_EPA.csv file(s) under {root}, fewer than --min-files {min_files}; "
            "refusing to classify legacy rows against a partial tree"
        )
    keys: set[str] = set()
    for path in files:
        with open(path, newline="", encoding="utf-8") as fh:
            for r in csv.DictReader(fh):
                keys.add(base_key({
                    "year": r.get("Year"), "make": r.get("Make"), "model": r.get("Model"),
                    "trim": r.get("Trim"), "engine_description": r.get("engineOptions"),
                    "trany": r.get("transmissionOptions"), "drive": r.get("drivetrainOptions"),
                    "displacement": r.get("displacement"), "cylinders": r.get("cylinders"),
                    "fuel_type": r.get("fuelType"),
                }))
    return keys


def check_quiescent(conn) -> None:
    """Refuse while any other client session on this database is active or idle in transaction."""
    rows = conn.execute(
        "SELECT pid, state, left(coalesce(application_name, ''), 40) FROM pg_stat_activity "
        "WHERE pid <> pg_backend_pid() AND datname = current_database() "
        "AND backend_type = 'client backend' "
        "AND state IN ('active', 'idle in transaction', 'idle in transaction (aborted)') "
        "ORDER BY pid"
    ).fetchall()
    if rows:
        shown = ", ".join(f"pid {p} {s} {a!r}" for p, s, a in rows[:10])
        raise Refused(f"{len(rows)} other session(s) active or idle in transaction ({shown}); "
                      "stop the writers (mini shards included) and retry")


def classify(rows: Sequence[Mapping[str, Any]], live_ids: set[int],
             dict_keys: set[str]) -> dict[int, str]:
    """``id -> source`` for *rows*; raises Refused listing rows that match no class."""
    from backend.catalog.epa_key import base_key, epa_vehicle_id

    out: dict[int, str] = {}
    unmatched: list[int] = []
    for r in rows:
        rid = int(r["id"])
        if rid in live_ids:
            out[rid] = SRC_DICTIONARY_EPA
        elif epa_vehicle_id(r) is not None:
            out[rid] = SRC_EPA_VEHICLES_CSV
        elif rid <= LEGACY_MAX_ID and base_key(r) in dict_keys:
            out[rid] = SRC_LEGACY
        else:
            unmatched.append(rid)
    if unmatched:
        raise Refused(f"{len(unmatched)} row(s) match no provenance class, first 20 ids: {unmatched[:20]}")
    return out


def _protected_counts(conn) -> dict[str, int]:
    sel = ", ".join(f"count({c})" for c in PROTECTED_COLUMNS)
    row = conn.execute(f"SELECT {sel} FROM public.epa_master").fetchone()
    return dict(zip(PROTECTED_COLUMNS, (int(x) for x in row)))


def backfill(conn, dict_keys: set[str], *, apply: bool = False, recompute: bool = False,
             check_activity: bool = True) -> dict[str, Any]:
    """Classify and (with *apply*) stamp ``epa_master``. Returns counters; raises Refused.

    The connection is the caller's; nothing is written without *apply* and nothing is
    committed on a refusal or a failed assertion.
    """
    from backend.catalog.epa_key import assign_natural_keys

    if check_activity:
        check_quiescent(conn)
    live_ids = {int(r[0]) for r in conn.execute(
        "SELECT live_id FROM public.epa_master_dump_id_map WHERE live_id IS NOT NULL").fetchall()}
    all_rows = [dict(zip(_FIELDS, r)) for r in conn.execute(_SELECT).fetchall()]
    targets = [r for r in all_rows if recompute or r["source"] is None]
    others = [r for r in all_rows if not (recompute or r["source"] is None)]

    sources = classify(targets, live_ids, dict_keys)
    keys = assign_natural_keys(targets, taken=[r["natural_key"] for r in others if r["natural_key"]])
    plan = [(r["id"], sources[int(r["id"])], k) for r, k in zip(targets, keys)]

    seen: dict[tuple[str, str], int] = {}
    collisions: list[tuple[str, str, int, int]] = []
    final = [(r["id"], r["source"], r["natural_key"]) for r in others] + plan
    for rid, src, key in final:
        if not src or not key:
            continue
        if (src, key) in seen:
            collisions.append((src, key, seen[(src, key)], rid))
        else:
            seen[(src, key)] = rid
    if collisions:
        listing = "; ".join(f"({s}, {k}) ids {a} and {b}" for s, k, a, b in collisions[:20])
        raise Refused(f"{len(collisions)} (source, natural_key) collision(s), first 20: {listing}")

    per_source = Counter(src for _, src, _ in plan)
    stats: dict[str, Any] = {
        "rows": len(all_rows), "targets": len(targets), "classified": len(plan),
        "per_source": dict(per_source), "already_stamped": len(others),
        "dry_run": not apply, "applied": 0,
    }
    before = _protected_counts(conn)
    stats["protected_columns"] = before
    if not apply or not plan:
        conn.rollback()
        return stats

    guard = "" if recompute else " AND source IS NULL"
    try:
        conn.execute("SET LOCAL lock_timeout = '5s'")
        with conn.cursor() as cur:
            cur.executemany(
                "UPDATE public.epa_master SET source = %s, natural_key = %s WHERE id = %s" + guard,
                [(src, key, rid) for rid, src, key in plan],
            )
        have = Counter(dict(conn.execute(
            "SELECT source, count(*) FROM public.epa_master GROUP BY 1").fetchall()))
        want = Counter(r["source"] for r in others if r["source"])
        want.update(per_source)
        if dict(have) != dict(want):
            raise Refused(f"post-write per-source counts {dict(have)} differ from the plan {dict(want)}")
        after = _protected_counts(conn)
        if after != before:
            raise Refused(f"protected column counts changed: {before} -> {after}")
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    stats["applied"] = len(plan)
    return stats


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--apply", action="store_true", help="Write (default is a dry run).")
    ap.add_argument("--recompute", action="store_true",
                    help="Also re-stamp rows whose source is already set (natural_key is kept).")
    ap.add_argument("--dictionary-dir", default=str(_REPO_ROOT / "DICTIONARY"),
                    help="The root DICTIONARY/ folder used to classify legacy rows.")
    ap.add_argument("--min-files", type=int, default=DEFAULT_MIN_FILES)
    args = ap.parse_args(argv)

    from backend.utils.project_env import load_project_dotenv

    load_project_dotenv()
    from backend.db.inventory_pg import inventory_postgres_dsn, pg_connect

    if not inventory_postgres_dsn():
        print("backfill_epa_master_provenance: set INVENTORY_DATABASE_URL to a postgresql:// DSN",
              file=sys.stderr)
        return EXIT_REFUSED
    conn = None
    try:
        keys = dictionary_base_keys(args.dictionary_dir, min_files=args.min_files)
        conn = pg_connect()
        if not args.apply:
            conn.read_only = True
        stats = backfill(conn, keys, apply=args.apply, recompute=args.recompute)
    except Refused as exc:
        print(f"backfill_epa_master_provenance: refused: {exc}", file=sys.stderr)
        return EXIT_REFUSED
    finally:
        if conn is not None:
            conn.close()
    print(json.dumps(stats, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
