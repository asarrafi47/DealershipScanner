#!/usr/bin/env python3
"""
Load the dictionary ``*_EPA.csv`` files into ``epa_master`` on Postgres, keeping every id.

Source: every ``*_EPA.csv`` under ``dictionary_paths.EPA_DIR`` (``backend/dictionary/epa``,
sharded one directory per make, about 12,100 files), read recursively. ``--source-dir``
points it somewhere else. The stale root ``DICTIONARY/`` copy (10,200 files, old schema)
is no longer read; it is deleted in remediation plan P10D.8.

Modes (the default is ``--dry-run``; nothing writes without ``--apply``):

* ``--dry-run`` (P10B.1) reads the ``source='dictionary_epa'`` rows of ``epa_master`` on a
  read-only connection and prints a JSON diff: CSV rows to insert, rows whose value columns
  (city08, highway08, body_style, engine_display, forced_induction, city_e, highway_e, atv_type)
  would change, unchanged rows, and stored rows the CSVs no longer carry. Pairing uses the EPA
  vehicle id when both sides have it, else ``backend.catalog.epa_key.match_rows``. A blank CSV
  value never counts as a change to a stored display, atv_type, body_style or city_e/highway_e.
  It refuses a catalog without the V036 columns or without provenance (run the P10B.4 backfill).
* ``--apply`` (P10B.2) does that diff's writes in ONE transaction: INSERT new keys (natural key
  from ``epa_key``, label from the CSV or ``forced_induction_from_epa``), UPDATE matched rows by
  id with only the changed value columns. ``engine_description`` is part of the key and never
  changes; a non-null vid, atv_type or city_e is never nulled and atv_type is set only when the
  stored one is NULL. Before commit it asserts the extended-specs count is unchanged, no new
  dangling ``cars.epa_master_id`` and no duplicate (source, natural_key); any failure rolls back.
* ``--archive-missing`` (with ``--apply``) moves stored rows the CSVs no longer carry into
  ``epa_master_archive`` and deletes them, but only rows no car links to and with no
  ``epa_extended_specs`` row. Referenced missing rows are kept and listed.

Guards (refused with exit 2 on ``--apply``, reported on ``--dry-run``): CSV rows under 90% of
the active ``dictionary_epa`` rows; more than 5% of the active rows to update, unless
``--allow-large-update``. A source directory holding fewer than ``--min-files`` (default
10,000) CSVs is refused too: a partial tree would make every missing model look absent.

``--rebuild`` is refused in every form (it deleted ``epa_master``, cascaded to
``epa_extended_specs`` and left every ``cars.epa_master_id`` dangling). Ids are permanent.

Requires INVENTORY_DATABASE_URL or DATABASE_URL (postgresql://).

Usage:
  PYTHONPATH=. python backend/scripts/build_epa_master_pg.py --dry-run
  PYTHONPATH=. python backend/scripts/build_epa_master_pg.py --apply --archive-missing   # owner-gated
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
from pathlib import Path
from typing import Sequence

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

try:
    from backend.utils.project_env import load_project_dotenv

    load_project_dotenv()
except ImportError:
    pass

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

EXIT_REFUSED = 2
DEFAULT_MIN_FILES = 10_000
MIN_CSV_FRACTION = 0.90        # CSV rows vs active dictionary_epa rows
MAX_UPDATE_FRACTION = 0.05     # updates vs active dictionary_epa rows


class BuildRefused(RuntimeError):
    """The builder refused to run; ``main`` turns it into exit code 2."""


def default_source_dir() -> Path:
    """``dictionary_paths.EPA_DIR``, read at call time so a redirected tree is honoured."""
    from backend.enrichment import dictionary_paths

    return Path(dictionary_paths.EPA_DIR)


def resolve_mode(*, dry_run: bool = False, apply: bool = False, rebuild: bool = False,
                 archive_missing: bool = False, allow_large_update: bool = False) -> str:
    """``'dry_run'`` or ``'apply'``, or :class:`BuildRefused` for a contradictory request.

    Neither flag means a dry run. ``--rebuild`` is refused in every form.
    """
    if rebuild:
        raise BuildRefused("--rebuild refused: it deleted epa_master and dangled every car link; "
                           "use --apply (ids are kept)")
    if apply and dry_run:
        raise BuildRefused("--apply and --dry-run are contradictory")
    if (archive_missing or allow_large_update) and not apply:
        raise BuildRefused("--archive-missing and --allow-large-update need --apply")
    return "apply" if apply else "dry_run"


def find_epa_files(source_dir: Path | str | None = None, *, min_files: int = DEFAULT_MIN_FILES) -> list[Path]:
    """Every ``*_EPA.csv`` under *source_dir* (recursive), or :class:`BuildRefused`.

    Refuses a missing directory, a ``min_files`` below 1, and a tree holding fewer
    than ``min_files`` files.
    """
    if min_files < 1:
        raise BuildRefused(f"--min-files must be at least 1 (got {min_files})")
    source = Path(source_dir) if source_dir is not None else default_source_dir()
    if not source.is_dir():
        raise BuildRefused(f"EPA source directory not found: {source}")
    files = sorted(source.rglob("*_EPA.csv"))
    if len(files) < min_files:
        raise BuildRefused(
            f"found {len(files)} *_EPA.csv file(s) under {source}, fewer than --min-files "
            f"{min_files}; refusing a partial catalog source"
        )
    return files


def _safe_int(v: str | None) -> int | None:
    try:
        return int(str(v).strip()) if v and str(v).strip() else None
    except (ValueError, TypeError):
        return None


def _safe_float(v: object) -> float | None:
    try:
        return float(str(v).strip()) if v is not None and str(v).strip() else None
    except (ValueError, TypeError):
        return None


def _text(v: object) -> str | None:
    s = str(v).strip() if v is not None else ""
    return s or None


def _load_csv(path: Path) -> list[dict]:
    try:
        with open(path, newline="", encoding="utf-8") as f:
            return list(csv.DictReader(f))
    except Exception as exc:
        logger.debug("skip %s: %s", path.name, exc)
        return []


# --- P10B.1: loader and diff (no writes) -----------------------------------------------------

DB_SOURCE = "dictionary_epa"
# the columns an apply may change on a matched row (P10B.2); engine_description is part of the
# natural key and is never compared
VALUE_COLUMNS = ("city08", "highway08", "body_style", "engine_display", "forced_induction",
                 "city_e", "highway_e", "atv_type")
_NUMERIC_COLUMNS = ("city08", "highway08", "city_e", "highway_e")
# a blank in the CSV never erases a stored value in these (P10B.2 "never null")
_KEEP_WHEN_BLANK = ("engine_display", "city_e", "highway_e", "atv_type", "body_style")
REQUIRED_DB_COLUMNS = ("source", "natural_key", "t_charger", "s_charger")
SAMPLE_LIMIT = 10


def csv_row_to_catalog(row: dict) -> dict | None:
    """One dictionary CSV row as ``epa_master``-typed fields, or None when it has no year/make/model."""
    year = _safe_int(row.get("Year"))
    make = (row.get("Make") or "").strip()
    model = (row.get("Model") or "").strip()
    if not year or not make or not model:
        return None
    vid = _safe_int(row.get("epaId"))
    return {
        "year": year,
        "make": make,
        "model": model,
        "trim": (row.get("Trim") or "").strip(),
        "engine_description": _text(row.get("engineOptions")),
        "engine_display": _text(row.get("engineDisplay")),
        "forced_induction": _text(row.get("forcedInduction")),
        "trany": _text(row.get("transmissionOptions")),
        "drive": _text(row.get("drivetrainOptions")),
        "fuel_type": _text(row.get("fuelType")),
        "body_style": _text(row.get("bodyStyle")),
        "cylinders": _safe_int(row.get("cylinders")),
        "displacement": _safe_float(row.get("displacement")),
        "city08": _safe_float(row.get("mpg_city")),
        "highway08": _safe_float(row.get("mpg_highway")),
        "epa_vehicle_id": vid if vid and vid > 0 else None,
        "atv_type": _text(row.get("atvType")),
        "city_e": _safe_float(row.get("cityE")),
        "highway_e": _safe_float(row.get("highwayE")),
    }


def load_catalog_rows(epa_files: Sequence[Path]) -> tuple[list[dict], int]:
    """``(rows, skipped_bad_row)`` over every CSV, in file order. Each row remembers its file."""
    rows: list[dict] = []
    bad = 0
    for path in epa_files:
        for raw in _load_csv(path):
            row = csv_row_to_catalog(raw)
            if row is None:
                bad += 1
                continue
            row["_file"] = path.name
            rows.append(row)
    return rows, bad


def _same(col: str, a: object, b: object) -> bool:
    if col in _NUMERIC_COLUMNS:
        x, y = _safe_float(a), _safe_float(b)
        return x is None and y is None or (x is not None and y is not None and abs(x - y) < 1e-9)
    return (_text(a) or "") == (_text(b) or "")


def changed_columns(csv_row: dict, db_row: dict) -> dict[str, tuple]:
    """``{column: (stored, csv)}`` for the value columns an apply would change on this pair."""
    out: dict[str, tuple] = {}
    for col in VALUE_COLUMNS:
        new, old = csv_row.get(col), db_row.get(col)
        if col in _KEEP_WHEN_BLANK and _text(new) is None:
            continue
        if not _same(col, old, new):
            out[col] = (old, new)
    return out


def diff_catalog(csv_rows: Sequence[dict], db_rows: Sequence[dict]) -> dict:
    """Pair CSV rows with stored ``dictionary_epa`` rows and classify them. Pure, no IO.

    Pairing: an EPA vehicle id present on both sides pairs first (the permanent key, D-ER2);
    the rest pair by ``epa_key.match_rows`` (base key, then value assignment). Result:
    ``insert`` (CSV indexes), ``updates`` ([(csv index, db index, changes)]), ``unchanged``
    (count) and ``missing`` (db indexes the CSVs no longer carry).
    """
    from backend.catalog import epa_key

    pairs: list[tuple[int, int]] = []
    db_by_vid: dict[int, list[int]] = {}
    for j, r in enumerate(db_rows):
        vid = epa_key.epa_vehicle_id(r)
        if vid is not None:
            db_by_vid.setdefault(vid, []).append(j)
    csv_by_vid: dict[int, int] = {}
    for r in csv_rows:
        if r.get("epa_vehicle_id"):
            csv_by_vid[r["epa_vehicle_id"]] = csv_by_vid.get(r["epa_vehicle_id"], 0) + 1
    used_c: set[int] = set()
    used_d: set[int] = set()
    for i, r in enumerate(csv_rows):
        vid = r.get("epa_vehicle_id")
        bucket = db_by_vid.get(vid, []) if vid else []
        # only an unambiguous id pairs here; duplicated ids fall through to value matching
        if len(bucket) == 1 and csv_by_vid.get(vid) == 1:
            pairs.append((i, bucket[0]))
            used_c.add(i)
            used_d.add(bucket[0])
    rest_c = [i for i in range(len(csv_rows)) if i not in used_c]
    rest_d = [j for j in range(len(db_rows)) if j not in used_d]
    sub_pairs, c_left, d_left = epa_key.match_rows([csv_rows[i] for i in rest_c],
                                                   [db_rows[j] for j in rest_d])
    pairs += [(rest_c[i], rest_d[j]) for i, j in sub_pairs]

    updates: list[tuple[int, int, dict]] = []
    unchanged = 0
    for i, j in sorted(pairs):
        ch = changed_columns(csv_rows[i], db_rows[j])
        if ch:
            updates.append((i, j, ch))
        else:
            unchanged += 1
    return {
        "insert": sorted(rest_c[i] for i in c_left),
        "updates": updates,
        "unchanged": unchanged,
        "missing": sorted(rest_d[j] for j in d_left),
    }


def _label(row: dict) -> str:
    return f"{row.get('year')} {row.get('make')} {row.get('model')} {row.get('trim') or ''}".strip()


def build_report(csv_rows: Sequence[dict], db_rows: Sequence[dict], d: dict, *, sample: int = SAMPLE_LIMIT) -> dict:
    """Counts plus a bounded sample of each class, for the owner to review."""
    per_col: dict[str, int] = {}
    for _, _, ch in d["updates"]:
        for col in ch:
            per_col[col] = per_col.get(col, 0) + 1
    return {
        "csv_rows": len(csv_rows),
        "db_rows": len(db_rows),
        "insert": len(d["insert"]),
        "update": len(d["updates"]),
        "unchanged": d["unchanged"],
        "missing": len(d["missing"]),
        "update_by_column": dict(sorted(per_col.items())),
        "sample_insert": [_label(csv_rows[i]) for i in d["insert"][:sample]],
        "sample_update": [
            {"id": db_rows[j].get("id"), "row": _label(db_rows[j]),
             "changes": {c: list(v) for c, v in ch.items()}}
            for _, j, ch in d["updates"][:sample]
        ],
        "sample_missing": [{"id": db_rows[j].get("id"), "row": _label(db_rows[j])}
                           for j in d["missing"][:sample]],
    }


_DB_COLUMNS = ("id", "epa_vehicle_id", "year", "make", "model", "trim", "engine_description",
               "trany", "drive", "fuel_type", "body_style", "cylinders", "displacement",
               "city08", "highway08", "engine_display", "forced_induction", "city_e",
               "highway_e", "atv_type", "natural_key")


def read_db_rows(conn, *, release: bool = True) -> list[dict]:
    """The active ``source='dictionary_epa'`` rows. Refuses a catalog without V036 columns or
    without provenance (run the P10B.4 backfill first). ``release=False`` keeps the
    transaction open (an apply reads and writes in one)."""
    cur = conn.cursor()
    cur.execute("SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = 'epa_master'")
    have = {r[0] for r in cur.fetchall()}
    missing = [c for c in REQUIRED_DB_COLUMNS if c not in have]
    if missing:
        raise BuildRefused(f"epa_master lacks column(s) {missing}: apply migration V036 first")
    cols = ", ".join(f'"{c}"' for c in _DB_COLUMNS)
    cur.execute(f"SELECT {cols} FROM public.epa_master WHERE source = %s ORDER BY id", (DB_SOURCE,))
    rows = [dict(zip(_DB_COLUMNS, r)) for r in cur.fetchall()]
    if release:
        conn.rollback()
    if not rows:
        raise BuildRefused(f"no epa_master rows with source='{DB_SOURCE}': run the provenance "
                           "backfill (P10B.4) before diffing")
    return rows


# --- P10B.2: guards and apply ------------------------------------------------------------------

# every epa_master column except id and loaded_at, in table order: what an archive copy keeps
_ARCHIVE_COLUMNS = ("id", "epa_vehicle_id", "year", "make", "model", "cylinders", "displacement",
                    "trany", "drive", "fuel_type", "city08", "highway08", "city_e", "highway_e",
                    "atv_type", "trim", "body_style", "engine_description", "engine_display",
                    "forced_induction", "t_charger", "s_charger", "source", "natural_key",
                    "loaded_at")
_INSERT_COLUMNS = ("year", "make", "model", "trim", "engine_description", "engine_display",
                   "forced_induction", "trany", "drive", "fuel_type", "body_style", "cylinders",
                   "displacement", "city08", "highway08", "epa_vehicle_id", "atv_type",
                   "city_e", "highway_e")
ARCHIVE_REASON = "builder: absent from the dictionary CSVs"


def guard_violations(n_csv: int, n_db: int, n_updates: int, *, allow_large_update: bool = False) -> list[str]:
    """Reasons an apply must not run (empty when it may)."""
    out: list[str] = []
    if n_csv < MIN_CSV_FRACTION * n_db:
        out.append(f"CSV rows ({n_csv}) are under {MIN_CSV_FRACTION:.0%} of the {n_db} active "
                   f"{DB_SOURCE} rows: a partial source would archive or skip most of the catalog")
    if n_updates > MAX_UPDATE_FRACTION * n_db and not allow_large_update:
        out.append(f"{n_updates} updates exceed {MAX_UPDATE_FRACTION:.0%} of the {n_db} active rows; "
                   "review the dry run and pass --allow-large-update")
    return out


def _insert_values(row: dict) -> dict:
    """Column values for a new row. A blank CSV label falls back to EPA's own flags/description."""
    from backend.utils.forced_induction import forced_induction_from_epa

    vals = {c: row.get(c) for c in _INSERT_COLUMNS}
    if _text(vals["forced_induction"]) is None:
        vals["forced_induction"] = forced_induction_from_epa(
            None, None, row.get("engine_description"), row.get("model"), row.get("atv_type"))
    return vals


def _update_assignments(changes: dict[str, tuple], stored: dict) -> dict:
    """The columns to write for one matched row, after the never-null / never-downgrade rules."""
    out: dict = {}
    for col, (old, new) in changes.items():
        if col == "atv_type" and _text(old) is not None:
            continue            # set only when the stored value is NULL
        if col in _KEEP_WHEN_BLANK and _text(new) is None:
            continue
        out[col] = new
    return out


def _count(cur, sql: str, params=()) -> int:
    cur.execute(sql, params)
    return int(cur.fetchone()[0])


def apply_catalog(conn, csv_rows: Sequence[dict], db_rows: Sequence[dict], d: dict, *,
                  archive_missing: bool = False) -> dict:
    """Write the diff in the caller's open transaction and assert the invariants. No commit.

    Raises :class:`BuildRefused` on a broken invariant; the caller rolls back.
    """
    from backend.catalog import epa_key

    cur = conn.cursor()
    before = {
        "extended_specs": _count(cur, "SELECT count(*) FROM public.epa_extended_specs"),
        "dangling": _count(cur, "SELECT count(*) FROM public.cars c WHERE c.epa_master_id IS NOT NULL "
                                "AND NOT EXISTS (SELECT 1 FROM public.epa_master m WHERE m.id = c.epa_master_id)"),
    }
    out = {"inserted": 0, "updated": 0, "archived": 0, "kept_referenced": []}

    # UPDATE by id, changed value columns only
    for _, j, changes in d["updates"]:
        cols = _update_assignments(changes, db_rows[j])
        if not cols:
            continue
        sets = ", ".join(f'"{c}" = %s' for c in cols)
        cur.execute(f"UPDATE public.epa_master SET {sets}, loaded_at = now() WHERE id = %s",
                    (*cols.values(), db_rows[j]["id"]))
        out["updated"] += 1

    # INSERT new keys; ordinals come from epa_key and skip every key ever stored
    if d["insert"]:
        cur.execute("SELECT natural_key FROM public.epa_master WHERE natural_key IS NOT NULL")
        taken = {r[0] for r in cur.fetchall()}
        # explicit-id loads (dump imports) leave the sequence behind max(id): a new row would collide
        cur.execute("SELECT setval(pg_get_serial_sequence('public.epa_master', 'id'), "
                    "GREATEST((SELECT max(id) FROM public.epa_master), 1))")
        for i in d["insert"]:
            row = csv_rows[i]
            keyed = row
            vid = epa_key.epa_vehicle_id(row)
            if vid is not None and f"vid:{vid}" in taken:
                keyed = {**row, "epa_vehicle_id": None}     # id already used: key by content
            key = epa_key.assign_natural_keys([keyed], taken)[0]
            taken.add(key)
            vals = _insert_values(row)
            cols = ", ".join(f'"{c}"' for c in vals)
            cur.execute(
                f'INSERT INTO public.epa_master ({cols}, source, natural_key, loaded_at) '
                f"VALUES ({', '.join(['%s'] * len(vals))}, %s, %s, now())",
                (*vals.values(), DB_SOURCE, key))
            out["inserted"] += 1

    # ARCHIVE unreferenced missing rows; referenced ones stay and are listed
    if d["missing"]:
        ids = [db_rows[j]["id"] for j in d["missing"]]
        cur.execute("SELECT DISTINCT epa_master_id FROM public.cars WHERE epa_master_id = ANY(%s)", (ids,))
        linked = {r[0] for r in cur.fetchall()}
        cur.execute("SELECT epa_master_id FROM public.epa_extended_specs WHERE epa_master_id = ANY(%s)", (ids,))
        specced = {r[0] for r in cur.fetchall()}
        keep = linked | specced
        out["kept_referenced"] = [
            {"id": db_rows[j]["id"], "row": _label(db_rows[j]),
             "cars_link": db_rows[j]["id"] in linked, "extended_specs": db_rows[j]["id"] in specced}
            for j in d["missing"] if db_rows[j]["id"] in keep
        ]
        free = [i for i in ids if i not in keep]
        if archive_missing and free:
            cols = ", ".join(f'"{c}"' for c in _ARCHIVE_COLUMNS)
            cur.execute(f"INSERT INTO public.epa_master_archive ({cols}, archive_reason) "
                        f"SELECT {cols}, %s FROM public.epa_master WHERE id = ANY(%s)",
                        (ARCHIVE_REASON, free))
            cur.execute("DELETE FROM public.epa_master WHERE id = ANY(%s)", (free,))
            out["archived"] = len(free)

    # pre-commit assertions
    problems: list[str] = []
    n_x = _count(cur, "SELECT count(*) FROM public.epa_extended_specs")
    if n_x != before["extended_specs"]:
        problems.append(f"epa_extended_specs count changed {before['extended_specs']} -> {n_x}")
    dangling = _count(cur, "SELECT count(*) FROM public.cars c WHERE c.epa_master_id IS NOT NULL "
                           "AND NOT EXISTS (SELECT 1 FROM public.epa_master m WHERE m.id = c.epa_master_id)")
    if dangling > before["dangling"]:
        problems.append(f"{dangling - before['dangling']} new dangling cars.epa_master_id link(s)")
    dupes = _count(cur, "SELECT count(*) FROM (SELECT 1 FROM public.epa_master WHERE natural_key IS NOT NULL "
                        "GROUP BY source, natural_key HAVING count(*) > 1) t")
    if dupes:
        problems.append(f"{dupes} duplicate (source, natural_key) group(s)")
    if problems:
        raise BuildRefused("apply rolled back: " + "; ".join(problems))
    out["extended_specs"] = n_x
    out["dangling_links"] = dangling
    return out


def build_epa_master(
    *,
    dry_run: bool = False,
    apply: bool = False,
    rebuild: bool = False,
    archive_missing: bool = False,
    allow_large_update: bool = False,
    source_dir: Path | str | None = None,
    min_files: int = DEFAULT_MIN_FILES,
    sample: int = SAMPLE_LIMIT,
) -> dict:
    """Diff the dictionary CSVs against the stored rows; with ``apply=True`` also write the diff.

    The default (no flag) is a dry run on a read-only connection and never writes.
    """
    mode = resolve_mode(dry_run=dry_run, apply=apply, rebuild=rebuild,
                        archive_missing=archive_missing, allow_large_update=allow_large_update)
    epa_files = find_epa_files(source_dir, min_files=min_files)
    logger.info("Found %d EPA CSV files under %s", len(epa_files), source_dir or default_source_dir())

    from backend.db.inventory_pg import inventory_postgres_dsn, pg_connect

    if not inventory_postgres_dsn():
        raise SystemExit("Set INVENTORY_DATABASE_URL or DATABASE_URL to a postgresql:// DSN")

    csv_rows, bad = load_catalog_rows(epa_files)
    conn = pg_connect()
    applied = None
    try:
        if mode == "apply":
            # One transaction; the lock keeps a concurrent writer from changing the rows diffed.
            conn.cursor().execute("LOCK TABLE public.epa_master IN SHARE ROW EXCLUSIVE MODE")
            db_rows = read_db_rows(conn, release=False)
        else:
            # Read-only session: the server rejects any write this dry run could reach.
            conn.read_only = True
            db_rows = read_db_rows(conn)
        logger.info("CSV rows: %d, stored dictionary_epa rows: %d", len(csv_rows), len(db_rows))
        d = diff_catalog(csv_rows, db_rows)
        violations = guard_violations(len(csv_rows), len(db_rows), len(d["updates"]),
                                      allow_large_update=allow_large_update)
        if mode == "apply":
            if violations:
                raise BuildRefused("; ".join(violations))
            applied = apply_catalog(conn, csv_rows, db_rows, d, archive_missing=archive_missing)
            conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()

    report = build_report(csv_rows, db_rows, d, sample=sample)
    report.update({
        "files": len(epa_files),
        "skipped_bad_row": bad,
        "mode": mode,
        "guard_violations": violations,
        # names the first version reported
        "would_insert": report["insert"],
        "skipped_existing": report["update"] + report["unchanged"],
    })
    if applied is not None:
        report["applied"] = applied
    return report


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Diff (default) or apply the epa_master load from backend/dictionary/epa/**/*_EPA.csv, "
        "keeping every row id."
    )
    parser.add_argument("--dry-run", action="store_true", help="Report the diff only (the default).")
    parser.add_argument("--apply", action="store_true",
                        help="Write the diff in one transaction (inserts, updates by id). Owner-gated.")
    parser.add_argument("--archive-missing", action="store_true",
                        help="With --apply: move unreferenced rows the CSVs no longer carry to epa_master_archive.")
    parser.add_argument("--allow-large-update", action="store_true",
                        help="With --apply: allow more than 5%% of the active rows to change.")
    parser.add_argument(
        "--rebuild",
        action="store_true",
        help="Refused (exit 2): it deleted epa_master and dangled every car link.",
    )
    parser.add_argument(
        "--source-dir",
        default=None,
        help="Directory searched recursively for *_EPA.csv (default: dictionary_paths.EPA_DIR).",
    )
    parser.add_argument(
        "--min-files",
        type=int,
        default=DEFAULT_MIN_FILES,
        help=f"Refuse a source holding fewer *_EPA.csv files than this (default {DEFAULT_MIN_FILES}).",
    )
    parser.add_argument("--sample", type=int, default=SAMPLE_LIMIT,
                        help=f"Rows listed per class in the report (default {SAMPLE_LIMIT}).")
    args = parser.parse_args(argv)

    # Refused here, during argument handling: nothing below has opened a connection.
    try:
        resolve_mode(dry_run=bool(args.dry_run), apply=bool(args.apply), rebuild=bool(args.rebuild),
                     archive_missing=bool(args.archive_missing),
                     allow_large_update=bool(args.allow_large_update))
        stats = build_epa_master(
            dry_run=bool(args.dry_run),
            apply=bool(args.apply),
            archive_missing=bool(args.archive_missing),
            allow_large_update=bool(args.allow_large_update),
            source_dir=args.source_dir,
            min_files=int(args.min_files),
            sample=int(args.sample),
        )
    except BuildRefused as exc:
        print(f"build_epa_master_pg: {exc}", file=sys.stderr)
        return EXIT_REFUSED
    logger.info(
        "%s: files=%d insert=%d update=%d unchanged=%d missing=%d skipped_bad_row=%d",
        stats["mode"], stats["files"], stats["insert"], stats["update"], stats["unchanged"],
        stats["missing"], stats["skipped_bad_row"],
    )
    print(json.dumps({k: v for k, v in stats.items() if k not in ("would_insert", "skipped_existing")},
                     indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
