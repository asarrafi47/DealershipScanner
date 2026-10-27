#!/usr/bin/env python3
"""
Dry-run the ``epa_master`` load from the dictionary ``*_EPA.csv`` files on Postgres.

Source: every ``*_EPA.csv`` under ``dictionary_paths.EPA_DIR`` (``backend/dictionary/epa``,
sharded one directory per make, about 12,100 files), read recursively. ``--source-dir``
points it somewhere else. The stale root ``DICTIONARY/`` copy (10,200 files, old schema)
is no longer read; it is deleted in remediation plan P10D.8.

Writes are disabled (remediation plan P1A.2). Every load gave rows fresh ids, and
``--rebuild`` ran ``DELETE FROM epa_master``, which cascades to ``epa_extended_specs``
(ON DELETE CASCADE) and leaves every ``cars.epa_master_id`` pointing at an id that no
longer exists (``cars.epa_master_id`` has no foreign key). So any run without
``--dry-run``, and ``--rebuild`` in every form, exits 2 while the arguments are being
handled, before a database connection is opened. The id-preserving builder (P10B.2)
lifts the refusal; rows inserted before it lands would later be mislabelled as legacy
rows.

A source directory holding fewer than ``--min-files`` (default 10,000) CSVs is refused
too: a partial tree would make every missing model look absent from the catalog.

``--dry-run`` reads the ``source='dictionary_epa'`` rows of ``epa_master`` on a read-only
connection (P10B.1) and prints a JSON diff: CSV rows to insert, rows whose value columns
(city08, highway08, body_style, engine_display, forced_induction, city_e, highway_e, atv_type)
would change, unchanged rows, and stored rows the CSVs no longer carry. Pairing uses the EPA
vehicle id when both sides have it, else ``backend.catalog.epa_key.match_rows``. A blank CSV
value never counts as a change to a stored display, atv_type, body_style or city_e/highway_e.
It refuses a catalog without the V036 columns or without provenance (run the P10B.4 backfill).

Requires INVENTORY_DATABASE_URL or DATABASE_URL (postgresql://) for ``--dry-run``.

Usage:
  PYTHONPATH=. python backend/scripts/build_epa_master_pg.py --dry-run
  PYTHONPATH=. python backend/scripts/build_epa_master_pg.py --dry-run --source-dir /path/to/epa
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
WRITES_DISABLED_MSG = "writes disabled until the id-preserving builder lands (P10B.2)"


class BuildRefused(RuntimeError):
    """The builder refused to run; ``main`` turns it into exit code 2."""


def default_source_dir() -> Path:
    """``dictionary_paths.EPA_DIR``, read at call time so a redirected tree is honoured."""
    from backend.enrichment import dictionary_paths

    return Path(dictionary_paths.EPA_DIR)


def refuse_writes(*, dry_run: bool, rebuild: bool) -> None:
    """Raise :class:`BuildRefused` for every mode that would write ``epa_master``."""
    if rebuild:
        raise BuildRefused(f"--rebuild refused: {WRITES_DISABLED_MSG}")
    if not dry_run:
        raise BuildRefused(f"only --dry-run is allowed: {WRITES_DISABLED_MSG}")


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


def read_db_rows(conn) -> list[dict]:
    """The active ``source='dictionary_epa'`` rows, read-only. Refuses a catalog without V036
    columns or without provenance (run the P10B.4 backfill first)."""
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
    conn.rollback()
    if not rows:
        raise BuildRefused(f"no epa_master rows with source='{DB_SOURCE}': run the provenance "
                           "backfill (P10B.4) before diffing")
    return rows


def build_epa_master(
    *,
    dry_run: bool = False,
    rebuild: bool = False,
    source_dir: Path | str | None = None,
    min_files: int = DEFAULT_MIN_FILES,
    sample: int = SAMPLE_LIMIT,
) -> dict:
    """Diff the dictionary CSVs against the stored rows. Only ``dry_run=True`` runs; it never writes."""
    refuse_writes(dry_run=dry_run, rebuild=rebuild)
    epa_files = find_epa_files(source_dir, min_files=min_files)
    logger.info("Found %d EPA CSV files under %s", len(epa_files), source_dir or default_source_dir())

    from backend.db.inventory_pg import inventory_postgres_dsn, pg_connect

    if not inventory_postgres_dsn():
        raise SystemExit("Set INVENTORY_DATABASE_URL or DATABASE_URL to a postgresql:// DSN")

    csv_rows, bad = load_catalog_rows(epa_files)
    conn = pg_connect()
    try:
        # Read-only session: the server rejects any write this dry run could reach.
        conn.read_only = True
        db_rows = read_db_rows(conn)
    finally:
        conn.close()
    logger.info("CSV rows: %d, stored dictionary_epa rows: %d", len(csv_rows), len(db_rows))

    report = build_report(csv_rows, db_rows, diff_catalog(csv_rows, db_rows), sample=sample)
    report.update({
        "files": len(epa_files),
        "skipped_bad_row": bad,
        # names the first version reported
        "would_insert": report["insert"],
        "skipped_existing": report["update"] + report["unchanged"],
    })
    return report


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Dry-run the epa_master load from backend/dictionary/epa/**/*_EPA.csv "
        f"(writes are disabled: {WRITES_DISABLED_MSG})."
    )
    parser.add_argument("--dry-run", action="store_true", help="Count only. The only mode that runs.")
    parser.add_argument(
        "--rebuild",
        action="store_true",
        help=f"Refused (exit 2): it deleted epa_master and dangled every car link. {WRITES_DISABLED_MSG}.",
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
        refuse_writes(dry_run=bool(args.dry_run), rebuild=bool(args.rebuild))
    except BuildRefused as exc:
        print(f"build_epa_master_pg: {exc}", file=sys.stderr)
        return EXIT_REFUSED

    try:
        stats = build_epa_master(
            dry_run=True,
            source_dir=args.source_dir,
            min_files=int(args.min_files),
            sample=int(args.sample),
        )
    except BuildRefused as exc:
        print(f"build_epa_master_pg: {exc}", file=sys.stderr)
        return EXIT_REFUSED
    logger.info(
        "dry-run: files=%d would_insert=%d skipped_existing=%d skipped_bad_row=%d",
        stats["files"], stats["would_insert"], stats["skipped_existing"], stats["skipped_bad_row"],
    )
    print(json.dumps({k: v for k, v in stats.items() if k not in ("would_insert", "skipped_existing")},
                     indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
