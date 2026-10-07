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

``--dry-run`` reads ``epa_master`` on a read-only connection and reports how many CSV
rows are new by (year, make, model, trim).

Requires INVENTORY_DATABASE_URL or DATABASE_URL (postgresql://) for ``--dry-run``.

Usage:
  PYTHONPATH=. python backend/scripts/build_epa_master_pg.py --dry-run
  PYTHONPATH=. python backend/scripts/build_epa_master_pg.py --dry-run --source-dir /path/to/epa
"""
from __future__ import annotations

import argparse
import csv
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


def _load_csv(path: Path) -> list[dict]:
    try:
        with open(path, newline="", encoding="utf-8") as f:
            return list(csv.DictReader(f))
    except Exception as exc:
        logger.debug("skip %s: %s", path.name, exc)
        return []


def build_epa_master(
    *,
    dry_run: bool = False,
    rebuild: bool = False,
    source_dir: Path | str | None = None,
    min_files: int = DEFAULT_MIN_FILES,
) -> dict[str, int]:
    """Count the rows a load would add. Only ``dry_run=True`` runs; it never writes."""
    refuse_writes(dry_run=dry_run, rebuild=rebuild)
    epa_files = find_epa_files(source_dir, min_files=min_files)
    logger.info("Found %d EPA CSV files under %s", len(epa_files), source_dir or default_source_dir())

    from backend.db.inventory_pg import inventory_postgres_dsn, pg_connect

    if not inventory_postgres_dsn():
        raise SystemExit("Set INVENTORY_DATABASE_URL or DATABASE_URL to a postgresql:// DSN")

    conn = pg_connect()
    try:
        # Read-only session: the server rejects any write this dry run could reach.
        conn.read_only = True
        cur = conn.cursor()
        cur.execute("SELECT year, lower(make), lower(model), lower(coalesce(trim,'')) FROM epa_master")
        existing = set(cur.fetchall())
        conn.rollback()
    finally:
        conn.close()
    logger.info("Existing rows: %d", len(existing))

    would_insert = 0
    skipped_existing = 0
    skipped_bad_row = 0
    for path in epa_files:
        for row in _load_csv(path):
            year = _safe_int(row.get("Year"))
            make = (row.get("Make") or "").strip()
            model = (row.get("Model") or "").strip()
            trim = (row.get("Trim") or "").strip()
            if not year or not make or not model:
                skipped_bad_row += 1
                continue
            key = (year, make.lower(), model.lower(), trim.lower())
            if key in existing:
                skipped_existing += 1
                continue
            existing.add(key)
            would_insert += 1

    return {
        "files": len(epa_files),
        "would_insert": would_insert,
        "skipped_existing": skipped_existing,
        "skipped_bad_row": skipped_bad_row,
    }


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
        )
    except BuildRefused as exc:
        print(f"build_epa_master_pg: {exc}", file=sys.stderr)
        return EXIT_REFUSED
    logger.info(
        "dry-run: files=%d would_insert=%d skipped_existing=%d skipped_bad_row=%d",
        stats["files"], stats["would_insert"], stats["skipped_existing"], stats["skipped_bad_row"],
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
