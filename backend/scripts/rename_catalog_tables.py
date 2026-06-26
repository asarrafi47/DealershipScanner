#!/usr/bin/env python3
"""
Rename legacy reference-catalog tables to catalog_* names (Tier 1 schema cleanup).

Old names suggested "live inventory"; these tables hold trim/config reference data only.
The live listing table remains ``cars``.

Mapping:
  vehicles           -> catalog_trims
  exterior_colors    -> catalog_exterior_colors
  interior_colors    -> catalog_interior_colors
  packages           -> catalog_packages
  package_features   -> catalog_package_features
  standalone_options -> catalog_options

Requires INVENTORY_DATABASE_URL or DATABASE_URL (postgresql://).

Usage:
  PYTHONPATH=. python backend/scripts/rename_catalog_tables.py --dry-run
  PYTHONPATH=. python backend/scripts/rename_catalog_tables.py
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

try:
    from backend.utils.project_env import load_project_dotenv

    load_project_dotenv()
except ImportError:
    pass

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Child tables first (conventional); Postgres preserves FKs regardless of order.
_RENAMES: tuple[tuple[str, str], ...] = (
    ("package_features", "catalog_package_features"),
    ("packages", "catalog_packages"),
    ("exterior_colors", "catalog_exterior_colors"),
    ("interior_colors", "catalog_interior_colors"),
    ("standalone_options", "catalog_options"),
    ("vehicles", "catalog_trims"),
)


def _table_exists(cur, name: str) -> bool:
    cur.execute(
        """
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = %s
        """,
        (name,),
    )
    return cur.fetchone() is not None


def rename_catalog_tables(*, dry_run: bool = False) -> list[tuple[str, str]]:
    from backend.db.inventory_pg import inventory_postgres_dsn, pg_connect

    dsn = inventory_postgres_dsn()
    if not dsn:
        raise SystemExit("Set INVENTORY_DATABASE_URL or DATABASE_URL to a postgresql:// DSN")

    applied: list[tuple[str, str]] = []
    conn = pg_connect()
    try:
        cur = conn.cursor()
        for old, new in _RENAMES:
            has_old = _table_exists(cur, old)
            has_new = _table_exists(cur, new)
            if has_new and not has_old:
                logger.info("skip (already renamed): %s -> %s", old, new)
                continue
            if not has_old:
                if has_new:
                    continue
                logger.warning("skip (missing): %s", old)
                continue
            if has_new:
                raise SystemExit(
                    f"Both {old!r} and {new!r} exist; resolve manually before re-running."
                )
            sql = f'ALTER TABLE "{old}" RENAME TO "{new}"'
            logger.info("%s", sql)
            if not dry_run:
                cur.execute(sql)
            applied.append((old, new))
        if not dry_run:
            conn.commit()
    finally:
        conn.close()
    return applied


def main() -> int:
    parser = argparse.ArgumentParser(description="Rename reference catalog tables to catalog_*.")
    parser.add_argument("--dry-run", action="store_true", help="Print planned renames only.")
    args = parser.parse_args()
    applied = rename_catalog_tables(dry_run=bool(args.dry_run))
    if args.dry_run:
        logger.info("dry-run: would rename %d table(s)", len(applied))
    else:
        logger.info("renamed %d table(s)", len(applied))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
