#!/usr/bin/env python3
"""
Drop EPA-duplicated columns from ``catalog_trims`` (mechanical specs live in ``epa_master``).

Usage (from repo root):
  PYTHONPATH=. python3 backend/scripts/migrate_catalog_trims_slim.py
  PYTHONPATH=. python3 backend/scripts/migrate_catalog_trims_slim.py --dry-run
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from backend.utils.project_env import load_project_dotenv

    load_project_dotenv()
except ImportError:
    pass

from backend.db.catalog_schema import (  # noqa: E402
    CATALOG_TRIMS_EPA_DUPLICATE_COLUMNS,
    migrate_catalog_trims_drop_epa_duplicate_columns,
)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dry-run", action="store_true", help="List columns only; do not ALTER.")
    args = p.parse_args()

    from backend.db.inventory_pg import inventory_postgres_dsn, is_inventory_postgres

    if not is_inventory_postgres():
        print("Postgres inventory required (INVENTORY_DATABASE_URL).")
        return 1

    cols = list(CATALOG_TRIMS_EPA_DUPLICATE_COLUMNS)
    if args.dry_run:
        print("Would drop from catalog_trims:", ", ".join(cols))
        return 0

    import psycopg

    dsn = inventory_postgres_dsn()
    assert dsn is not None
    with psycopg.connect(dsn) as conn:
        cur = conn.cursor()
        dropped = migrate_catalog_trims_drop_epa_duplicate_columns(cur, postgres=True)
        conn.commit()
    print(f"Dropped {len(dropped)} EPA-duplicate column(s) from catalog_trims.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
