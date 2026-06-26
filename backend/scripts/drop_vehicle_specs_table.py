#!/usr/bin/env python3
"""
Drop legacy ``vehicle_specs`` from the Postgres inventory database.

That table was loaded by the removed root ``load_vehicle_specs.py`` script and is
superseded by ``epa_master`` (EPA specs) and ``dictionary_options`` (packages/options).

Usage:
  PYTHONPATH=. python -m backend.scripts.drop_vehicle_specs_table
  PYTHONPATH=. python -m backend.scripts.drop_vehicle_specs_table --dry-run
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_REPO_ROOT))

try:
    from backend.utils.project_env import load_project_dotenv

    load_project_dotenv()
except ImportError:
    pass


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report row count only; do not drop the table",
    )
    args = parser.parse_args(argv)

    from backend.db.inventory_pg import inventory_postgres_dsn, is_inventory_postgres

    if not is_inventory_postgres():
        print(
            "vehicle_specs existed only on Postgres (DATABASE_URL / INVENTORY_DATABASE_URL). "
            "No action needed for SQLite inventory.",
            file=sys.stderr,
        )
        return 0

    import psycopg

    dsn = inventory_postgres_dsn()
    assert dsn is not None
    with psycopg.connect(dsn) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = 'vehicle_specs'
            )
            """
        )
        exists = bool(cur.fetchone()[0])
        if not exists:
            print("vehicle_specs: table not present (already dropped or never created)")
            return 0

        cur.execute("SELECT COUNT(*) FROM vehicle_specs")
        count = int(cur.fetchone()[0])
        if args.dry_run:
            print(f"vehicle_specs: would drop table ({count} rows)")
            return 0

        cur.execute("DROP TABLE vehicle_specs")
        conn.commit()
        print(f"vehicle_specs: dropped ({count} rows removed)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
