#!/usr/bin/env python3
"""
Wipe the cars table so you can run a fresh scan with real data.

Usage:
  python backend/scripts/reset_db.py --dry-run   # show row count only, no writes
  python backend/scripts/reset_db.py --yes       # actually delete
"""
import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))

try:
    from backend.utils.project_env import load_project_dotenv

    load_project_dotenv()
except ImportError:
    pass


def main() -> int:
    ap = argparse.ArgumentParser(description="Wipe the cars table (destructive).")
    ap.add_argument("--dry-run", action="store_true", help="Show row count only, no writes.")
    ap.add_argument("--yes", action="store_true", help="Required to actually delete (safety guard).")
    args = ap.parse_args()

    from backend.db.inventory_db import get_conn
    from backend.db.inventory_pg import is_inventory_postgres

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM cars")
        n = cur.fetchone()[0]
        backend_name = "Postgres" if is_inventory_postgres() else "SQLite"

        if args.dry_run:
            print(f"[{backend_name}] dry-run: would delete {n} row(s) from cars. No writes made.")
            return 0

        if not args.yes:
            print(
                f"Refusing to delete {n} row(s) from the {backend_name} cars table without --yes. "
                "Use --dry-run to preview, or --yes to confirm."
            )
            return 1

        cur.execute("DELETE FROM cars")
        conn.commit()
        print(f"[{backend_name}] Deleted {n} row(s) from cars. Database is ready for a fresh scan.")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
