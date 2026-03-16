#!/usr/bin/env python3
"""
Wipe the cars table so you can run a fresh scan with real data.
Usage: python reset_db.py
"""
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))

DB_PATH = os.environ.get("INVENTORY_DB_PATH", str(ROOT / "inventory.db"))


def main():
    import sqlite3
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM cars")
    deleted = cursor.rowcount
    conn.commit()
    conn.close()
    print(f"Deleted {deleted} row(s) from cars. Database is ready for a fresh scan.")


if __name__ == "__main__":
    main()
