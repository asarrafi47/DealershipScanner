"""Backfill forced_induction column for all cars in inventory.db."""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from backend.utils.project_env import load_project_dotenv
load_project_dotenv()

import sqlite3
from backend.db.inventory_db import DB_PATH
from backend.utils.forced_induction import classify_forced_induction_from_car_row

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

# Ensure column exists
cur.execute("PRAGMA table_info(cars)")
cols = {r[1] for r in cur.fetchall()}
if "forced_induction" not in cols:
    cur.execute("ALTER TABLE cars ADD COLUMN forced_induction TEXT")
    conn.commit()

cur.execute(
    "SELECT id, make, model, trim, year, cylinders, engine_l, engine_description, fuel_type, description "
    "FROM cars WHERE listing_active IS NULL OR listing_active != 0"
)
rows = cur.fetchall()
print(f"Processing {len(rows)} cars...")

updated = 0
skipped = 0
counts: dict[str, int] = {}

for row in rows:
    car = dict(row)
    fi = classify_forced_induction_from_car_row(car)
    counts[fi or "None"] = counts.get(fi or "None", 0) + 1
    cur.execute("UPDATE cars SET forced_induction = ? WHERE id = ?", (fi, car["id"]))
    if fi:
        updated += 1
    else:
        skipped += 1

conn.commit()
conn.close()

print(f"Updated: {updated}  |  Skipped (N/A or unknown): {skipped}")
print("Breakdown:")
for label, count in sorted(counts.items(), key=lambda x: -x[1]):
    print(f"  {label}: {count}")
