#!/usr/bin/env python3
"""Re-filter stored gallery JSON using VDP junk URL rules."""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.scanner.utils.gallery_url_filter import filter_vdp_gallery_urls


def main() -> int:
    ap = argparse.ArgumentParser(description="Clean junk URLs from cars.gallery JSON")
    ap.add_argument("--db", default=os.environ.get("INVENTORY_DB_PATH", "inventory.db"))
    ap.add_argument("--dealer-id", default=None)
    args = ap.parse_args()
    db = str(Path(args.db).expanduser())
    conn = sqlite3.connect(db)
    where = "gallery IS NOT NULL AND TRIM(gallery) != '' AND TRIM(gallery) != '[]'"
    params: list[str] = []
    if args.dealer_id:
        where += " AND dealer_id = ?"
        params.append(args.dealer_id)
    rows = conn.execute(f"SELECT id, gallery FROM cars WHERE {where}", params).fetchall()
    touched = 0
    for car_id, raw in rows:
        try:
            urls = json.loads(raw) if isinstance(raw, str) else raw
        except (json.JSONDecodeError, TypeError):
            continue
        if not isinstance(urls, list):
            continue
        cleaned = filter_vdp_gallery_urls([u for u in urls if isinstance(u, str)])
        if cleaned == urls:
            continue
        conn.execute(
            "UPDATE cars SET gallery = ?, image_url = ? WHERE id = ?",
            (
                json.dumps(cleaned, separators=(",", ":")),
                cleaned[0] if cleaned else None,
                car_id,
            ),
        )
        touched += 1
    conn.commit()
    conn.close()
    print(f"Cleaned gallery on {touched} of {len(rows)} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
