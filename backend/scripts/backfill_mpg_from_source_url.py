#!/usr/bin/env python3
"""Backfill mpg_city/mpg_highway from Dealer.com VDP inline JSON (cityFuelEconomy fields)."""
from __future__ import annotations

import argparse
import os
import re
import sqlite3
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

_VIN_FUEL_RE = re.compile(
    r'"vin"\s*:\s*"(?P<vin>[A-HJ-NPR-Z0-9]{17})"[^}]{0,4000}?'
    r'"cityFuelEconomy"\s*:\s*(?P<city>\d{1,2})[^}]{0,400}?'
    r'"highwayFuelEconomy"\s*:\s*(?P<hwy>\d{1,2})',
    re.I | re.S,
)


def _fetch_mpg(url: str, expected_vin: str) -> tuple[int | None, int | None]:
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 (compatible; DealershipScanner/1.0)"},
    )
    with urllib.request.urlopen(req, timeout=45) as resp:
        html = resp.read(800_000).decode("utf-8", errors="replace")
    m = _VIN_FUEL_RE.search(html)
    if not m:
        return None, None
    if m.group("vin").upper() != expected_vin.upper():
        return None, None
    return int(m.group("city")), int(m.group("hwy"))


def main() -> int:
    ap = argparse.ArgumentParser(description="Backfill MPG from listing source_url HTML")
    ap.add_argument("--db", default=os.environ.get("INVENTORY_DB_PATH", "inventory.db"))
    ap.add_argument("--dealer-id", default=None)
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()
    db = str(Path(args.db).expanduser())
    conn = sqlite3.connect(db)
    where = "(mpg_city IS NULL OR mpg_city = 0) AND (mpg_highway IS NULL OR mpg_highway = 0)"
    params: list[str] = []
    if args.dealer_id:
        where += " AND dealer_id = ?"
        params.append(args.dealer_id)
    lim = f" LIMIT {int(args.limit)}" if args.limit and args.limit > 0 else ""
    rows = conn.execute(
        f"SELECT id, vin, source_url FROM cars WHERE {where} AND source_url LIKE 'http%' {lim}",
        params,
    ).fetchall()
    updated = 0
    for car_id, vin, url in rows:
        try:
            city, hwy = _fetch_mpg(str(url), str(vin))
        except Exception as exc:
            print(f"skip {vin}: {exc}")
            continue
        if city is None or hwy is None:
            continue
        conn.execute(
            "UPDATE cars SET mpg_city = ?, mpg_highway = ? WHERE id = ?",
            (city, hwy, car_id),
        )
        updated += 1
        print(f"ok {vin}: {city}/{hwy}")
    conn.commit()
    conn.close()
    print(f"Updated {updated} of {len(rows)} candidates")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
