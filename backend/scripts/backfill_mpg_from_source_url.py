#!/usr/bin/env python3
"""Backfill mpg_city/mpg_highway from listing source_url (Dealer.com JSON + generic VDP parse)."""
from __future__ import annotations

import argparse
import os
import re
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


def _fetch_mpg_dealer_dot_com(url: str, expected_vin: str) -> tuple[int | None, int | None]:
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 (compatible; DealershipScanner/1.0)"},
    )
    with urllib.request.urlopen(req, timeout=45) as resp:
        html = resp.read(800_000).decode("utf-8", errors="replace")
    m = _VIN_FUEL_RE.search(html)
    if m and m.group("vin").upper() == expected_vin.upper():
        return int(m.group("city")), int(m.group("hwy"))
    from backend.utils.vdp_spec_parse import parse_html_for_vehicle_specs

    specs = parse_html_for_vehicle_specs(html)
    c, h = specs.get("mpg_city"), specs.get("mpg_highway")
    if c is not None and h is not None:
        return int(c), int(h)
    return None, None


def main() -> int:
    ap = argparse.ArgumentParser(description="Backfill MPG from listing source_url HTML")
    ap.add_argument("--dealer-id", default=None)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    from backend.db.inventory_db import get_conn, init_inventory_db

    init_inventory_db()
    where = "(mpg_city IS NULL OR mpg_city = 0) AND (mpg_highway IS NULL OR mpg_highway = 0)"
    params: list[str] = []
    if args.dealer_id:
        where += " AND dealer_id = ?"
        params.append(args.dealer_id)
    lim = f" LIMIT {int(args.limit)}" if args.limit and args.limit > 0 else ""
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            f"SELECT id, vin, source_url FROM cars WHERE {where} AND source_url LIKE 'http%' {lim}",
            params,
        )
        rows = cur.fetchall()
        updated = 0
        for row in rows:
            car_id = row[0] if not isinstance(row, dict) else row["id"]
            vin = row[1] if not isinstance(row, dict) else row["vin"]
            url = row[2] if not isinstance(row, dict) else row["source_url"]
            try:
                city, hwy = _fetch_mpg_dealer_dot_com(str(url), str(vin))
            except Exception as exc:
                print(f"skip {vin}: {exc}")
                continue
            if city is None or hwy is None:
                continue
            if args.dry_run:
                print(f"ok {vin}: {city}/{hwy}")
                updated += 1
                continue
            cur.execute(
                "UPDATE cars SET mpg_city = ?, mpg_highway = ? WHERE id = ?",
                (city, hwy, car_id),
            )
            updated += 1
            print(f"ok {vin}: {city}/{hwy}")
        if not args.dry_run:
            conn.commit()
    finally:
        conn.close()
    print(f"Updated {updated} of {len(rows)} candidates")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

