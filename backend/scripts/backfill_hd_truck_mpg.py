#!/usr/bin/env python3
"""
Backfill MPG for EPA-exempt heavy-duty trucks (Ram/Ford/GM 2500+).

EPA / fueleconomy.gov does not rate HD pickups. This script:
  1. Seeds ``epa_master.city08`` / ``highway08`` for Ram 2500/3500 reference trims
  2. Backfills ``cars.mpg_city`` / ``mpg_highway`` from listing text, VDP scrape, or reference

Usage:
  INVENTORY_DATABASE_URL=postgresql://user@localhost/dealership_scanner \\
    PYTHONPATH=. python backend/scripts/backfill_hd_truck_mpg.py

  PYTHONPATH=. python backend/scripts/backfill_hd_truck_mpg.py --fill-epa-master-only
  PYTHONPATH=. python backend/scripts/backfill_hd_truck_mpg.py --scrape-vdp --limit 20
"""
from __future__ import annotations

import argparse
import sys
import urllib.request
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from backend.db.inventory_db import get_conn, init_inventory_db  # noqa: E402
from backend.enrichment.hd_truck_mpg import (  # noqa: E402
    is_epa_exempt_hd_truck,
    mpg_for_epa_master_trim,
    resolve_hd_truck_mpg,
)


def fill_epa_master_ram_hd(*, dry_run: bool = False) -> int:
    """Set manufacturer-estimate MPG on Ram 2500/3500 ``epa_master`` seed rows."""
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, trim FROM epa_master
            WHERE make = 'Ram' AND model IN ('2500', '3500')
              AND (city08 IS NULL OR highway08 IS NULL)
            """
        )
        rows = cur.fetchall()
        updated = 0
        for row in rows:
            rid = row[0] if not isinstance(row, dict) else row["id"]
            trim = row[1] if not isinstance(row, dict) else row["trim"]
            mpg = mpg_for_epa_master_trim(str(trim or ""))
            if not mpg:
                continue
            city, hwy = mpg
            if dry_run:
                print(f"  [dry-run] epa_master id={rid} trim={trim!r} -> {city}/{hwy}")
            else:
                cur.execute(
                    "UPDATE epa_master SET city08 = ?, highway08 = ? WHERE id = ?",
                    (city, hwy, rid),
                )
            updated += 1
        if not dry_run:
            conn.commit()
        return updated
    finally:
        conn.close()


def _fetch_html(url: str) -> str:
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 (compatible; DealershipScanner/1.0)"},
    )
    with urllib.request.urlopen(req, timeout=45) as resp:
        return resp.read(900_000).decode("utf-8", errors="replace")


def backfill_cars(
    *,
    dry_run: bool = False,
    scrape_vdp: bool = False,
    limit: int | None = None,
) -> dict[str, int]:
    """Fill missing MPG on inventory rows for EPA-exempt HD trucks."""
    conn = get_conn()
    stats = {"reference": 0, "listing": 0, "vdp": 0, "skipped": 0}
    try:
        cur = conn.cursor()
        sql = """
            SELECT id, vin, make, model, trim, title, description, drivetrain,
                   fuel_type, engine_description, source_url, mpg_city, mpg_highway
            FROM cars
            WHERE (mpg_city IS NULL OR mpg_highway IS NULL)
              AND make IS NOT NULL AND model IS NOT NULL
            ORDER BY id
        """
        if limit:
            sql += f" LIMIT {int(limit)}"
        cur.execute(sql)
        cols = [d[0] for d in cur.description] if cur.description else []
        for row in cur.fetchall():
            car = dict(row) if isinstance(row, dict) else dict(zip(cols, row))
            if not is_epa_exempt_hd_truck(car.get("make"), car.get("model")):
                continue
            html = None
            if scrape_vdp:
                url = str(car.get("source_url") or "").strip()
                if url.startswith("http"):
                    try:
                        html = _fetch_html(url)
                    except OSError as exc:
                        print(f"  skip scrape {car.get('vin')}: {exc}")
            resolved = resolve_hd_truck_mpg(car, html=html)
            if not resolved:
                stats["skipped"] += 1
                continue
            city, hwy, src = resolved
            if src == "listing_text":
                stats["listing"] += 1
            elif src == "vdp_html":
                stats["vdp"] += 1
            else:
                stats["reference"] += 1
            if dry_run:
                print(
                    f"  [dry-run] {car.get('vin')} {car.get('year')} "
                    f"{car.get('make')} {car.get('model')} -> {city}/{hwy} ({src})"
                )
                continue
            cur.execute(
                "UPDATE cars SET mpg_city = ?, mpg_highway = ? WHERE id = ?",
                (city, hwy, car["id"]),
            )
        if not dry_run:
            conn.commit()
    finally:
        conn.close()
    return stats


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--fill-epa-master-only",
        action="store_true",
        help="Only update epa_master Ram 2500/3500 rows.",
    )
    parser.add_argument(
        "--scrape-vdp",
        action="store_true",
        help="Fetch source_url HTML before reference fallback.",
    )
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    init_inventory_db()

    n_epa = fill_epa_master_ram_hd(dry_run=args.dry_run)
    print(f"epa_master Ram HD rows updated: {n_epa}")

    if args.fill_epa_master_only:
        return 0

    stats = backfill_cars(
        dry_run=args.dry_run,
        scrape_vdp=args.scrape_vdp,
        limit=args.limit,
    )
    print(
        f"cars: reference={stats['reference']} listing={stats['listing']} "
        f"vdp={stats['vdp']} skipped={stats['skipped']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
