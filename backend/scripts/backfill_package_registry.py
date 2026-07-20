"""Seed the package/option value registry from existing cars.packages data.

Replays two signals already sitting in ``cars.packages`` into
``package_observations`` / ``package_values``:

  * ``sticker_options_priced`` — real OEM window-sticker options with codes and
    prices (source=oem_sticker, ground truth).
  * ``factory_packages`` — package names pulled from dealer listings
    (source=dealer_listing, names only, prices filled later by stickers).

Idempotent: re-running folds the same observations in without duplicating them.

Usage:
    INVENTORY_DATABASE_URL=... python -m backend.scripts.backfill_package_registry
    ... --limit 500        # smoke test on a subset
"""
from __future__ import annotations

import argparse
import json

import backend.enrichment.knowledge_engine as ke
from backend.enrichment.package_registry import classify_kind, record_package_observations


def _sticker_items(priced: list) -> list[dict]:
    items = []
    for entry in priced:
        if not isinstance(entry, dict):
            continue
        name = str(entry.get("name") or "").strip()
        if not name:
            continue
        price = entry.get("price")
        items.append(
            {
                "kind": classify_kind(name),
                "name": name,
                "code": entry.get("code"),
                "price": price if isinstance(price, (int, float)) else None,
            }
        )
    return items


def _factory_package_items(pkgs: list) -> list[dict]:
    items = []
    for p in pkgs:
        name = str(p or "").strip()
        if name:
            items.append({"kind": "package", "name": name})
    return items


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0, help="cap rows scanned (0 = all)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    conn = ke._conn()
    cur = conn.cursor()
    sql = (
        "SELECT id, vin, year, make, model, trim, packages FROM cars "
        "WHERE packages IS NOT NULL AND packages NOT IN ('{}','[]','null','') "
        "AND (packages LIKE '%sticker_options_priced%' OR packages LIKE '%factory_packages%')"
    )
    if args.limit:
        sql += f" LIMIT {int(args.limit)}"
    cur.execute(sql)
    rows = cur.fetchall()

    stats = {"cars": 0, "sticker_obs": 0, "listing_obs": 0, "bad_json": 0}
    for cid, vin, year, make, model, trim, packages in rows:
        try:
            d = json.loads(packages)
        except Exception:
            stats["bad_json"] += 1
            continue
        if not isinstance(d, dict):
            continue
        car = {"vin": vin, "year": year, "make": make, "model": model, "trim": trim}

        sticker = _sticker_items(d.get("sticker_options_priced") or [])
        listing = _factory_package_items(d.get("factory_packages") or [])
        if not (sticker or listing):
            continue
        stats["cars"] += 1
        if args.dry_run:
            continue
        if sticker:
            stats["sticker_obs"] += record_package_observations(
                car, "oem_sticker", sticker, conn=conn
            )
        if listing:
            stats["listing_obs"] += record_package_observations(
                car, "dealer_listing", listing, conn=conn
            )

    if not args.dry_run:
        conn.commit()
    conn.close()
    print(
        f"backfill complete: cars={stats['cars']} "
        f"sticker_obs={stats['sticker_obs']} listing_obs={stats['listing_obs']} "
        f"bad_json={stats['bad_json']} dry_run={args.dry_run}"
    )


if __name__ == "__main__":
    main()
