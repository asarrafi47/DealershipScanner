#!/usr/bin/env python3
"""Coverage audit for inventory.db — gap report per dealer or whole DB."""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _gallery_len(raw: str | None) -> int:
    if not raw:
        return 0
    try:
        g = json.loads(raw)
        return len(g) if isinstance(g, list) else 0
    except (json.JSONDecodeError, TypeError):
        return 0


def _packages_norm_len(raw: str | None) -> int:
    if not raw:
        return 0
    try:
        p = json.loads(raw) if isinstance(raw, str) else raw
        if isinstance(p, dict):
            norm = p.get("packages_normalized") or []
            return len(norm) if isinstance(norm, list) else 0
    except (json.JSONDecodeError, TypeError):
        pass
    return 0


def audit(db_path: str, *, dealer_id: str | None = None) -> dict:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    where = "COALESCE(listing_active, 1) = 1"
    params: list[str] = []
    if dealer_id:
        where += " AND dealer_id = ?"
        params.append(dealer_id)

    rows = conn.execute(
        f"""
        SELECT vin, condition, description, carfax_url, fuel_type, packages,
               gallery, price, trim, transmission, drivetrain, spec_source_json
        FROM cars WHERE {where}
        """,
        params,
    ).fetchall()
    conn.close()

    n = len(rows)
    if n == 0:
        return {"total": 0, "dealer_id": dealer_id}

    cond = Counter()
    missing = Counter()
    gallery_bins = Counter()
    pkg_bins = Counter()

    for r in rows:
        c = (r["condition"] or "").strip() or "(blank)"
        cond[c] += 1

        desc = (r["description"] or "").strip()
        if len(desc) < 40:
            missing["description"] += 1
        if not (r["carfax_url"] or "").strip().startswith("http"):
            missing["carfax_url"] += 1
        if not (r["fuel_type"] or "").strip():
            missing["fuel_type"] += 1
        if not (r["trim"] or "").strip():
            missing["trim"] += 1
        if not (r["transmission"] or "").strip():
            missing["transmission"] += 1
        if not (r["price"] or 0):
            missing["price"] += 1

        glen = _gallery_len(r["gallery"])
        if glen == 0:
            gallery_bins["0"] += 1
        elif glen == 1:
            gallery_bins["1"] += 1
        elif glen < 5:
            gallery_bins["2_4"] += 1
        else:
            gallery_bins["5p"] += 1

        plen = _packages_norm_len(r["packages"])
        if plen == 0:
            pkg_bins["0"] += 1
        elif plen < 5:
            pkg_bins["1_4"] += 1
        else:
            pkg_bins["5p"] += 1

        try:
            from backend.utils.listing_completeness import listing_missing_field_codes

            car = dict(r)
            for code in listing_missing_field_codes(car, for_public_filter=True):
                missing[code] += 1
        except Exception:
            pass

    return {
        "total": n,
        "dealer_id": dealer_id,
        "condition_breakdown": dict(cond),
        "missing_field_counts": dict(missing.most_common()),
        "gallery_bins": dict(gallery_bins),
        "packages_normalized_bins": dict(pkg_bins),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Inventory coverage gap report")
    ap.add_argument("--db", default=os.environ.get("INVENTORY_DB_PATH", "inventory.db"))
    ap.add_argument("--dealer-id", default=None)
    args = ap.parse_args()
    db = str(Path(args.db).expanduser())
    if not Path(db).is_file():
        print(f"DB not found: {db}", file=sys.stderr)
        return 1
    report = audit(db, dealer_id=args.dealer_id)
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
