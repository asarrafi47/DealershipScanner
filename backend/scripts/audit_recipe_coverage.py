#!/usr/bin/env python3
"""Replay each dealer's stored recipes and report what fields they actually carry.

Why: recipe acceptance used to count VINs only. On 2026-08-04 a fleet capture-only
run promoted VIN-list endpoints (Team Velocity ``GetKeyFeaturesByVins``, Gatsby
``page-data`` blobs, a WordPress plugin API with no price field) and the 70 %-of-
known-VINs rule then let those replays REPLACE the browser scrape. This audit shows,
per dealer, the price / trim / exterior-colour coverage a replay yields today and
whether it would be allowed to stand in for the browser under the current gate
(``recipes.recipe_yield_replaces_browser``).

Read-only against the inventory DB. Network: one replay per non-stale recipe.
Successful replays refresh ``field_coverage`` on the recipe file, which is the
same side effect a scan has.

Usage:
  python -m backend.scripts.audit_recipe_coverage --dealers jordanford-net,gardenahonda-com
  python -m backend.scripts.audit_recipe_coverage --all --json out.json
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from urllib.parse import urlparse

from backend.utils.project_env import load_project_dotenv

load_project_dotenv()

from backend.scanner.recipes import (  # noqa: E402
    RECIPES_DIR,
    last_known_vin_count,
    load_recipes,
    recipe_yield_replaces_browser,
    try_fetch_via_recipes,
)

logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")
logging.getLogger("scanner").setLevel(logging.WARNING)


def _all_dealer_ids() -> list[str]:
    return sorted(p.stem for p in RECIPES_DIR.glob("*.json") if not p.name.startswith("_"))


def audit_dealer(dealer_id: str) -> dict:
    recipes = [r for r in load_recipes(dealer_id) if not r.stale]
    row: dict = {"dealer_id": dealer_id, "recipes": len(recipes)}
    if not recipes:
        row["verdict"] = "no_recipes"
        return row
    base_url = f"https://{urlparse(recipes[0].url).netloc}"
    provider = recipes[0].provider_hint or "unknown"
    cov: dict = {}
    hit = asyncio.run(
        try_fetch_via_recipes(dealer_id, provider, base_url, dealer_id, union=True, coverage_out=cov)
    )
    known = last_known_vin_count(dealer_id)
    row["known_vins"] = known
    if not hit:
        row["verdict"] = "replay_failed"
        return row
    _records, n_vins = hit
    ok, why = recipe_yield_replaces_browser(n_vins, known, cov)
    row.update(
        {
            "replay_vins": n_vins,
            "price": round(cov.get("price", 0.0), 2),
            "trim": round(cov.get("trim", 0.0), 2),
            "exterior_color": round(cov.get("exterior_color", 0.0), 2),
            "replaces_browser": ok,
            "why": why,
            "endpoints": [urlparse(r.url).path[:60] for r in recipes][:4],
        }
    )
    row["verdict"] = "rich" if ok else "thin"
    return row


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--dealers", help="comma-separated dealer ids")
    g.add_argument("--all", action="store_true", help="every dealer with a recipe file")
    ap.add_argument("--json", help="also write rows to this path")
    args = ap.parse_args()

    ids = _all_dealer_ids() if args.all else [d.strip() for d in args.dealers.split(",") if d.strip()]
    rows = []
    print(f"{'dealer':30s} {'known':>6s} {'replay':>6s} {'price':>6s} {'trim':>6s} {'color':>6s}  verdict  why")
    for did in ids:
        try:
            row = audit_dealer(did)
        except Exception as exc:  # noqa: BLE001 - one dealer must not abort the audit
            row = {"dealer_id": did, "verdict": f"error: {str(exc)[:80]}"}
        rows.append(row)
        print(
            f"{did:30s} {row.get('known_vins', '-')!s:>6s} {row.get('replay_vins', '-')!s:>6s} "
            f"{row.get('price', '-')!s:>6s} {row.get('trim', '-')!s:>6s} {row.get('exterior_color', '-')!s:>6s}  "
            f"{row.get('verdict', '')}  {row.get('why', '')}"
        )
    if args.json:
        with open(args.json, "w", encoding="utf-8") as fh:
            json.dump(rows, fh, indent=1)
    thin = sum(1 for r in rows if r.get("verdict") == "thin")
    print(f"\n{len(rows)} dealer(s): {thin} thin, {sum(1 for r in rows if r.get('verdict') == 'rich')} rich")
    return 0


if __name__ == "__main__":
    sys.exit(main())
