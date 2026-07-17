#!/usr/bin/env python3
"""
Mine captured network-traffic recipes for car fields and patch inventory gaps.

The NetworkObserver already records each dealer's inventory API endpoints as
replayable recipes (workspace/recipes/<dealer_id>.json). Scan-time replay only
uses them to fetch VIN lists; the payloads carry far more — DealerOn "cosmos"
SRP responses include price (``TaggingPrice``), both color labels, trim,
engine, and mpg even for dealers whose HTML pages hide those fields.

This script replays every non-stale recipe over plain HTTP, routes each payload
by SHAPE (``DisplayCards`` → DealerOn card mapper; anything else → the provider
parser the scanner would use), and fills NULL/empty columns on matching
``cars`` rows by VIN — never overwriting a stored value. Patched rows are
re-synced in the incomplete-listings index.

Usage (repo root)::

  python3 backend/scripts/heal_from_recipes.py --dealer-id cadillacoflagunaniguel-com
  python3 backend/scripts/heal_from_recipes.py            # all dealers with recipes
  python3 backend/scripts/heal_from_recipes.py --dry-run  # report, no writes
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import os
import sys
from pathlib import Path
from typing import Any
from urllib.parse import urlparse, urlunparse

_REPO_ROOT = Path(__file__).resolve().parents[2]
os.chdir(_REPO_ROOT)
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from backend.utils.project_env import load_project_dotenv

load_project_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("heal_from_recipes")

# Columns this heal may fill (only when currently NULL/empty).
_FILLABLE = (
    "price",
    "exterior_color",
    "interior_color",
    "trim",
    "mileage",
    "transmission",
    "drivetrain",
    "fuel_type",
    "body_style",
    "engine_description",
)

_COSMOS_PAGE_SIZE = 96
_COSMOS_MAX_PAGES = 30


def _clean_value(field: str, value: Any) -> Any:
    from backend.scanner.utils.vdp_price_merge import _clamp_vehicle_price

    if value is None:
        return None
    if field == "price":
        try:
            return _clamp_vehicle_price(float(value))
        except (TypeError, ValueError):
            return None
    if field == "mileage":
        try:
            m = int(float(value))
            return m if m >= 0 else None
        except (TypeError, ValueError):
            return None
    s = str(value).strip()
    return s or None


def _http_get_json(url: str) -> dict | None:
    import urllib.request

    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"
            ),
            "Accept": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=25) as resp:
            return json.loads(resp.read().decode("utf-8", errors="replace"))
    except Exception as exc:
        log.debug("GET %s failed: %s", url[:100], exc)
        return None


def _cosmos_pages(recipe_url: str) -> list[dict]:
    """Walk a DealerOn cosmos SRP endpoint session-free via ?pg=N&pn=96."""
    clean = urlunparse(urlparse(recipe_url)._replace(query="", fragment=""))
    first = _http_get_json(f"{clean}?pg=1&pn={_COSMOS_PAGE_SIZE}")
    if not isinstance(first, dict) or "DisplayCards" not in first:
        return []
    bodies = [first]
    paging = (first.get("Paging") or {}).get("PaginationDataModel") or {}
    total = int(paging.get("TotalCount") or 0)
    pages = min(_COSMOS_MAX_PAGES, max(1, math.ceil(total / _COSMOS_PAGE_SIZE)))
    for pg in range(2, pages + 1):
        body = _http_get_json(f"{clean}?pg={pg}&pn={_COSMOS_PAGE_SIZE}")
        if not isinstance(body, dict) or not body.get("DisplayCards"):
            break
        bodies.append(body)
    return bodies


def _vehicles_from_body(
    body: Any, provider_hint: str, base_url: str, dealer_id: str
) -> list[dict[str, Any]]:
    """Shape-routed extraction: cosmos cards first, provider parser otherwise."""
    if isinstance(body, dict) and isinstance(body.get("DisplayCards"), list):
        from backend.scanner.scrapers.dealer_on import _extract_vehicles_from_srp_body

        return _extract_vehicles_from_srp_body(body, base_url, dealer_id, dealer_id, base_url)
    try:
        from backend.parsers import parse

        return list(
            parse(
                provider_hint or "unknown",
                body,
                base_url=base_url,
                dealer_id=dealer_id,
                dealer_name=dealer_id,
                dealer_url=base_url,
            )
        )
    except Exception:
        return []


def collect_vehicles_for_dealer(dealer_id: str) -> dict[str, dict[str, Any]]:
    """Replay all non-stale recipes; return VIN → merged field dict."""
    import asyncio

    from backend.scanner.recipes import _replay_request, load_recipes

    by_vin: dict[str, dict[str, Any]] = {}

    def _absorb(vehicles: list[dict[str, Any]]) -> None:
        for v in vehicles:
            vin = str(v.get("vin") or "").strip().upper()
            if len(vin) != 17:
                continue
            slot = by_vin.setdefault(vin, {})
            for field in _FILLABLE:
                val = _clean_value(field, v.get(field))
                if val is not None and slot.get(field) is None:
                    slot[field] = val

    for recipe in load_recipes(dealer_id):
        if recipe.stale:
            continue
        base_url = f"https://{urlparse(recipe.url).netloc}"
        if "cosmos/srp/vehicles" in recipe.url:
            for body in _cosmos_pages(recipe.url):
                _absorb(_vehicles_from_body(body, recipe.provider_hint, base_url, dealer_id))
            continue
        template = None
        if recipe.post_template:
            try:
                template = json.loads(recipe.post_template)
            except ValueError:
                continue
        if recipe.method != "GET" and template is None:
            continue
        _status, parsed = _replay_request(recipe, template, base_url)
        if parsed is None:
            continue
        _absorb(_vehicles_from_body(parsed, recipe.provider_hint, base_url, dealer_id))
    return by_vin


def patch_dealer(dealer_id: str, *, dry_run: bool) -> dict[str, Any]:
    from backend.db.incomplete_listings_db import sync_incomplete_listing_for_car_id
    from backend.db.inventory_db import db_conn

    by_vin = collect_vehicles_for_dealer(dealer_id)
    stats: dict[str, Any] = {"vins_from_recipes": len(by_vin), "rows_patched": 0, "fields": {}}
    if not by_vin:
        return stats

    patched_ids: list[int] = []
    with db_conn() as conn:
        cur = conn.cursor()
        for vin, fields in by_vin.items():
            cur.execute(
                f"SELECT id, {', '.join(_FILLABLE)} FROM cars"
                " WHERE UPPER(vin) = ? AND (COALESCE(listing_active, 1) = 1)",
                (vin,),
            )
            for row in cur.fetchall():
                car_id, *current = row
                updates = {
                    f: fields[f]
                    for f, cur_val in zip(_FILLABLE, current)
                    if fields.get(f) is not None
                    and (cur_val is None or str(cur_val).strip() == "")
                }
                if not updates:
                    continue
                stats["rows_patched"] += 1
                for f in updates:
                    stats["fields"][f] = stats["fields"].get(f, 0) + 1
                if dry_run:
                    continue
                set_sql = ", ".join(f"{f} = ?" for f in updates)
                cur.execute(
                    f"UPDATE cars SET {set_sql} WHERE id = ?",
                    (*updates.values(), car_id),
                )
                patched_ids.append(car_id)
        if not dry_run:
            conn.commit()

    for car_id in patched_ids:
        try:
            sync_incomplete_listing_for_car_id(car_id)
        except Exception:
            log.exception("index sync failed for car %s", car_id)
    return stats


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dealer-id", action="append", default=None)
    ap.add_argument("--dry-run", action="store_true")
    ns = ap.parse_args()

    recipes_dir = _REPO_ROOT / "workspace" / "recipes"
    dealer_ids = ns.dealer_id or sorted(p.stem for p in recipes_dir.glob("*.json"))

    grand = {"rows_patched": 0, "fields": {}}
    for did in dealer_ids:
        stats = patch_dealer(did, dry_run=ns.dry_run)
        if stats["vins_from_recipes"] or stats["rows_patched"]:
            log.info("[%s] %s", did, json.dumps(stats))
        grand["rows_patched"] += stats["rows_patched"]
        for f, n in stats["fields"].items():
            grand["fields"][f] = grand["fields"].get(f, 0) + n
    log.info("TOTAL%s: %s", " (dry-run)" if ns.dry_run else "", json.dumps(grand))


if __name__ == "__main__":
    main()
