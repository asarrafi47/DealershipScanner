#!/usr/bin/env python3
"""
File group-feed cars under the rooftop that actually holds them.

Many storefronts serve a whole dealer group's inventory: nissanofcostamesa.com
returns seven rooftops' cars, fletcherjones.com five. Every one of those cars
inherits the storefront's coordinates, so a Henderson NV car shows up as being
in Newport Beach -- 240 miles away -- and radius search puts it in the wrong
metro.

Dropping the foreign rows is the wrong fix: measured 2026-07-21, every one of
Nissan of Costa Mesa's would-drop VINs exists ONLY under that storefront, so
dropping deletes real inventory instead of deduping it. The cars are real; the
store they are filed under is wrong.

The feed already names the true rooftop per vehicle -- CarsCommerce nests a
``dealer`` object on each listing, Dealer.com puts dealerName/dealerCity/
dealerZip on the row -- often with a street address and a link to that
rooftop's own website. So each rooftop is registered in ``dealerships`` and
each car is pointed at it via ``cars.dealership_registry_id``, which the
listings API already serves as ``registry_coords`` and the client already
prefers over the dealer-URL point. No frontend change needed.

Usage:
  python -m backend.scripts.attribute_feed_rooftops --dealer fletcherjones-com
  python -m backend.scripts.attribute_feed_rooftops --dealer fletcherjones-com --apply
  python -m backend.scripts.attribute_feed_rooftops --all --apply
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_REPO_ROOT))

from backend.db.dealer_geo import normalize_dealer_host
from backend.db.inventory_db import db_conn
from backend.scripts.geocode_dealers import _places_component, _places_search

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("attribute_feed_rooftops")

_HREF_RE = re.compile(r'href=["\']([^"\']+)', re.I)
_VIN_RE = re.compile(r"^[A-HJ-NPR-Z0-9]{17}$")


def _listing_nodes(records: list) -> list[dict]:
    """Every feed object that carries a VIN."""
    out: list[dict] = []

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            if any(str(k).lower() == "vin" for k in node):
                out.append(node)
                return
            for val in node.values():
                walk(val)
        elif isinstance(node, list):
            for val in node:
                walk(val)

    for _url, body in records:
        try:
            walk(body if isinstance(body, (dict, list)) else json.loads(body))
        except Exception:
            continue
    return out


def _clean(val: Any) -> str:
    s = str(val or "").strip()
    return "" if s.lower() in ("none", "null", "n/a") else s


def rooftop_of(listing: dict) -> dict | None:
    """
    The rooftop a listing belongs to, from whichever shape the platform uses.

    CarsCommerce nests it under ``dealer``; Dealer.com flattens it onto the row.
    """
    src = listing.get("dealer") if isinstance(listing.get("dealer"), dict) else listing
    name = _clean(src.get("name") or src.get("dealerName") or src.get("dealer_name"))
    city = _clean(src.get("city") or src.get("dealerCity") or src.get("dealer_city"))
    state = _clean(src.get("state") or src.get("dealerState") or src.get("dealer_state"))
    zip_raw = _clean(src.get("zipcode") or src.get("zip") or src.get("dealerZip")
                     or src.get("postalCode") or src.get("dealer_zip"))
    digits = "".join(ch for ch in zip_raw if ch.isdigit())[:5]
    if not name and not digits:
        return None
    site = ""
    m = _HREF_RE.search(str(src.get("location") or ""))
    if m:
        site = normalize_dealer_host(m.group(1))
    return {
        "name": name,
        "city": city,
        "state": state.upper()[:2],
        "zip": digits,
        "site": site,
        "address": _clean(src.get("address") or src.get("street_address")),
    }


def rooftop_key(rt: dict) -> tuple:
    """A rooftop's identity: its own site when it has one, else name+ZIP."""
    return (rt["site"],) if rt["site"] else (rt["name"].lower(), rt["zip"])


def resolve_rooftop(rt: dict) -> dict | None:
    """
    Coordinates for a rooftop, accepted only on an identity match.

    Verified by the rooftop's own website host when the feed linked one, else by
    the returned address landing in the ZIP the feed gave. A name-only match is
    refused -- that is what put South Bay BMW in San Francisco.
    """
    api_key = os.getenv("GOOGLE_MAPS_API_KEY")
    if not api_key:
        log.warning("GOOGLE_MAPS_API_KEY unset — cannot resolve rooftops")
        return None
    # With no name in the feed, an address-first query returns the street itself
    # -- a result whose displayName is "7300 W Sahara Ave". Look the site up as a
    # business first so the rooftop gets its real name and a websiteUri to verify
    # against; fall back to the address only to place an unnamed, unlinked lot.
    named = [f"{rt['name']} {rt['address']}".strip(),
             f"{rt['name']} {rt['city']} {rt['state']} {rt['zip']}".strip()]
    queries = [q for q in ([rt["site"]] + named if not rt["name"] else named + [rt["site"]])
               if q.strip()]
    seen: set[str] = set()
    for query in queries:
        if query in seen:
            continue
        seen.add(query)
        for place in _places_search(query, api_key):
            loc = place.get("location") or {}
            if "latitude" not in loc:
                continue
            phost = normalize_dealer_host(place.get("websiteUri") or "")
            pzip = _places_component(place, "postal_code") or ""
            by_site = bool(rt["site"]) and phost == rt["site"]
            by_zip = bool(rt["zip"]) and pzip == rt["zip"]
            if not (by_site or by_zip):
                continue
            return {
                "lat": float(loc["latitude"]),
                "lon": float(loc["longitude"]),
                "zip_code": pzip or rt["zip"],
                "city": _places_component(place, "locality") or rt["city"],
                "state": _places_component(place, "administrative_area_level_1") or rt["state"],
                "street_address": place.get("formattedAddress") or rt["address"],
                # Some feeds give the rooftop a link but no name; the verified
                # listing's own name beats registering it as a bare ZIP.
                "display_name": ((place.get("displayName") or {}).get("text") or "").strip(),
                "verified_by": "site" if by_site else "zip",
            }
    return None


def rooftop_name(rt: dict, geo: dict, storefront: str) -> str:
    """
    A registry row needs a name a human can recognize.

    Some feeds (Norm Reeves, Welborn) give a lot only a city and ZIP. Registering
    that as "Cerritos 90703" puts a placeholder in the dealer registry and will
    not dedupe against the same store found later under its real name, so fall
    back to the group that published the feed: "Norm Reeves Buick/GMC (Cerritos,
    CA)" says what we actually know -- this group has a lot there.
    """
    if rt["name"]:
        return rt["name"]
    if geo.get("display_name"):
        return geo["display_name"]
    where = ", ".join(p for p in (geo.get("city") or rt["city"], geo.get("state") or rt["state"]) if p)
    if storefront and where:
        return f"{storefront} ({where})"
    return where or f"{rt['city']} {rt['zip']}".strip()


def _register(rt: dict, geo: dict, storefront: str = "") -> int:
    from backend.db.dealerships_db import upsert_discovery_row

    return upsert_discovery_row({
        "name": rooftop_name(rt, geo, storefront),
        "city": geo["city"],
        "state": geo["state"],
        "zip_code": geo["zip_code"],
        "street_address": geo["street_address"],
        "latitude": geo["lat"],
        "longitude": geo["lon"],
        "dealer_website_url": (f"https://www.{rt['site']}" if rt["site"] else ""),
        "source_web": True,
    })


def process_dealer(dealer_id: str, dealer_url: str, dealer_name: str, apply: bool) -> dict:
    from backend.scanner.recipes import try_fetch_via_recipes

    try:
        res = asyncio.run(
            try_fetch_via_recipes(dealer_id, "", dealer_url, dealer_id, union=True)
        )
    except Exception as exc:
        log.warning("%s: feed fetch failed: %s", dealer_name, exc)
        return {}
    if not res:
        log.info("%s: no recipe", dealer_name)
        return {}

    listings = _listing_nodes(res[0])
    by_roof: dict[tuple, dict] = {}
    vins_by_roof: dict[tuple, set[str]] = defaultdict(set)
    for item in listings:
        vin = str(item.get("vin") or "").strip().upper()
        rt = rooftop_of(item)
        if not rt or not _VIN_RE.match(vin):
            continue
        key = rooftop_key(rt)
        by_roof.setdefault(key, rt)
        vins_by_roof[key].add(vin)

    if len(by_roof) <= 1:
        log.info("%s: single rooftop — nothing to re-attribute", dealer_name)
        return {}

    with db_conn() as conn:
        db_vins = {
            str(r[0]).upper()
            for r in conn.execute(
                "SELECT vin FROM cars WHERE COALESCE(listing_active,1)=1 AND dealer_url = ?",
                (dealer_url,),
            ).fetchall()
            if r[0]
        }

    log.info("%s: %d feed rooftops, %d active cars in DB", dealer_name, len(by_roof), len(db_vins))
    stats: dict[str, int] = Counter()
    for key, rt in sorted(by_roof.items(), key=lambda kv: -len(vins_by_roof[kv[0]])):
        mine = vins_by_roof[key] & db_vins
        label = rt["name"] or rt["site"] or f"{rt['city']} {rt['zip']}".strip()
        if not mine:
            log.info("   %-38s %3d feed VINs, none in this storefront's inventory",
                     label[:38], len(vins_by_roof[key]))
            continue
        geo = resolve_rooftop(rt)
        if not geo:
            log.warning("   %-38s %3d cars — UNRESOLVED, left as-is", label[:38], len(mine))
            stats["unresolved_cars"] += len(mine)
            continue
        log.info("   %-38s %3d cars -> %.4f,%.4f %s [%s] as %r", label[:38], len(mine),
                 geo["lat"], geo["lon"], geo["city"], geo["verified_by"],
                 rooftop_name(rt, geo, dealer_name))
        if not apply:
            stats["would_link"] += len(mine)
            continue
        reg_id = _register(rt, geo, dealer_name)
        vins = sorted(mine)
        with db_conn() as conn:
            for i in range(0, len(vins), 200):
                chunk = vins[i:i + 200]
                ph = ",".join("?" for _ in chunk)
                conn.execute(
                    f"UPDATE cars SET dealership_registry_id = ? "
                    f"WHERE UPPER(vin) IN ({ph}) AND dealer_url = ?",
                    (reg_id, *chunk, dealer_url),
                )
            conn.commit()
        stats["linked"] += len(mine)
        stats["rooftops"] += 1
    return dict(stats)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--dealer", help="dealer_id to process")
    group.add_argument("--all", action="store_true", help="every active dealer")
    parser.add_argument("--apply", action="store_true", help="write (default: dry run)")
    parser.add_argument("--min-cars", type=int, default=50,
                        help="with --all, skip storefronts below this size")
    args = parser.parse_args(argv)

    with db_conn() as conn:
        rows = conn.execute("""
            SELECT MAX(dealer_id) AS dealer_id, dealer_url,
                   MAX(dealer_name) AS dealer_name, COUNT(*) AS cnt
            FROM cars WHERE COALESCE(listing_active,1)=1 AND dealer_url IS NOT NULL
            GROUP BY dealer_url ORDER BY COUNT(*) DESC
        """).fetchall()

    targets = [
        (r[0], r[1], r[2])
        for r in rows
        if (args.all and r[3] >= args.min_cars) or (r[0] == args.dealer)
    ]
    if not targets:
        log.error("no matching dealer")
        return

    total: Counter = Counter()
    for dealer_id, dealer_url, dealer_name in targets:
        total.update(process_dealer(dealer_id, dealer_url, dealer_name or dealer_id, args.apply))

    log.info("")
    log.info("=== Summary (%s) ===", "applied" if args.apply else "dry run")
    for k, v in sorted(total.items()):
        log.info("  %-16s %d", k, v)


if __name__ == "__main__":
    main()
