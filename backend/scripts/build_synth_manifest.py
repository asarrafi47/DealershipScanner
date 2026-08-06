#!/usr/bin/env python3
"""Build a manifest of dealers that need a one-time recipe-synthesis scan.

WHY
---
Delta scanning replays a stored HTTP "recipe" and is entirely browser-free, but
a dealer only HAS a recipe once a full scan has watched a browser talk to its
site. On 2026-08-03, 436 of 650 ``dealer_recipes`` rows had ``recipe_count = 0``
— 426 of them had never produced a single car — so the browser-free nightly
could reach 224 dealers out of a 592-dealer registry.

Playwright is therefore a ONE-TIME cost per dealer: capture the recipe once, and
every later scan replays it over plain HTTP forever. Pair this manifest with
``scanner.py --capture-only``, which grabs the inventory endpoint and rows while
visiting ZERO per-car VDP pages — the expensive part — so first contact takes
seconds per dealer instead of minutes.

RESUMABILITY
------------
Re-running regenerates the list from the CURRENT database state, so any dealer
that gained a usable recipe on a previous pass drops out automatically. Kill the
scan at any point, rebuild, and rerun to continue where it left off.

WHAT IS DELIBERATELY EXCLUDED
-----------------------------
Dealers whose ``scan_hints.skip_reason`` says they are not a used-car storefront
at all — OEM brand/direct-sales sites, motorcycle/ATV, RV, commercial truck and
national used-car chains. Those were triaged by hand and are correctly out of
scope; scanning them would add noise, not inventory.

Usage:
    .venv/bin/python backend/scripts/build_synth_manifest.py [-o PATH]
    .venv/bin/python scanner.py --manifest workspace/manifest_synth.json --capture-only
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from urllib.parse import urlparse

sys.path.insert(0, ".")

DEFAULT_OUT = "workspace/manifest_synth.json"


def _slug_to_host(dealer_id: str) -> str:
    """``davekirk-com`` -> ``davekirk.com``. The id IS the host, dots to dashes."""
    host = re.sub(r"-(com|net|org|us|ca|cars|auto)$", r".\1", dealer_id or "")
    return host if "." in host else ""


def build(conn) -> list[dict]:
    cur = conn.cursor()
    # Three ways a dealer needs the browser, not just "has no recipe":
    #   1. recipe_count = 0            — never captured anything
    #   2. has live listings that have not been re-scraped in 7+ days — it holds
    #      a recipe, but the recipe yields nothing, so the delta skips it every
    #      night and its inventory silently rots. 12 dealers / 2,719 listings sat
    #      frozen at 2026-07-21 this way, and 6 of them were invisible to the
    #      original recipe_count=0 test.
    #   3. price_requires_full_scan    — the feed replays fine but carries no
    #      price and the pricing API is bot-walled, so the delta's price gate
    #      skips the upsert forever (gardenahonda-com: 648 live rows, 0 priced).
    cur.execute(
        """
        WITH needs_browser AS (
            SELECT dealer_id FROM dealer_recipes WHERE COALESCE(recipe_count, 0) = 0
            UNION
            SELECT dealer_id FROM dealer_recipes
             WHERE COALESCE(scan_hints::text, '') LIKE '%price_requires_full_scan%'
            UNION
            SELECT dealer_id FROM cars
             GROUP BY dealer_id
            HAVING COUNT(*) FILTER (WHERE COALESCE(listing_active, 1) = 1) > 0
               AND MAX(scraped_at)::timestamptz < now() - interval '7 days'
        )
        SELECT dr.dealer_id,
               dr.provider_hint,
               dr.scan_hints,
               (SELECT MAX(d.name) FROM dealerships d
                 WHERE REPLACE(REPLACE(REPLACE(LOWER(COALESCE(d.website_url,'')),
                       'https://',''),'http://',''),'www.','')
                       LIKE REPLACE(dr.dealer_id,'-','.') || '%'),
               (SELECT MAX(d.website_url) FROM dealerships d
                 WHERE REPLACE(REPLACE(REPLACE(LOWER(COALESCE(d.website_url,'')),
                       'https://',''),'http://',''),'www.','')
                       LIKE REPLACE(dr.dealer_id,'-','.') || '%')
          FROM dealer_recipes dr
         WHERE dr.dealer_id IN (SELECT dealer_id FROM needs_browser)
         ORDER BY dr.dealer_id
        """
    )
    rows = cur.fetchall()

    out: list[dict] = []
    skipped: dict[str, int] = {}
    for dealer_id, provider, hints, reg_name, reg_url in rows:
        did = str(dealer_id or "").strip()
        if not did:
            continue

        # Hand-triaged "this is not a used-car storefront" verdicts stay out.
        reason = ""
        if hints:
            try:
                h = hints if isinstance(hints, dict) else json.loads(hints)
                reason = str(h.get("skip_reason") or "").strip()
            except (TypeError, ValueError, json.JSONDecodeError):
                reason = ""
        if reason:
            skipped[reason[:60]] = skipped.get(reason[:60], 0) + 1
            continue

        # URL from the registry when its host agrees with the slug, else the slug
        # itself. A group feed rewrites hosts, so a stored URL is never trusted
        # over the id.
        slug_host = _slug_to_host(did)
        url = f"https://www.{slug_host}" if slug_host else ""
        if reg_url:
            u = urlparse(reg_url if "//" in reg_url else "https://" + reg_url)
            host = (u.netloc or "").lower()
            host = host[4:] if host.startswith("www.") else host
            if host and (not slug_host or host == slug_host):
                url = f"{u.scheme or 'https'}://{u.netloc}"
        if not url:
            skipped["no resolvable URL"] = skipped.get("no resolvable URL", 0) + 1
            continue

        entry = {"dealer_id": did, "url": url, "name": (reg_name or did).strip()}
        if provider and str(provider).strip() and str(provider).strip() != "unknown":
            entry["provider"] = str(provider).strip()
        out.append(entry)

    print(f"  candidates written : {len(out)}", file=sys.stderr)
    for k, v in sorted(skipped.items(), key=lambda kv: -kv[1]):
        print(f"  excluded ({v:3d})    : {k}", file=sys.stderr)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("-o", "--out", default=DEFAULT_OUT)
    args = ap.parse_args()

    import psycopg

    url = re.search(
        r"^INVENTORY_DATABASE_URL=(.+)$", open(".env").read(), re.M
    ).group(1).strip()
    with psycopg.connect(url) as conn:
        dealers = build(conn)

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(dealers, f, indent=2)
    print(f"wrote {len(dealers)} dealers -> {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
