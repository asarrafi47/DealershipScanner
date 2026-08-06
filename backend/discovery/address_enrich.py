#!/usr/bin/env python3
"""Fill ``dealerships.street_address`` / ``zip_code`` from each store's OWN site.

WHY THIS EXISTS
---------------
The rooftop-attribution gate (``backend.parsers.resolve_rooftop_attribution``)
tells a store apart from its group siblings by name, host, slug or ADDRESS. Some
dealer-group feeds label every rooftop with an unnamed postal address block, so
address is the only evidence available — and on 2026-08-03, 150 of the 178
dealers with active inventory had no ``street_address`` at all. Those dealers
resolve to ``target_rooftop_unidentified``: safe (nothing is un-listed) but
frozen, because no new rows can be written either. bmwofmurrieta-com is the
worked example — two Murrieta rooftops in its feed, 1,941 live cars, and no
street address to say which rooftop is the store.

THIS IS DISCOVERY, NOT SCANNING
------------------------------
It lives in ``backend/discovery`` because establishing WHO a dealer is — name,
site, coordinates, postal address — is discovery's job. Scanning reads the
roster and must never reach for a map provider to do its work; a scan that
depends on Google or OSM being up, keyed and quota-free is a scan that breaks
for reasons that have nothing to do with the dealer's inventory. Run this when
the roster gains dealers, never from a scan path.

SOURCE AND WHY IT IS TRUSTWORTHY
--------------------------------
One source only: the dealer's own website, parsing schema.org JSON-LD
``AutoDealer`` / ``LocalBusiness`` / ``Organization`` ``address`` nodes, with a
microdata/RDFa fallback. Being first-party, the host check a third-party
directory would need is inherent: we asked THIS dealer's domain and it answered
about itself.

Plain HTTP by default. ``--browser`` fetches through headless Chromium instead,
which is the only way past the Cloudflare TLS-fingerprint 403 that refused 108
of 150 dealers on the HTTP-only pass — including every dealer whose rooftop the
attribution gate cannot identify, which is precisely the set that needs an
address. That browser belongs to THIS discovery tool; no scan path calls it, so
the nightly refresh stays browser-free.

No map provider is used, and OSM reverse geocoding was REMOVED after being
measured, not on principle alone. For bmwofmurrieta-com the roster coordinate
reverse-geocodes to "French Valley Parkway", postcode 92390; the store's real
address, stated on 148 rows of its own feed, is 41430 Auto Mall Pkwy, 92562 —
wrong street and wrong postcode, and the city check cannot catch it because the
city is right either way. The roster geocodes were verified by website-host
match, which establishes which BUSINESS a point is, not that the point is
street-accurate; at zoom 18 the lookup snaps to the nearest road. A name-only
directory lookup is worse still — that is what once put 15 of 135 dealers in the
wrong city, one 2,497 miles off.

REFUSAL BEATS A WRONG ANSWER
----------------------------
A blank street_address means "unidentified", which is safe: the gate refuses to
write but never un-lists. A WRONG street_address is worse than blank — it makes
the gate confidently attribute cars to the wrong rooftop. So this script writes
only when the answer is unambiguous, and refuses when:

  * the page yields no postal address at all;
  * it yields several DIFFERENT street addresses (a group site listing every
    rooftop) and more than one is consistent with the roster city/state;
  * the roster names a city/state and the address contradicts it.

Every refusal is reported with its reason. Nothing is overwritten: a dealer that
already has a street_address is skipped.

Usage:
    .venv/bin/python -m backend.discovery.address_enrich [--dry-run]
                                                         [--limit N]
                                                         [--dealer-id ID]
Idempotent and safe to re-run; re-running only revisits dealers still missing an
address.
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from urllib.parse import urlparse

sys.path.insert(0, ".")

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("backfill_addresses")

# Polite pacing: dealer sites are small shops behind CDNs and several sit behind
# Cloudflare. Low concurrency plus a per-request delay keeps this to a crawl a
# site would not notice, and this only ever runs once per missing dealer.
_CONCURRENCY = 4
_DELAY_S = 0.4
_TIMEOUT_S = 20

_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)

_ZIP_RE = re.compile(r"\b(\d{5})(?:-\d{4})?\b")
_JSONLD_RE = re.compile(
    r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
    re.I | re.S,
)


def _norm(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())


def _host(url: str) -> str:
    raw = (url or "").strip()
    if not raw:
        return ""
    if "//" not in raw:
        raw = "https://" + raw.lstrip("/")
    host = (urlparse(raw).netloc or "").lower()
    return host[4:] if host.startswith("www.") else host


def _walk(node: Any):
    """Yield every dict in a JSON-LD tree (graphs, arrays and nesting included)."""
    if isinstance(node, dict):
        yield node
        for v in node.values():
            yield from _walk(v)
    elif isinstance(node, list):
        for v in node:
            yield from _walk(v)


def _addresses_from_jsonld(html: str) -> list[dict[str, str]]:
    """Every distinct PostalAddress in the page's JSON-LD blocks."""
    out: list[dict[str, str]] = []
    for block in _JSONLD_RE.findall(html or ""):
        text = block.strip()
        if not text:
            continue
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            # Some sites emit trailing commas or concatenated objects; salvage
            # the first parseable object rather than dropping the whole page.
            try:
                data = json.loads(re.sub(r",\s*([}\]])", r"\1", text))
            except json.JSONDecodeError:
                continue
        for node in _walk(data):
            addr = node.get("address") if isinstance(node, dict) else None
            for cand in (addr if isinstance(addr, list) else [addr]):
                if not isinstance(cand, dict):
                    continue
                street = str(cand.get("streetAddress") or "").strip()
                if not street:
                    continue
                out.append({
                    "street": " ".join(street.split()),
                    "city": str(cand.get("addressLocality") or "").strip(),
                    "state": str(cand.get("addressRegion") or "").strip(),
                    "zip": str(cand.get("postalCode") or "").strip()[:10],
                })
    return out


def _addresses_from_microdata(html: str) -> list[dict[str, str]]:
    """Fallback for sites that mark up an address without JSON-LD."""
    out: list[dict[str, str]] = []
    for m in re.finditer(
        r'itemprop=["\']streetAddress["\'][^>]*>([^<]{4,120})<', html or "", re.I
    ):
        street = " ".join(m.group(1).split())
        window = html[m.end(): m.end() + 400]
        city = re.search(r'itemprop=["\']addressLocality["\'][^>]*>([^<]{2,60})<', window, re.I)
        state = re.search(r'itemprop=["\']addressRegion["\'][^>]*>([^<]{2,30})<', window, re.I)
        postal = re.search(r'itemprop=["\']postalCode["\'][^>]*>([^<]{3,12})<', window, re.I)
        out.append({
            "street": street,
            "city": (city.group(1).strip() if city else ""),
            "state": (state.group(1).strip() if state else ""),
            "zip": (postal.group(1).strip() if postal else ""),
        })
    return out


# Set by main() when --browser is passed. Off by default: plain HTTP resolves
# most dealers and costs nothing.
_USE_BROWSER = False


def _fetch_via_browser(url: str) -> str:
    """Fetch through headless Chromium, for hosts that refuse bare HTTP.

    Cloudflare 403s plain ``requests`` on Dealer Inspire hosts by TLS
    fingerprint, which no header change fixes — 108 of 150 dealers refused this
    way on the HTTP-only pass, including every dealer whose rooftop the
    attribution gate cannot identify. A real browser presents a real TLS
    fingerprint and gets through; the recipe-synthesis pass demonstrated this on
    the same hosts.

    This is a discovery-time tool, not a scan-time one. Nothing on the nightly
    path calls it, so the browser-free guarantee of the scan is untouched.
    """
    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        try:
            page = browser.new_page(user_agent=_UA)
            page.goto(url, timeout=_TIMEOUT_S * 1000, wait_until="domcontentloaded")
            # Some sites inject the LocalBusiness JSON-LD after first paint.
            page.wait_for_timeout(1500)
            return page.content() or ""
        finally:
            browser.close()


def _fetch(url: str) -> str:
    if _USE_BROWSER:
        return _fetch_via_browser(url)

    import requests

    resp = requests.get(
        url, timeout=_TIMEOUT_S, headers={"User-Agent": _UA, "Accept": "text/html"},
        allow_redirects=True,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"HTTP {resp.status_code}")
    return resp.text or ""


def resolve_address(dealer: dict[str, Any]) -> dict[str, Any]:
    """``{ok, street, zip, source, reason}`` for one dealer. Never raises."""
    site = (dealer.get("dealer_website_url") or dealer.get("website_url") or "").strip()
    result: dict[str, Any] = {
        "id": dealer["id"], "name": dealer.get("name") or "", "site": site,
        "ok": False, "street": "", "zip": "", "source": "", "reason": "",
    }
    if not _host(site):
        result["reason"] = "no website_url in roster"
        return result

    base = site if site.startswith("http") else "https://" + site.lstrip("/")
    html = ""
    for path in ("", "/contact-us", "/contact", "/hours-directions"):
        try:
            html = _fetch(base.rstrip("/") + path)
        except Exception as exc:
            result["reason"] = f"fetch failed: {str(exc)[:60]}"
            time.sleep(_DELAY_S)
            continue
        found = _addresses_from_jsonld(html)
        source = "jsonld"
        if not found:
            found = _addresses_from_microdata(html)
            source = "microdata"
        time.sleep(_DELAY_S)
        if found:
            break
    else:
        found, source = [], ""

    if not html:
        return result
    if not found:
        result["reason"] = result["reason"] or "no postal address on page"
        return result

    roster_city, roster_state = _norm(dealer.get("city")), _norm(dealer.get("state"))

    # Collapse duplicates, then keep only addresses consistent with the roster
    # city/state. A group site lists every rooftop it owns; the roster locality
    # is what says which of them is THIS store.
    uniq: dict[str, dict[str, str]] = {}
    for a in found:
        uniq.setdefault(_norm(a["street"]), a)
    candidates = list(uniq.values())

    if roster_city:
        consistent = [
            a for a in candidates
            if _norm(a["city"]) == roster_city
            and (not roster_state or not _norm(a["state"]) or _norm(a["state"]) == roster_state)
        ]
        if consistent:
            candidates = consistent
        elif len(candidates) > 1:
            result["reason"] = (
                f"{len(candidates)} addresses, none in roster city "
                f"{dealer.get('city')!r} — refusing"
            )
            return result
        else:
            only = candidates[0]
            if _norm(only["city"]) and _norm(only["city"]) != roster_city:
                result["reason"] = (
                    f"site says {only['city']!r}, roster says {dealer.get('city')!r} — refusing"
                )
                return result

    if len(candidates) > 1:
        result["reason"] = f"{len(candidates)} distinct addresses in roster city — ambiguous, refusing"
        return result

    pick = candidates[0]
    postal = ""
    zip_hit = _ZIP_RE.search(pick.get("zip") or "")
    if zip_hit:
        postal = zip_hit.group(1)
    result.update(ok=True, street=pick["street"], zip=postal, source=source)
    return result


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="resolve and report, write nothing")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--dealer-id", default="", help="cars.dealer_id slug, for a single store")
    ap.add_argument("--browser", action="store_true",
                    help="fetch via headless Chromium — needed for Cloudflare-walled hosts")
    args = ap.parse_args()

    global _USE_BROWSER
    _USE_BROWSER = bool(args.browser)

    import psycopg

    url = re.search(
        r"^INVENTORY_DATABASE_URL=(.+)$", open(".env").read(), re.M
    ).group(1).strip()
    conn = psycopg.connect(url)
    cur = conn.cursor()

    where = ["COALESCE(d.street_address,'') = ''"]
    params: list[Any] = []
    if args.dealer_id:
        ids = [x.strip() for x in args.dealer_id.split(",") if x.strip()]
        where.append(
            "d.id IN (SELECT dealership_registry_id FROM cars WHERE dealer_id = ANY(%s))"
        )
        params.append(ids)
    else:
        where.append(
            "d.id IN (SELECT DISTINCT dealership_registry_id FROM cars WHERE listing_active = 1)"
        )
    sql = f"""SELECT d.id, d.name, d.website_url, d.dealer_website_url, d.city, d.state,
                     d.latitude, d.longitude
              FROM dealerships d WHERE {' AND '.join(where)} ORDER BY d.id"""
    cur.execute(sql, params)
    cols = [c.name for c in cur.description]
    dealers = [dict(zip(cols, r)) for r in cur.fetchall()]
    if args.limit:
        dealers = dealers[: args.limit]

    cur.execute(
        """SELECT COUNT(*) FILTER (WHERE COALESCE(street_address,'') <> ''), COUNT(*)
           FROM dealerships
           WHERE id IN (SELECT DISTINCT dealership_registry_id FROM cars WHERE listing_active = 1)"""
    )
    have, total = cur.fetchone()
    log.info("coverage BEFORE: %d/%d dealers have a street_address", have, total)
    log.info("resolving %d dealer(s) over HTTP (no browser)...\n", len(dealers))

    with ThreadPoolExecutor(max_workers=_CONCURRENCY) as pool:
        results = list(pool.map(resolve_address, dealers))

    wrote = 0
    by_source: dict[str, int] = {}
    refused: list[dict[str, Any]] = []
    for r in results:
        if not r["ok"]:
            refused.append(r)
            continue
        by_source[r["source"]] = by_source.get(r["source"], 0) + 1
        log.info("  OK   %-38s %s%s", (r["name"] or "")[:38],
                 r["street"], f"  {r['zip']}" if r["zip"] else "")
        if not args.dry_run:
            cur.execute(
                """UPDATE dealerships
                   SET street_address = %s,
                       zip_code = COALESCE(NULLIF(zip_code,''), NULLIF(%s,''))
                   WHERE id = %s AND COALESCE(street_address,'') = ''""",
                (r["street"], r["zip"], r["id"]),
            )
            wrote += cur.rowcount
    if not args.dry_run:
        conn.commit()

    log.info("\n--- REFUSED (%d) — left blank on purpose ---", len(refused))
    for r in refused:
        log.info("  --   %-38s %s", (r["name"] or "")[:38], r["reason"])

    cur.execute(
        """SELECT COUNT(*) FILTER (WHERE COALESCE(street_address,'') <> ''), COUNT(*)
           FROM dealerships
           WHERE id IN (SELECT DISTINCT dealership_registry_id FROM cars WHERE listing_active = 1)"""
    )
    have2, total2 = cur.fetchone()
    log.info(
        "\nresolved %d, refused %d, rows written %d%s",
        len(results) - len(refused), len(refused), wrote,
        " (DRY RUN — nothing written)" if args.dry_run else "",
    )
    log.info("by source: %s", by_source or "{}")
    log.info("coverage AFTER: %d/%d dealers have a street_address", have2, total2)
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
