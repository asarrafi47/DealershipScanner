#!/usr/bin/env python3
"""Scrape + store dealer LEASE / finance / manager specials over plain HTTP.

Reads a scan manifest (default ``workspace/manifest_socal_50mi.json``; each entry
has ``dealer_id`` and ``url``) or explicit ``--dealer id=url`` pairs, fetches each
dealer's likely specials pages, extracts offer cards, stores them in
``dealer_specials``, and prints a coverage report (hit rate + a sample offer).

Additive research surface — does NOT touch inventory / search.

Examples
--------
    python3 scripts/scan_dealer_specials.py --limit 8
    python3 scripts/scan_dealer_specials.py --manifest workspace/manifest_socal_50mi.json
    python3 scripts/scan_dealer_specials.py --dealer tustintoyota-com=https://www.tustintoyota.com
    python3 scripts/scan_dealer_specials.py --no-store --limit 3   # dry run
"""
from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir)))

from backend.scanner.specials.scan import scan_dealer_specials  # noqa: E402


def _load_manifest(path: str) -> list[dict]:
    with open(path, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    out = []
    for d in data:
        did = d.get("dealer_id")
        url = d.get("url") or d.get("website_url")
        if did and url:
            out.append({"dealer_id": did, "url": url, "name": d.get("name", did)})
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--manifest", default="workspace/manifest_socal_50mi.json")
    ap.add_argument("--dealer", action="append", default=[], help="dealer_id=url (repeatable)")
    ap.add_argument("--limit", type=int, default=0, help="max dealers from manifest (0 = all)")
    ap.add_argument("--no-store", action="store_true", help="dry run: extract but do not write DB")
    ap.add_argument("--timeout", type=int, default=25)
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    dealers: list[dict] = []
    for pair in args.dealer:
        if "=" in pair:
            did, url = pair.split("=", 1)
            dealers.append({"dealer_id": did.strip(), "url": url.strip(), "name": did.strip()})
    if not dealers:
        dealers = _load_manifest(args.manifest)
    if args.limit:
        dealers = dealers[: args.limit]

    print(f"Scanning specials for {len(dealers)} dealer(s) "
          f"({'DRY RUN' if args.no_store else 'storing to dealer_specials'})\n")

    total = len(dealers)
    with_specials = 0
    total_offers = 0
    sample = None
    rows = []
    for d in dealers:
        res = scan_dealer_specials(
            d["dealer_id"], d["url"],
            store=not args.no_store, timeout=args.timeout,
        )
        n = len(res.offers)
        total_offers += n
        if res.had_specials:
            with_specials += 1
        rows.append((d["dealer_id"], n, res.stored, res.error, res.hit_urls))
        flag = "OK " if n else "-- "
        print(f"  {flag} {d['dealer_id']:<34} offers={n:<3} stored={res.stored:<3} "
              f"pages_ok={res.pages_ok}"
              + (f"  ERR {res.error}" if res.error else ""))
        if args.verbose and res.hit_urls:
            print(f"        hits: {', '.join(res.hit_urls)}")
        if sample is None:
            for off in res.offers:
                if off.get("type") == "lease" and off.get("payment"):
                    sample = (d["dealer_id"], off)
                    break

    rate = (with_specials / total * 100) if total else 0
    print("\n" + "=" * 62)
    print(f"COVERAGE: {with_specials}/{total} dealers had scrapeable specials "
          f"({rate:.0f}%)  |  {total_offers} offers total")
    if sample:
        did, off = sample
        print(f"\nSAMPLE LEASE OFFER  ({did})")
        print(f"  title:      {off.get('title')}")
        print(f"  vehicle:    {off.get('vehicle_year')} {off.get('vehicle_make')} "
              f"{off.get('vehicle_model')} {off.get('vehicle_trim') or ''}".rstrip())
        print(f"  payment:    ${off.get('payment')}/mo x {off.get('term_months')} mo")
        print(f"  due:        ${off.get('due_at_signing')}   msrp: ${off.get('msrp')}")
        print(f"  expires:    {off.get('expires')}")
        print(f"  source:     {off.get('source_url')}")
        fp = (off.get("fine_print") or "")[:600]
        print(f"  fine_print: {fp}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
