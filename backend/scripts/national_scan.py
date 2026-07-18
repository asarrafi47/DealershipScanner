#!/usr/bin/env python3
"""
National browser-free scan: discover dealers across the top-20 US cities via
Google Places, classify each dealer's platform (DNS-first, then HTTP), and
report synthesizability — how many are browser-free vs need a browser vs are a
brand-new platform we must learn.

Phase 1 here = discovery + classification (fast, browser-free). It writes the
full dealer roster to workspace/national_dealers.json and prints the platform
breakdown so we can see issues (unknown platforms) as they arise. Recipe
synthesis + inventory load are separate follow-up steps.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from dataclasses import asdict, is_dataclass
from pathlib import Path

# Pace every browser-free fetch (the HTTP classification fallback below shares
# recipe_synth's global clock) so a sweep of unknown dealers stays SEQUENTIAL
# with a >=4s gap. Our egress IP is rate-limited; bursts trip the Cloudflare
# "Just a moment" challenge, paced requests succeed. Set BEFORE any backend
# import so recipe_synth reads it at import time. Operator-overridable.
os.environ.setdefault("SCANNER_SYNTH_FETCH_DELAY", "4")

# Billing guardrail: cap total billable Google Places searchNearby calls for a
# nationwide run (~1 call per metro at our radius). 200 covers a large metro
# list; the run stops calling Places past this so it can't silently overspend.
# Operator-overridable; also set a per-day quota in the Google Cloud console.
os.environ.setdefault("GOOGLE_PLACES_MAX_CALLS", "200")

_ROOT = Path(__file__).resolve().parents[2]
os.chdir(_ROOT)
sys.path.insert(0, str(_ROOT))
from backend.utils.project_env import load_project_dotenv

load_project_dotenv()

# Top 20 US cities by population, one representative downtown ZIP each.
CITIES = [
    ("New York, NY", "10001"), ("Los Angeles, CA", "90012"), ("Chicago, IL", "60601"),
    ("Houston, TX", "77002"), ("Phoenix, AZ", "85004"), ("Philadelphia, PA", "19107"),
    ("San Antonio, TX", "78205"), ("San Diego, CA", "92101"), ("Dallas, TX", "75201"),
    ("Jacksonville, FL", "32202"), ("Austin, TX", "78701"), ("Fort Worth, TX", "76102"),
    ("San Jose, CA", "95113"), ("Columbus, OH", "43215"), ("Charlotte, NC", "28202"),
    ("Indianapolis, IN", "46204"), ("San Francisco, CA", "94103"), ("Seattle, WA", "98101"),
    ("Denver, CO", "80202"), ("Washington, DC", "20001"),
]
RADIUS_MI = 15.0


def _dealer_id(url: str) -> str:
    import re
    return re.sub(r"^https?://(www\.)?", "", (url or "")).split("/")[0].replace(".", "-")


def discover() -> list[dict]:
    from backend.discovery.pipeline import run_discovery

    seen: dict[str, dict] = {}
    for city, zc in CITIES:
        try:
            cands = run_discovery(zc, RADIUS_MI, within_seed_zip_only=False, fill_urls_via_ddg=True)
        except Exception as exc:
            print(f"[discover] {city} ({zc}) FAILED: {type(exc).__name__}: {exc}", flush=True)
            continue
        n = 0
        for c in cands:
            d = asdict(c) if is_dataclass(c) else dict(getattr(c, "__dict__", {}))
            url = d.get("website_url") or d.get("dealer_website_url")
            if not url:
                continue
            did = _dealer_id(url)
            if did in seen:
                continue
            seen[did] = {"dealer_id": did, "name": d.get("name"), "url": url,
                         "city": d.get("city"), "state": d.get("state"), "seed_city": city,
                         "oem_brand": d.get("oem_brand", ""),
                         "business_status": d.get("business_status", ""),
                         "google_primary_type": d.get("google_primary_type", ""),
                         "latitude": d.get("latitude"), "longitude": d.get("longitude"),
                         "zip_code": d.get("zip_code", ""), "phone": d.get("phone", ""),
                         "google_rating": d.get("google_rating"),
                         "google_review_count": d.get("google_review_count")}
            n += 1
        print(f"[discover] {city:<20} ({zc}): +{n} dealers (total {len(seen)})", flush=True)
    return list(seen.values())


def classify_all(dealers: list[dict]) -> list[dict]:
    from backend.scanner.platform_registry import classify_dealer

    for i, d in enumerate(dealers, 1):
        try:
            # DNS-first, then a single paced HTTP fingerprint fallback when DNS is
            # inconclusive (do_http defaults True). Cheap for the DNS-identifiable
            # majority; only the unknowns pay the paced HTTP cost, which catches
            # DNS-invisible known platforms and untemplated-but-JSON-LD dealers.
            res = classify_dealer(d["url"])
        except Exception as exc:
            res = {"platform": None, "strategy": "error", "source": "error", "error": str(exc)}
        d["platform"] = res.get("platform")
        d["strategy"] = res.get("strategy")
        d["synthesizable"] = res.get("synthesizable")
        d["classify_source"] = res.get("source")
        if i % 25 == 0:
            print(f"[classify] {i}/{len(dealers)}", flush=True)
    return dealers


def _roster_row_to_db(d: dict) -> dict:
    """Map an in-memory national-roster dealer dict onto the dealerships upsert schema."""
    url = d.get("url") or d.get("website_url") or d.get("dealer_website_url") or ""
    return {
        "name": d.get("name") or "",
        "city": d.get("city") or "",
        "state": d.get("state") or "",
        "zip_code": d.get("zip_code") or "",
        "latitude": d.get("latitude"),
        "longitude": d.get("longitude"),
        "website_url": url,
        "dealer_website_url": url,
        "oem_brand": d.get("oem_brand") or "",
        "business_status": d.get("business_status") or "",
        "google_primary_type": d.get("google_primary_type") or "",
        "phone": d.get("phone") or "",
        "platform": d.get("platform") or "",
        "strategy": d.get("strategy") or "",
        # Google reputation — collected at discovery, feeds dealer_score's
        # reputation term. Persisted here so a scan populates it.
        "google_rating": d.get("google_rating"),
        "google_review_count": d.get("google_review_count"),
        # Discovered via Google Places / DDG (web tier). No Google API call here —
        # we only persist already-collected roster data.
        "source_web": True,
    }


def persist_roster(dealers: list[dict], *, only_dealers: bool = True) -> int:
    """
    Upsert each roster dealer into the local Postgres ``dealerships`` table via
    ``upsert_discovery_row``. Does NOT hit Google Places — it persists data the
    discovery phase already collected. Returns the number of rows upserted.
    """
    from backend.db.dealerships_db import upsert_discovery_row

    persisted = 0
    for d in dealers:
        if only_dealers and not d.get("is_dealer", True):
            continue
        if not (d.get("url") or d.get("website_url") or d.get("dealer_website_url")):
            continue
        upsert_discovery_row(_roster_row_to_db(d))
        persisted += 1
    return persisted


def main() -> None:
    ap = argparse.ArgumentParser(description="National browser-free dealer scan (discovery + classification).")
    ap.add_argument(
        "--persist-db",
        dest="persist_db",
        action="store_true",
        default=True,
        help="Upsert discovered dealers into the local dealerships table (default: on). No Google calls.",
    )
    ap.add_argument(
        "--no-persist-db",
        dest="persist_db",
        action="store_false",
        help="Skip persisting the roster into the dealerships table.",
    )
    args = ap.parse_args()

    print("=== PHASE 1: discovery ===", flush=True)
    dealers = discover()
    print(f"\nTotal unique dealers across {len(CITIES)} cities: {len(dealers)}\n", flush=True)

    print("=== PHASE 2: platform classification (DNS-first, paced HTTP fallback) ===", flush=True)
    dealers = classify_all(dealers)

    out = _ROOT / "workspace" / "national_dealers.json"
    json.dump(dealers, open(out, "w"), indent=1)

    if args.persist_db:
        print("\n=== PHASE 3: persist roster -> dealerships table (no Google calls) ===", flush=True)
        n_persisted = persist_roster(dealers)
        print(f"persisted {n_persisted} dealers into dealerships table", flush=True)

    plat = Counter(d.get("platform") or "UNKNOWN" for d in dealers)
    strat = Counter(d.get("strategy") or "none" for d in dealers)
    synth = sum(1 for d in dealers if d.get("strategy") == "synthesize")
    print("\n=== RESULTS ===", flush=True)
    print(f"dealers: {len(dealers)}  |  browser-free synthesizable: {synth} ({round(100*synth/max(1,len(dealers)))}%)")
    print("\nby platform:")
    for p, n in plat.most_common():
        print(f"  {p:<22} {n}")
    print("\nby strategy:")
    for s, n in strat.most_common():
        print(f"  {s:<16} {n}")
    print(f"\nroster -> {out}")


if __name__ == "__main__":
    main()
