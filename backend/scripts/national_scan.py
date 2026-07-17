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

import json
import os
import sys
from collections import Counter
from dataclasses import asdict, is_dataclass
from pathlib import Path

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
                         "city": d.get("city"), "state": d.get("state"), "seed_city": city}
            n += 1
        print(f"[discover] {city:<20} ({zc}): +{n} dealers (total {len(seen)})", flush=True)
    return list(seen.values())


def classify_all(dealers: list[dict]) -> list[dict]:
    from backend.scanner.platform_registry import classify_dealer

    for i, d in enumerate(dealers, 1):
        try:
            res = classify_dealer(d["url"], do_http=False)  # DNS-only: fast, works behind Cloudflare
        except Exception as exc:
            res = {"platform": None, "strategy": "error", "source": "error", "error": str(exc)}
        d["platform"] = res.get("platform")
        d["strategy"] = res.get("strategy")
        d["synthesizable"] = res.get("synthesizable")
        d["classify_source"] = res.get("source")
        if i % 25 == 0:
            print(f"[classify] {i}/{len(dealers)}", flush=True)
    return dealers


def main() -> None:
    print("=== PHASE 1: discovery ===", flush=True)
    dealers = discover()
    print(f"\nTotal unique dealers across {len(CITIES)} cities: {len(dealers)}\n", flush=True)

    print("=== PHASE 2: platform classification (DNS-first, browser-free) ===", flush=True)
    dealers = classify_all(dealers)

    out = _ROOT / "workspace" / "national_dealers.json"
    json.dump(dealers, open(out, "w"), indent=1)

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
