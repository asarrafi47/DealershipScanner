#!/usr/bin/env python3
"""
Of the dealers a manifest targeted, how many actually produced inventory?

Counting by hostname overstates the gap: a manifest lists lagunahyundai.com but
the scan stored the cars under lagunaniguelhyundai.com, and normreeves.com's
inventory lives under portcharlottehonda.com. Both share a dealer_id with the
row that holds the cars, so coverage is resolved by dealer_id and the host is
only the starting point.

Dealers carrying a ``skip_reason`` hint (OEM brand sites, CarMax, RV lots) are
counted as correctly skipped, not as misses.

Usage:
  python -m backend.scripts.audit_scan_coverage workspace/manifest_92694_100mi.json
  python -m backend.scripts.audit_scan_coverage --write-gap-manifest out.json m1.json m2.json
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_REPO_ROOT))

from backend.db.dealer_geo import normalize_dealer_host
from backend.db.inventory_db import db_conn


def manifest_entries(path: str) -> list[dict]:
    data = json.loads(Path(path).read_text())
    return data if isinstance(data, list) else data.get("dealers", [])


def entry_host(entry: dict) -> str:
    return normalize_dealer_host(
        entry.get("url") or entry.get("website") or entry.get("dealer_url") or ""
    )


def audit(paths: list[str], gap_manifest: str | None = None) -> dict:
    with db_conn() as conn:
        active_by_id: dict[str, int] = defaultdict(int)
        for dealer_id, n in conn.execute(
            "SELECT dealer_id, COUNT(*) FROM cars "
            "WHERE COALESCE(listing_active,1)=1 GROUP BY dealer_id"
        ).fetchall():
            active_by_id[str(dealer_id)] += int(n)
        active_hosts = {
            normalize_dealer_host(str(u))
            for u, in conn.execute(
                "SELECT DISTINCT dealer_url FROM cars WHERE COALESCE(listing_active,1)=1"
            ).fetchall()
        }
        hints = {
            str(d): str(s or "")
            for d, s in conn.execute(
                "SELECT dealer_id, scan_hints FROM dealer_recipes"
            ).fetchall()
        }

    gaps: list[dict] = []
    totals = defaultdict(int)
    for path in paths:
        counts = defaultdict(int)
        for entry in manifest_entries(path):
            host = entry_host(entry)
            if not host:
                continue
            counts["targeted"] += 1
            dealer_id = entry.get("dealer_id") or host.replace(".", "-")
            hint = hints.get(dealer_id, "")
            if host in active_hosts:
                counts["covered"] += 1
            elif active_by_id.get(dealer_id):
                counts["covered_via_alias"] += 1
            elif '"skip_reason"' in hint:
                counts["correctly_skipped"] += 1
            else:
                counts["gap"] += 1
                gaps.append(entry)
                if "proxy" in hint:
                    counts["gap_needs_proxy"] += 1
        print(f"\n{Path(path).name}: {counts['targeted']} dealers targeted")
        print(f"   covered                {counts['covered']}")
        print(f"   covered via alias url  {counts['covered_via_alias']}")
        print(f"   correctly skipped      {counts['correctly_skipped']}")
        print(f"   GAP                    {counts['gap']}"
              f" ({100 * counts['gap'] / max(1, counts['targeted']):.0f}%)"
              f"  of which proxy-walled: {counts['gap_needs_proxy']}")
        for k, v in counts.items():
            totals[k] += v

    if gap_manifest:
        Path(gap_manifest).write_text(json.dumps(gaps, indent=1))
        print(f"\nwrote {len(gaps)} gap dealers -> {gap_manifest}")
    return dict(totals)


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("manifests", nargs="+")
    ap.add_argument("--write-gap-manifest", metavar="PATH", default=None,
                    help="write the uncovered dealers out as a scannable manifest")
    args = ap.parse_args(argv)
    audit(args.manifests, args.write_gap_manifest)


if __name__ == "__main__":
    main()
