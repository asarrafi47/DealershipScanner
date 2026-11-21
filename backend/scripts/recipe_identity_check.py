#!/usr/bin/env python3
"""Recipe identity check (N8.5): which dealers replay the SAME endpoint, and whose rows come
from a foreign site.

Two findings, both read-only:

  shared_endpoints   a recipe (method + URL + request body) stored for more than one dealer.
                     A shared CarsCommerce / Motive listing id that is a group feed is fine
                     and is the whole reason the rooftop gate exists; a byte copy of another
                     store's recipe is the mbbeverlyhills-com = mbontario-com bug (1,284
                     rows served from www.mbontario.com). The report lists every group so a
                     person can tell which it is; it never decides.
  foreign_vdp_hosts  active rows of a dealer whose detail-page host is neither the dealer's
                     own host nor a recorded alias (``attribution.place.host_aliases``).
                     The host is a pointer for a person to check, never ownership evidence:
                     group sites serve sister cars on their own domain.

Reads recipe files (``RECIPES_DIR/<dealer>.json``) and, with ``--cars``, SELECTs
``cars.source_url`` counts. Writes nothing, makes no request.

  PYTHONPATH=. python backend/scripts/recipe_identity_check.py [--recipes-dir DIR] [--cars] [--json OUT]
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlparse

from backend.attribution.place import host_aliases, host_key


def recipe_signature(recipe: dict[str, Any]) -> tuple[str, str, str]:
    """``(method, host+path+query, body digest)``: identical signatures are the same request."""
    p = urlparse(str(recipe.get("url") or ""))
    where = f"{(p.hostname or '').lower()}{p.path}" + (f"?{p.query}" if p.query else "")
    body = str(recipe.get("post_template") or "")
    digest = hashlib.sha1(body.encode("utf-8")).hexdigest()[:12] if body else ""
    return (str(recipe.get("method") or "GET").upper(), where, digest)


def load_recipe_sets(recipes_dir: Path) -> dict[str, list[dict[str, Any]]]:
    """``dealer_id -> recipe rows`` from the recipe files (stale rows skipped)."""
    out: dict[str, list[dict[str, Any]]] = {}
    for path in sorted(Path(recipes_dir).glob("*.json")):
        if path.name.startswith("_"):
            continue
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        rows = data.get("recipes") if isinstance(data, dict) else data
        live = [r for r in (rows or []) if isinstance(r, dict) and not r.get("stale") and r.get("url")]
        if live:
            out[path.stem] = live
    return out


def find_shared_endpoints(sets: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    """Every request (signature) stored for two or more dealers, largest group first."""
    by_sig: dict[tuple[str, str, str], set[str]] = defaultdict(set)
    for dealer_id, rows in sets.items():
        for r in rows:
            by_sig[recipe_signature(r)].add(dealer_id)
    found = [
        {"method": m, "endpoint": where, "body_digest": digest, "dealers": sorted(ids)}
        for (m, where, digest), ids in by_sig.items() if len(ids) > 1
    ]
    return sorted(found, key=lambda f: (-len(f["dealers"]), f["endpoint"], f["body_digest"]))


def foreign_vdp_hosts(
    rows: Iterable[tuple[str, str]], dealer_hosts: dict[str, str] | None = None,
) -> dict[str, dict[str, int]]:
    """``dealer_id -> {foreign host: row count}`` for ``(dealer_id, source_url)`` pairs.

    A host is own when it is the dealer's host (``dealer_hosts`` or, absent that, the dealer
    id read back as a host) or one of :func:`host_aliases`. Site-root URLs are skipped.
    """
    counts: dict[str, Counter] = defaultdict(Counter)
    for dealer_id, url in rows:
        url = str(url or "").strip()
        host = host_key(url)
        if not host or not urlparse(url if "//" in url else "https://" + url).path.strip("/"):
            continue
        own = (dealer_hosts or {}).get(dealer_id) or ""
        if host == own or host in host_aliases(dealer_id):
            continue
        if not own and host.replace(".", "-") == dealer_id:
            continue
        counts[dealer_id][host] += 1
    return {d: dict(c.most_common()) for d, c in sorted(counts.items())}


def _cars_rows() -> list[tuple[str, str]]:
    from backend.db.inventory_db import db_conn

    with db_conn() as conn:
        return [(r[0], r[1]) for r in conn.execute(
            "SELECT dealer_id, source_url FROM cars WHERE listing_active = 1 AND source_url IS NOT NULL"
        ).fetchall()]


def run(recipes_dir: Path, *, cars: bool = False, cars_rows: Iterable[tuple[str, str]] | None = None) -> dict[str, Any]:
    sets = load_recipe_sets(recipes_dir)
    report: dict[str, Any] = {"dealers": len(sets), "shared_endpoints": find_shared_endpoints(sets)}
    if cars_rows is not None or cars:
        report["foreign_vdp_hosts"] = foreign_vdp_hosts(cars_rows if cars_rows is not None else _cars_rows())
    return report


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--recipes-dir", default=None, help="recipe files (default: the scanner's RECIPES_DIR)")
    ap.add_argument("--cars", action="store_true", help="also SELECT active rows' source_url hosts")
    ap.add_argument("--json", default=None, help="write the report here as well")
    args = ap.parse_args(argv)
    if args.recipes_dir:
        rdir = Path(args.recipes_dir)
    else:
        from backend.scanner.recipes import RECIPES_DIR as rdir
    report = run(Path(rdir), cars=args.cars)
    print(f"{report['dealers']} dealers with live recipes; {len(report['shared_endpoints'])} shared endpoint(s)")
    for f in report["shared_endpoints"]:
        print(f"  {f['method']} {f['endpoint']} [{f['body_digest'] or 'no body'}]: {', '.join(f['dealers'])}")
    for dealer_id, hosts in (report.get("foreign_vdp_hosts") or {}).items():
        print(f"  {dealer_id}: " + ", ".join(f"{h} x{n}" for h, n in list(hosts.items())[:4]))
    if args.json:
        Path(args.json).write_text(json.dumps(report, indent=1, sort_keys=True), encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
