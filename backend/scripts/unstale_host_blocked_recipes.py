"""Re-open recipes a datacenter scan host marked stale for a 401/403 its IP caused.

2026-09-29: the first Railway fleet runs (before SCANNER_EGRESS_TAG) got
Cloudflare 403s on 25+ dealers whose recipes replay fine from a home IP. The
replay marked those recipes stale in the shared ``dealer_recipes`` store, and
the pipeline counts only live recipes, so every scanner then skipped them.

Run this from a HOME IP (never with SCANNER_EGRESS_TAG set). For each dealer
whose every recipe is stale with ``stale_reason`` http_401 / http_403 (and a
``stale:`` / ``rejected:auth_needed`` / ``blocked:`` status), it replays the
recipes once through ``try_fetch_via_recipes``. That replay path already
un-stales a recipe that answers and resets ``recipe_status`` to ok; one that
still answers 401/403 from home stays stale (it really is dead). Nothing is
written to ``cars``.

    python -m backend.scripts.unstale_host_blocked_recipes            # list candidates
    python -m backend.scripts.unstale_host_blocked_recipes --apply    # replay + un-stale
    python -m backend.scripts.unstale_host_blocked_recipes --apply --dealers a-com,b-com
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from typing import Any

AUTH_REASONS = ("http_401", "http_403")


def candidates(only: list[str] | None = None) -> list[dict[str, Any]]:
    from backend.scripts.dealer_pipeline import _rows, get_conn as pipeline_conn

    conn = pipeline_conn()
    try:
        rows = _rows(conn, "SELECT dealer_id, recipes_json, scan_hints FROM dealer_recipes")
    finally:
        conn.close()
    out: list[dict[str, Any]] = []
    for r in rows:
        did = str(r.get("dealer_id") or "")
        if only and did not in only:
            continue
        try:
            recipes = json.loads(r.get("recipes_json") or "[]")
        except ValueError:
            continue
        if not recipes or any(not x.get("stale") for x in recipes):
            continue  # has a live recipe: not skipped by the pipeline
        if not all(str(x.get("stale_reason") or "") in AUTH_REASONS for x in recipes):
            continue  # some recipe is stale for a real reason (unreplayable body, ...)
        try:
            hints = json.loads(r.get("scan_hints") or "{}") or {}
        except ValueError:
            hints = {}
        out.append({"dealer_id": did, "recipes": len(recipes), "recipe_status": str(hints.get("recipe_status") or "")})
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--apply", action="store_true", help="replay and un-stale (default: list only)")
    ap.add_argument("--dealers", default="", help="comma list to restrict to")
    args = ap.parse_args(argv)
    if (os.environ.get("SCANNER_EGRESS_TAG") or "").strip():
        print("refusing: SCANNER_EGRESS_TAG is set; run from a home IP", file=sys.stderr)
        return 2
    only = [d.strip() for d in args.dealers.split(",") if d.strip()] or None
    cands = candidates(only)
    print(f"{len(cands)} dealer(s) with only auth-staled recipes")
    if not args.apply:
        for c in cands:
            print(f"  {c['dealer_id']:40s} recipes={c['recipes']} status={c['recipe_status']}")
        return 0

    from backend.scanner.recipes import load_recipes, try_fetch_via_recipes
    from backend.scripts.dealer_pipeline import dealers_from_db

    info = dealers_from_db([c["dealer_id"] for c in cands])
    fixed = still = 0
    for c in cands:
        did = c["dealer_id"]
        d = info.get(did)
        if not d:
            print(f"  {did:40s} skipped: no active cars / url")
            continue
        res = asyncio.run(try_fetch_via_recipes(did, d.get("provider") or "unknown", d["url"].rstrip("/"), d["name"]))
        live = sum(1 for r in load_recipes(did) if not r.stale)
        rows = res[1] if res else 0
        if live:
            fixed += 1
            print(f"  {did:40s} un-staled {live} recipe(s); page replay rows={rows}")
        else:
            still += 1
            print(f"  {did:40s} still 401/403 from this IP; left stale")
    print(f"done: {fixed} re-opened, {still} still blocked")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
