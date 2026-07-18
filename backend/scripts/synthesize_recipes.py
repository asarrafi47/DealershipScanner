#!/usr/bin/env python3
"""
HTTP-first recipe synthesis: give a dealer a replayable inventory-API recipe
WITHOUT ever launching a browser, for platforms we already understand.

The scanner learns a dealer's inventory API by watching Playwright network
traffic. But the browser is only needed once per *platform*, not per *dealer*:
once we know a platform's endpoint shape, any dealer on that platform can have
its recipe SYNTHESIZED over plain HTTP (see backend.scanner.recipe_synth) —
fetch the dealer's page, fingerprint the platform, extract the per-dealer params
(domain / account id / api key), and fill a platform template.

For each dealer this script:
  fetch HTML -> fingerprint platform -> synthesize recipe
    -> VALIDATE by replaying the synthesized recipe over HTTP and counting VINs
    -> SAVE the recipe only when validation yields >= --min-vins real VINs
       (and no healthy recipe already exists for that dealer).

It NEVER launches a browser and NEVER overwrites a healthy existing recipe.

Usage (repo root)::

  set -a; source .env >/dev/null 2>&1; set +a; PYTHONPATH=. \\
    python3 backend/scripts/synthesize_recipes.py --manifest workspace/manifest_phoenix_sample.json
  python3 backend/scripts/synthesize_recipes.py --dealer-id courtesychev-com --url https://www.courtesychev.com
  python3 backend/scripts/synthesize_recipes.py --manifest ... --dry-run
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import time
from dataclasses import asdict
from pathlib import Path

# Hard per-dealer wall-clock cap so one dead/slow-loris site can't stall the
# whole sweep (a socket that accepts but never responds hangs past urllib's
# per-op timeout). SIGALRM interrupts the blocked read -> _DealerTimeout ->
# caught by the loop -> next dealer. Main-thread only (this script is). Env
# override for large paced validations.
_PER_DEALER_TIMEOUT = int(os.environ.get("SYNTH_PER_DEALER_TIMEOUT", "180"))


class _DealerTimeout(Exception):
    pass


def _on_alarm(signum, frame):
    raise _DealerTimeout()

_REPO_ROOT = Path(__file__).resolve().parents[2]
os.chdir(_REPO_ROOT)
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from backend.utils.project_env import load_project_dotenv

load_project_dotenv()

from backend.scanner.platform_registry import classify_dealer
from backend.scanner.recipe_synth import (
    detect_html_harvest,
    fetch_dealer_html,
    fingerprint_platform,
    is_synthesizable,
    synthesize_recipes,
    validate_recipe,
)
from backend.scanner.recipes import EndpointRecipe, load_recipes, save_recipes

logging.basicConfig(level=logging.WARNING, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("synthesize_recipes")

# Inventory pages to try when the homepage doesn't fingerprint / synthesize.
_INVENTORY_PATHS = ("/used-inventory", "/inventory/used", "/used-vehicles", "/inventory")

_DEFAULT_MANIFEST = "workspace/manifest_phoenix_sample.json"


def _healthy_recipe_exists(dealer_id: str) -> bool:
    """True if the dealer already has a proven-working (non-stale, replayed) recipe."""
    return any((not r.stale) and r.last_ok_at > 0 for r in load_recipes(dealer_id))


def _upsert_recipe(dealer_id: str, recipe: EndpointRecipe) -> None:
    """Merge *recipe* into the dealer's recipe file by (method, host+path) key.

    Preserves other recipes for the dealer; only replaces the same endpoint.
    """
    existing = load_recipes(dealer_id)
    merged: dict = {r.key(): r for r in existing}
    merged[recipe.key()] = recipe
    save_recipes(dealer_id, list(merged.values()))


def _gather_html(dealer_url: str) -> tuple[str | None, str | None]:
    """Fetch homepage; if it doesn't fingerprint, try inventory paths.

    Returns ``(html, platform)`` for the first page that fingerprints to a
    synthesizable platform, else the homepage html (+ whatever it fingerprints
    to) so callers can still report *why* it wasn't synthesizable.
    """
    home = fetch_dealer_html(dealer_url)
    home_platform = fingerprint_platform(home, dealer_url) if home else None
    if home and is_synthesizable(home_platform):
        return home, home_platform
    origin = dealer_url.rstrip("/")
    for path in _INVENTORY_PATHS:
        html = fetch_dealer_html(origin + path)
        if not html:
            continue
        platform = fingerprint_platform(html, dealer_url)
        if is_synthesizable(platform):
            return html, platform
    return home, home_platform


def _try_html_harvest(row: dict, dealer_url: str, html: str, min_vins: int) -> bool:
    """Universal browser-free fallback: if the reachable site server-renders
    schema.org Vehicle JSON-LD, the generic harvester lists the lot over plain
    HTTP — no per-platform template needed. On success, marks *row* as
    browser-free (strategy ``html_harvest``) and returns True."""
    try:
        n, src = detect_html_harvest(dealer_url, html, min_vins=min_vins)
    except Exception:  # a bad page must not abort the dealer
        log.debug("html_harvest fallback failed for %s", dealer_url, exc_info=True)
        return False
    if n < min_vins:
        return False
    row["strategy"] = "html_harvest"
    row["synthesized"] = False
    row["saved"] = False
    row["vins"] = n
    where = " (SRP)" if src and src != dealer_url else ""
    row["note"] = f"browser-free (html_harvest{where}, {n} vehicles via JSON-LD)"
    return True


def _process_dealer(
    dealer_id: str, dealer_name: str, dealer_url: str, *, min_vins: int, dry_run: bool, force: bool
) -> dict:
    row = {
        "dealer": dealer_name or dealer_id,
        "platform": "-",
        "strategy": "",  # "synthesize" | "html_harvest" | "" (needs browser)
        "synthesized": False,
        "vins": 0,
        "saved": False,
        "note": "",
    }
    # DNS CNAME pre-identification: works even when the site is HTTP-walled
    # (Cloudflare doesn't hide DNS), and names html_harvest / browser platforms
    # that HTML fingerprinting alone can't. Cheap (DNS only, no HTTP here).
    dns = classify_dealer(dealer_url, do_http=False, log_unknown=False)

    html, platform = _gather_html(dealer_url)
    if not html:
        # HTTP blocked / JS-shell — but DNS may still tell us the platform.
        if dns.get("platform"):
            row["platform"] = dns["platform"]
            if dns.get("strategy") == "html_harvest":
                hint = " (+proxy: set SCANNER_HTTP_PROXY)" if dns.get("cloudflare") else ""
                row["note"] = f"use HTML harvester (harvest_html_jsonld){hint} [platform via DNS]"
            elif dns.get("synthesizable"):
                row["note"] = f"{dns['platform']} via DNS but HTTP walled — needs proxy to read params"
            else:
                row["note"] = f"needs browser ({dns['platform']} via DNS, no template)"
        else:
            classify_dealer(dealer_url)  # capture signals to the unclassified log
            row["note"] = "needs browser (fetch blocked / JS-shell, platform unknown)"
        return row
    platform = platform or fingerprint_platform(html, dealer_url)
    row["platform"] = platform or dns.get("platform") or "unknown"
    if not is_synthesizable(platform):
        # No API template. Before declaring "needs browser", try the UNIVERSAL
        # browser-free fallback: if the reachable site server-renders schema.org
        # Vehicle JSON-LD, the generic harvester lists the lot over plain HTTP.
        if _try_html_harvest(row, dealer_url, html, min_vins):
            return row
        # Route server-rendered-HTML platforms (e.g. Jazel/McKenna family) to the
        # HTML harvester instead of a bare "unknown"; log true unknowns.
        if dns.get("strategy") == "html_harvest":
            hint = " (+proxy: set SCANNER_HTTP_PROXY)" if dns.get("cloudflare") else ""
            row["note"] = f"use HTML harvester (harvest_html_jsonld){hint}"
        elif platform or dns.get("platform"):
            row["note"] = "needs browser (platform recognized, no template yet)"
        else:
            classify_dealer(dealer_url)  # log signals for later naming
            row["note"] = "needs browser (platform unknown — logged to registry)"
        return row

    recipes = synthesize_recipes(dealer_id, dealer_url, html, platform)
    if not recipes:
        if _try_html_harvest(row, dealer_url, html, min_vins):
            return row
        row["note"] = "needs browser (params not extractable from HTML)"
        return row
    row["synthesized"] = True
    row["strategy"] = "synthesize"

    # A platform may emit several recipes (e.g. Team Velocity used + new feeds);
    # validate each and keep the ones that yield real VINs. The reported VIN count
    # is the combined total across kept recipes (feeds are disjoint by condition).
    kept: list[EndpointRecipe] = []
    total_vins = 0
    for recipe in recipes:
        n = validate_recipe(recipe, dealer_url.rstrip("/"), dealer_id, dealer_name or dealer_id)
        if n <= 0:
            continue
        recipe.vehicle_rows = n
        if not recipe.total_count:
            recipe.total_count = n
        recipe.saved_at = time.time()
        recipe.last_ok_at = time.time()
        kept.append(recipe)
        total_vins += n
    row["vins"] = total_vins
    if total_vins < min_vins or not kept:
        if _try_html_harvest(row, dealer_url, html, min_vins):
            return row
        row["synthesized"] = True  # a recipe WAS built; it just didn't validate
        row["strategy"] = ""
        row["vins"] = total_vins
        row["note"] = f"synthesized but failed validation ({total_vins} < {min_vins} VINs)"
        return row

    if not force and _healthy_recipe_exists(dealer_id):
        row["note"] = "healthy recipe already exists — not overwritten"
        return row
    if dry_run:
        row["note"] = f"dry-run (would save {len(kept)} recipe(s))"
        return row
    for recipe in kept:
        _upsert_recipe(dealer_id, recipe)
    row["saved"] = True
    row["note"] = f"browser-free ({len(kept)} recipe(s))" if len(kept) > 1 else "browser-free"
    return row


def _load_manifest(path: str) -> list[dict]:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    if isinstance(data, dict):
        data = data.get("dealers") or data.get("dealerships") or []
    return [d for d in data if isinstance(d, dict)]


def _print_table(rows: list[dict]) -> None:
    cols = [
        ("dealer", 26),
        ("platform", 15),
        ("strategy", 13),
        ("vins", 5),
        ("saved", 6),
        ("note", 44),
    ]
    header = "  ".join(name.upper().ljust(w) for name, w in cols)
    print(header)
    print("-" * len(header))
    for r in rows:
        cells = [
            str(r["dealer"])[:26].ljust(26),
            str(r["platform"])[:15].ljust(15),
            str(r.get("strategy") or ("synthesize" if r["synthesized"] else "-"))[:13].ljust(13),
            str(r["vins"]).ljust(5),
            ("yes" if r["saved"] else "no").ljust(6),
            str(r["note"])[:44].ljust(44),
        ]
        print("  ".join(cells))


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--manifest", default=_DEFAULT_MANIFEST, help="dealer manifest JSON")
    ap.add_argument("--dealer-id", help="one-off: dealer id (with --url)")
    ap.add_argument("--url", help="one-off: dealer homepage URL")
    ap.add_argument("--name", help="one-off: dealer display name")
    ap.add_argument("--min-vins", type=int, default=5, help="min unique VINs to accept (default 5)")
    ap.add_argument("--dry-run", action="store_true", help="fingerprint/synthesize/validate but do not save")
    ap.add_argument("--force", action="store_true", help="save even if a healthy recipe already exists")
    args = ap.parse_args(argv)

    if args.url:
        dealers = [{
            "dealer_id": args.dealer_id or "",
            "name": args.name or args.dealer_id or args.url,
            "url": args.url,
        }]
    else:
        dealers = _load_manifest(args.manifest)

    try:
        signal.signal(signal.SIGALRM, _on_alarm)
    except (ValueError, AttributeError):
        pass  # not main thread / unsupported platform — per-dealer cap disabled

    rows: list[dict] = []
    for d in dealers:
        dealer_url = d.get("url") or d.get("website") or ""
        dealer_id = d.get("dealer_id") or ""
        dealer_name = d.get("name") or dealer_id
        if not dealer_url:
            rows.append({"dealer": dealer_name, "platform": "-", "synthesized": False,
                         "vins": 0, "saved": False, "note": "no url in manifest"})
            continue
        if not dealer_id:
            from urllib.parse import urlparse
            host = (urlparse(dealer_url).hostname or dealer_url).replace("www.", "")
            dealer_id = host.replace(".", "-")
        try:
            signal.alarm(_PER_DEALER_TIMEOUT)  # hard cap; 0 disables
            row = _process_dealer(
                dealer_id, dealer_name, dealer_url,
                min_vins=args.min_vins, dry_run=args.dry_run, force=args.force,
            )
        except _DealerTimeout:
            log.warning("dealer %s timed out after %ds — skipping", dealer_id, _PER_DEALER_TIMEOUT)
            row = {"dealer": dealer_name, "platform": "-", "synthesized": False,
                   "vins": 0, "saved": False, "note": f"timeout>{_PER_DEALER_TIMEOUT}s"}
        except Exception as e:  # never let one dealer abort the sweep
            log.exception("dealer %s failed", dealer_id)
            row = {"dealer": dealer_name, "platform": "-", "synthesized": False,
                   "vins": 0, "saved": False, "note": f"error: {str(e)[:40]}"}
        finally:
            signal.alarm(0)
        rows.append(row)
        print(f"  processed {dealer_name}: {row['platform']} / {row['vins']} VINs / {row['note']}")

    print()
    _print_table(rows)
    # Browser-free = a validated synthesized recipe OR the universal JSON-LD
    # harvest fallback, both reaching the VIN floor.
    browser_free = [r for r in rows
                    if r["vins"] >= args.min_vins and r.get("strategy") in ("synthesize", "html_harvest")]
    synth_free = [r for r in browser_free if r.get("strategy") == "synthesize"]
    harvest_free = [r for r in browser_free if r.get("strategy") == "html_harvest"]
    need_browser = [r for r in rows if r not in browser_free]
    print()
    print(f"SUMMARY: {len(browser_free)} browser-free "
          f"({len(synth_free)} synthesized-API, {len(harvest_free)} html_harvest JSON-LD), "
          f"{len(need_browser)} still need a browser  [of {len(rows)} dealers]")
    if args.dry_run:
        print("(dry-run: no recipes written)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
