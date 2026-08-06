"""
Fingerprint the rooftops nothing else could scan, and group them by platform.

    .venv/bin/python -m backend.scripts.discover_platforms

Synthesis handles any dealer on a platform we have modelled. The cascade
(backend/scanner/recipe_cascade.py) handles a dealer whose platform we have not modelled
but whose request shape matches one we have seen elsewhere. What is left over is the set
of dealers running something genuinely unfamiliar -- and for those, the useful question is
not "why did this dealer fail" but "how many dealers share this unknown platform", because
one handler written once unlocks all of them.

Audi Marin is the case that motivated this. Synthesis fingerprinted it as dealer_dot_com,
but its markup contains no dealer.com markers and every dealer.com endpoint 404s. It
actually runs Audi's own site platform -- oneaudi-falcon.prod.renderer.one.audi,
nar-dam.audi.com, /api/gfa/v1/* -- which nothing in the handler map knows. A wrong
fingerprint sent it down a road that could never have worked, and no amount of retrying
would have revealed that.

So this reports evidence rather than a verdict: the third-party hosts a site loads, the
API paths it references, and whether inventory is present in the HTML at all. Dealers are
then clustered by their strongest signal, and clusters are ranked by size. A cluster of
one is a bespoke site and probably not worth a handler; a cluster of eight is a platform.

It also separates out a category worth knowing about before any engineering: registry
entries that are not dealerships. Discovery has seeded this table with a restaurant
chain (North Italia), two manufacturer head offices and a government surplus store. Those
do not need a recipe, they need removing.
"""

from __future__ import annotations

import argparse
import collections
import json
import logging
import re
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_log = logging.getLogger("discover_platforms")

# Hosts that appear on every commercial site and identify nothing.
_BORING_HOSTS = re.compile(
    r"google|gstatic|facebook|doubleclick|youtube|cloudflare|jquery|bootstrap|fontawesome"
    r"|adobedtm|hotjar|bing|linkedin|twitter|tiktok|pinterest|cookielaw|onetrust|userway"
    r"|recaptcha|segment|newrelic|datadog|sentry|clarity|criteo|taboola",
    re.I,
)

# Evidence that the page itself carries vehicle rows -- if so, an HTML/JSON-LD harvest
# may work with no API at all, which is a much cheaper handler to write.
_INVENTORY_IN_HTML = ("\"vin\"", "'vin'", "vehicleidentificationnumber", "\"offers\"", "autodealer")


def _dsn() -> str:
    import os

    dsn = (os.environ.get("INVENTORY_DATABASE_URL") or os.environ.get("DATABASE_URL") or "").strip()
    if dsn:
        return dsn
    env = _REPO_ROOT / ".env"
    if env.exists():
        m = re.search(r"^INVENTORY_DATABASE_URL=(.+)$", env.read_text(), re.M)
        if m:
            return m.group(1).strip().strip("'\"")
    raise SystemExit("INVENTORY_DATABASE_URL is not set")


def fingerprint(url: str) -> dict[str, Any]:
    """Signals that identify a site's platform, or an error explaining the silence."""
    from backend.scanner.recipe_synth import fetch_dealer_html

    out: dict[str, Any] = {
        "url": url, "ok": False, "hosts": [], "api_paths": [],
        "inventory_in_html": False, "generator": None, "error": None,
    }
    try:
        html = fetch_dealer_html(url)
    except Exception as exc:
        out["error"] = f"{type(exc).__name__}: {str(exc)[:80]}"
        return out
    if not html:
        out["error"] = "fetch failed / challenge"
        return out

    out["ok"] = True
    low = html.lower()

    hosts = [h for h in set(re.findall(r'src=["\']https?://([a-z0-9.-]+)', low)) if not _BORING_HOSTS.search(h)]
    out["hosts"] = sorted(hosts)[:12]

    out["api_paths"] = sorted(set(
        re.findall(r'["\'](/(?:api|apis|graphql|inventory|vehicles|srp)[a-z0-9/_.-]{0,40})', low)
    ))[:10]

    out["inventory_in_html"] = any(m in low for m in _INVENTORY_IN_HTML)

    gen = re.search(r'<meta[^>]+name=["\']generator["\'][^>]+content=["\']([^"\']{2,60})', low)
    if gen:
        out["generator"] = gen.group(1)
    return out


def cluster_signal(fp: dict[str, Any]) -> str:
    """
    The single strongest platform signal for this site.

    Third-party hosts beat API paths: a site loading oneaudi-falcon.prod.renderer.one.audi
    is unambiguous, whereas /api/content is generic enough to appear on unrelated stacks.
    """
    if not fp["ok"]:
        return f"UNREACHABLE ({fp['error']})"
    if fp["generator"]:
        return f"generator:{fp['generator'][:40]}"
    for h in fp["hosts"]:
        # A vendor host is one that is not the dealer's own domain.
        if h.split(".")[-2:] != (fp["url"].split("//")[-1].split("/")[0].split(".")[-2:]):
            return f"host:{h}"
    if fp["api_paths"]:
        return f"api:{fp['api_paths'][0]}"
    return "no distinguishing signal"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--out", default="workspace/platform_discovery.json")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    import psycopg

    conn = psycopg.connect(_dsn(), connect_timeout=15)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(
        """
        SELECT s.dealer_key, s.dealer_name, s.website_url
        FROM dealer_scan_status s
        LEFT JOIN dealer_recipes r ON r.dealer_id = s.dealer_key
        WHERE COALESCE(r.recipe_count, 0) = 0
        ORDER BY s.dealer_key
        """
    )
    targets = cur.fetchall()
    _log.info("rooftops with no recipe: %d", len(targets))
    if not targets:
        return 0

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        fps = list(pool.map(lambda t: fingerprint(t[2]), targets))

    clusters: dict[str, list[str]] = collections.defaultdict(list)
    records = []
    for (key, name, url), fp in zip(targets, fps):
        sig = cluster_signal(fp)
        clusters[sig].append(name or key)
        records.append({"dealer_key": key, "name": name, "signal": sig, **fp})

    Path(args.out).write_text(json.dumps(records, indent=1))

    _log.info("platform clusters, largest first:")
    for sig, names in sorted(clusters.items(), key=lambda kv: -len(kv[1])):
        _log.info("  %2d  %s", len(names), sig[:76])
        for n in names[:4]:
            _log.info("        %s", (n or "")[:60])
        if len(names) > 4:
            _log.info("        ... and %d more", len(names) - 4)

    harvestable = [r for r in records if r.get("inventory_in_html")]
    _log.info("")
    _log.info("%d site(s) carry vehicle data in the HTML itself -- an HTML/JSON-LD", len(harvestable))
    _log.info("harvester would work on these without reverse-engineering any API:")
    for r in harvestable[:10]:
        _log.info("    %s", (r["name"] or r["dealer_key"])[:60])
    _log.info("")
    _log.info("full evidence written to %s", args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
