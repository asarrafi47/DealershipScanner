#!/usr/bin/env python3
"""
Classify dealers' site platforms using the platform-learning registry.

For each dealer this runs :func:`backend.scanner.platform_registry.classify_dealer`
— DNS CNAME first (works even behind Cloudflare), then proxy-aware HTTP HTML
markers — and prints a table plus a summary. Dealers on an unrecognized platform
are logged (with every gathered signal) to ``workspace/unclassified_platforms.json``
so the platform can be named later with a one-line registry entry.

This NEVER launches a browser (DNS + proxy-aware HTTP only).

Usage (repo root)::

  set -a; source .env >/dev/null 2>&1; set +a; PYTHONPATH=. \\
    python3 backend/scripts/classify_dealers.py --manifest workspace/manifest_phoenix_sample.json
  python3 backend/scripts/classify_dealers.py --url https://www.sandersonford.com
  python3 backend/scripts/classify_dealers.py --manifest ... --no-http   # DNS only
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
os.chdir(_REPO_ROOT)
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

try:
    from backend.utils.project_env import load_project_dotenv

    load_project_dotenv()
except Exception:
    pass

from backend.scanner.platform_registry import UNCLASSIFIED_LOG, classify_dealer, load_registry

_DEFAULT_MANIFEST = "workspace/manifest_phoenix_sample.json"


def _load_manifest(path: str) -> list[dict]:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    if isinstance(data, dict):
        data = data.get("dealers") or data.get("dealerships") or []
    return [d for d in data if isinstance(d, dict)]


def _browser_free(res: dict) -> str:
    """Human hint on whether this platform can be scanned without a browser."""
    strat = res.get("strategy")
    if strat == "synthesize":
        return "yes (API)"
    if strat == "html_harvest":
        return "yes* (HTML)" if not res.get("cloudflare") else "yes+proxy"
    if strat == "browser":
        return "no"
    return "?"


def _print_table(rows: list[dict]) -> None:
    cols = [("dealer", 26), ("platform", 16), ("strategy", 13), ("source", 8), ("browser-free", 12)]
    header = "  ".join(name.upper().ljust(w) for name, w in cols)
    print(header)
    print("-" * len(header))
    for r in rows:
        res = r["result"]
        cells = [
            str(r["name"])[:26].ljust(26),
            str(res.get("platform") or "UNKNOWN")[:16].ljust(16),
            str(res.get("strategy") or "-")[:13].ljust(13),
            str(res.get("source") or "-")[:8].ljust(8),
            _browser_free(res)[:12].ljust(12),
        ]
        print("  ".join(cells))


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--manifest", default=_DEFAULT_MANIFEST, help="dealer manifest JSON")
    ap.add_argument("--url", help="one-off: classify a single dealer URL")
    ap.add_argument("--no-http", action="store_true", help="DNS CNAME only (skip HTTP HTML fallback)")
    ap.add_argument("--no-log", action="store_true", help="do not append unknowns to the unclassified log")
    args = ap.parse_args(argv)

    reg = load_registry()
    if args.url:
        dealers = [{"name": args.url, "url": args.url}]
    else:
        dealers = _load_manifest(args.manifest)

    rows: list[dict] = []
    newly_logged = 0
    for d in dealers:
        url = d.get("url") or d.get("website") or ""
        name = d.get("name") or url
        if not url:
            rows.append({"name": name, "result": {"platform": None, "source": "-"}})
            continue
        res = classify_dealer(
            url, registry=reg, do_http=not args.no_http, log_unknown=not args.no_log
        )
        if res.get("logged_new"):
            newly_logged += 1
        rows.append({"name": name, "result": res})
        plat = res.get("platform") or "UNKNOWN"
        print(f"  {name}: {plat} via {res.get('source')} "
              f"[{res.get('strategy') or '-'}]  cf={res.get('cloudflare')}")

    print()
    _print_table(rows)

    classified = [r for r in rows if r["result"].get("platform")]
    by_dns = [r for r in classified if r["result"].get("source") == "dns"]
    browser_free = [r for r in classified if r["result"].get("strategy") in ("synthesize", "html_harvest")]
    print()
    print(f"SUMMARY: {len(classified)}/{len(rows)} classified "
          f"({len(by_dns)} via DNS), {len(browser_free)} browser-free-capable, "
          f"{len(rows) - len(classified)} unknown.")
    if newly_logged:
        print(f"  {newly_logged} new unknown dealer(s) written to {UNCLASSIFIED_LOG.relative_to(_REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
