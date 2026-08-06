"""
Diagnose every registered rooftop that yields no inventory, and record what to do next.

    .venv/bin/python -m backend.scripts.audit_unscannable_dealers            # probe + record
    .venv/bin/python -m backend.scripts.audit_unscannable_dealers --report   # read the table

593 rooftops are registered and 432 have live inventory. The remaining 161 are re-scanned
every sweep and fail the same way each time, because "silent" was never broken down into
causes. This walks that list, probes each site over plain HTTP (no browser -- see below),
and writes a diagnosis plus a remediation recipe into dealer_scan_status.

The remediation recipes, by class:

  dns_fail                domain does not resolve. Verify once by hand, then deactivate:
                          the rooftop has closed or been renamed. Keeps it out of every
                          future sweep instead of costing a timeout each time.

  redirect_offsite        the domain now lands on a different host, which is what an
                          acquisition looks like. Point the registry at the final host,
                          re-run capture there, and check the new host is not already a
                          rooftop we scan (else it becomes a duplicate).

  http_error              reachable but 4xx/5xx. Usually a dead path rather than a dead
                          dealer; re-check the registered URL against the site's own
                          inventory link.

  needs_browser_probe     403 even after every TLS-impersonation profile was tried, so
                          this is a hard block rather than the usual fingerprint rejection.
                          Only 1 of 182 rooftops lands here. Try headless Chromium.

  reachable_no_recipe     site loads and no capture has ever run. Run capture for it --
                          this is the cheapest class to fix and the most likely to pay off.

  reachable_empty_recipe  site loads, capture ran, no inventory endpoint was found. Either
                          the platform is not in the handler map or the endpoint is behind
                          an interaction. Identify the platform from platform_guess, then
                          add a handler or capture with the browser path.

  recipe_decayed          recipes that used to work now return nothing. The site changed
                          shape; re-capture rather than re-scan.

Probing is HTTP-only on purpose, and that turned out to be sufficient. A first pass
using a stock HTTP client put 112 of 182 rooftops in the 403 bucket, which reads like
being blocked; probing again with TLS impersonation dropped that to 1. So access was
never the real obstacle. 172 of the 182 answer fine over plain HTTP -- what they lack is
a captured inventory endpoint, which makes this a handler/capture problem rather than an
anti-bot one, and means it is fixable without a browser.
"""

from __future__ import annotations

import argparse
import logging
import re
import ssl
import sys
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_log = logging.getLogger("dealer_audit")

_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0 Safari/537.36"
)

# Substrings that identify the inventory platform from page markup. Enough to tell a
# handler-supported platform from an unrecognised one.
_PLATFORM_MARKERS = (
    ("dealerinspire", "dealer_inspire"),
    ("di-vehicle", "dealer_inspire"),
    ("dealer.com", "dealer_dot_com"),
    ("ddc-", "dealer_dot_com"),
    ("cars.com", "carscommerce"),
    ("dealeron", "dealeron"),
    ("cosmos", "cosmos"),
    ("typesense", "typesense"),
    ("teamvelocity", "team_velocity"),
    ("apollo", "team_velocity"),
    ("jazel", "jazel"),
    ("overfuel", "overfuel"),
    ("dealermasters", "dealermasters"),
    ("chapman", "chapman"),
    ("motive", "motive_ridemotive"),
    ("autowall", "autowall"),
    ("shopperexpress", "shopper_express"),
    ("sincro", "sincro"),
    ("fox dealer", "foxdealer"),
    ("foxdealer", "foxdealer"),
)


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


def _dealer_key(url: str) -> str:
    host = urlparse(url if "://" in url else f"https://{url}").netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    return host.replace(".", "-")


# Cloudflare in front of most dealer platforms rejects on TLS fingerprint, not on
# headers -- bare curl and curl with a complete browser header set both get 403 from the
# same host. curl_cffi reproduces a real browser's TLS/JA3 signature, which clears it.
#
# Rotating profiles matters: over 30 dealers from the 403 cohort, "chrome" alone cleared
# about half, while chrome + chrome124 + safari17_0 cleared 29 of 30. Different edges run
# different rule sets, so one signature is not enough.
_IMPERSONATE_PROFILES = ("chrome", "chrome124", "safari17_0")


def _probe_impersonated(url: str) -> dict[str, Any] | None:
    """Try each browser signature; first 200 wins. None if curl_cffi is unavailable."""
    try:
        from curl_cffi import requests as cffi_requests
    except ImportError:
        return None

    last: dict[str, Any] | None = None
    for profile in _IMPERSONATE_PROFILES:
        try:
            resp = cffi_requests.get(url, impersonate=profile, timeout=25)
        except Exception as exc:  # curl_cffi raises its own error hierarchy
            last = {"status": None, "final_url": None, "platform": None, "error": str(exc)[:120]}
            continue
        body = (resp.text or "")[:300_000].lower()
        platform = next((p for m, p in _PLATFORM_MARKERS if m in body), None)
        result = {
            "status": resp.status_code,
            "final_url": str(resp.url),
            "platform": platform,
            "error": None,
            "impersonate": profile,
        }
        if resp.status_code == 200:
            return result
        last = result
    return last


def _probe(url: str) -> dict[str, Any]:
    """Fetch a site's home page. Returns status, final URL, and a platform guess."""
    impersonated = _probe_impersonated(url)
    if impersonated and impersonated.get("status") == 200:
        return impersonated

    out: dict[str, Any] = {"status": None, "final_url": None, "platform": None, "error": None}
    ctx = ssl.create_default_context()
    # Dealer sites are frequently misconfigured; a chain error should be reported as
    # reachable-with-a-cert-problem, not confused with a dead domain.
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    try:
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": _UA,
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "en-US,en;q=0.9",
            },
        )
        with urllib.request.urlopen(req, timeout=20, context=ctx) as resp:
            out["status"] = resp.status
            out["final_url"] = resp.geturl()
            body = resp.read(300_000).decode("utf-8", "ignore").lower()
        for marker, platform in _PLATFORM_MARKERS:
            if marker in body:
                out["platform"] = platform
                break
    except urllib.error.HTTPError as exc:
        out["status"] = exc.code
        out["final_url"] = getattr(exc, "url", None)
    except urllib.error.URLError as exc:
        reason = str(getattr(exc, "reason", exc))
        out["error"] = "dns" if ("not known" in reason or "Name or service" in reason
                                 or "nodename nor servname" in reason) else reason[:120]
    except (OSError, ValueError) as exc:
        out["error"] = str(exc)[:120]
    return out


def _classify(row: dict[str, Any], probe: dict[str, Any]) -> tuple[str, str]:
    """(reason, remediation) for one dealer."""
    if probe.get("error") == "dns":
        return "dns_fail", "verify by hand, then set dealerships.is_active=0"
    if probe.get("error"):
        return "http_error", f"connection failed: {probe['error']}; re-check registered URL"

    status = probe.get("status")
    if status == 403:
        # Reached only after every TLS-impersonation profile also failed, so this is a
        # genuinely hard block rather than the ordinary fingerprint rejection.
        return "needs_browser_probe", "TLS impersonation did not clear it; try headless Chromium"
    if status and status >= 400:
        return "http_error", f"HTTP {status}; re-check the registered inventory URL"

    final = probe.get("final_url") or ""
    origin_host = urlparse(row["website_url"]).netloc.lower().removeprefix("www.")
    final_host = urlparse(final).netloc.lower().removeprefix("www.")
    if final_host and origin_host and final_host != origin_host:
        return (
            "redirect_offsite",
            f"now redirects to {final_host}; repoint the registry and re-capture there",
        )

    if row["recipe_count"] and not row["last_ok_at"]:
        return "reachable_empty_recipe", "capture found no endpoint; identify platform, add handler"
    if row["recipe_count"] and row["last_ok_at"]:
        return "recipe_decayed", "site changed shape; re-run capture for this dealer"
    if row["has_recipe_row"]:
        return "reachable_empty_recipe", "capture found no endpoint; identify platform, add handler"
    return "reachable_no_recipe", "run capture for this dealer -- cheapest class to fix"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--report", action="store_true", help="print the stored table, probe nothing")
    ap.add_argument("--workers", type=int, default=10)
    ap.add_argument("--limit", type=int)
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    import psycopg

    conn = psycopg.connect(_dsn(), connect_timeout=15)
    conn.autocommit = True
    cur = conn.cursor()

    if args.report:
        cur.execute(
            "SELECT reason, count(*), min(remediation) FROM dealer_scan_status GROUP BY 1 ORDER BY 2 DESC"
        )
        rows = cur.fetchall()
        if not rows:
            _log.info("nothing recorded yet; run without --report first")
            return 0
        _log.info("unscannable rooftops by cause:")
        for reason, n, remedy in rows:
            _log.info("  %4d  %-24s %s", n, reason, remedy)
        return 0

    cur.execute(
        """
        SELECT d.name,
               COALESCE(d.website_url, d.dealer_website_url) AS w,
               r.recipe_count, r.last_ok_at, (r.dealer_id IS NOT NULL) AS has_recipe_row
        FROM dealerships d
        LEFT JOIN dealer_recipes r
               ON r.dealer_id = replace(replace(replace(
                    COALESCE(d.website_url, d.dealer_website_url),
                    'https://www.',''), 'http://www.',''), '.','-')
        WHERE COALESCE(d.is_active,1)=1
          AND d.duplicate_of_id IS NULL
          AND COALESCE(d.website_url, d.dealer_website_url) IS NOT NULL
          AND NOT EXISTS (
                SELECT 1 FROM cars c
                WHERE c.listing_removed_at IS NULL
                  AND c.dealer_id = replace(replace(replace(
                        COALESCE(d.website_url, d.dealer_website_url),
                        'https://www.',''), 'http://www.',''), '.','-')
          )
        ORDER BY d.name
        """
    )
    dealers = [
        {
            "name": n,
            "website_url": w,
            "recipe_count": rc or 0,
            "last_ok_at": ok,
            "has_recipe_row": bool(hrr),
            "key": _dealer_key(w),
        }
        for n, w, rc, ok, hrr in cur.fetchall()
    ]
    if args.limit:
        dealers = dealers[: args.limit]
    _log.info("silent rooftops to diagnose: %d", len(dealers))

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        probes = list(pool.map(lambda d: _probe(d["website_url"]), dealers))

    counts: dict[str, int] = {}
    for row, probe in zip(dealers, probes):
        reason, remediation = _classify(row, probe)
        counts[reason] = counts.get(reason, 0) + 1
        cur.execute(
            """
            INSERT INTO dealer_scan_status (
                dealer_key, dealer_name, website_url, reason, remediation,
                http_status, final_url, platform_guess, recipe_count, last_ok_at, checked_at
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
            ON CONFLICT (dealer_key) DO UPDATE SET
                reason = EXCLUDED.reason,
                remediation = EXCLUDED.remediation,
                http_status = EXCLUDED.http_status,
                final_url = EXCLUDED.final_url,
                platform_guess = EXCLUDED.platform_guess,
                recipe_count = EXCLUDED.recipe_count,
                last_ok_at = EXCLUDED.last_ok_at,
                checked_at = NOW()
            """,
            (
                row["key"], row["name"], row["website_url"], reason, remediation,
                probe.get("status"), probe.get("final_url"), probe.get("platform"),
                row["recipe_count"], row["last_ok_at"],
            ),
        )

    _log.info("recorded %d rooftop(s) in dealer_scan_status:", len(dealers))
    for reason, n in sorted(counts.items(), key=lambda kv: -kv[1]):
        _log.info("  %4d  %s", n, reason)
    _log.info("re-read anytime with --report")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
