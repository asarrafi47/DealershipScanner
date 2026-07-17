#!/usr/bin/env python3
"""
Recover vehicle images for the Team Velocity dealers — no browser.

Team Velocity / secureoffersites VDPs are a Vue SPA whose inventory JSON feeds
carry ``imageUrls: null``. But the VDP HTML is *server-rendered* for real
browsers: the full photo gallery is emitted into the page as a Vue custom-element
attribute::

    <oem-gallery-component :vin="'<vin>'" :imageid="'1'"
        :photoUrls="'https://assets.cai-media-management.com/.../a.jpg,https://.../b.jpg,...'">

The catch is a User-Agent gate: fetched with a bot/library UA (``python-urllib``)
the server omits the gallery entirely; fetched with a real browser UA the full
comma-separated ``:photoUrls`` list is present. So image recovery is fully
browser-free — plain HTTP with a browser UA, then a regex to pull the attribute.

This script, for every Team Velocity car that still has a placeholder / empty /
non-HTTP ``image_url``, fetches its VDP over the proxy-aware HTTP path, extracts
the gallery, and fills ``cars.image_url`` (first photo) + ``cars.gallery`` (JSON
list). It NEVER overwrites a real http image (mirrors ``_is_fillable`` in
harvest_carscommerce.py). Patched rows are re-synced in the incomplete-listings
index.

Usage (repo root)::

  python3 backend/scripts/recover_team_velocity_images.py --dry-run
  python3 backend/scripts/recover_team_velocity_images.py --limit 30
  python3 backend/scripts/recover_team_velocity_images.py --dealer-id markkia-com
  python3 backend/scripts/recover_team_velocity_images.py            # all 3 dealers
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parents[2]
os.chdir(_REPO_ROOT)
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from backend.utils.project_env import load_project_dotenv

load_project_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("recover_tv_images")

from backend.scanner.http_fetch import open_url

TEAM_VELOCITY_DEALERS = ("righttoyota-com", "markkia-com", "righthonda-com")

# A real browser UA is REQUIRED: with a library UA the server drops the gallery.
_BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)
_HEADERS = {
    "User-Agent": _BROWSER_UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# <oem-gallery-component ... :photoUrls="'url,url,...'">
_GALLERY_RE = re.compile(r"<oem-gallery-component[^>]*?:photoUrls=\"'([^\"]*?)'\"", re.S)


def _image_url_fillable(cur_val: Any) -> bool:
    """image_url is fillable when empty or holding a placeholder / non-HTTP path
    (e.g. /static/placeholder.svg)."""
    if cur_val is None or str(cur_val).strip() == "":
        return True
    return not str(cur_val).strip().startswith("http")


def _gallery_fillable(cur_val: Any) -> bool:
    """gallery (JSON-text list) is fillable when empty, unparseable, or when it
    contains NO real http image — e.g. the placeholder ``["/static/placeholder.svg"]``."""
    if cur_val is None or str(cur_val).strip() in ("", "[]", "null"):
        return True
    try:
        items = json.loads(cur_val)
    except (ValueError, TypeError):
        return True
    if not isinstance(items, list) or not items:
        return True
    return not any(isinstance(u, str) and u.strip().startswith("http") for u in items)


def extract_gallery(html: str) -> list[str]:
    """Pull the ordered, de-duplicated http photo list from the VDP HTML."""
    m = _GALLERY_RE.search(html)
    if not m:
        return []
    seen: set[str] = set()
    out: list[str] = []
    for raw in m.group(1).split(","):
        u = raw.strip()
        if u.startswith("http") and u not in seen:
            seen.add(u)
            out.append(u)
    return out


def fetch_gallery(url: str, *, timeout: float = 30.0) -> list[str]:
    req = urllib.request.Request(url, headers=_HEADERS)
    resp = open_url(req, timeout=timeout)
    charset = resp.headers.get_content_charset() or "utf-8"
    html = resp.read().decode(charset, errors="replace")
    return extract_gallery(html)


def _rows_needing_images(dealer_id: str, limit: int | None):
    from backend.db.inventory_db import db_conn

    sql = (
        "SELECT id, vin, source_url, image_url, gallery FROM cars "
        "WHERE dealer_id = ? "
        "AND ((image_url IS NULL OR image_url NOT LIKE 'http%') "
        "     OR (gallery IS NULL OR gallery NOT LIKE '%http%')) "
        "AND COALESCE(listing_active, 1) = 1 "
        "AND source_url IS NOT NULL AND source_url LIKE 'http%' "
        "ORDER BY id"
    )
    params: tuple = (dealer_id,)
    if limit:
        sql += " LIMIT ?"
        params = (dealer_id, limit)
    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        return cur.fetchall()


def recover_dealer(
    dealer_id: str, *, dry_run: bool, limit: int | None, workers: int
) -> dict:
    from backend.db.incomplete_listings_db import sync_incomplete_listing_for_car_id
    from backend.db.inventory_db import db_conn

    rows = _rows_needing_images(dealer_id, limit)
    stats = {
        "candidates": len(rows),
        "fetched": 0,
        "with_images": 0,
        "rows_patched": 0,
        "no_gallery": 0,
        "errors": 0,
        "total_photos": 0,
    }
    if not rows:
        return stats

    # Fetch galleries concurrently; keep DB writes single-threaded.
    results: dict[int, list[str]] = {}

    def _job(row):
        car_id, vin, source_url, image_url, gallery = row
        try:
            imgs = fetch_gallery(source_url)
            return car_id, image_url, gallery, imgs, None
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, OSError) as exc:
            return car_id, image_url, gallery, None, str(exc)

    patched_ids: list[int] = []
    with ThreadPoolExecutor(max_workers=workers) as pool, db_conn() as conn:
        cur = conn.cursor()
        futures = [pool.submit(_job, r) for r in rows]
        for i, fut in enumerate(as_completed(futures), 1):
            car_id, image_url, gallery, imgs, err = fut.result()
            if err is not None:
                stats["errors"] += 1
                log.debug("[%s] car %s fetch error: %s", dealer_id, car_id, err)
                continue
            stats["fetched"] += 1
            if not imgs:
                stats["no_gallery"] += 1
                continue
            stats["with_images"] += 1
            stats["total_photos"] += len(imgs)

            updates: dict[str, Any] = {}
            if _image_url_fillable(image_url):
                updates["image_url"] = imgs[0]
            if _gallery_fillable(gallery):
                updates["gallery"] = json.dumps(imgs)
            if not updates:
                continue
            stats["rows_patched"] += 1
            if dry_run:
                continue
            set_sql = ", ".join(f"{f} = ?" for f in updates)
            cur.execute(
                f"UPDATE cars SET {set_sql} WHERE id = ?", (*updates.values(), car_id)
            )
            patched_ids.append(car_id)
            if len(patched_ids) % 200 == 0:
                conn.commit()
                log.info("[%s] committed %d patched so far", dealer_id, len(patched_ids))
            if i % 250 == 0:
                log.info(
                    "[%s] progress %d/%d (patched=%d)",
                    dealer_id,
                    i,
                    len(rows),
                    stats["rows_patched"],
                )
        if not dry_run:
            conn.commit()

    for car_id in patched_ids:
        try:
            sync_incomplete_listing_for_car_id(car_id)
        except Exception:
            log.exception("index sync failed for car %s", car_id)
    return stats


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dealer-id", action="append", default=None,
                    help="restrict to one/more dealers (default: all 3 Team Velocity)")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=None,
                    help="max cars per dealer (for pilots)")
    ap.add_argument("--workers", type=int, default=8, help="concurrent VDP fetches")
    ns = ap.parse_args()

    dealer_ids = ns.dealer_id or list(TEAM_VELOCITY_DEALERS)
    log.info("Team Velocity image recovery: dealers=%s dry_run=%s limit=%s workers=%d",
             dealer_ids, ns.dry_run, ns.limit, ns.workers)

    grand = {k: 0 for k in
             ("candidates", "fetched", "with_images", "rows_patched",
              "no_gallery", "errors", "total_photos")}
    for did in dealer_ids:
        stats = recover_dealer(did, dry_run=ns.dry_run, limit=ns.limit, workers=ns.workers)
        log.info("[%s] %s", did, json.dumps(stats))
        for k, v in stats.items():
            grand[k] += v
    log.info("TOTAL%s: %s", " (dry-run)" if ns.dry_run else "", json.dumps(grand))


if __name__ == "__main__":
    main()
