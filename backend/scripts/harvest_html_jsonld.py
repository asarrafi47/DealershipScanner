#!/usr/bin/env python3
"""
Fill inventory gaps for the dealer-group HTML platform (the McKenna Cars
pattern) — no browser, no recipe.

These dealers have NO inventory JSON API to replay: each vehicle's data is
embedded as schema.org JSON-LD in server-rendered HTML that fetches fine over
plain HTTP. For every ACTIVE incomplete car at a target dealer we fetch its
``source_url``, parse the JSON-LD via
``backend.scanner.html_jsonld_harvest.harvest_fields_from_html``, and fill ONLY
empty columns (never overwrite a stored value). Images — the biggest gap —
populate ``image_url`` (when NULL/empty/placeholder) and ``gallery`` (JSON text,
when empty). Patched rows are re-synced in the incomplete-listings index.

Usage (repo root)::

  python3 backend/scripts/harvest_html_jsonld.py --dry-run
  python3 backend/scripts/harvest_html_jsonld.py --dealer-id hondaofelcajon-com
  python3 backend/scripts/harvest_html_jsonld.py --dealer-id hondaofelcajon-com --dealer-id pacificvolkswagen-com
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
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
log = logging.getLogger("harvest_html_jsonld")

from backend.scanner.html_jsonld_harvest import harvest_fields_from_html

# Dealer-group HTML-platform dealers with no usable recipe (big image gaps).
_DEFAULT_DEALERS = (
    "hondaofelcajon-com",
    "pacificvolkswagen-com",
    "tonyhondakona-com",
    "economyautosuperstore-com",
    "economyhonda-com",
    "bigislandhyundai-com",
    "cannonchevroletofcleveland-com",
)

# cars columns we can fill (order matches the SELECT below).
_FILL = (
    "price",
    "exterior_color",
    "interior_color",
    "image_url",
    "gallery",
    "trim",
    "mileage",
    "engine_description",
    "drivetrain",
    "fuel_type",
    "body_style",
)

_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)
_HEADERS = {"User-Agent": _UA, "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"}


def _is_fillable(field: str, cur_val: Any) -> bool:
    """Empty columns are fillable. ``image_url`` is also fillable when it holds a
    placeholder / non-HTTP path (e.g. /static/placeholder.svg); ``gallery`` when
    it holds an empty JSON array."""
    if cur_val is None or str(cur_val).strip() == "":
        return True
    s = str(cur_val).strip()
    if field == "image_url" and not s.startswith("http"):
        return True
    if field == "gallery" and s in ("[]", "null", "{}"):
        return True
    return False


def _harvested_value(field: str, harvest: dict[str, Any]) -> Any:
    """Map a normalized harvest dict onto a cars column value."""
    imgs = harvest.get("images") or []
    if field == "image_url":
        return imgs[0] if imgs else None
    if field == "gallery":
        return json.dumps(imgs) if imgs else None
    return harvest.get(field)


def _fetch(url: str, timeout: float) -> tuple[str, str | None, str]:
    """Return (url, html_or_None, status_note). Marks Cloudflare/edge blocks."""
    import requests

    try:
        resp = requests.get(url, headers=_HEADERS, timeout=timeout)
    except Exception as exc:  # noqa: BLE001 — network errors are expected/logged
        return url, None, f"error:{type(exc).__name__}"
    ct = resp.headers.get("content-type") or ""
    if resp.status_code != 200 or "html" not in ct:
        note = f"http:{resp.status_code}"
        if "Just a moment" in resp.text[:2000] or "challenge-platform" in resp.text[:5000]:
            note = "cloudflare_challenge"
        return url, None, note
    if "Just a moment" in resp.text[:2000]:
        return url, None, "cloudflare_challenge"
    return url, resp.text, "ok"


def patch_dealer(
    dealer_id: str,
    *,
    dry_run: bool,
    limit: int,
    workers: int,
    delay: float,
    timeout: float,
) -> dict:
    from backend.db.incomplete_listings_db import sync_incomplete_listing_for_car_id
    from backend.db.inventory_db import db_conn

    # 1) Select this dealer's active incomplete cars + current column values.
    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            f"SELECT c.id, c.source_url, {', '.join('c.' + f for f in _FILL)} "
            "FROM incomplete_listings il JOIN cars c ON c.id = il.car_id "
            "WHERE c.dealer_id = ? AND COALESCE(c.listing_active, 1) = 1 "
            "AND c.source_url LIKE 'http%'",
            (dealer_id,),
        )
        rows = cur.fetchall()

    stats: dict[str, Any] = {
        "candidates": len(rows),
        "fetched": 0,
        "blocked": 0,
        "no_jsonld": 0,
        "rows_patched": 0,
        "images_filled": 0,
        "fields": {},
        "block_notes": {},
    }
    if not rows:
        return stats

    rows = rows[:limit]
    by_id = {r[0]: (r[1], list(r[2:])) for r in rows}

    # 2) Fetch VDPs concurrently (network-bound), with a small per-task delay.
    html_by_id: dict[int, str] = {}
    id_by_url = {}
    for cid, (url, _cols) in by_id.items():
        id_by_url.setdefault(url, cid)

    def _task(url: str) -> tuple[str, str | None, str]:
        if delay:
            time.sleep(delay)
        return _fetch(url, timeout)

    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futures = [pool.submit(_task, url) for url in id_by_url]
        for fut in as_completed(futures):
            url, html, note = fut.result()
            cid = id_by_url[url]
            if html is None:
                if note == "cloudflare_challenge" or note.startswith("http") or note.startswith("error"):
                    stats["blocked"] += 1
                stats["block_notes"][note] = stats["block_notes"].get(note, 0) + 1
                continue
            stats["fetched"] += 1
            html_by_id[cid] = html

    # 3) Parse + build per-row updates (fill only empty columns).
    patched_ids: list[int] = []
    updates_by_id: dict[int, dict[str, Any]] = {}
    for cid, html in html_by_id.items():
        harvest = harvest_fields_from_html(html)
        has_jsonld = harvest.get("vin") or harvest.get("images") or harvest.get("price")
        if not has_jsonld:
            stats["no_jsonld"] += 1
        _url, current = by_id[cid]
        updates = {}
        for field, cur_val in zip(_FILL, current):
            if not _is_fillable(field, cur_val):
                continue
            val = _harvested_value(field, harvest)
            if val is None or (isinstance(val, str) and not val.strip()):
                continue
            updates[field] = val
        if updates:
            updates_by_id[cid] = updates

    # 4) Apply.
    for cid, updates in updates_by_id.items():
        stats["rows_patched"] += 1
        if "image_url" in updates or "gallery" in updates:
            stats["images_filled"] += 1
        for f in updates:
            stats["fields"][f] = stats["fields"].get(f, 0) + 1

    if not dry_run and updates_by_id:
        with db_conn() as conn:
            cur = conn.cursor()
            for cid, updates in updates_by_id.items():
                set_sql = ", ".join(f"{f} = ?" for f in updates)
                cur.execute(
                    f"UPDATE cars SET {set_sql} WHERE id = ?",
                    (*updates.values(), cid),
                )
                patched_ids.append(cid)
            conn.commit()

        for cid in patched_ids:
            try:
                sync_incomplete_listing_for_car_id(cid)
            except Exception:
                log.exception("index sync failed for car %s", cid)

    return stats


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dealer-id", action="append", default=None, help="repeatable")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=1000, help="max cars fetched per dealer (runaway guard)")
    ap.add_argument("--workers", type=int, default=4, help="concurrent HTTP fetches")
    ap.add_argument("--delay", type=float, default=0.25, help="per-fetch delay (seconds)")
    ap.add_argument("--timeout", type=float, default=30.0)
    ns = ap.parse_args()

    dealer_ids = ns.dealer_id or list(_DEFAULT_DEALERS)
    log.info("HTML/JSON-LD dealers: %d %s", len(dealer_ids), dealer_ids)

    grand = {"candidates": 0, "fetched": 0, "blocked": 0, "rows_patched": 0, "images_filled": 0, "fields": {}}
    for did in dealer_ids:
        stats = patch_dealer(
            did,
            dry_run=ns.dry_run,
            limit=ns.limit,
            workers=ns.workers,
            delay=ns.delay,
            timeout=ns.timeout,
        )
        log.info("[%s] %s", did, json.dumps(stats))
        for k in ("candidates", "fetched", "blocked", "rows_patched", "images_filled"):
            grand[k] += stats[k]
        for f, n in stats["fields"].items():
            grand["fields"][f] = grand["fields"].get(f, 0) + n
    log.info("TOTAL%s: %s", " (dry-run)" if ns.dry_run else "", json.dumps(grand))


if __name__ == "__main__":
    main()
