"""
Read every car's gallery photos and cache what the text in them says.

This is a post-scan enrichment pass, run deliberately -- not a nightly cron. It is
resumable: a car already recorded at the current IMAGE_TEXT_VERSION is skipped, so
killing this at any point and restarting costs only the batch in flight.

    .venv/bin/python -m backend.scripts.run_image_text_extraction --limit 50
    .venv/bin/python -m backend.scripts.run_image_text_extraction --dealer hileyvwhuntsville-com
    .venv/bin/python -m backend.scripts.run_image_text_extraction --all

Nothing here writes to `cars`. Results land in car_image_text as candidates; see
migrations/V006__car_image_text.sql for why adoption is a separate gated step.

Politeness: image CDNs are the same hosts the scanner fetches from, and sustained
parallel pulls have soft-blocked this project before (the Team Velocity photo recovery
episode). So fetches are paced and bounded, and a car whose images fail to download is
recorded as attempted rather than retried in a tight loop.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import re
import sys
import time
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from backend.vision.image_text import (  # noqa: E402
    IMAGE_TEXT_VERSION,
    ocr_available,
    ocr_provider,
    read_gallery,
    summarize_gallery,
)

_log = logging.getLogger("image_text")

_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0 Safari/537.36"
)

# Per-image ceiling. Galleries run to 40-50 images; the sticker and highlights slides
# are usually in the back half, so we do not truncate aggressively, but we do cap.
_MAX_IMAGES_PER_CAR = 60


def _dsn() -> str:
    dsn = (os.environ.get("INVENTORY_DATABASE_URL") or os.environ.get("DATABASE_URL") or "").strip()
    if dsn:
        return dsn
    # python-dotenv's find_dotenv() asserts under some interpreter entry points here,
    # so the file is parsed directly rather than loaded through it.
    env = _REPO_ROOT / ".env"
    if env.exists():
        m = re.search(r"^INVENTORY_DATABASE_URL=(.+)$", env.read_text(), re.M)
        if m:
            return m.group(1).strip().strip("'\"")
    raise SystemExit("INVENTORY_DATABASE_URL is not set")


def _make_fetcher(delay: float):
    import urllib.error
    import urllib.parse
    import urllib.request

    def fetch(url: str) -> bytes | None:
        # Galleries carry relative placeholders ("/static/placeholder.svg") alongside
        # real CDN URLs. Those are not fetchable and must not abort the run.
        if not (url or "").lower().startswith(("http://", "https://")):
            return None
        # Real galleries carry unescaped spaces ("/jellies/Chevrolet/Express Cargo Van/..").
        # urllib raises http.client.InvalidURL for those, which is an HTTPException and so
        # slips past OSError/ValueError -- one such URL aborted a whole fleet pass.
        try:
            safe_url = urllib.parse.quote(url, safe=":/?#[]@!$&'()*+,;=%~")
            req = urllib.request.Request(safe_url, headers={"User-Agent": _UA, "Accept": "image/*"})
            with urllib.request.urlopen(req, timeout=20) as resp:
                if resp.status != 200:
                    return None
                blob = resp.read(12 * 1024 * 1024)
        except Exception:
            # Deliberately broad: this runs across ~3M third-party image URLs, and no
            # single one may be allowed to end the pass. Losing an image is acceptable;
            # losing the run is not.
            return None
        finally:
            # Jittered so a gallery does not produce a metronomic request pattern.
            time.sleep(delay * (0.6 + random.random() * 0.8))
        return blob or None

    return fetch


def _select_cars(
    cur, *, limit: int | None, dealer: str | None, redo: bool, only_documents: bool = False
) -> list[tuple[int, str, str]]:
    where = ["c.listing_removed_at IS NULL", "c.gallery IS NOT NULL", "c.gallery NOT IN ('', '[]')"]
    params: list[Any] = []
    if dealer:
        where.append("c.dealer_id = %s")
        params.append(dealer)
    if only_documents:
        # Cheap pass first, expensive pass only where it found something. Requires a
        # prior run to have populated car_image_text.
        where.append(
            "EXISTS (SELECT 1 FROM car_image_text d WHERE d.car_id = c.id "
            "AND (d.has_sticker OR d.equipment_count > 0))"
        )
    if not redo:
        # Resumability: skip anything already recorded at the current version.
        where.append(
            "NOT EXISTS (SELECT 1 FROM car_image_text t "
            "WHERE t.car_id = c.id AND t.version >= %s)"
        )
        params.append(IMAGE_TEXT_VERSION)

    # Randomised rather than ordered by id: consecutive ids belong to the same dealer,
    # so with several workers an id-ordered sweep points every worker at one CDN at
    # once. Spreading the order spreads the load, which is what keeps a fleet-wide run
    # from looking like an attack to the image hosts.
    sql = (
        "SELECT c.id, COALESCE(c.vin,''), c.gallery FROM cars c "
        f"WHERE {' AND '.join(where)} ORDER BY random()"
    )
    if limit:
        sql += " LIMIT %s"
        params.append(limit)
    cur.execute(sql, params)
    return cur.fetchall()


def _merge_summary(prior: dict[str, Any] | None, new: dict[str, Any]) -> dict[str, Any]:
    """
    Union *new* findings into *prior*'s, rather than replacing prior's outright.

    A second pass (different engine, or the same engine re-sampling a large gallery)
    has different strengths and can legitimately see less than a prior pass did --
    overwriting the summary wholesale would silently drop a fact the prior pass found
    (a sticker MSRP, a package) even though the DENORMALIZED columns (has_sticker,
    equipment_count) were already merge-safe ("never regress"). That asymmetry is
    exactly how a car ended up with equipment_count=2 while its own summary showed
    zero equipment: the column remembered the old high-water mark, the JSON did not.
    Merging here means the column and the JSON that is supposed to back it can no
    longer disagree -- both are derived from the same unioned facts.
    """
    if not prior:
        return dict(new)

    def _dedupe_list(a: list, b: list) -> list:
        out: list = []
        seen_keys: set = set()
        for item in list(a or []) + list(b or []):
            key = json.dumps(item, sort_keys=True) if isinstance(item, dict) else item
            if key not in seen_keys:
                seen_keys.add(key)
                out.append(item)
        return out

    def _merge_priced_options(new_opts: list, old_opts: list) -> list:
        # New pass's price for a name wins (it saw the actual pixels this time);
        # old entries whose name isn't in the new pass are kept, not dropped.
        merged = {opt.get("name"): opt for opt in (old_opts or []) if isinstance(opt, dict)}
        for opt in new_opts or []:
            if isinstance(opt, dict):
                merged[opt.get("name")] = opt
        return list(merged.values())

    merged = dict(new)
    merged["equipment"] = _dedupe_list(new.get("equipment"), prior.get("equipment"))
    merged["packages"] = _dedupe_list(new.get("packages"), prior.get("packages"))
    merged["sticker_image_urls"] = _dedupe_list(new.get("sticker_image_urls"), prior.get("sticker_image_urls"))
    merged["dealer_domains"] = _dedupe_list(new.get("dealer_domains"), prior.get("dealer_domains"))
    merged["priced_options"] = _merge_priced_options(new.get("priced_options"), prior.get("priced_options"))
    merged["sticker_msrp"] = new.get("sticker_msrp") if new.get("sticker_msrp") is not None else prior.get("sticker_msrp")
    # images_read / images_seen / images_rejected are NOT merged: they describe what
    # THIS pass observed, not a cumulative fact -- carrying prior's forward would make
    # a fresh run look like it inherited stale rejects it never actually saw.
    return merged


def _record(cur, car_id: int, vin: str, seen: int, summary: dict[str, Any]) -> None:
    cur.execute("SELECT summary FROM car_image_text WHERE car_id = %s", (car_id,))
    row = cur.fetchone()
    prior_summary = row[0] if row and row[0] else None
    merged = _merge_summary(prior_summary, summary)

    cur.execute(
        """
        INSERT INTO car_image_text (
            car_id, vin, version, extracted_at, summary,
            images_seen, images_read, images_rejected_json,
            has_sticker, sticker_msrp, equipment_count
        ) VALUES (%s, %s, %s, NOW(), %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (car_id) DO UPDATE SET
            vin = EXCLUDED.vin,
            version = EXCLUDED.version,
            extracted_at = EXCLUDED.extracted_at,
            summary = EXCLUDED.summary,
            images_seen = EXCLUDED.images_seen,
            images_read = EXCLUDED.images_read,
            images_rejected_json = EXCLUDED.images_rejected_json,
            -- summary is now pre-merged in Python (see _merge_summary), so these three
            -- are plain overwrites derived from that SAME merged summary -- the column
            -- and the JSON backing it can no longer disagree.
            has_sticker = EXCLUDED.has_sticker,
            sticker_msrp = EXCLUDED.sticker_msrp,
            equipment_count = EXCLUDED.equipment_count
        """,
        (
            car_id,
            vin or None,
            IMAGE_TEXT_VERSION,
            json.dumps(merged),
            seen,
            int(summary.get("images_read") or 0),
            json.dumps(summary.get("images_rejected") or []),
            bool(merged.get("sticker_image_urls")),
            merged.get("sticker_msrp"),
            len(merged.get("equipment") or []),
        ),
    )


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--limit", type=int, default=25, help="cars to process (default 25)")
    ap.add_argument("--all", action="store_true", help="no limit")
    ap.add_argument("--dealer", help="restrict to one dealer_id")
    ap.add_argument("--redo", action="store_true", help="re-extract even if already at this version")
    ap.add_argument(
        "--only-documents", action="store_true",
        help=(
            "only cars a previous pass found a sticker or equipment on. This is how the "
            "VLM is meant to be used: qwen3-vl is ~10s per image against ~0.05s for Apple "
            "Vision, so a fleet-wide VLM sweep is 70+ days while re-reading just the "
            "documents a cheap pass already located is hours."
        ),
    )
    ap.add_argument("--delay", type=float, default=0.25, help="seconds between image fetches")
    ap.add_argument(
        "--workers", type=int, default=1,
        help="cars processed concurrently (fetch+OCR is I/O bound; 8-10 is sane on this box)",
    )
    ap.add_argument(
        "--max-images", type=int, default=_MAX_IMAGES_PER_CAR,
        help=(
            "images per car. Stickers and highlights slides usually sit in the back half "
            "of a gallery, so trimming this trades recall for a large amount of bandwidth."
        ),
    )
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    if not ocr_available():
        _log.error(
            "no OCR provider available (provider=%s). Build it with: "
            "swiftc -O tools/ocr/macocr.swift -o tools/ocr/bin/macocr",
            ocr_provider(),
        )
        return 2

    import psycopg

    conn = psycopg.connect(_dsn(), connect_timeout=15)
    conn.autocommit = True
    cur = conn.cursor()

    rows = _select_cars(
        cur, limit=None if args.all else args.limit, dealer=args.dealer,
        redo=args.redo, only_documents=args.only_documents,
    )
    _log.info("provider=%s  cars to process=%d", ocr_provider(), len(rows))
    if not rows:
        return 0

    fetch = _make_fetcher(args.delay)
    totals = {"cars": 0, "with_sticker": 0, "with_equipment": 0, "images_read": 0, "rejected": 0}
    started = time.time()

    def _process(row: tuple[int, str, str]) -> tuple[int, str, int, dict[str, Any]] | None:
        """Fetch + OCR one car. Runs on a worker thread; does no DB work."""
        car_id, vin, gallery_raw = row
        try:
            urls = json.loads(gallery_raw) if gallery_raw else []
        except (json.JSONDecodeError, TypeError):
            urls = []
        if not isinstance(urls, list) or not urls:
            return None
        findings = read_gallery(
            urls, expected_vin=vin or None, fetch=fetch, max_images=args.max_images
        )
        return car_id, vin, len(urls), summarize_gallery(findings)

    if args.workers > 1:
        from concurrent.futures import ThreadPoolExecutor

        pool = ThreadPoolExecutor(max_workers=args.workers)
        produced = pool.map(_process, rows)
    else:
        pool = None
        produced = (_process(r) for r in rows)

    # Only this loop touches the database: psycopg connections are not thread-safe, and
    # a single writer keeps the resumability marker consistent if the run is killed.
    for item in produced:
        if item is None:
            continue
        car_id, vin, seen, summary = item

        try:
            _record(cur, car_id, vin, seen, summary)
        except Exception as exc:  # pragma: no cover - operational
            _log.warning("car %s: record failed: %s", car_id, exc)
            continue

        totals["cars"] += 1
        totals["images_read"] += int(summary.get("images_read") or 0)
        totals["rejected"] += len(summary.get("images_rejected") or [])
        if summary.get("sticker_image_urls"):
            totals["with_sticker"] += 1
        if summary.get("equipment") or summary.get("packages"):
            totals["with_equipment"] += 1

        if totals["cars"] % 10 == 0:
            _log.info(
                "%d/%d cars | equipment on %d | stickers %d | %.1fs elapsed",
                totals["cars"], len(rows), totals["with_equipment"],
                totals["with_sticker"], time.time() - started,
            )

    if pool is not None:
        pool.shutdown(wait=True)

    elapsed = time.time() - started
    _log.info(
        "DONE %d cars in %.1fs | images read %d, rejected %d | "
        "equipment found on %d cars (%.1f%%) | stickers on %d (%.1f%%)",
        totals["cars"], elapsed, totals["images_read"], totals["rejected"],
        totals["with_equipment"],
        100.0 * totals["with_equipment"] / max(1, totals["cars"]),
        totals["with_sticker"],
        100.0 * totals["with_sticker"] / max(1, totals["cars"]),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
