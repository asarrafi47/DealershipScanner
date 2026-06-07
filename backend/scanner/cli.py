#!/usr/bin/env python3
"""
Manifest-driven dealership inventory scanner. Uses playwright + stealth,
session warmup to avoid 403, network interception for JSON, HTML fallback, and
optional ``__NEXT_DATA__`` (Next.js) extraction when the raw HTML shell has no rows.

Implementation is split across:

- ``orchestrator`` — browser pool, concurrency, post-scan delegation
- ``phases/dealer_run`` — per-dealer warmup → inventory → recovery → VDP → upsert
- ``phases/inventory_scrape`` — single inventory path JSON intercept
- ``phases/nav`` — Playwright navigation helpers
- ``manifest`` — dealers.json loading and filters
- ``post_scan_job`` — standalone repair/enrichment (``python post_scan.py``)

Run from project root: ``python scanner.py``
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))

try:
    from backend.utils.project_env import load_project_dotenv

    load_project_dotenv()
except ImportError:
    pass

from backend.db.inventory_pg import is_inventory_postgres
from backend.scanner.constants import DEBUG_DIR, MANIFEST_PATH
from backend.scanner.manifest import (
    filter_manifest_by_dealer_id,
    filter_manifest_by_shard,
    filter_skip_dealers,
    load_manifest,
    resolve_shard_cli_and_env,
)
from backend.scanner.orchestrator import main, on_scanner_shutdown_signal
from backend.scanner.phases.dealer_run import (
    apply_monroney_vision_to_vehicles as _apply_monroney_vision_to_vehicles,
    run_dealer,
)
from backend.scanner.post_pipeline import (
    apply_gallery_vision_filter_to_vehicles as _apply_gallery_vision_filter_to_vehicles,
    gallery_vision_filter_env_enabled,
    monroney_vision_env_enabled,
    post_dealer_google_ratings_env_enabled,
    post_enrich_env_enabled,
    post_enrich_vision_env_enabled,
    post_gas_prices_env_enabled,
    post_interior_vision_env_enabled,
    post_listing_description_env_enabled,
    post_listing_gap_fill_env_enabled,
    post_repair_env_enabled,
    post_window_sticker_env_enabled,
)
from backend.scanner.scan_efficiency import (
    apply_fast_mode_env_defaults,
    apply_scan_only_env_defaults,
    gallery_vision_inline_enabled,
    scanner_fast_mode_enabled,
    scanner_scan_only_enabled,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("scanner")

# Back-compat for tests / dev console that import shutdown hook from cli
_scanner_shutdown_requested = False
_on_scanner_shutdown_signal = on_scanner_shutdown_signal


def run_cli_entry() -> None:
    ap = argparse.ArgumentParser(
        description="Manifest-driven dealership inventory scanner (Playwright + stealth)."
    )
    ap.add_argument(
        "--manifest",
        metavar="PATH",
        default=None,
        help=(
            "Dealer manifest JSON (default: dealers.json or DEALERS_MANIFEST_PATH). "
            "Example: workspace/manifest_92694_25mi.json"
        ),
    )
    ap.add_argument(
        "--dealer-id",
        metavar="ID",
        default=None,
        help="Scan only this dealer_id from the manifest (e.g. from the /dev console).",
    )
    ap.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit the number of dealerships to scan.",
    )
    ap.add_argument(
        "--shard-index",
        type=int,
        default=None,
        metavar="N",
        help=(
            "0-based shard for parallel scans (requires --shard-count). "
            "Dealer at manifest position i runs on shard (i %% count) == N."
        ),
    )
    ap.add_argument(
        "--shard-count",
        type=int,
        default=None,
        metavar="M",
        help="Number of shards / parallel workers (requires --shard-index when using CLI).",
    )
    ap.add_argument(
        "--provider",
        type=str,
        default=None,
        help="Only scan dealerships matching this provider (e.g., dealer_dot_com).",
    )
    ap.add_argument(
        "--scan-only",
        action="store_true",
        help=(
            "Inventory capture only; skip post-scan repair/enrichment in this process. "
            "Equivalent to SCANNER_SCAN_ONLY=1. Run python post_scan.py afterward."
        ),
    )
    ap.add_argument(
        "--no-post-repair",
        action="store_true",
        help="Skip SQLite repair for VINs touched in this run (see SCANNER_POST_REPAIR).",
    )
    ap.add_argument(
        "--no-post-listing-description",
        action="store_true",
        help="Skip parsing each listing description into packages JSON (see SCANNER_POST_LISTING_DESCRIPTION).",
    )
    ap.add_argument(
        "--post-enrich",
        action="store_true",
        help="After repair + listing parse, run InventoryEnricher for scanned rows (requires indexed EPA catalog).",
    )
    ap.add_argument(
        "--post-enrich-vision-only",
        action="store_true",
        help="After repair + listing parse, vision-only enrichment for scanned rows (catalog not required).",
    )
    ap.add_argument(
        "--enable-interior-vision",
        action="store_true",
        help="Enable post-scan interior/cabin analysis (default is off; run separately via post_scan.py).",
    )
    ap.add_argument(
        "--enable-gallery-vision",
        action="store_true",
        help="Run gallery cleanup after scan (post-scan batch; does not block upsert).",
    )
    ap.add_argument(
        "--enable-gallery-vision-inline",
        action="store_true",
        help="Run gallery cleanup inside each dealer before upsert (slow; may hit API rate limits).",
    )
    ap.add_argument(
        "--enable-monroney-vision",
        action="store_true",
        help="Enable Monroney / sticker pass before upsert (default is off).",
    )
    ap.add_argument(
        "--enrichment-workers",
        type=int,
        default=None,
        help="Worker threads for post-scan enrichment (default: ENRICHMENT_MAX_WORKERS or enricher default).",
    )
    ap.add_argument(
        "--post-listing-gap-fill",
        action="store_true",
        help="After post-scan repair: backfill missing specs from listing pages. Or SCANNER_POST_LISTING_GAP_FILL=1.",
    )
    ap.add_argument(
        "--no-post-window-sticker",
        action="store_true",
        help="Skip OEM window sticker PDF fetch during post-scan (default: on; SCANNER_POST_WINDOW_STICKER=0).",
    )
    ap.add_argument(
        "--no-post-gas-prices",
        action="store_true",
        help="Skip AAA gas price cache refresh after scan (default: on; SCANNER_POST_GAS_PRICES=0).",
    )
    ap.add_argument(
        "--no-post-dealer-ratings",
        action="store_true",
        help="Skip dealer Google rating backfill cadence after scan (default: on).",
    )
    args = ap.parse_args()
    global MANIFEST_PATH
    if args.manifest:
        mp = Path(args.manifest).expanduser()
        if not mp.is_absolute():
            mp = (ROOT / mp).resolve()
        MANIFEST_PATH = mp
        os.environ["DEALERS_MANIFEST_PATH"] = str(mp)
    if is_inventory_postgres():
        from backend.db.inventory_db import init_inventory_db

        init_inventory_db()
    if not MANIFEST_PATH.is_file() and (os.environ.get("DEALERS_FROM_DB") or "").strip().lower() not in (
        "1",
        "true",
        "yes",
    ):
        logger.error("Manifest not found: %s", MANIFEST_PATH.resolve())
        sys.exit(1)

    if args.scan_only:
        os.environ["SCANNER_SCAN_ONLY"] = "1"
    apply_fast_mode_env_defaults()
    apply_scan_only_env_defaults()

    to_run = filter_skip_dealers(load_manifest())
    if args.dealer_id:
        to_run = filter_manifest_by_dealer_id(to_run, args.dealer_id)
        if not to_run:
            logger.error(
                "No dealer with dealer_id %r in %s — save the dealer in /dev first.",
                args.dealer_id.strip(),
                MANIFEST_PATH,
            )
            sys.exit(1)

    if args.provider is not None:
        prov = args.provider.strip()
        if not prov:
            logger.error("--provider is empty after stripping whitespace.")
            sys.exit(1)
        before = len(to_run)
        to_run = [d for d in to_run if d.get("provider") == prov]
        logger.info("Provider filter %r: %d of %d dealer(s) in manifest.", prov, len(to_run), before)
        if not to_run:
            logger.error(
                "No dealers with provider %r in %s — check manifest provider values.",
                prov,
                MANIFEST_PATH,
            )
            sys.exit(1)

    if args.limit is not None:
        if args.limit < 0:
            logger.error("--limit must be >= 0 (got %s).", args.limit)
            sys.exit(1)
        before = len(to_run)
        to_run = to_run[: args.limit]
        logger.info("Limit %d: scanning %d of %d dealer(s).", args.limit, len(to_run), before)

    shard_index, shard_count = resolve_shard_cli_and_env(args)
    if shard_count > 1:
        before_shard = len(to_run)
        to_run = filter_manifest_by_shard(to_run, shard_index, shard_count)
        logger.info(
            "Shard %d/%d: %d of %d dealer(s) after prior filters.",
            shard_index,
            shard_count,
            len(to_run),
            before_shard,
        )
        if not to_run:
            logger.warning(
                "Shard %d/%d selected zero dealers — check manifest size vs shard count.",
                shard_index,
                shard_count,
            )

    scan_only = bool(args.scan_only) or scanner_scan_only_enabled()
    do_repair = not scan_only and not args.no_post_repair and post_repair_env_enabled()
    do_listing = not scan_only and not args.no_post_listing_description and post_listing_description_env_enabled()
    do_vision = bool(args.post_enrich_vision_only) or post_enrich_vision_env_enabled()
    do_enrich = bool(args.post_enrich) or post_enrich_env_enabled()
    do_interior_vision = (
        not scan_only and args.enable_interior_vision and post_interior_vision_env_enabled()
    )
    if args.enable_gallery_vision:
        os.environ.setdefault("SCANNER_GALLERY_VISION_POST", "1")
    do_post_gallery = not scan_only and bool(args.enable_gallery_vision)
    do_gallery_vision = args.enable_gallery_vision_inline and gallery_vision_inline_enabled()
    do_monroney = args.enable_monroney_vision and monroney_vision_env_enabled()
    do_listing_gap_fill = (
        not scan_only and (bool(args.post_listing_gap_fill) or post_listing_gap_fill_env_enabled())
    )
    do_window_sticker = not scan_only and not args.no_post_window_sticker and post_window_sticker_env_enabled()
    do_gas_prices = not scan_only and not args.no_post_gas_prices and post_gas_prices_env_enabled()
    do_dealer_ratings = not scan_only and not args.no_post_dealer_ratings and post_dealer_google_ratings_env_enabled()

    try:
        asyncio.run(
            main(
                to_run,
                scan_only=scan_only,
                post_repair=do_repair,
                post_listing_description=do_listing,
                post_interior_vision=do_interior_vision,
                post_enrich=do_enrich and not do_vision,
                post_enrich_vision_only=do_vision,
                post_listing_gap_fill=do_listing_gap_fill,
                post_window_sticker=do_window_sticker,
                post_gas_prices=do_gas_prices,
                post_dealer_ratings=do_dealer_ratings,
                post_gallery_vision=do_post_gallery,
                enrichment_max_workers=args.enrichment_workers,
                gallery_vision_filter=do_gallery_vision,
                monroney_vision=do_monroney,
            )
        )
    except KeyboardInterrupt:
        logger.info("Scanner gracefully stopped by user.")
        sys.exit(0)
    except asyncio.CancelledError:
        logger.info("Scanner gracefully stopped by user.")
        sys.exit(0)


if __name__ == "__main__":
    run_cli_entry()


__all__ = [
    "DEBUG_DIR",
    "MANIFEST_PATH",
    "filter_manifest_by_dealer_id",
    "filter_manifest_by_shard",
    "load_manifest",
    "main",
    "run_cli_entry",
    "run_dealer",
    "_apply_gallery_vision_filter_to_vehicles",
    "_apply_monroney_vision_to_vehicles",
]
