"""
Standalone post-scan enrichment job (decoupled from inventory capture).

Run after ``scanner.py --scan-only`` or on a schedule::

    python post_scan.py
    python post_scan.py --hours 8
    python post_scan.py --vin 1HGCM82633A004352

Selects VINs from ``cars.scraped_at`` within the lookback window unless explicit VINs are passed.
Does not launch Playwright for inventory; may use Playwright inside listing gap-fill when enabled.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
os.chdir(ROOT)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from backend.utils.project_env import load_project_dotenv

    load_project_dotenv()
except ImportError:
    pass

from backend.db.inventory_pg import is_inventory_postgres
from backend.scanner.post_pipeline import (
    post_dealer_google_ratings_env_enabled,
    post_dict_enrich_env_enabled,
    post_enrich_env_enabled,
    post_enrich_vision_env_enabled,
    post_gas_prices_env_enabled,
    post_interior_vision_env_enabled,
    post_listing_description_env_enabled,
    post_listing_gap_fill_env_enabled,
    post_repair_env_enabled,
    post_window_sticker_env_enabled,
    run_dealer_google_rating_backfill_if_due,
    run_dictionary_enrich_for_vins,
    run_listing_gap_fill_stage,
    run_post_scan,
    run_sync_gas_prices,
)
from backend.scanner.scan_efficiency import scanner_scan_only_enabled

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("scanner.post_scan_job")


def _default_lookback_hours() -> int:
    raw = (os.environ.get("SCANNER_POST_SCAN_LOOKBACK_HOURS") or "6").strip()
    try:
        return max(1, min(168, int(raw)))
    except ValueError:
        return 6


def vins_scraped_within_hours(hours: int) -> list[str]:
    """Distinct VINs upserted/scraped within the last *hours*."""
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    if is_inventory_postgres():
        from backend.db.inventory_db import db_conn

        with db_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT DISTINCT UPPER(TRIM(vin)) AS vin
                FROM cars
                WHERE vin IS NOT NULL AND LENGTH(TRIM(vin)) = 17
                  AND scraped_at IS NOT NULL AND scraped_at >= %s
                ORDER BY vin
                """,
                (cutoff,),
            )
            rows = cur.fetchall()
        return [str(r[0] if not isinstance(r, dict) else r["vin"]) for r in rows if r]

    import sqlite3

    from backend.db.inventory_db import db_conn

    with db_conn(row_factory=sqlite3.Row) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT DISTINCT UPPER(TRIM(vin)) AS vin
            FROM cars
            WHERE vin IS NOT NULL AND LENGTH(TRIM(vin)) = 17
              AND scraped_at IS NOT NULL AND scraped_at >= ?
            ORDER BY vin
            """,
            (cutoff,),
        )
        return [str(r["vin"]) for r in cur.fetchall() if r["vin"]]


async def run_post_scan_job(
    vins: list[str] | None = None,
    *,
    hours: int | None = None,
    post_repair: bool = True,
    post_listing_description: bool = True,
    post_interior_vision: bool = False,
    post_enrich: bool = False,
    post_enrich_vision_only: bool = False,
    post_listing_gap_fill: bool = False,
    post_window_sticker: bool = True,
    post_gas_prices: bool = True,
    post_dealer_ratings: bool = True,
    post_gallery_vision: bool = False,
    enrichment_max_workers: int | None = None,
) -> dict[str, Any]:
    lookback = hours if hours is not None else _default_lookback_hours()
    scanned_vins = [v.strip().upper() for v in (vins or []) if (v or "").strip()]
    if not scanned_vins:
        scanned_vins = vins_scraped_within_hours(lookback)
    summary: dict[str, Any] = {
        "lookback_hours": lookback,
        "vin_count": len(scanned_vins),
    }
    if not scanned_vins:
        logger.warning("Post-scan job: no VINs in lookback window (%dh)", lookback)
        return summary

    logger.info("Post-scan job: %d VIN(s) (lookback=%dh)", len(scanned_vins), lookback)

    if (
        post_repair
        or post_listing_description
        or post_interior_vision
        or post_enrich
        or post_enrich_vision_only
        or post_window_sticker
        or post_gallery_vision
    ):
        try:
            post_summary = run_post_scan(
                scanned_vins,
                post_repair=post_repair,
                post_listing_description=post_listing_description,
                post_interior_vision=post_interior_vision,
                post_enrich=post_enrich,
                post_enrich_vision_only=post_enrich_vision_only,
                post_window_sticker=post_window_sticker,
                post_gallery_vision=post_gallery_vision,
                enrichment_max_workers=enrichment_max_workers,
            )
            summary["post_scan"] = post_summary
            logger.info("Post-scan summary: %s", json.dumps(post_summary, default=str)[:1800])
        except Exception:
            logger.exception("Post-scan pipeline failed")

    if post_listing_gap_fill and scanned_vins:
        try:
            gap_summary = await asyncio.to_thread(run_listing_gap_fill_stage, scanned_vins)
            summary["listing_gap_fill"] = gap_summary
            logger.info("Listing gap fill: %s", json.dumps(gap_summary, default=str)[:1600])
        except Exception:
            logger.exception("Listing gap fill failed")

    try:
        from backend.scanner.database import apply_model_specs_corrections

        corrected = await asyncio.to_thread(apply_model_specs_corrections)
        if corrected:
            summary["model_specs_corrected"] = corrected
            logger.info("model_specs final pass: %d rows corrected", corrected)
    except Exception:
        logger.exception("model_specs correction pass failed")

    if scanned_vins and post_dict_enrich_env_enabled():
        try:
            dict_stats = await asyncio.to_thread(run_dictionary_enrich_for_vins, scanned_vins)
            summary["dictionary_enrich"] = dict_stats
            if dict_stats.get("updated"):
                logger.info(
                    "EPA dictionary enrichment: %d/%d VINs updated",
                    dict_stats["updated"],
                    dict_stats["vins"],
                )
        except Exception:
            logger.exception("EPA dictionary enrichment failed")

    try:
        from backend.db.incomplete_listings_db import fast_rebuild_incomplete_listings_index

        n_incomplete = await asyncio.to_thread(fast_rebuild_incomplete_listings_index)
        summary["incomplete_listings"] = n_incomplete
        logger.info("incomplete_listings resynced: %d incomplete listings", n_incomplete)
    except Exception:
        logger.exception("incomplete_listings resync failed")

    if post_gas_prices:
        try:
            gas_summary = await asyncio.to_thread(run_sync_gas_prices)
            summary["gas_prices"] = gas_summary
        except Exception:
            logger.exception("Gas price sync failed")

    if post_dealer_ratings:
        try:
            rating_summary = await asyncio.to_thread(
                run_dealer_google_rating_backfill_if_due,
                enabled=True,
            )
            summary["dealer_ratings"] = rating_summary
        except Exception:
            logger.exception("Dealer Google rating batch failed")

    return summary


def run_cli_entry() -> None:
    ap = argparse.ArgumentParser(description="Post-scan repair/enrichment for recently scraped VINs.")
    ap.add_argument("--hours", type=int, default=None, help="Lookback window for scraped_at (default: 6).")
    ap.add_argument("--vin", action="append", default=[], help="Explicit VIN (repeatable).")
    ap.add_argument("--no-post-repair", action="store_true")
    ap.add_argument("--no-post-listing-description", action="store_true")
    ap.add_argument("--enable-interior-vision", action="store_true")
    ap.add_argument("--enable-gallery-vision", action="store_true")
    ap.add_argument("--post-enrich", action="store_true")
    ap.add_argument("--post-enrich-vision-only", action="store_true")
    ap.add_argument("--post-listing-gap-fill", action="store_true")
    ap.add_argument("--no-post-window-sticker", action="store_true")
    ap.add_argument("--no-post-gas-prices", action="store_true")
    ap.add_argument("--no-post-dealer-ratings", action="store_true")
    args = ap.parse_args()

    if is_inventory_postgres():
        from backend.db.inventory_db import init_inventory_db

        init_inventory_db()

    if scanner_scan_only_enabled():
        logger.info("SCANNER_SCAN_ONLY is set — post-scan job is the intended enrichment path.")

    vins = [v.strip().upper() for v in args.vin if v and v.strip()] or None
    summary = asyncio.run(
        run_post_scan_job(
            vins,
            hours=args.hours,
            post_repair=not args.no_post_repair and post_repair_env_enabled(),
            post_listing_description=not args.no_post_listing_description
            and post_listing_description_env_enabled(),
            post_interior_vision=args.enable_interior_vision and post_interior_vision_env_enabled(),
            post_enrich=bool(args.post_enrich) or post_enrich_env_enabled(),
            post_enrich_vision_only=bool(args.post_enrich_vision_only)
            or post_enrich_vision_env_enabled(),
            post_listing_gap_fill=bool(args.post_listing_gap_fill) or post_listing_gap_fill_env_enabled(),
            post_window_sticker=not args.no_post_window_sticker and post_window_sticker_env_enabled(),
            post_gas_prices=not args.no_post_gas_prices and post_gas_prices_env_enabled(),
            post_dealer_ratings=not args.no_post_dealer_ratings and post_dealer_google_ratings_env_enabled(),
            post_gallery_vision=bool(args.enable_gallery_vision),
        )
    )
    logger.info("Post-scan job finished: %s", json.dumps(summary, default=str)[:2000])


if __name__ == "__main__":
    run_cli_entry()
