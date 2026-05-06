#!/usr/bin/env python3
"""
Standalone image analyzer for dealership listings.

Analyzes images to detect interior/exterior paint colors, filters gallery, reads Monroney stickers.
Run independently to enrich vehicles with vision data.

Usage:
  python image_analyzer.py                        # Analyze all cars missing interior/exterior color
  python image_analyzer.py --vin 1HGBH41JXMN...  # Analyze specific VIN
  python image_analyzer.py --dealer-id <id>      # Analyze dealer's cars
  python image_analyzer.py --limit 50             # Analyze up to 50 cars
  python image_analyzer.py --all                  # Reanalyze all cars (overwrite)

Options:
  --vin VIN                    Analyze only this VIN
  --dealer-id ID               Analyze only this dealer's vehicles
  --limit N                    Max vehicles to analyze (default: 1000)
  --all                        Reanalyze all (overwrite existing colors)
  --skip-gallery-vision        Skip gallery filtering
  --skip-interior-vision       Skip interior color detection
  --skip-exterior-vision       Skip exterior/paint color detection
  --skip-monroney-vision       Skip Monroney sticker reading
  --workers N                  Parallel vision workers (default: 2)
  --provider {ollama,claude}   Vision provider (default: ollama from OLLAMA_HOST)
  --dry-run                    Log what would be done, don't update DB

Environment:
  OLLAMA_HOST               Ollama server URL (default: http://127.0.0.1:11434)
  OLLAMA_VISION_MODEL       Model name (default: llava:13b)
  ANTHROPIC_API_KEY         For Claude vision provider
  INVENTORY_DB_PATH         Path to inventory.db (default: ./inventory.db)
"""
import argparse
import asyncio
import json
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))

try:
    from backend.utils.project_env import load_project_dotenv
    load_project_dotenv()
except ImportError:
    pass

from backend.db.inventory_db import get_conn
from backend.vision import ollama_llava
from backend.vision.interior_vision_merge import build_updates_from_llava_result

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("image_analyzer")


def get_vehicles_to_analyze(
    db_path: str,
    vin: str | None = None,
    dealer_id: str | None = None,
    limit: int = 1000,
    all_vehicles: bool = False,
) -> list[dict[str, Any]]:
    """Query database for vehicles to analyze."""
    conn = get_conn()
    cursor = conn.cursor()

    if vin:
        cursor.execute(
            "SELECT id, vin, gallery, dealer_name, interior_color, exterior_color FROM cars WHERE vin = ?",
            (vin,),
        )
    elif dealer_id:
        cursor.execute(
            "SELECT id, vin, gallery, dealer_name, interior_color, exterior_color FROM cars WHERE dealer_id = ? ORDER BY vin LIMIT ?",
            (dealer_id, limit),
        )
    elif all_vehicles:
        cursor.execute(
            "SELECT id, vin, gallery, dealer_name, interior_color, exterior_color FROM cars WHERE gallery IS NOT NULL ORDER BY scraped_at DESC LIMIT ?",
            (limit,),
        )
    else:
        # Default: vehicles missing interior_color OR exterior_color
        cursor.execute(
            "SELECT id, vin, gallery, dealer_name, interior_color, exterior_color FROM cars "
            "WHERE gallery IS NOT NULL AND (interior_color IS NULL OR exterior_color IS NULL) "
            "ORDER BY scraped_at DESC LIMIT ?",
            (limit,),
        )

    rows = cursor.fetchall()
    conn.close()
    return [
        {"id": r[0], "vin": r[1], "gallery": r[2], "dealer_name": r[3], "interior_color": r[4], "exterior_color": r[5]}
        for r in rows
    ]


def parse_gallery_json(gallery_str: str | None) -> list[str]:
    """Parse gallery JSON string to URL list."""
    if not gallery_str:
        return []
    try:
        return json.loads(gallery_str)
    except (json.JSONDecodeError, TypeError):
        return []


def analyze_vehicle_colors(
    vin: str,
    gallery_urls: list[str],
    skip_gallery_vision: bool = False,
    skip_interior_vision: bool = False,
    skip_exterior_vision: bool = False,
    skip_monroney_vision: bool = False,
) -> dict[str, Any]:
    """Analyze a vehicle's images for interior/exterior colors, gallery filtering, and stickers."""
    result = {
        "vin": vin,
        "interior_color": None,
        "interior_buckets": None,
        "interior_confidence": None,
        "exterior_color": None,
        "exterior_buckets": None,
        "exterior_confidence": None,
        "errors": [],
    }

    if not gallery_urls:
        result["errors"].append("No gallery URLs")
        return result

    try:
        # 1. Gallery filtering (remove junk)
        if not skip_gallery_vision:
            logger.info(f"[{vin}] Filtering gallery ({len(gallery_urls)} URLs)...")
            try:
                filtered_urls = ollama_llava.filter_gallery_urls_for_vehicle_listing(
                    gallery_urls, max_workers=1
                )
                logger.info(f"[{vin}] Gallery: {len(gallery_urls)} → {len(filtered_urls)} kept")
            except Exception as e:
                logger.warning(f"[{vin}] Gallery filtering failed: {e}")
                filtered_urls = gallery_urls
        else:
            filtered_urls = gallery_urls

        # 2. Interior color detection
        if not skip_interior_vision and filtered_urls:
            logger.info(f"[{vin}] Analyzing interior from gallery ({len(filtered_urls)} images)...")
            for url in filtered_urls:
                if not url:
                    continue
                try:
                    interior_result = ollama_llava.analyze_interior_from_image_url(url)
                    if interior_result:
                        confidence = interior_result.get("confidence", 0)
                        if confidence > 0.4:  # Confidence threshold
                            result["interior_color"] = interior_result.get(
                                "interior_guess_text"
                            )
                            result["interior_buckets"] = interior_result.get(
                                "interior_buckets"
                            )
                            result["interior_confidence"] = confidence
                            logger.info(
                                f"[{vin}] Interior: {result['interior_color']} (conf={confidence:.2f})"
                            )
                            break
                except Exception as e:
                    logger.debug(f"[{vin}] Interior analysis failed for {url}: {e}")
                    continue

        # 3. Exterior color detection
        if not skip_exterior_vision and filtered_urls:
            logger.info(f"[{vin}] Analyzing exterior from gallery ({len(filtered_urls)} images)...")
            for url in filtered_urls:
                if not url:
                    continue
                try:
                    exterior_result = ollama_llava.analyze_exterior_color_from_image_url(url)
                    if exterior_result:
                        confidence = exterior_result.get("confidence", 0)
                        if confidence > 0.4:  # Confidence threshold
                            result["exterior_color"] = exterior_result.get(
                                "exterior_guess_text"
                            )
                            result["exterior_buckets"] = exterior_result.get(
                                "exterior_buckets"
                            )
                            result["exterior_confidence"] = confidence
                            logger.info(
                                f"[{vin}] Exterior: {result['exterior_color']} (conf={confidence:.2f})"
                            )
                            break
                except Exception as e:
                    logger.debug(f"[{vin}] Exterior analysis failed for {url}: {e}")
                    continue

        # 4. Monroney sticker (optional)
        if not skip_monroney_vision and filtered_urls:
            logger.info(f"[{vin}] Checking for Monroney stickers...")
            for url in filtered_urls:
                if ollama_llava.is_probable_sticker_image_url(url):
                    try:
                        sticker_result = ollama_llava.analyze_monroney_sticker_from_image_url(
                            url
                        )
                        if sticker_result:
                            logger.info(f"[{vin}] Found Monroney: {sticker_result}")
                            result["monroney"] = sticker_result
                            break
                    except Exception as e:
                        logger.debug(f"[{vin}] Monroney analysis failed for {url}: {e}")
                        continue

    except Exception as e:
        result["errors"].append(str(e))
        logger.error(f"[{vin}] Analysis failed: {e}")

    return result


def update_vehicle_in_db(
    db_path: str,
    vin: str,
    interior_color: str | None,
    interior_buckets: list[str] | None,
    interior_confidence: float | None,
    exterior_color: str | None = None,
    exterior_buckets: list[str] | None = None,
    exterior_confidence: float | None = None,
    dry_run: bool = False,
) -> bool:
    """Update vehicle row with analysis results."""
    if dry_run:
        updates_info = []
        if interior_color:
            updates_info.append(f"interior_color={interior_color}")
        if exterior_color:
            updates_info.append(f"exterior_color={exterior_color}")
        logger.info(f"[DRY-RUN] Would update {vin}: {', '.join(updates_info)}")
        return True

    try:
        conn = get_conn()
        cursor = conn.cursor()

        updates = {}
        if interior_color:
            updates["interior_color"] = interior_color
        if interior_buckets:
            updates["interior_buckets_json"] = json.dumps(interior_buckets)
        if interior_confidence is not None:
            updates["interior_vision_confidence"] = interior_confidence
        if exterior_color:
            updates["exterior_color"] = exterior_color
        if exterior_buckets:
            updates["exterior_buckets_json"] = json.dumps(exterior_buckets)
        if exterior_confidence is not None:
            updates["exterior_vision_confidence"] = exterior_confidence

        if not updates:
            return False

        set_clause = ", ".join(f"{k} = ?" for k in updates.keys())
        values = list(updates.values()) + [vin]
        cursor.execute(f"UPDATE cars SET {set_clause} WHERE vin = ?", values)
        conn.commit()
        conn.close()
        logger.info(f"[{vin}] Updated DB: {updates}")
        return True
    except Exception as e:
        logger.error(f"[{vin}] DB update failed: {e}")
        return False


def analyze_batch(
    vehicles: list[dict[str, Any]],
    db_path: str,
    skip_gallery_vision: bool = False,
    skip_interior_vision: bool = False,
    skip_exterior_vision: bool = False,
    skip_monroney_vision: bool = False,
    workers: int = 2,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Analyze a batch of vehicles."""
    stats = {
        "total": len(vehicles),
        "succeeded": 0,
        "failed": 0,
        "updated": 0,
    }

    for vehicle in vehicles:
        vin = vehicle["vin"]
        gallery_urls = parse_gallery_json(vehicle["gallery"])

        logger.info(f"\n[{vin}] Processing {vehicle['dealer_name']}...")
        result = analyze_vehicle_colors(
            vin,
            gallery_urls,
            skip_gallery_vision=skip_gallery_vision,
            skip_interior_vision=skip_interior_vision,
            skip_exterior_vision=skip_exterior_vision,
            skip_monroney_vision=skip_monroney_vision,
        )

        if result["errors"]:
            stats["failed"] += 1
            logger.warning(f"[{vin}] Failed: {result['errors']}")
        else:
            stats["succeeded"] += 1

            if result["interior_color"] or result["exterior_color"]:
                updated = update_vehicle_in_db(
                    db_path,
                    vin,
                    result["interior_color"],
                    result["interior_buckets"],
                    result["interior_confidence"],
                    exterior_color=result["exterior_color"],
                    exterior_buckets=result["exterior_buckets"],
                    exterior_confidence=result["exterior_confidence"],
                    dry_run=dry_run,
                )
                if updated:
                    stats["updated"] += 1

    return stats


def main():
    ap = argparse.ArgumentParser(
        description="Analyze dealership listing images (interior color, gallery, stickers).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    ap.add_argument(
        "--vin",
        help="Analyze only this VIN",
    )
    ap.add_argument(
        "--dealer-id",
        help="Analyze only vehicles from this dealer_id",
    )
    ap.add_argument(
        "--limit",
        type=int,
        default=1000,
        help="Max vehicles to analyze (default: 1000)",
    )
    ap.add_argument(
        "--all",
        action="store_true",
        help="Analyze all vehicles (reanalyze, overwrite existing)",
    )
    ap.add_argument(
        "--skip-gallery-vision",
        action="store_true",
        help="Skip gallery filtering (junk removal)",
    )
    ap.add_argument(
        "--skip-interior-vision",
        action="store_true",
        help="Skip interior color detection",
    )
    ap.add_argument(
        "--skip-exterior-vision",
        action="store_true",
        help="Skip exterior/paint color detection",
    )
    ap.add_argument(
        "--skip-monroney-vision",
        action="store_true",
        help="Skip Monroney sticker reading",
    )
    ap.add_argument(
        "--workers",
        type=int,
        default=2,
        help="Parallel vision workers (default: 2)",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Log what would be done, don't update DB",
    )

    args = ap.parse_args()

    db_path = os.environ.get("INVENTORY_DB_PATH", "inventory.db")

    logger.info(
        f"Image Analyzer starting — DB: {db_path}, "
        f"Ollama: {os.environ.get('OLLAMA_HOST', 'http://127.0.0.1:11434')}"
    )

    vehicles = get_vehicles_to_analyze(
        db_path,
        vin=args.vin,
        dealer_id=args.dealer_id,
        limit=args.limit,
        all_vehicles=args.all,
    )

    if not vehicles:
        logger.info("No vehicles to analyze")
        return

    logger.info(f"Found {len(vehicles)} vehicles to analyze")

    stats = analyze_batch(
        vehicles,
        db_path,
        skip_gallery_vision=args.skip_gallery_vision,
        skip_interior_vision=args.skip_interior_vision,
        skip_exterior_vision=args.skip_exterior_vision,
        skip_monroney_vision=args.skip_monroney_vision,
        workers=args.workers,
        dry_run=args.dry_run,
    )

    logger.info("\n" + "=" * 60)
    logger.info(f"Analysis Complete:")
    logger.info(f"  Total: {stats['total']}")
    logger.info(f"  Succeeded: {stats['succeeded']}")
    logger.info(f"  Failed: {stats['failed']}")
    logger.info(f"  Updated DB: {stats['updated']}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
