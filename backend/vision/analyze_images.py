"""
Unified listing **image analysis** via Ollama LLaVA (and helpers tied to ``inventory.db``).

**Run from the repository root** (so ``backend`` is importable)::

    python -m backend.vision.analyze_images interior --url 'https://...'
    python -m backend.vision.analyze_images interior --vin 1HGBH41JXMN109186
    python -m backend.vision.analyze_images classify --url 'https://...'
    python -m backend.vision.analyze_images filter-gallery 'https://a/1.jpg' 'https://a/2.jpg'
    python -m backend.vision.analyze_images monroney --url 'https://...'
    python -m backend.vision.analyze_images backfill-interior --dry-run

This module **re-exports** the low-level vision functions from :mod:`backend.vision.ollama_llava`
and adds a CLI. Inventory paths default to ``<repo>/inventory.db`` when ``INVENTORY_DB_PATH`` is
unset (so running from ``scripts/`` still finds the DB).

Requires ``OLLAMA_HOST`` (default ``http://127.0.0.1:11434``) and a pulled vision model
(``OLLAMA_VISION_MODEL``, default ``llava:13b``).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

# Repository root: backend/vision/analyze_images.py -> parents[2]
REPO_ROOT = Path(__file__).resolve().parents[2]

# --- Re-exports: use this file as the single import surface for image analysis -----------------
from backend.vision.ollama_llava import (  # noqa: E402
    INTERIOR_BUCKET_ALLOWLIST,
    OLLAMA_HOST,
    OLLAMA_INTERIOR_VISION_TIMEOUT_S,
    OLLAMA_VISION_MODEL,
    analyze_interior_from_image_b64,
    analyze_interior_from_image_url,
    analyze_monroney_from_page_texts,
    analyze_monroney_sticker_from_image_b64,
    analyze_monroney_sticker_from_image_url,
    classify_listing_image_from_image_b64,
    classify_listing_image_from_url,
    filter_gallery_urls_for_vehicle_listing,
    image_bytes_to_b64_jpeg,
    is_probable_sticker_image_url,
    last_ollama_diagnostics,
)

__all__ = [
    "INTERIOR_BUCKET_ALLOWLIST",
    "OLLAMA_HOST",
    "OLLAMA_INTERIOR_VISION_TIMEOUT_S",
    "OLLAMA_VISION_MODEL",
    "analyze_interior_from_image_b64",
    "analyze_interior_from_image_url",
    "analyze_monroney_from_page_texts",
    "analyze_monroney_sticker_from_image_b64",
    "analyze_monroney_sticker_from_image_url",
    "apply_inventory_db_defaults",
    "classify_listing_image_from_image_b64",
    "classify_listing_image_from_url",
    "collect_vins_missing_interior_with_images",
    "filter_gallery_urls_for_vehicle_listing",
    "image_bytes_to_b64_jpeg",
    "is_probable_sticker_image_url",
    "main",
    "pick_image_for_interior_vision",
    "run_interior_vision_for_inventory_vins",
]


def apply_inventory_db_defaults(explicit_db: str | None = None) -> str:
    """
    Set ``INVENTORY_DB_PATH`` to an absolute path and sync ``inventory_db.DB_PATH``.

    If *explicit_db* is set, it wins. Else if the env var is unset or empty, use
    ``<repo root>/inventory.db``.
    """
    if explicit_db:
        path = os.path.abspath(explicit_db.strip())
        os.environ["INVENTORY_DB_PATH"] = path
    elif not (os.environ.get("INVENTORY_DB_PATH") or "").strip():
        path = str(REPO_ROOT / "inventory.db")
        os.environ["INVENTORY_DB_PATH"] = path
    else:
        path = os.path.abspath(os.environ["INVENTORY_DB_PATH"].strip())
        os.environ["INVENTORY_DB_PATH"] = path

    import backend.db.inventory_db as inv

    inv.DB_PATH = os.environ["INVENTORY_DB_PATH"]
    return path


def pick_image_for_interior_vision(urls: list[str]) -> tuple[str | None, str]:
    """Prefer cabin URL; else hero + ``through_windows`` when fallback env allows."""
    from backend.scanner_post_pipeline import pick_listing_image_for_interior_vision

    return pick_listing_image_for_interior_vision(urls)


def run_interior_vision_for_inventory_vins(
    vins: list[str],
    *,
    skip_if_interior_present: bool = False,
) -> dict[str, Any]:
    """Persist interior inference for VINs (same as post-scan interior pass)."""
    from backend.scanner_post_pipeline import run_interior_vision_for_vins

    return run_interior_vision_for_vins(vins, skip_if_interior_present=skip_if_interior_present)


def collect_vins_missing_interior_with_images(
    *,
    limit: int | None,
    offset: int,
) -> tuple[list[str], dict[str, int]]:
    from backend.db.inventory_db import get_conn
    from backend.scanner_post_pipeline import http_listing_image_urls_for_row
    from backend.utils.field_clean import is_effectively_empty

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT vin, interior_color, image_url, gallery
            FROM cars
            WHERE (COALESCE(listing_active, 1) = 1)
              AND vin IS NOT NULL
              AND LENGTH(TRIM(vin)) >= 11
            ORDER BY id
            """
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    stats: dict[str, int] = {
        "rows_active": 0,
        "interior_missing": 0,
        "has_http_image": 0,
        "queued": 0,
        "truncated_after_limit": 0,
    }
    vins: list[str] = []
    off = max(0, offset)

    def _row_dict(vin: str, interior_color, image_url, gallery) -> dict:
        return {
            "vin": vin,
            "interior_color": interior_color,
            "image_url": image_url,
            "gallery": gallery,
        }

    for vin, interior_color, image_url, gallery in rows:
        stats["rows_active"] += 1
        if not is_effectively_empty(interior_color):
            continue
        stats["interior_missing"] += 1
        row = _row_dict(vin, interior_color, image_url, gallery)
        if not http_listing_image_urls_for_row(row):
            continue
        stats["has_http_image"] += 1
        if off > 0:
            off -= 1
            continue
        if limit is not None and len(vins) >= limit:
            stats["truncated_after_limit"] += 1
            continue
        vins.append(str(vin).strip())
    stats["queued"] = len(vins)
    return vins, stats


def _cmd_interior(args: argparse.Namespace) -> None:
    if args.url:
        ctx = "through_windows" if args.through_windows else "cabin"
        out = analyze_interior_from_image_url(args.url.strip(), inference_context=ctx)
        print(json.dumps(out, indent=2))
        return

    apply_inventory_db_defaults(args.db)
    from backend.db.inventory_db import get_car_by_vin
    from backend.scanner_post_pipeline import candidate_urls_for_interior_vision, http_listing_image_urls_for_row

    row = get_car_by_vin(args.vin.strip())
    if not row:
        print("VIN not found in database.", file=sys.stderr)
        sys.exit(2)
    urls = http_listing_image_urls_for_row(row)
    candidates = candidate_urls_for_interior_vision(urls)
    if not candidates:
        print("No HTTP(S) listing image on row (hero/gallery).", file=sys.stderr)
        sys.exit(3)
    out = None
    tried: list[dict[str, str]] = []
    for primary, ctx in candidates:
        tried.append({"url": primary, "inference_context": ctx})
        out = analyze_interior_from_image_url(primary, inference_context=ctx)
        if out:
            break
    diag = None if out else last_ollama_diagnostics()
    print(
        json.dumps(
            {
                "vin": args.vin.strip(),
                "image_url": tried[-1]["url"] if tried else None,
                "inference_context": tried[-1]["inference_context"] if tried else None,
                "tried": tried,
                "result": out,
                "diagnostics": diag,
            },
            indent=2,
        )
    )


def _cmd_classify(args: argparse.Namespace) -> None:
    out = classify_listing_image_from_url(args.url.strip())
    print(json.dumps(out, indent=2))


def _cmd_filter_gallery(args: argparse.Namespace) -> None:
    kept = filter_gallery_urls_for_vehicle_listing(args.urls, max_workers=max(1, args.workers))
    print(json.dumps(kept, indent=2))


def _cmd_monroney(args: argparse.Namespace) -> None:
    out = analyze_monroney_sticker_from_image_url(args.url.strip())
    print(json.dumps(out, indent=2))


def _cmd_backfill_interior(args: argparse.Namespace) -> None:
    apply_inventory_db_defaults(args.db)
    vins, scan_stats = collect_vins_missing_interior_with_images(
        limit=args.limit,
        offset=max(0, args.offset),
    )
    print("Scan (active listings):")
    for k in (
        "rows_active",
        "interior_missing",
        "has_http_image",
        "queued",
        "truncated_after_limit",
    ):
        if k in scan_stats:
            print(f"  {k}: {scan_stats[k]}")
    if args.dry_run:
        print("\nDry run — no vision calls. First 20 VINs that would run:")
        for v in vins[:20]:
            print(f"  {v}")
        return
    if not vins:
        print("Nothing to process.")
        return
    print(f"\nRunning interior vision on {len(vins)} VIN(s) (Ollama LLaVA)…")
    result = run_interior_vision_for_inventory_vins(vins, skip_if_interior_present=True)
    print(json.dumps(result, indent=2))


def main(argv: list[str] | None = None) -> None:
    argv = argv if argv is not None else sys.argv[1:]
    parser = argparse.ArgumentParser(
        description="Listing image analysis (Ollama LLaVA). Run from repo root: python -m backend.vision.analyze_images …",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_i = sub.add_parser("interior", help="Infer interior colors from one image URL or a VIN row")
    g = p_i.add_mutually_exclusive_group(required=True)
    g.add_argument("--url", help="Direct HTTPS image URL")
    g.add_argument("--vin", help="VIN in inventory.db (picks gallery/hero like post-scan)")
    p_i.add_argument(
        "--through-windows",
        action="store_true",
        help="With --url: treat frame as exterior and read cabin through glass (default for --url is direct cabin)",
    )
    p_i.add_argument(
        "--db",
        default=None,
        help="inventory.db path (only for --vin; default: <repo>/inventory.db)",
    )
    p_i.set_defaults(_handler=_cmd_interior)

    p_c = sub.add_parser("classify", help="Classify a single gallery image (keep / category)")
    p_c.add_argument("--url", required=True)
    p_c.set_defaults(_handler=_cmd_classify)

    p_f = sub.add_parser("filter-gallery", help="Filter URL list to vehicle-related images")
    p_f.add_argument("urls", nargs="+", help="Image URLs in order")
    p_f.add_argument("--workers", type=int, default=1)
    p_f.set_defaults(_handler=_cmd_filter_gallery)

    p_m = sub.add_parser("monroney", help="Parse a window-sticker photo URL")
    p_m.add_argument("--url", required=True)
    p_m.set_defaults(_handler=_cmd_monroney)

    p_b = sub.add_parser(
        "backfill-interior",
        help="Backfill missing interior_color for rows with HTTP images",
    )
    p_b.add_argument("--db", default=None, help="inventory.db path (default: <repo>/inventory.db)")
    p_b.add_argument("--limit", type=int, default=None)
    p_b.add_argument("--offset", type=int, default=0)
    p_b.add_argument("--dry-run", action="store_true")
    p_b.set_defaults(_handler=_cmd_backfill_interior)

    args = parser.parse_args(argv)
    handler = getattr(args, "_handler", None)
    if handler is None:
        parser.print_help()
        sys.exit(2)
    handler(args)


if __name__ == "__main__":
    main()
