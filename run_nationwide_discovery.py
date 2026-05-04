#!/usr/bin/env python3
"""
Nationwide US car dealership discovery via Overture Maps Places (DuckDB + S3 GeoParquet).

Loads every ``car_dealer`` place in the US from the latest STAC-resolved Overture release, then
optionally merges **scanner-ready** rows into project-root ``dealers.json`` — same contract as
``run_city_discovery.py`` / ``backend.discovery.manifest_merge`` (HTTPS ``url``, ``dealer_dot_com``,
``dealer_id`` from hostname). Compatible with ``python scanner.py`` and Node ``scanner.js``.

Examples::

  python run_nationwide_discovery.py
  python run_nationwide_discovery.py --no-merge-manifest
  python run_nationwide_discovery.py --manifest ./dealers.json --release 2026-04-15.0
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from backend.utils.project_env import load_project_dotenv

    load_project_dotenv()
except ImportError:
    pass


def _build_manifest_rows(rows: list[dict]) -> list[dict[str, str]]:
    """
    Build ``merge_json_export_rows`` payloads: ``name`` + manifest-normalized ``url``.

    Keeps only OEM/franchised names (see :func:`backend.discovery.overture_discovery.is_franchised_dealer`).
    Deduplicates by normalized URL so we do not upsert the same ``dealer_id`` repeatedly when
    Overture repeats a site across POIs.
    """
    from backend.dev.dealers import normalize_manifest_url
    from backend.discovery.overture_discovery import is_franchised_dealer

    export: list[dict[str, str]] = []
    seen_url: set[str] = set()

    for r in rows:
        raw_site = str(r.get("website") or "").strip()
        url = normalize_manifest_url(raw_site)
        name = str(r.get("name") or "").strip()
        if not url or not name:
            continue
        if not is_franchised_dealer(name):
            continue
        key = url.lower()
        if key in seen_url:
            continue
        seen_url.add(key)
        export.append({"name": name, "url": url})

    return export


def main() -> int:
    p = argparse.ArgumentParser(
        description="Discover all US car dealers from Overture Places and merge into dealers.json.",
    )
    p.add_argument(
        "--manifest",
        type=Path,
        default=None,
        metavar="PATH",
        help="Path to dealers.json (default: <repo root>/dealers.json)",
    )
    p.add_argument(
        "--no-merge-manifest",
        action="store_true",
        help="Only query Overture and print stats; do not write dealers.json",
    )
    p.add_argument(
        "--release",
        default=None,
        metavar="ID",
        help="Pin Overture release folder (e.g. 2026-04-15.0); default: latest from STAC",
    )
    p.add_argument(
        "--catalog-url",
        default=None,
        help="Override STAC catalog URL (default: Overture public catalog)",
    )
    p.add_argument(
        "--s3-prefix",
        default=None,
        help="Override S3 release prefix (default: s3://overturemaps-us-west-2/release)",
    )
    p.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Limit Overture rows (testing only)",
    )
    p.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Debug logging",
    )
    args = p.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(message)s",
    )
    log = logging.getLogger("run_nationwide_discovery")

    manifest_path = (args.manifest or (ROOT / "dealers.json")).resolve()

    from backend.dev.dealers import normalize_manifest_url
    from backend.discovery.manifest_merge import merge_json_export_rows
    from backend.discovery.overture_discovery import (
        STAC_CATALOG_URL_DEFAULT,
        OVERTURE_S3_BUCKET_RELEASE_PREFIX_DEFAULT,
        connect_overture_duckdb,
        fetch_us_car_dealers_rows,
        is_franchised_dealer,
    )

    catalog_url = (args.catalog_url or STAC_CATALOG_URL_DEFAULT).strip()
    s3_prefix = (args.s3_prefix or OVERTURE_S3_BUCKET_RELEASE_PREFIX_DEFAULT).strip()

    log.info("Nationwide Overture dealership discovery")
    log.info("Downloading DuckDB extensions if needed (httpfs, spatial)...")
    con = connect_overture_duckdb()

    if args.release:
        log.info("Using pinned Overture release folder: %s", args.release.strip())
    else:
        log.info("Release ID will be resolved from STAC (latest).")

    log.info(
        "Querying Overture Places on S3 (US car_dealer). "
        "This may take roughly 1–10+ minutes depending on network and DuckDB caching..."
    )
    release_id, rows = fetch_us_car_dealers_rows(
        con,
        release_id=args.release.strip() if args.release else None,
        catalog_url=catalog_url,
        s3_release_prefix=s3_prefix,
        limit=args.limit,
    )
    log.info("Resolved Overture release: %s", release_id)

    total = len(rows)
    log.info("Found %s US dealership places from Overture (release %s).", total, release_id)

    valid_scanner_urls = sum(
        1 for r in rows if normalize_manifest_url(str(r.get("website") or "").strip())
    )
    log.info(
        "Of those, %s have scanner-ready website URLs (normalize to non-empty HTTPS).",
        valid_scanner_urls,
    )

    with_url_and_name = 0
    franchised_with_url_and_name = 0
    for r in rows:
        raw_site = str(r.get("website") or "").strip()
        if not normalize_manifest_url(raw_site):
            continue
        name = str(r.get("name") or "").strip()
        if not name:
            continue
        with_url_and_name += 1
        if is_franchised_dealer(name):
            franchised_with_url_and_name += 1
    independent_lots = with_url_and_name - franchised_with_url_and_name
    log.info(
        "Filtered out %s independent lots. Kept %s franchised dealerships.",
        independent_lots,
        franchised_with_url_and_name,
    )

    inserted = updated = skipped = 0
    export_rows = _build_manifest_rows(rows)

    if not args.no_merge_manifest:
        log.info("Merging into manifest %s ...", manifest_path)
        stats = merge_json_export_rows(export_rows, manifest_path=manifest_path)
        inserted = int(stats.get("inserted", 0))
        updated = int(stats.get("updated", 0))
        skipped = int(stats.get("skipped", 0))
        log.info(
            "Manifest merge finished: inserted=%s updated=%s skipped=%s",
            inserted,
            updated,
            skipped,
        )
    else:
        log.info("Skipping dealers.json (--no-merge-manifest).")

    # Final operator-facing summary (stdout)
    print("")
    print("=== Nationwide discovery summary ===")
    print(f"  Overture release:        {release_id}")
    print(f"  Total US dealers found:  {total}")
    print(f"  With valid websites:     {valid_scanner_urls}")
    print(f"  Independent lots dropped:{independent_lots} (have URL + name, no OEM token)")
    print(f"  Franchised (URL + name): {franchised_with_url_and_name}")
    if args.no_merge_manifest:
        print("  New rows appended:       (merge skipped)")
        print(f"  Rows eligible to merge: {len(export_rows)} (unique URLs with name + HTTPS)")
    else:
        print(f"  New rows appended:       {inserted}")
        print(f"  Existing rows updated:   {updated}")
        print(f"  Merge skips:             {skipped}")
    print(f"  Manifest path:           {manifest_path}")
    print("")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
