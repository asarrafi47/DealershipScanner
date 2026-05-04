"""
CLI for tiered dealership discovery (DMV → OSM → DDG URL gap-fill).

Run from repository root::

  python -m backend.discovery.cli --zip 28210 --radius 25 --json

Or use ``scripts/discover_dealerships.py`` (same implementation).

Suppresses known-noisy ``urllib3``/charset mismatch noise from ``requests`` on import
without importing ``requests`` first (avoids triggering that warning during ``--help``).
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import warnings
from pathlib import Path

warnings.filterwarnings(
    "ignore",
    message=r".*doesn't match a supported version.*",
)


def _default_project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Discover dealerships near a US ZIP + radius.")
    p.add_argument("--zip", required=True, help="5-digit US ZIP")
    p.add_argument("--radius", type=float, required=True, help="Radius in miles (positive, max 500)")
    p.add_argument(
        "--dmv-state",
        default=None,
        help="Optional 2-letter state code for DMV CSV tier (e.g. NC)",
    )
    p.add_argument(
        "--no-ddg",
        action="store_true",
        help="Skip DuckDuckGo Instant Answer URL gap-fill",
    )
    p.add_argument(
        "--persist",
        action="store_true",
        help="Upsert rows into inventory.db dealerships table",
    )
    p.add_argument("--json", action="store_true", help="Print JSON array to stdout")
    p.add_argument("--overpass-timeout", type=float, default=90.0, help="Overpass HTTP timeout (seconds)")
    p.add_argument(
        "--overpass-url",
        default=None,
        metavar="URL",
        help="Primary Overpass API endpoint (default: overpass-api.de; mirrors retried on 502/504 unless DISCOVERY_OVERPASS_NO_FALLBACK=1)",
    )
    p.add_argument("--ddg-timeout", type=float, default=15.0, help="DDG HTTP timeout (seconds)")
    p.add_argument("-v", "--verbose", action="store_true", help="Log to stderr")
    p.add_argument(
        "--merge-manifest",
        action="store_true",
        help="After discovery, upsert rows with HTTPS urls into dealers.json for python scanner.py",
    )
    p.add_argument(
        "--zcta-gazetteer",
        default=None,
        metavar="PATH",
        help="Pipe-delimited ZCTA gazetteer (GEOID|…|INTPTLAT|INTPTLONG); "
        "default: backend/ZIPs/*.txt or DISCOVERY_ZCTA_GAZETTEER",
    )
    p.add_argument(
        "--allow-adjacent-zips",
        action="store_true",
        help="Include dealerships whose ZIP is not the seed ZIP after enrichment "
        "(default: keep only dealers tagged with the seed ZCTA/ZIP)",
    )
    p.add_argument(
        "--skip-osm",
        action="store_true",
        help="Skip OpenStreetMap tier (useful for city-based discovery or when Overpass unavailable)",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None, *, project_root: Path | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.WARNING,
        format="%(levelname)s %(name)s: %(message)s",
    )
    root = project_root or _default_project_root()

    from backend.discovery.pipeline import run_discovery

    try:
        gazetteer_path = (
            Path(args.zcta_gazetteer).expanduser() if args.zcta_gazetteer else None
        )
        rows = run_discovery(
            args.zip,
            args.radius,
            dmv_state=args.dmv_state,
            persist=args.persist,
            fill_urls_via_ddg=not args.no_ddg,
            project_root=root,
            gazetteer_path=gazetteer_path,
            within_seed_zip_only=not args.allow_adjacent_zips,
            overpass_url=args.overpass_url,
            overpass_timeout_s=args.overpass_timeout,
            ddg_timeout_s=args.ddg_timeout,
            skip_osm=args.skip_osm,
        )
    except ValueError as e:
        print(str(e), file=sys.stderr)
        return 2

    if args.merge_manifest:
        from backend.discovery.manifest_merge import merge_candidates_into_scanner_manifest

        manifest_path = (root / "dealers.json").resolve()
        if not rows:
            print(
                "WARNING: discovery returned 0 dealerships (Overpass timeout/unavailable or empty DMV/OSM). "
                "dealers.json unchanged — retry later or set DISCOVERY_OVERPASS_URLS / increase --overpass-timeout.",
                file=sys.stderr,
            )
        stats = merge_candidates_into_scanner_manifest(rows, manifest_path=manifest_path)
        print(
            "dealers.json [%s] merge: inserted=%d updated=%d skipped=%d "
            "(manifest requires HTTPS url — skipped rows had none after DMV+OSM+DDG tiers)"
            % (manifest_path, stats["inserted"], stats["updated"], stats["skipped"]),
            file=sys.stderr,
        )

    if args.json:
        payload = []
        for r in rows:
            d = r.to_db_dict()
            payload.append(
                {
                    "name": d["name"],
                    "street_address": d["street_address"],
                    "zip_code": d["zip_code"],
                    "city": d["city"],
                    "state": d["state"],
                    "latitude": d["latitude"],
                    "longitude": d["longitude"],
                    "url": d["dealer_website_url"] or d["website_url"],
                    "source_dmv": bool(d["source_dmv"]),
                    "source_osm": bool(d["source_osm"]),
                    "source_web": bool(d["source_web"]),
                    "osm_id": d.get("osm_id") or "",
                }
            )
        print(json.dumps(payload, indent=2))
        return 0

    for r in rows:
        d = r.to_db_dict()
        addr = d["street_address"] or "(no street)"
        z = d["zip_code"] or "(no zip)"
        u = d["dealer_website_url"] or "(no url)"
        flags = []
        if d["source_dmv"]:
            flags.append("dmv")
        if d["source_osm"]:
            flags.append("osm")
        if d["source_web"]:
            flags.append("web")
        src = "+".join(flags) if flags else "?"
        print(f"{d['name']} | {addr} | {z} | {u} | [{src}]")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
