#!/usr/bin/env python3
"""
Discover dealerships (DMV → OSM → DDG), merge HTTPS rows into ``dealers.json``,
and optionally run the inventory scanner.

With **no** ``--zip``: every ZCTA in your gazetteer file under ``backend/ZIPs/``
(or ``DISCOVERY_ZCTA_GAZETTEER`` / ``--zcta-gazetteer``) is processed in order.
Search radius defaults from each ZCTA's land area unless you pass ``--radius``.

Examples::

  python run_dealership_pipeline.py
  python run_dealership_pipeline.py --max-zips 20 -v
  python run_dealership_pipeline.py --zip 28210
  python run_dealership_pipeline.py --zip 28210 --radius 25 --dmv-state NC
  python run_dealership_pipeline.py --max-zips 5 --scan
  python run_dealership_pipeline.py --scan -- --dealer-id 5

Arguments after ``--`` are passed to ``scanner.py`` (only when ``--scan`` is set).
"""
from __future__ import annotations

import argparse
import copy
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
os.chdir(ROOT)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from backend.utils.project_env import load_project_dotenv

    load_project_dotenv()
except ImportError:
    pass


def _split_argv(argv: list[str]) -> tuple[list[str], list[str]]:
    if "--" in argv:
        i = argv.index("--")
        return argv[:i], argv[i + 1 :]
    return argv, []


def _build_discovery_argv(ns: argparse.Namespace) -> list[str]:
    out = [
        "--zip",
        ns.zip,
        "--radius",
        str(ns.radius),
    ]
    if ns.merge_manifest:
        out.append("--merge-manifest")
    if ns.dmv_state:
        out.extend(["--dmv-state", ns.dmv_state])
    if ns.no_ddg:
        out.append("--no-ddg")
    if ns.persist:
        out.append("--persist")
    if ns.verbose:
        out.append("-v")
    if ns.overpass_timeout is not None:
        out.extend(["--overpass-timeout", str(ns.overpass_timeout)])
    if ns.overpass_url:
        out.extend(["--overpass-url", ns.overpass_url])
    if ns.ddg_timeout is not None:
        out.extend(["--ddg-timeout", str(ns.ddg_timeout)])
    if ns.zcta_gazetteer:
        out.extend(["--zcta-gazetteer", ns.zcta_gazetteer])
    if ns.allow_adjacent_zips:
        out.append("--allow-adjacent-zips")
    if ns.json:
        out.append("--json")
    return out


def main() -> int:
    pipeline_raw, scanner_argv = _split_argv(sys.argv[1:])
    p = argparse.ArgumentParser(
        description="Run dealership discovery from gazetteer ZCTAs or a single ZIP; optionally scan inventory.",
    )
    p.add_argument(
        "--zip",
        default=None,
        metavar="ZIP",
        help="Single 5-digit ZIP/ZCTA (omit to process every code in the gazetteer file)",
    )
    p.add_argument(
        "--radius",
        type=float,
        default=None,
        help="Search radius in miles (default: from ZCTA land area in gazetteer, else 15)",
    )
    p.add_argument(
        "--max-zips",
        type=int,
        default=0,
        metavar="N",
        help="When using gazetteer batch mode, stop after N ZCTAs (0 = no limit)",
    )
    p.add_argument("--dmv-state", default=None, metavar="ST", help="DMV CSV tier (e.g. NC)")
    p.add_argument(
        "--no-merge-manifest",
        action="store_true",
        help="Skip merging discovery rows into dealers.json (default: merge)",
    )
    p.add_argument("--no-ddg", action="store_true", help="Skip DuckDuckGo URL gap-fill")
    p.add_argument("--persist", action="store_true", help="Upsert into inventory.db dealerships table")
    p.add_argument("--json", action="store_true", help="Print discovery JSON to stdout (single-ZIP only)")
    p.add_argument("-v", "--verbose", action="store_true", help="Verbose discovery logging")
    p.add_argument("--overpass-timeout", type=float, default=None, help="Overpass HTTP timeout (s)")
    p.add_argument("--overpass-url", default=None, metavar="URL", help="Primary Overpass endpoint")
    p.add_argument("--ddg-timeout", type=float, default=None, help="DDG HTTP timeout (s)")
    p.add_argument("--zcta-gazetteer", default=None, metavar="PATH", help="ZCTA gazetteer file path")
    p.add_argument(
        "--allow-adjacent-zips",
        action="store_true",
        help="Keep dealers outside the seed ZIP (default: seed ZCTA only)",
    )
    p.add_argument(
        "--scan",
        action="store_true",
        help="After discovery, run scanner.py (pass extra scanner flags after --)",
    )
    ns = p.parse_args(pipeline_raw)
    ns.merge_manifest = not ns.no_merge_manifest

    from backend.discovery.normalize import normalize_zip
    from backend.discovery.zcta_gazetteer import (
        default_gazetteer_file,
        iter_zcta_zip_codes,
        lookup_zcta_row,
        suggested_search_radius_miles,
    )

    if ns.zcta_gazetteer:
        gp = Path(ns.zcta_gazetteer).expanduser()
        if not gp.is_file():
            print(f"error: --zcta-gazetteer not found: {gp}", file=sys.stderr)
            return 2
        gazetteer_path: Path | None = gp
    else:
        gazetteer_path = default_gazetteer_file(ROOT)

    if ns.zip:
        z_norm = normalize_zip(ns.zip)
        if not z_norm:
            print("error: --zip must be a valid 5-digit US ZIP/ZCTA", file=sys.stderr)
            return 2
        zips = [z_norm]
    else:
        if gazetteer_path is None:
            print(
                "error: no gazetteer found. Add a pipe-delimited ZCTA file under backend/ZIPs/, "
                "set DISCOVERY_ZCTA_GAZETTEER, or pass --zcta-gazetteer / --zip.",
                file=sys.stderr,
            )
            return 2
        zips = iter_zcta_zip_codes(gazetteer_path)
        if ns.max_zips > 0:
            zips = zips[: ns.max_zips]
        if len(zips) > 500:
            print(
                f"warning: batch discovery for {len(zips)} ZCTAs (long run, heavy on Overpass/DDG); "
                "use --max-zips to cap",
                file=sys.stderr,
            )

    json_flag = bool(ns.json)
    if len(zips) > 1 and ns.json:
        print("warning: ignoring --json in batch mode", file=sys.stderr)
        json_flag = False

    from backend.discovery.cli import main as discovery_main

    for z in zips:
        run_ns = copy.copy(ns)
        run_ns.zip = z
        if run_ns.radius is not None:
            run_ns.radius = float(run_ns.radius)
        else:
            row = lookup_zcta_row(gazetteer_path, z) if gazetteer_path else None
            run_ns.radius = float(suggested_search_radius_miles(row))
        run_ns.json = json_flag
        d_code = discovery_main(_build_discovery_argv(run_ns), project_root=ROOT)
        if d_code != 0:
            return d_code

    if ns.scan:
        cmd = [sys.executable, str(ROOT / "scanner.py"), *scanner_argv]
        return subprocess.call(cmd)

    if scanner_argv:
        print("warning: scanner arguments ignored (use --scan before --)", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
