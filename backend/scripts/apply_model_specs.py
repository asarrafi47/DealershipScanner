#!/usr/bin/env python3
"""
Fleet-wide model_specs backfill (P8B.8).

Post-scan no longer calls ``apply_model_specs_corrections()`` for the whole fleet
(the per-VIN call after each upsert stays), so this script keeps the fleet pass
reachable. Every fill is a ``model_specs`` proposal (lowest rank) decided by the
fact-precedence helper: a stored higher-ranked value stays, the vPIC two-wheel
constraint vetoes an all-wheel fill, and provenance is written per field.

Usage:
  python apply_model_specs.py --all --dry-run     # count what would be filled
  python apply_model_specs.py --all               # fleet backfill
  python apply_model_specs.py --vin <VIN> [--vin <VIN> ...]

Defaults to a dry run unless ``--apply`` is given.
"""
import argparse
import logging
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))

try:
    from backend.utils.project_env import load_project_dotenv
    load_project_dotenv()
except ImportError:
    pass

logger = logging.getLogger("apply_model_specs")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--all", action="store_true", help="every row in cars (the fleet-wide backfill)")
    ap.add_argument("--vin", action="append", default=[], help="a VIN to fill (repeatable)")
    ap.add_argument("--apply", action="store_true", help="write the fills (default is a dry run)")
    ap.add_argument("--dry-run", action="store_true", help="report only (the default)")
    args = ap.parse_args(argv)

    if bool(args.all) == bool(args.vin):
        ap.error("give exactly one of --all or --vin")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    from backend.scanner.database import apply_model_specs_corrections

    dry = args.dry_run or not args.apply
    n = apply_model_specs_corrections(vins=None if args.all else list(args.vin), dry_run=dry)
    print(f"{'would fill' if dry else 'filled'} {n} row(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
