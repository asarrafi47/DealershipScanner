#!/usr/bin/env python3
"""
Remove `{Year}_{Make}_{Model}_Complete_Options.csv` files that are not mainstream U.S. passenger
vehicles (cars / SUVs / light trucks / consumer pickups / minivans), based on Make + Model in the
first data row. Intended for output from ``car_data_scraper.py`` after NHTSA mixes in motorcycles,
buses, medium-duty trucks, junk makes, etc.

  python scripts/prune_car_options_csv_passenger.py [--dry-run] [--dir csv_out]
"""
from __future__ import annotations

import argparse
import csv
import os
import re
import sys


ALLOWED_MAKES = frozenset({
    "Acura", "Alfa Romeo", "Audi", "BMW", "Buick", "Cadillac", "Chevrolet", "Chrysler", "Dodge",
    "FIAT", "Ford", "Genesis", "GMC", "Honda", "Hyundai", "Infiniti", "Jaguar", "Jeep", "Kia",
    "Land Rover", "Lexus", "Lincoln", "Lucid", "Mazda", "Mercedes", "MINI", "Mitsubishi", "Nissan",
    "Polestar", "Porsche", "Ram", "Rivian", "Subaru", "Tesla", "Toyota", "Volkswagen", "Volvo",
})

HONDA_PASSENGER = frozenset({
    "Accord", "Civic", "CR-V", "HR-V", "Odyssey", "Passport", "Pilot", "Ridgeline", "Insight",
    "Fit", "Clarity", "CR-Z", "Element", "Crosstour",
})

VOLVO_PASSENGER = frozenset({
    "S60", "S90", "V60", "V60CC", "V90", "V90CC", "XC40", "XC60", "XC90", "C40", "EX30", "EX40",
    "EC40", "EX90", "V40", "C30",
})

_BMW_MOTORCYCLE = re.compile(
    r"S\s*1000|K\s*1600|R\s*nineT|G\s*310|F\s*800|R\s*1200|C\s*400|G\s*650|K\s*1300|R\s*1250|"
    r"R\s*18\b|F\s*750|F\s*700|F\s*850|C\s*650|F\s*900",
    re.I,
)


def keep_passenger(make: str, model: str) -> bool:
    mk = make.strip()
    md = model.strip()
    if mk not in ALLOWED_MAKES:
        return False
    if mk == "Honda":
        return md in HONDA_PASSENGER
    if mk == "Volvo":
        return md in VOLVO_PASSENGER
    if mk == "Ram":
        return (
            bool(re.fullmatch(r"(1500|2500|3500|4500|5500)", md))
            or md.startswith("ProMaster")
            or md.startswith("Promaster")
        )
    if mk == "Tesla":
        return md != "Semi"
    if mk == "Rivian":
        return md in ("R1T", "R1S")
    if mk == "BMW":
        return _BMW_MOTORCYCLE.search(md) is None
    if mk == "Mercedes":
        if re.match(r"^(L[P]?|LPS)\d", md):
            return False
        if md in ("Sprinter", "Metris", "eSprinter"):
            return False
        return True
    if mk == "Mitsubishi":
        if md.startswith(("FE", "FM", "FK", "FG", "FH", "FEC", "FGB")):
            return False
        if "Canter" in md or md.startswith("Low Speed"):
            return False
        return True
    if mk == "Chevrolet":
        deny_exact = {
            "7500XD", "6500XD", "5500XD", "4500XD", "W7", "W5", "W4", "Bolt Incomplete",
            "Express", "Caprice Police Vehicle",
        }
        if md in deny_exact:
            return False
        if re.fullmatch(r"(4500|5500|3500)(HD|HG|XG)?", md):
            return False
        return True
    if mk == "Ford":
        deny = {
            "Commercial Chassis", "Motorhome Chassis", "Bradford Built", "Cordova Sedan", "Classic Sedan",
            "Malibu Sedan", "Medford Steel", "Milford Pipe & Supply", "Bradford #1", "Swinford Mfg",
            "Lyford Overland",
        }
        if md in deny:
            return False
        if md.startswith("Transit") and not md.startswith("Transit Connect"):
            return False
        if md in ("F-650", "F-750", "F-600"):
            return False
        return True
    if mk == "GMC":
        return md not in ("Cruise Origin AV", "Savana")
    if mk == "Nissan":
        if md in ("NV", "NV200") or md.startswith("NV2500") or md.startswith("NV3500"):
            return False
        return True
    if mk == "Dodge":
        return md not in ("480 Series", "580 Series", "Dodgen Industries", "Sprinter")
    if mk == "Toyota":
        return md != "FCHV-adv"
    return True


def main() -> int:
    ap = argparse.ArgumentParser(description="Prune non-passenger rows from car_data_scraper CSV output.")
    ap.add_argument("--dir", default="csv_out", help="Directory containing *_Complete_Options.csv files.")
    ap.add_argument("--dry-run", action="store_true", help="Print counts only; do not delete.")
    args = ap.parse_args()
    root = os.path.abspath(args.dir)
    if not os.path.isdir(root):
        print(f"Not a directory: {root}", file=sys.stderr)
        return 2

    to_remove: list[str] = []
    for fn in os.listdir(root):
        if not fn.endswith("_Complete_Options.csv"):
            continue
        path = os.path.join(root, fn)
        try:
            with open(path, newline="", encoding="utf-8") as f:
                r = csv.DictReader(f)
                row = next(r, None)
            if not row:
                to_remove.append(path)
                continue
            mk = (row.get("Make") or "").strip()
            md = (row.get("Model") or "").strip()
            if not keep_passenger(mk, md):
                to_remove.append(path)
        except OSError as e:
            print(f"Skip {path}: {e}", file=sys.stderr)

    print(f"{'Would remove' if args.dry_run else 'Removing'} {len(to_remove)} file(s) under {root}")
    if args.dry_run:
        return 0
    for path in to_remove:
        try:
            os.remove(path)
        except OSError as e:
            print(f"Failed {path}: {e}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
