#!/usr/bin/env python3
"""
Generate ``backend/dictionary/trim_ladders_epa.json`` from EPA ``*_EPA.csv`` files.

EPA FuelEconomy.gov is the authoritative U.S. source for which trims exist per
year/make/model (xDrive40i, xDrive50i, Limited, etc.) plus engine/drivetrain facts.
Unlike ``*_Complete_Options.csv`` (Wikipedia scrapes), EPA rows do not contain
package prose or "includes codes for…" placeholders.

Prerequisites — refresh EPA dictionary files::

    curl -L -o /tmp/vehicles.csv.zip https://www.fueleconomy.gov/feg/epadata/vehicles.csv.zip
    unzip -o /tmp/vehicles.csv.zip -d /tmp/
    python backend/scripts/import_epa_to_dictionary.py --epa-csv /tmp/vehicles.csv --min-year 2000

Then build EPA ladders::

    python -m backend.scripts.build_trim_ladders_from_epa
    python -m backend.scripts.build_epa_master

Live API (no bulk download) uses the same data via ``backend/oem/vehicle_reference/sources/epa_client.py``::
    menu/model?year=2019&make=BMW  →  X5 xDrive40i, X5 xDrive50i, …
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from backend.enrichment.dictionary_catalog import iter_dictionary_csv_paths
from backend.enrichment.dictionary_paths import DICTIONARY_ROOT, trim_ladders_epa_path
from backend.enrichment.trim_ladder import (  # noqa: E402
    _ladder_from_epa_csv,
    _ladder_steps_plausible_for_model,
)

_OUTPUT = trim_ladders_epa_path()


def _parse_epa_path(path: Path) -> tuple[int, str, str] | None:
    m = re.match(r"^(\d{4})_(.+)_EPA$", path.stem)
    if not m:
        return None
    year = int(m.group(1))
    rest = m.group(2)
    parts = rest.split("_", 1)
    if len(parts) < 2:
        return year, rest.replace("_", " "), ""
    return year, parts[0].replace("_", " "), parts[1].replace("_", " ")


def main() -> int:
    paths = sorted(iter_dictionary_csv_paths("epa"))
    if not paths:
        print(f"No *_EPA.csv files under {DICTIONARY_ROOT}. Run import_epa_to_dictionary.py first.")
        return 1

    ladders: list[dict] = []
    seen_ids: set[str] = set()
    skipped = 0

    for path in paths:
        parsed = _parse_epa_path(path)
        if not parsed:
            skipped += 1
            continue
        year, make, model = parsed
        if not make or not model:
            skipped += 1
            continue
        ladder = _ladder_from_epa_csv(path, make, model, year)
        if not ladder:
            skipped += 1
            continue
        if not _ladder_steps_plausible_for_model(ladder.get("steps") or [], make, model):
            skipped += 1
            continue
        lid = str(ladder.get("id") or "").strip().lower()
        if not lid or lid in seen_ids:
            continue
        seen_ids.add(lid)
        ladder["source"] = "EPA CSV"
        ladders.append(ladder)

    payload = {
        "version": 1,
        "generated_from": "EPA FuelEconomy.gov *_EPA.csv dictionary files",
        "source_url": "https://www.fueleconomy.gov/feg/epadata/vehicles.csv.zip",
        "ladder_count": len(ladders),
        "ladders": ladders,
    }
    _OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    _OUTPUT.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Wrote {len(ladders)} EPA trim ladders to {_OUTPUT} ({skipped} files skipped)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
