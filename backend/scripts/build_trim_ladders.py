#!/usr/bin/env python3
"""
Generate ``backend/dictionary/trim_ladders_generated.json`` from dictionary CSVs.

Run from repo root::

    python -m backend.scripts.build_trim_ladders
"""
from __future__ import annotations

import csv
import json
import re
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from backend.enrichment.trim_ladder import (  # noqa: E402
    _DICTIONARY_DIR,
    _complete_options_ladder_is_junk,
    _extract_trim_from_cell,
    _is_valid_trim_name,
    _ladder_from_complete_options_csv,
    _ladder_from_dt_csv,
    _norm_make,
    _steps_from_csv_rows,
)
from backend.enrichment.trim_ladder_knowledge import (  # noqa: E402
    extract_trims_from_text,
    merge_trim_names,
    normalize_ladder_steps,
)

_OUTPUT = _DICTIONARY_DIR / "trim_ladders_generated.json"
_MIN_STEPS = 2


def _parse_year_make_model(path: Path) -> tuple[int | None, str, str]:
    stem = path.stem.replace("_Complete_Options", "")
    m = re.match(r"^(\d{4})_(.+)$", stem)
    if not m:
        return None, "", ""
    year = int(m.group(1))
    rest = m.group(2)
    parts = rest.split("_", 1)
    if len(parts) < 2:
        return year, rest.replace("_", " "), ""
    return year, parts[0].replace("_", " "), parts[1].replace("_", " ")


def _column_trims(path: Path, make: str, model: str) -> list[str]:
    try:
        with path.open(encoding="utf-8", newline="") as fh:
            rows = list(csv.DictReader(fh))
    except OSError:
        return []
    names: list[str] = []
    for row in rows:
        raw = (row.get("Trim") or "").strip()
        if not raw:
            continue
        name = _extract_trim_from_cell(raw, make, model)
        if name and _is_valid_trim_name(name):
            names.append(name)
    return names


def _ladder_from_path(path: Path) -> dict | None:
    if path.name.endswith("_DT_Complete_Options.csv"):
        return _ladder_from_dt_csv(path)

    year, make, model = _parse_year_make_model(path)
    if not make or not model:
        return None

    try:
        blob = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        blob = ""

    col_trims = _column_trims(path, make, model)
    text_trims = extract_trims_from_text(make, blob)
    merged = merge_trim_names(col_trims, text_trims, make=make, limit=14)

    steps: list[dict] = []
    if len(merged) >= _MIN_STEPS:
        for name in merged:
            steps.append(
                {
                    "name": name,
                    "aliases": [],
                    "adds": [f"Equipment and features typical of the {name} trim."],
                }
            )
    else:
        ladder = _ladder_from_complete_options_csv(path, make, model, year)
        if ladder and len(ladder.get("steps") or []) >= _MIN_STEPS:
            return ladder
        try:
            with path.open(encoding="utf-8", newline="") as fh:
                rows = list(csv.DictReader(fh))
            for step in _steps_from_csv_rows(rows, make, model):
                steps.append(step)
        except OSError:
            pass

    if len(steps) < _MIN_STEPS:
        merged2 = merge_trim_names(merged, extract_trims_from_text(make, blob, limit=20), make=make)
        if len(merged2) >= _MIN_STEPS:
            steps = [
                {
                    "name": n,
                    "aliases": [],
                    "adds": [f"Equipment and features typical of the {n} trim."],
                }
                for n in merged2
            ]

    if len(steps) < _MIN_STEPS:
        return None

    steps = normalize_ladder_steps(steps, make)
    if len(steps) < _MIN_STEPS:
        return None

    try:
        blob = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        blob = ""
    if _complete_options_ladder_is_junk(steps, make, model, source_blob=blob):
        return None

    yr = year or 0
    lid = path.stem.lower()
    return {
        "id": lid,
        "make": make,
        "models": [model, re.sub(r"\s+", " ", model).strip()],
        "year_min": (yr - 2) if yr else 0,
        "year_max": (yr + 2) if yr else 9999,
        "label": f"{make} {model} trim lineup",
        "source": path.name,
        "steps": steps,
    }


def main() -> int:
    paths = sorted(_DICTIONARY_DIR.glob("*_Complete_Options.csv"))
    ladders: list[dict] = []
    seen_ids: set[str] = set()
    skipped = 0

    for path in paths:
        ladder = _ladder_from_path(path)
        if not ladder:
            skipped += 1
            continue
        lid = str(ladder.get("id") or "")
        if not lid or lid in seen_ids:
            continue
        seen_ids.add(lid)
        ladders.append(ladder)

    payload = {
        "version": 1,
        "generated_from": "dictionary Complete_Options CSVs",
        "ladder_count": len(ladders),
        "ladders": ladders,
    }
    _OUTPUT.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Wrote {len(ladders)} ladders to {_OUTPUT} ({skipped} files skipped)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
