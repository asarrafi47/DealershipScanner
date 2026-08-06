"""Build ladders from EPA, DT-options and complete-options CSVs."""
from __future__ import annotations

import csv
import logging
import re
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

from ._common import (
    _is_valid_trim_name,
    _norm_make,
    _norm_token,
)
from .epa import (
    _epa_models_match,
    _normalize_epa_trim,
)
from .loaders import (
    _steps_from_csv_rows,
)
from .plausibility import (
    _complete_options_ladder_is_junk,
    _finalize_ladder_steps,
    _ladder_steps_usable,
)

def _ladder_from_epa_csv(path: Path, make: str, model: str, year: Any) -> dict[str, Any] | None:
    from backend.enrichment.trim_ladder_knowledge import epa_model_search_name

    model_label = epa_model_search_name(make, model)
    try:
        with path.open(encoding="utf-8", newline="") as fh:
            rows = list(csv.DictReader(fh))
    except OSError:
        return None

    steps: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        trim_raw = (row.get("Trim") or "").strip()
        if not trim_raw:
            continue
        row_make = (row.get("Make") or make or "").strip()
        row_model = (row.get("Model") or model or "").strip()
        if _norm_make(row_make) != _norm_make(make):
            continue
        if not _epa_models_match(model_label, row_model):
            continue
        name = _normalize_epa_trim(trim_raw, make, model)
        if not name:
            continue
        if not _is_valid_trim_name(name, make=make, model=model):
            continue
        key = _norm_token(name)
        if key in seen:
            continue
        seen.add(key)
        adds: list[str] = []
        engine = (row.get("engineDisplay") or row.get("engineOptions") or "").strip()
        if engine:
            adds.append(engine)
        drive = (row.get("drivetrainOptions") or "").strip()
        if drive:
            adds.append(drive)
        steps.append(
            {
                "name": name,
                "aliases": [],
                "adds": adds[:3],
            }
        )

    if len(steps) < 2:
        return None

    y_match = re.match(r"^(\d{4})_", path.name)
    yr = int(y_match.group(1)) if y_match else None
    ladder = {
        "id": path.stem.lower(),
        "make": make,
        "models": [model],
        "year_min": (yr - 2) if yr else 0,
        "year_max": (yr + 2) if yr else 9999,
        "label": f"{make} {model} trim lineup",
        "source": path.name,
        "steps": steps,
    }
    return _finalize_ladder_steps(ladder, make, model)


def _ladder_from_dt_csv(path: Path) -> dict[str, Any] | None:
    try:
        with path.open(encoding="utf-8", newline="") as fh:
            rows = list(csv.DictReader(fh))
    except OSError:
        return None
    if not rows:
        return None

    make = ""
    model = ""
    year_min: int | None = None
    year_max: int | None = None
    for row in rows:
        if row.get("Make"):
            make = str(row["Make"]).strip()
        if row.get("Model"):
            model = str(row["Model"]).strip()
        yr_raw = (row.get("Year") or "").strip()
        if yr_raw.isdigit():
            yr = int(yr_raw)
            year_min = yr if year_min is None else min(year_min, yr)
            year_max = yr if year_max is None else max(year_max, yr)

    steps = _steps_from_csv_rows(rows, make, model)
    if not make or not model or len(steps) < 2:
        return None

    stem = path.stem.replace("_Complete_Options", "")
    ladder = {
        "id": stem.lower(),
        "make": make,
        "models": [model, re.sub(r"\s*\(dt\)\s*", "", model, flags=re.I).strip()],
        "year_min": year_min or 0,
        "year_max": year_max or 9999,
        "label": f"{make} {model} trim lineup",
        "source": path.name,
        "steps": steps,
    }
    return _finalize_ladder_steps(ladder, make, model)


def _ladder_from_complete_options_csv(path: Path, make: str, model: str, year: Any) -> dict[str, Any] | None:
    try:
        with path.open(encoding="utf-8", newline="") as fh:
            rows = list(csv.DictReader(fh))
    except OSError:
        return None
    csv_make = make
    csv_model = model
    for row in rows:
        if row.get("Make"):
            csv_make = str(row["Make"]).strip() or csv_make
        if row.get("Model"):
            csv_model = str(row["Model"]).strip() or csv_model
        if csv_make and csv_model:
            break

    steps = _steps_from_csv_rows(rows, csv_make, csv_model)
    if not _ladder_steps_usable(steps, csv_make):
        return None
    try:
        blob = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        blob = ""
    if _complete_options_ladder_is_junk(steps, csv_make, csv_model, source_blob=blob):
        return None

    y_match = re.match(r"^(\d{4})_", path.name)
    yr = int(y_match.group(1)) if y_match else None
    ladder = {
        "id": path.stem.lower(),
        "make": csv_make,
        "models": [csv_model, model],
        "year_min": (yr - 2) if yr else 0,
        "year_max": (yr + 2) if yr else 9999,
        "label": f"{csv_make} {csv_model} trim lineup",
        "source": path.name,
        "steps": steps,
    }
    return _finalize_ladder_steps(ladder, csv_make, model)
