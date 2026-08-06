"""Read ladder definitions off disk (curated / generated / merged / EPA JSON, CSV rows)."""
from __future__ import annotations

import json
import logging
from functools import lru_cache
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

from ._common import (
    _LADDERS_EPA_JSON,
    _LADDERS_GENERATED_JSON,
    _LADDERS_JSON,
    _LADDERS_MERGED_JSON,
    _YEAR_MIN_SUFFIX_RE,
    _YEAR_RANGE_SUFFIX_RE,
    _extract_trim_from_cell,
    _is_valid_trim_name,
    _norm_token,
)
from .attribution import (
    _row_trim_adds,
)

def _read_ladders_file(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    ladders = data.get("ladders")
    return ladders if isinstance(ladders, list) else []


@lru_cache(maxsize=1)
def _load_json_ladders() -> list[dict[str, Any]]:
    return _read_ladders_file(_LADDERS_JSON)


@lru_cache(maxsize=1)
def _load_generated_ladders_raw() -> list[dict[str, Any]]:
    return _read_ladders_file(_LADDERS_GENERATED_JSON)


@lru_cache(maxsize=1)
def _load_generated_ladders() -> list[dict[str, Any]]:
    merged = _load_merged_ladders()
    if merged:
        return [
            lad
            for lad in merged
            if str(lad.get("source") or "").lower()
            not in {
                "epa",
                "epa_fueleconomy",
                "epa csv",
                "epa_fueleconomy.gov",
                "brochure",
            }
        ]
    return _load_generated_ladders_raw()


@lru_cache(maxsize=1)
def _load_merged_ladders() -> list[dict[str, Any]]:
    return _read_ladders_file(_LADDERS_MERGED_JSON)


@lru_cache(maxsize=1)
def _load_epa_ladders() -> list[dict[str, Any]]:
    return _read_ladders_file(_LADDERS_EPA_JSON)


def _steps_from_csv_rows(
    rows: list[dict[str, str]],
    make: str,
    model: str,
) -> list[dict[str, Any]]:
    from backend.enrichment.trim_ladder_knowledge import extract_trims_from_text, merge_trim_names

    blob_parts: list[str] = []
    for row in rows:
        for key in ("Trim", "Packages", "packageDetails", "Options", "optionDetails"):
            val = (row.get(key) or "").strip()
            if val:
                blob_parts.append(val)
    text_trims = extract_trims_from_text(make, "\n".join(blob_parts), model=model)

    steps: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        trim_raw = (row.get("Trim") or "").strip()
        if not trim_raw:
            continue
        step_year_min = 0
        step_year_max = 9999
        m_min = _YEAR_MIN_SUFFIX_RE.search(trim_raw)
        if m_min:
            step_year_min = int(m_min.group(1))
        m_range = _YEAR_RANGE_SUFFIX_RE.search(trim_raw)
        if m_range:
            step_year_min = max(step_year_min, int(m_range.group(1)))
            step_year_max = int(m_range.group(2))
        name = _extract_trim_from_cell(trim_raw, make, model)
        if not name:
            continue
        key = _norm_token(name)
        if key in seen:
            continue
        seen.add(key)

        steps.append(
            {
                "name": name,
                "aliases": [],
                "adds": _row_trim_adds(row) if isinstance(row, dict) else [],
                **({"year_min": step_year_min} if step_year_min else {}),
                **({"year_max": step_year_max} if step_year_max < 9999 else {}),
            }
        )

    if len(steps) < 2 and text_trims:
        for name in merge_trim_names([s["name"] for s in steps], text_trims, make=make, model=model):
            if not _is_valid_trim_name(name, make=make):
                continue
            key = _norm_token(name)
            if key in seen:
                continue
            seen.add(key)
            steps.append(
                {
                    "name": name,
                    "aliases": [],
                    "adds": [],
                }
            )
    from backend.enrichment.trim_ladder_knowledge import normalize_ladder_steps

    return normalize_ladder_steps(steps, make, model=model)
