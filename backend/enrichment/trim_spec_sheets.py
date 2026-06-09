"""Structured OEM trim spec sheets for the car detail trim ladder (label + value rows)."""

from __future__ import annotations

import json
import logging
import re
from functools import lru_cache
from pathlib import Path
from typing import Any

from backend.enrichment.dictionary_paths import trim_spec_sheets_dir

logger = logging.getLogger(__name__)

_SHEETS_DIR = trim_spec_sheets_dir()


def _norm_token(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").strip().lower())


def _norm_make(s: str) -> str:
    return (s or "").strip().lower()


def _norm_model(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").strip().lower())


@lru_cache(maxsize=1)
def _load_all_sheets() -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if not _SHEETS_DIR.is_dir():
        return out
    for path in sorted(_SHEETS_DIR.glob("*.json")):
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            logger.exception("Failed to load trim spec sheet %s", path)
            continue
        if isinstance(raw, dict):
            out.append(raw)
    return out


def _sheet_matches(sheet: dict[str, Any], *, make: str, model: str, year: Any) -> bool:
    if _norm_make(sheet.get("make") or "") != _norm_make(make):
        return False
    models = sheet.get("models") or []
    model_norm = _norm_model(model)
    if not any(_norm_model(str(m)) in model_norm or model_norm in _norm_model(str(m)) for m in models):
        return False
    try:
        y = int(year)
    except (TypeError, ValueError):
        return True
    ymin = int(sheet.get("year_min") or 0)
    ymax = int(sheet.get("year_max") or 9999)
    return ymin <= y <= ymax


def _pick_sheet(*, make: str, model: str, year: Any, ladder_id: str | None = None) -> dict[str, Any] | None:
    lid = (ladder_id or "").strip().lower()
    matches: list[dict[str, Any]] = []
    for sheet in _load_all_sheets():
        sid = str(sheet.get("ladder_id") or sheet.get("id") or "").strip().lower()
        if lid and sid == lid:
            return sheet
        if _sheet_matches(sheet, make=make, model=model, year=year):
            matches.append(sheet)
    if not matches:
        return None
    if len(matches) == 1:
        return matches[0]

    def year_span(s: dict[str, Any]) -> int:
        try:
            return int(s.get("year_max") or 9999) - int(s.get("year_min") or 0)
        except (TypeError, ValueError):
            return 9999

    try:
        y = int(year)
    except (TypeError, ValueError):
        return min(matches, key=year_span)

    def year_dist(s: dict[str, Any]) -> int:
        ymin = int(s.get("year_min") or 0)
        ymax = int(s.get("year_max") or 9999)
        if ymin <= y <= ymax:
            return 0
        return min(abs(y - ymin), abs(y - ymax))

    matches.sort(key=lambda s: (year_dist(s), year_span(s)))
    return matches[0]


def _parse_trim_specs(raw: Any) -> list[dict[str, str]]:
    if raw is None:
        return []
    out: list[dict[str, str]] = []
    if isinstance(raw, dict):
        for label, value in raw.items():
            lbl = str(label or "").strip()
            val = str(value or "").strip()
            if lbl and val:
                out.append({"label": lbl, "value": val})
        return out
    if isinstance(raw, list):
        for row in raw:
            if not isinstance(row, dict):
                continue
            lbl = str(row.get("label") or "").strip()
            val = str(row.get("value") or "").strip()
            if lbl and val:
                out.append({"label": lbl, "value": val})
    return out


def lookup_trim_specs(
    trim_name: str,
    *,
    make: str,
    model: str,
    year: Any = None,
    ladder_id: str | None = None,
    adds: list[str] | None = None,
) -> list[dict[str, str]]:
    """Return ``[{label, value}, ...]`` for a trim, or empty list."""
    from backend.enrichment.trim_spec_extractor import extract_trim_specs, sanitize_trim_specs

    sheet = _pick_sheet(make=make, model=model, year=year, ladder_id=ladder_id)
    if sheet:
        trims = sheet.get("trims") or {}
        if isinstance(trims, dict):
            name = (trim_name or "").strip()
            if name:
                raw = trims.get(name)
                if raw is None:
                    key = _norm_token(name)
                    for k, v in trims.items():
                        if _norm_token(str(k)) == key:
                            raw = v
                            break
                parsed = sanitize_trim_specs(_parse_trim_specs(raw))
                if parsed:
                    return parsed

    adds_key = "\n".join(str(a).strip() for a in (adds or []) if str(a).strip())
    return sanitize_trim_specs(
        list(
            extract_trim_specs(
                trim_name,
                make=make,
                model=model,
                year=year,
                adds_key=adds_key,
            )
        )
    )
