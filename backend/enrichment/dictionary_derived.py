"""Manifest enrichment and derived-asset indexing for the vehicle dictionary."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from backend.enrichment.brochure_trim_candidates import (
    brochure_status_from_payload,
    build_trim_candidate,
    load_brochure_text_json,
    parse_trim_names_from_text,
)
from backend.enrichment.dictionary_catalog import catalog_key
from backend.enrichment.dictionary_paths import (
    BROCHURE_TEXT_DIR,
    BROCHURE_TEXT_SLIM_DIR,
    BROCHURE_TRIM_CANDIDATES_DIR,
    TRIM_ADDS_BY_YEAR_DIR,
)
_BROCHURE_INDEX = BROCHURE_TEXT_DIR / "_index.jsonl"
_BROCHURE_LADDER_BY_CK: dict[str, str] | None = None


def _rel(path: Path | None) -> str | None:
    if path is None or not path.is_file():
        return None
    from backend.enrichment.dictionary_paths import DICTIONARY_ROOT

    try:
        return str(path.resolve().relative_to(DICTIONARY_ROOT.resolve()))
    except ValueError:
        return str(path)


def load_brochure_ladder_ids() -> dict[str, str]:
    """catalog_key -> ladder id from trim_ladders_brochure.json."""
    global _BROCHURE_LADDER_BY_CK
    if _BROCHURE_LADDER_BY_CK is not None:
        return _BROCHURE_LADDER_BY_CK
    from backend.enrichment.dictionary_paths import trim_ladders_brochure_path

    path = trim_ladders_brochure_path()
    out: dict[str, str] = {}
    if path.is_file():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            for lad in data.get("ladders") or []:
                if not isinstance(lad, dict):
                    continue
                ck = lad.get("catalog_key")
                lid = lad.get("id")
                if ck and lid:
                    out[str(ck)] = str(lid)
        except (OSError, json.JSONDecodeError):
            pass
    _BROCHURE_LADDER_BY_CK = out
    return out


def load_brochure_index() -> dict[str, dict[str, Any]]:
    if not _BROCHURE_INDEX.is_file():
        return {}
    out: dict[str, dict[str, Any]] = {}
    for line in _BROCHURE_INDEX.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        ck = row.get("catalog_key")
        if ck:
            out[str(ck)] = row
    return out


def get_overlay_status(catalog_key_str: str) -> tuple[str | None, str]:
    path = TRIM_ADDS_BY_YEAR_DIR / f"{catalog_key_str.replace('|', '__')}.json"
    if not path.is_file():
        return None, "missing"
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None, "missing"
    src = str(data.get("source") or "")
    abt = data.get("adds_by_trim") or {}
    if isinstance(abt, dict) and any(abt.get(k) for k in abt):
        if "manual" in src or "curated" in src:
            return _rel(path), "curated"
        return _rel(path), "draft"
    return _rel(path), "empty"


def candidate_status(catalog_key_str: str) -> tuple[str | None, str]:
    path = BROCHURE_TRIM_CANDIDATES_DIR / f"{catalog_key_str.replace('|', '__')}.json"
    if not path.is_file():
        return None, "missing"
    return _rel(path), "draft"


def epa_trim_names(epa_rel: str | None) -> list[str]:
    if not epa_rel:
        return []
    from backend.enrichment.dictionary_paths import DICTIONARY_ROOT

    path = DICTIONARY_ROOT / epa_rel
    if not path.is_file():
        return []
    names: list[str] = []
    try:
        with path.open(encoding="utf-8", newline="") as fh:
            for row in csv.DictReader(fh):
                t = (row.get("Trim") or "").strip()
                if t and t not in names:
                    names.append(t)
    except OSError:
        pass
    return names[:24]


def slim_payload_from_full(data: dict[str, Any]) -> dict[str, Any]:
    return {
        "catalog_key": data.get("catalog_key"),
        "year": data.get("year"),
        "make": data.get("make"),
        "model": data.get("model"),
        "trim_hint_pages": data.get("trim_hint_pages") or [],
        "warnings": data.get("warnings") or [],
        "combined_trim_pages_text": data.get("combined_trim_pages_text") or "",
    }


def enrich_manifest_entry(entry: dict[str, Any], *, brochure_index: dict[str, dict[str, Any]] | None = None) -> dict[str, Any]:
    ck = str(entry.get("catalog_key") or "")
    year = entry.get("year")
    make = str(entry.get("make") or "")
    model = str(entry.get("model") or "")

    bt_path = BROCHURE_TEXT_DIR / f"{ck.replace('|', '__')}.json"
    brochure_text_path = _rel(bt_path) if bt_path.is_file() else None
    slim_path = BROCHURE_TEXT_SLIM_DIR / f"{ck.replace('|', '__')}.json"
    brochure_text_slim_path = _rel(slim_path) if slim_path.is_file() else None

    brochure_status = "missing"
    brochure_trim_names: list[str] = []
    pages_saved = 0
    if brochure_text_path:
        data = load_brochure_text_json(ck)
        if data:
            brochure_status = brochure_status_from_payload(data)
            pages_saved = len(data.get("pages") or [])
            brochure_trim_names = parse_trim_names_from_text(
                str(data.get("combined_trim_pages_text") or ""),
                make=make,
                model=model,
            )
    elif brochure_index and ck in brochure_index:
        row = brochure_index[ck]
        pages_saved = int(row.get("pages_saved") or 0)
        warnings = row.get("warnings") or []
        if "no_text_extracted" in warnings:
            brochure_status = "no_text"
        elif pages_saved == 0:
            brochure_status = "empty"
        elif "no_trim_hint_pages_used_all_text_pages" in warnings:
            brochure_status = "weak_hints"
        else:
            brochure_status = "ok"

    overlay_path, trim_overlay_status = get_overlay_status(ck)
    cand_path, cand_stat = candidate_status(ck)
    epa_trims = epa_trim_names(entry.get("epa_path"))

    ladder_ids = load_brochure_ladder_ids()
    ladder_id = ladder_ids.get(ck)
    ladder_source = "brochure" if ladder_id else None

    out = dict(entry)
    out.update(
        {
            "brochure_text_path": brochure_text_path,
            "brochure_text_slim_path": brochure_text_slim_path,
            "brochure_status": brochure_status,
            "brochure_pages_saved": pages_saved,
            "brochure_trim_names": brochure_trim_names,
            "trim_overlay_path": overlay_path,
            "trim_overlay_status": trim_overlay_status,
            "trim_candidate_path": cand_path,
            "trim_candidate_status": cand_stat,
            "epa_trim_names": epa_trims,
            "ladder_id": ladder_id,
            "ladder_source": ladder_source,
        }
    )
    return out


def enrich_manifest_entries(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    global _BROCHURE_LADDER_BY_CK
    _BROCHURE_LADDER_BY_CK = None
    index = load_brochure_index()
    return [enrich_manifest_entry(e, brochure_index=index) for e in entries]
