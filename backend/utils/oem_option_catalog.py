"""
Load versioned OEM option keyword catalogs from ``data/oem_option_catalogs/``.

Matching is data-driven only (JSON files). No per-brand logic in code beyond
file discovery and substring matching.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterator

_CATALOG_DIR = Path(__file__).resolve().parents[2] / "data" / "oem_option_catalogs"


@dataclass(frozen=True)
class CatalogHit:
    canonical_name: str
    matched_keyword: str


def _iter_catalog_files() -> Iterator[Path]:
    if not _CATALOG_DIR.is_dir():
        return
    for p in sorted(_CATALOG_DIR.glob("*.json")):
        if p.name.startswith("."):
            continue
        if p.name in ("colors_lexicon.json",):
            continue
        if "schema" in p.parts:
            continue
        yield p


def _year_in_range(year: int | None, y_min: int | None, y_max: int | None) -> bool:
    if year is None:
        return True
    if y_min is not None and year < y_min:
        return False
    if y_max is not None and year > y_max:
        return False
    return True


def _normalize_make(make: str | None) -> str:
    return (make or "").strip().lower()


def _load_json(path: Path) -> dict[str, Any] | None:
    try:
        raw = path.read_text(encoding="utf-8")
        data = json.loads(raw)
    except (OSError, json.JSONDecodeError, TypeError):
        return None
    return data if isinstance(data, dict) else None


def _mapping_rows(mappings: Any) -> list[tuple[list[str], str]]:
    rows: list[tuple[list[str], str]] = []
    if not isinstance(mappings, list):
        return rows
    for m in mappings:
        if not isinstance(m, dict):
            continue
        kws = m.get("keywords")
        name = m.get("canonical_name")
        if not isinstance(kws, list) or not isinstance(name, str):
            continue
        cleaned = [str(x).strip().lower() for x in kws if str(x).strip()]
        cn = name.strip()
        if cleaned and cn:
            rows.append((cleaned, cn))
    return rows


def _doc_make_matches(doc: dict[str, Any], make: str | None) -> bool:
    mk = str(doc.get("make") or "").strip().lower()
    if mk in ("*", ""):
        return True
    return mk == _normalize_make(make)


def _flatten_catalog_rules(doc: dict[str, Any]) -> list[tuple[list[str], str, int | None, int | None]]:
    """
    (keywords, canonical_name, year_min, year_max). year_* None means unrestricted.
    """
    rules: list[tuple[list[str], str, int | None, int | None]] = []
    for kws, cn in _mapping_rows(doc.get("entries")):
        rules.append((kws, cn, None, None))
    for block in doc.get("model_year_ranges") or []:
        if not isinstance(block, dict):
            continue
        y0 = block.get("min_year")
        y1 = block.get("max_year")
        try:
            yi0 = int(y0) if y0 is not None else None
        except (TypeError, ValueError):
            yi0 = None
        try:
            yi1 = int(y1) if y1 is not None else None
        except (TypeError, ValueError):
            yi1 = None
        for kws, cn in _mapping_rows(block.get("mappings")):
            rules.append((kws, cn, yi0, yi1))
    return rules


def resolve_catalog_name(
    text: str,
    *,
    make: str | None,
    year: int | None = None,
) -> CatalogHit | None:
    """
    If any catalog keyword appears as a substring of *text* (lowercased), return the
    first hit in stable file + entry order.
    """
    hay = (text or "").lower()
    if not hay.strip():
        return None
    for path in _iter_catalog_files():
        doc = _load_json(path)
        if not doc or not _doc_make_matches(doc, make):
            continue
        for kws, cn, y_min, y_max in _flatten_catalog_rules(doc):
            if not _year_in_range(year, y_min, y_max):
                continue
            for kw in kws:
                if kw in hay:
                    return CatalogHit(canonical_name=cn, matched_keyword=kw)
    return None


@lru_cache(maxsize=1)
def load_color_lexicon() -> frozenset[str]:
    path = _CATALOG_DIR / "colors_lexicon.json"
    doc = _load_json(path)
    if not doc:
        return frozenset()
    colors = doc.get("colors")
    if not isinstance(colors, list):
        return frozenset()
    out: set[str] = set()
    for c in colors:
        if isinstance(c, str) and c.strip():
            out.add(c.strip().lower())
    return frozenset(out)


def color_phrase_candidates(text: str) -> list[str]:
    """Return lowercased color tokens present in *text* from the shared lexicon."""
    lex = load_color_lexicon()
    if not lex or not text:
        return []
    low = text.lower()
    found: list[str] = []
    for c in sorted(lex, key=len, reverse=True):
        if re.search(rf"\b{re.escape(c)}\b", low):
            found.append(c)
    return found
