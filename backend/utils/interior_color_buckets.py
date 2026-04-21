"""
Map dealer interior / exterior color strings to normalized filter buckets.

Lexicon: ``data/lexicons/interior_color_buckets.json`` (``version`` field for migrations).
The same phrase list applies to **exterior paint** and **interior upholstery** for
listings filters (individual car pages keep the raw dealer string).

Unknown non-empty strings → ``["other"]``. Empty / placeholder → ``[]`` (stored as JSON ``[]``
or NULL at call sites).
"""
from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable

from backend.utils.field_clean import is_effectively_empty

_LEXICON_PATH = Path(__file__).resolve().parents[2] / "data" / "lexicons" / "interior_color_buckets.json"

# Keep in sync with LLaVA allowlist in ``backend.vision.ollama_llava``.
ALLOWED_BUCKETS: frozenset[str] = frozenset(
    {
        "black",
        "white",
        "gray",
        "silver",
        "red",
        "blue",
        "green",
        "brown",
        "tan",
        "beige",
        "orange",
        "yellow",
        "other",
    }
)


@lru_cache(maxsize=1)
def _load_lexicon() -> tuple[int, list[tuple[str, tuple[str, ...]]]]:
    raw = _LEXICON_PATH.read_text(encoding="utf-8")
    data = json.loads(raw)
    version = int(data.get("version") or 1)
    phrases = data.get("phrases") or []
    rows: list[tuple[str, tuple[str, ...]]] = []
    for p in phrases:
        if not isinstance(p, dict):
            continue
        m = str(p.get("match") or "").strip().lower()
        b = p.get("buckets")
        if not m or not isinstance(b, list):
            continue
        buckets = tuple(
            str(x).strip().lower()
            for x in b
            if str(x).strip().lower() in ALLOWED_BUCKETS
        )
        if not buckets:
            continue
        rows.append((m, buckets))
    rows.sort(key=lambda r: len(r[0]), reverse=True)
    return version, rows


def lexicon_version() -> int:
    return _load_lexicon()[0]


_FAMILY_SORT_ORDER: tuple[str, ...] = (
    "black",
    "white",
    "gray",
    "silver",
    "red",
    "blue",
    "green",
    "brown",
    "tan",
    "beige",
    "orange",
    "yellow",
    "other",
)


def sort_paint_family_ids(ids: Iterable[str]) -> list[str]:
    """Stable UI ordering for filter facets (lowercase bucket ids)."""
    uniq = {str(x).strip().lower() for x in ids if str(x).strip() and str(x).strip().lower() in ALLOWED_BUCKETS}

    def _key(b: str) -> tuple[int, str]:
        try:
            return (_FAMILY_SORT_ORDER.index(b), b)
        except ValueError:
            return (999, b)

    return sorted(uniq, key=_key)


def infer_paint_color_buckets(raw: str | None, make: str | None = None) -> list[str]:
    """
    Same as ``infer_interior_color_buckets``: map raw paint/upholstery text to bucket ids.

    Used for **exterior** listings filters and for interior when no stored buckets JSON exists.
    """
    return infer_interior_color_buckets(raw, make=make)


def infer_interior_color_buckets(raw: str | None, make: str | None = None) -> list[str]:
    """
    Return ordered unique bucket ids (lowercase) for *raw* interior color text.

    ``make`` is reserved for future make-specific rules; unused today.
    """
    _ = make
    if raw is None or is_effectively_empty(raw):
        return []
    s = str(raw).strip().lower()
    if not s:
        return []
    out: list[str] = []
    seen: set[str] = set()
    _, phrases = _load_lexicon()
    for phrase, buckets in phrases:
        if phrase in s:
            for b in buckets:
                if b not in seen:
                    seen.add(b)
                    out.append(b)
    if not out:
        return ["other"]
    return out


def interior_color_buckets_json(raw: str | None, make: str | None = None) -> str:
    """JSON array string for SQLite (``[]`` when interior text is empty / placeholder)."""
    buckets = infer_interior_color_buckets(raw, make=make)
    return json.dumps(buckets, separators=(",", ":"))


def merge_bucket_lists(*lists: list[str]) -> list[str]:
    """Ordered union of bucket ids; each list is lower-case ids."""
    out: list[str] = []
    seen: set[str] = set()
    for lst in lists:
        for b in lst:
            b = str(b).strip().lower()
            if b not in ALLOWED_BUCKETS:
                continue
            if b not in seen:
                seen.add(b)
                out.append(b)
    return out


def parse_stored_buckets(val: Any) -> list[str]:
    if val is None or is_effectively_empty(val):
        return []
    if isinstance(val, list):
        raw = val
    else:
        try:
            raw = json.loads(str(val))
        except (json.JSONDecodeError, TypeError):
            return []
    if not isinstance(raw, list):
        return []
    out: list[str] = []
    for x in raw:
        b = str(x).strip().lower()
        if b in ALLOWED_BUCKETS and b not in out:
            out.append(b)
    return out


def row_matches_interior_bucket_filter(car: dict[str, Any], selected: set[str]) -> bool:
    """True when any bucket on the row intersects ``selected`` (non-empty)."""
    if not selected:
        return True
    row_buckets = set(parse_stored_buckets(car.get("interior_color_buckets")))
    return bool(row_buckets & selected)
