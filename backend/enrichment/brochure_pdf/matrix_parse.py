"""Equipment-matrix and buyer's-guide parsing, ladder mapping, adds-over-lower.

Split out of ``brochure_extract`` (P17C.4); ``brochure_extract`` re-exports every name.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any

from backend.enrichment.brochure_pdf.archive import BrochureYMM
from backend.enrichment.brochure_pdf.pdf_text import _EQUIPMENT_PAGE, _extract_pdf_text

__all__ = [
    "_BULLET_SPLIT",
    "_BUYERS_GUIDE_TRIM_START",
    "_GENERIC_TRIM_STANDARD",
    "_NOISE_LINE",
    "_OPTIONAL_GROUP",
    "_SECTION_HEADER_ROW",
    "_STANDARD_MARKERS",
    "_TRIM_COLUMN_HEADER",
    "_TRIM_RAW_TO_CANONICAL",
    "_canonical_trim_name",
    "_compute_adds_over_lower",
    "_detect_trim_columns",
    "_extract_equipment_matrix",
    "_feature_column_index",
    "_feature_key",
    "_find_trim_sections",
    "_is_interleaved_standard_header",
    "_ladder_trim_order",
    "_looks_like_trim_column",
    "_map_extracted_to_ladder",
    "_marker_is_standard",
    "_normalize_feature",
    "_parse_matrix_table",
    "_raw_trim_to_canonical",
    "_split_features",
]

logger = logging.getLogger(__name__)


# Buyer's guide: spaced-letter trim title immediately before STANDARD – (FCA layout).
_GENERIC_TRIM_STANDARD = re.compile(
    r"(?P<raw>[A-Z][A-Za-z0-9®™'\-/]{1,28}(?:\s+[A-Z][A-Za-z0-9®™'\-/]{1,28}){0,3})\s+STANDARD\s*[-–]\s*",
)

_BUYERS_GUIDE_TRIM_START = re.compile(
    r"(?P<raw>"
    r"G\s*R\s*A\s*N\s*D\s*C\s*H\s*E\s*R\s*O\s*K\s*E\s*E\s*S\s*U\s*M\s*M\s*I\s*T|"
    r"H\s*I\s*G\s*H\s*A\s*L\s*T\s*I\s*T\s*U\s*D\s*E|"
    r"O\s*V\s*E\s*R\s*L\s*A\s*N\s*D|"
    r"T\s*R\s*A\s*I\s*L\s*H\s*A\s*W\s*K|"
    r"L\s*I\s*M\s*I\s*T\s*E\s*D\s*X|"
    r"L\s*I\s*M\s*I\s*T\s*E\s*D|"
    r"L\s*A\s*R\s*E\s*D\s*O|"
    r"T\s*R\s*A\s*C\s*K\s*H\s*A\s*W\s*K|"
    r"S\s*R\s*T\s*8|"
    r"S\s*R\s*T"
    r")\s*®?\s*STANDARD\s*[-–]\s*",
    re.I,
)

_TRIM_RAW_TO_CANONICAL: dict[str, str] = {
    "grandcherokeesummit": "Summit",
    "summit": "Summit",
    "highaltitude": "High Altitude",
    "overland": "Overland",
    "trailhawk": "Trailhawk",
    "limitedx": "Limited X",
    "limited": "Limited",
    "laredo": "Laredo",
    "trackhawk": "Trackhawk",
    "srt8": "SRT",
    "srt": "SRT",
}

_NOISE_LINE = re.compile(
    r"^(?:J\s*E\s*E\s*P|BUYER.?S\s*GUIDE|©20|WARRANTY|facebook\.com|"
    r"Actual mileage|GO MOBILE|JEEP SOCIAL|disclaimer)",
    re.I,
)

_OPTIONAL_GROUP = re.compile(
    r"\b(?:GROUP\s+[IVX\d]+|AVAILABLE\s+[A-Z]|\bOPTIONAL\b)\s*:",
    re.I,
)

_BULLET_SPLIT = re.compile(r"\s+[–—]\s+")

# OEM comparison tables: single-letter codes (BMW p=standard, Audi s=standard).
_STANDARD_MARKERS = frozenset({"p", "s"})


_TRIM_COLUMN_HEADER = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9®™\.\-\s]{0,28}$"
)

_SECTION_HEADER_ROW = re.compile(
    r"^(?:performance|handling|audio|interior|comfort|chassis|technical|"
    r"fuel|weight|tires|brakes|bmw services|stand-alone)\b",
    re.I,
)


def _marker_is_standard(marker: str) -> bool:
    m = re.sub(r"\s+", "", (marker or "").strip().lower())
    if not m or m in {"-", "–", "—", "na", "n/a"}:
        return False
    first = m[0]
    return first in _STANDARD_MARKERS


def _looks_like_trim_column(cell: str) -> bool:
    s = re.sub(r"\s+", " ", (cell or "").strip())
    if not s or len(s) > 16 or len(s.split()) > 2:
        return False
    if len(s) < 3 and s.lower() not in {"tt", "tts"}:
        return False
    low = s.lower()
    if any(
        tok in low
        for tok in (
            "standard equipment",
            "optional equipment",
            "technical data",
            "bmw services",
            "features",
            "chassis",
            "performance",
            "handling",
            "interior",
            "audio",
            "comfort",
            "weight",
            "fuel",
            "stand-alone",
            "and trim",
        )
    ):
        return False
    return bool(re.match(r"^[A-Za-z0-9][A-Za-z0-9®™\.\-]{1,14}$", s))


def _detect_trim_columns(table: list[list[Any]]) -> list[tuple[int, str]]:
    for header in table[:3]:
        if not header:
            continue
        trim_cols: list[tuple[int, str]] = []
        for ci, cell in enumerate(header):
            name = re.sub(r"\s+", " ", str(cell or "").strip())
            if _looks_like_trim_column(name):
                trim_cols.append((ci, name))
        if len(trim_cols) >= 2:
            return trim_cols
    return []


def _feature_column_index(row: list[Any], trim_cols: list[tuple[int, str]]) -> int:
    trim_indices = {ci for ci, _ in trim_cols}
    best_idx = 0
    best_len = 0
    for ci, cell in enumerate(row):
        if ci in trim_indices:
            continue
        s = str(cell or "").strip().replace("\n", " ")
        if len(s) > best_len:
            best_len = len(s)
            best_idx = ci
    return best_idx


def _parse_matrix_table(
    table: list[list[Any]],
    *,
    make: str,
    model: str,
    acc: dict[str, list[str]],
) -> None:
    if not table or len(table) < 2:
        return
    trim_cols = _detect_trim_columns(table)
    if len(trim_cols) < 2:
        return

    for row in table[1:]:
        if not row:
            continue
        feat_idx = _feature_column_index(row, trim_cols)
        feat_raw = str(row[feat_idx] if feat_idx < len(row) else "").strip().replace("\n", " ")
        if not feat_raw or len(feat_raw) < 10:
            continue
        if _SECTION_HEADER_ROW.match(feat_raw) and len(feat_raw) < 50:
            continue
        feat = _normalize_feature(feat_raw)
        if not feat:
            continue
        for ci, trim_name in trim_cols:
            if ci >= len(row):
                continue
            marker = str(row[ci] or "").strip()
            if not _marker_is_standard(marker):
                continue
            canon = _canonical_trim_name(trim_name, make=make, model=model)
            bucket = acc.setdefault(canon, [])
            if feat.lower() not in {x.lower() for x in bucket}:
                bucket.append(feat)


def _extract_equipment_matrix(
    path: Path,
    ymm: BrochureYMM,
    *,
    ladder_order: list[str] | None,
) -> tuple[dict[str, list[str]], list[int], list[str]]:
    """Parse BMW/Audi-style p/s comparison tables into trim→standard features."""
    try:
        import pdfplumber
    except ImportError:
        return {}, [], []

    acc: dict[str, list[str]] = {}
    column_order: list[str] = []
    pages_used: list[int] = []
    with pdfplumber.open(str(path)) as pdf:
        for i, page in enumerate(pdf.pages):
            try:
                t = page.extract_text() or ""
            except Exception:
                t = ""
            if not _EQUIPMENT_PAGE.search(t) and "Standard equipment" not in t:
                continue
            pages_used.append(i + 1)
            for table in page.extract_tables() or []:
                cols = _detect_trim_columns(table)
                for _, name in cols:
                    canon = _canonical_trim_name(name, make=ymm.make, model=ymm.model)
                    if canon not in column_order:
                        column_order.append(canon)
                _parse_matrix_table(table, make=ymm.make, model=ymm.model, acc=acc)

    if acc:
        return acc, pages_used, column_order

    # Text-line fallback: "feature ... p p" or "feature ... s s"
    text, pg = _extract_pdf_text(path)
    line_re = re.compile(
        r"^(.{12,240}?)\s+([pskoet/\-]+)\s+([pskoet/\-]+)\s*$",
        re.I | re.M,
    )
    for m in line_re.finditer(text):
        feat = _normalize_feature(m.group(1))
        if not feat:
            continue
        marks = [m.group(2).strip(), m.group(3).strip()]
        # Guess trim names from ladder or generic col1/col2
        trim_names = ladder_order[-2:] if ladder_order and len(ladder_order) >= 2 else ["Trim 1", "Trim 2"]
        if len(trim_names) < 2:
            trim_names = ["Base", "Upgraded"]
        for trim_name, mark in zip(trim_names, marks):
            if _marker_is_standard(mark):
                canon = _canonical_trim_name(trim_name, make=ymm.make, model=ymm.model)
                bucket = acc.setdefault(canon, [])
                if feat.lower() not in {x.lower() for x in bucket}:
                    bucket.append(feat)
    return acc, pages_used or pg, column_order


def _normalize_feature(raw: str) -> str:
    s = re.sub(r"\s+", " ", (raw or "").strip())
    s = s.strip("–—-|")
    s = re.sub(r"\s*®\s*", "® ", s)
    if len(s) < 6 or len(s) > 280:
        return ""
    if _NOISE_LINE.search(s):
        return ""
    if s.upper().startswith("AVAILABLE"):
        return ""
    return s


def _split_features(block: str) -> list[str]:
    """Pull equipment bullets from a STANDARD equipment block."""
    if not block.strip():
        return []
    work = block
    if "STANDARD" in work.upper():
        work = re.split(r"STANDARD\s*[-–]", work, maxsplit=1, flags=re.I)[-1]
    # Next trim header ends this block.
    work = _BUYERS_GUIDE_TRIM_START.split(work)[0]
    parts = _BULLET_SPLIT.split(work)
    if len(parts) < 3:
        parts = re.split(r"(?<=[a-z0-9%\)])\s+[-–]\s+", work)
    out: list[str] = []
    seen: set[str] = set()
    for part in parts:
        if _OPTIONAL_GROUP.search(part):
            continue
        if re.search(r"\bGROUP\s*:", part, re.I) and ":" in part[:40]:
            continue
        feat = _normalize_feature(part)
        if not feat:
            continue
        # Split very long mashed PDF lines into smaller bullets.
        if len(feat) > 140:
            subparts = re.split(r",\s+(?=[A-Z0-9®])|;\s+(?=[A-Z0-9®])", feat)
            if len(subparts) > 1:
                for sub in subparts:
                    sub_n = _normalize_feature(sub)
                    if sub_n and sub_n.lower() not in seen:
                        seen.add(sub_n.lower())
                        out.append(sub_n)
                continue
        key = feat.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(feat)
    return out


def _raw_trim_to_canonical(raw: str) -> str:
    compact = re.sub(r"[^a-zA-Z0-9]", "", raw).lower()
    if compact in _TRIM_RAW_TO_CANONICAL:
        return _TRIM_RAW_TO_CANONICAL[compact]
    if "summit" in compact and "cherokee" in compact:
        return "Summit"
    spaced = re.sub(r"\s+", " ", raw.replace("®", "")).strip().lower()
    return _TRIM_RAW_TO_CANONICAL.get(spaced.replace(" ", ""), raw.strip().title())


def _is_interleaved_standard_header(text: str, start: int, end: int) -> bool:
    """True when two+ STANDARD columns appear on one line (Summit | Overland layout)."""
    window = text[start : min(len(text), end + 220)]
    return len(re.findall(r"STANDARD\s*[-–]", window, re.I)) >= 2


def _find_trim_sections(
    text: str,
    *,
    allowed_trims: set[str] | None = None,
    make: str = "",
    model: str = "",
) -> list[tuple[int, str, str]]:
    """Return (start_pos, canonical_trim, matched_header) sorted by position."""
    hits: list[tuple[int, str, str]] = []
    for m in _BUYERS_GUIDE_TRIM_START.finditer(text):
        canon = _raw_trim_to_canonical(m.group("raw"))
        if allowed_trims and canon not in allowed_trims:
            continue
        line_end = text.find("\n", m.start())
        if line_end < 0:
            line_end = m.end() + 120
        if _is_interleaved_standard_header(text, m.start(), line_end):
            continue
        hits.append((m.start(), canon, m.group(0)))

    if not hits:
        for m in _GENERIC_TRIM_STANDARD.finditer(text):
            raw = m.group("raw").strip()
            if len(raw) > 40 or _NOISE_LINE.search(raw):
                continue
            canon = _canonical_trim_name(
                _raw_trim_to_canonical(raw), make=make, model=model
            )
            if allowed_trims and canon not in allowed_trims:
                canon = _canonical_trim_name(raw.title(), make=make, model=model)
                if allowed_trims and canon not in allowed_trims:
                    continue
            line_end = text.find("\n", m.start())
            if line_end < 0:
                line_end = m.end() + 120
            if _is_interleaved_standard_header(text, m.start(), line_end):
                continue
            hits.append((m.start(), canon, m.group(0)))

    hits.sort(key=lambda x: x[0])
    # Prefer the latest non-interleaved section per trim (page 24/25 columns).
    best_by_trim: dict[str, tuple[int, str, str]] = {}
    for start, canon, header in hits:
        prev = best_by_trim.get(canon)
        if not prev or start > prev[0]:
            best_by_trim[canon] = (start, canon, header)
    return sorted(best_by_trim.values(), key=lambda x: x[0])


def _canonical_trim_name(raw: str, *, make: str, model: str) -> str:
    from backend.enrichment.trim_ladder_knowledge import canonical_trim_name, preserve_trim_label

    return (
        canonical_trim_name(raw, make, model)
        or preserve_trim_label(raw, make, model)
        or raw.strip().title()
    )


def _ladder_trim_order(ymm: BrochureYMM) -> tuple[str | None, list[str]]:
    from backend.enrichment.trim_ladder import _pick_ladder_def, _filter_ladder_steps_for_year

    ladder = _pick_ladder_def(ymm.make, ymm.model, ymm.year)
    if not ladder:
        return None, []
    steps = _filter_ladder_steps_for_year(
        ladder.get("steps") or [],
        ymm.year,
        make=ymm.make,
        model=ymm.model,
        skip_trim_year_windows=False,
    )
    names = [str(s.get("name") or "").strip() for s in steps if str(s.get("name") or "").strip()]
    return str(ladder.get("id") or ""), names


def _map_extracted_to_ladder(
    extracted: dict[str, list[str]],
    ladder_order: list[str],
    *,
    make: str,
    model: str,
) -> dict[str, list[str]]:
    """Map brochure trim labels onto ladder step names."""
    mapped: dict[str, list[str]] = {}
    ladder_norm = {_canonical_trim_name(n, make=make, model=model).lower(): n for n in ladder_order}
    for raw_trim, feats in extracted.items():
        canon = _canonical_trim_name(raw_trim, make=make, model=model)
        key = canon.lower()
        if key in ladder_norm:
            mapped[ladder_norm[key]] = feats
        else:
            for ln, ladder_name in ladder_norm.items():
                if key in ln or ln in key:
                    mapped[ladder_name] = feats
                    break
            else:
                mapped[canon] = feats
    return mapped


def _feature_key(feature: str) -> str:
    """Normalize for set comparisons (strip marketing noise tokens)."""
    s = re.sub(r"\s+", " ", feature.lower())
    s = re.sub(r"®|™", "", s)
    return s[:200]


def _compute_adds_over_lower(
    ladder_order: list[str],
    standard_by_trim: dict[str, list[str]],
) -> dict[str, list[str]]:
    """
    ladder_order: base → top (Laredo … Summit).
    adds[trim] = features on trim not present on immediately lower trim.
    """
    adds: dict[str, list[str]] = {}
    prev_feats: set[str] = set()
    for i, name in enumerate(ladder_order):
        feats = standard_by_trim.get(name) or []
        feat_keys = {_feature_key(f) for f in feats}
        prev_feats |= feat_keys
        if i == 0:
            # Base rung: equipment is "standard on vehicle," not adds-over-lower.
            continue
        delta: list[str] = []
        seen: set[str] = set()
        lower_name = ladder_order[i - 1]
        lower_feats = {_feature_key(f) for f in (standard_by_trim.get(lower_name) or [])}
        for f in feats:
            key = _feature_key(f)
            if key in lower_feats or key in seen:
                continue
            seen.add(key)
            delta.append(f)
        if delta:
            adds[name] = delta
    return adds


