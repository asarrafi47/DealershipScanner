"""Extract trim-level standard equipment from OEM brochure PDFs."""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from backend.enrichment.dictionary_catalog import catalog_key
from backend.enrichment.dictionary_paths import (
    BROCHURE_FACTS_DIR,
    BROCHURE_TEXT_DIR,
    BROCHURES_DIR,
    OPTIONS_RAW_DIR,
    TRIM_ADDS_BY_YEAR_DIR,
    CANONICAL_CSV_COLUMNS,
)

logger = logging.getLogger(__name__)

_FILENAME_YMM = re.compile(
    r"^(\d{4})_([^_]+)_(.+?)(?:_Brochure)?$",
    re.IGNORECASE,
)

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

_EQUIPMENT_PAGE = re.compile(
    r"Standard\s+equipment|Specifications|Features\s+\w+\s+\w+",
    re.I,
)

_TRIM_COLUMN_HEADER = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9®™\.\-\s]{0,28}$"
)

_SECTION_HEADER_ROW = re.compile(
    r"^(?:performance|handling|audio|interior|comfort|chassis|technical|"
    r"fuel|weight|tires|brakes|bmw services|stand-alone)\b",
    re.I,
)

# Broader than FCA BUYER'S GUIDE — flags pages likely useful for manual trim review.
_TRIM_PAGE_HINT = re.compile(
    r"BUYER.?S\s*GUIDE|STANDARD\s*[-–]|"
    r"CHOOSE\s+YOUR\s+TRIM|CHOOSE\s+YOUR\s+PACKAGE|"
    r"ADDS\s+TO|ADDS\s+FROM|INCLUDES\s+\w+\s+EQUIPMENT\s+PLUS|"
    r"Trim\s+Levels?|>\s*0\d\s+TRIMS|06\s+TRIMS|"
    r"FEATURES\s*&\s*TRIMS|Features\s+&\s+Trims|"
    r"STYLES|TRIM\s+LEVEL|MODELS\s+TO\s+CHOOSE|"
    r"Four\s+models\s+to\s+choose|NINE\s+MODELS\s+TO\s+CHOOSE|"
    r"Standard\s+equipment|Specifications",
    re.I,
)


@dataclass
class BrochureYMM:
    year: int
    make: str
    model: str

    @property
    def catalog_key(self) -> str:
        return catalog_key(self.year, self.make, self.model)


@dataclass
class BrochureExtractResult:
    ymm: BrochureYMM
    ladder_id: str | None = None
    trims_available: list[str] = field(default_factory=list)
    adds_by_trim: dict[str, list[str]] = field(default_factory=dict)
    standard_by_trim: dict[str, list[str]] = field(default_factory=dict)
    source_pdf: str = ""
    pages_used: list[int] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


@dataclass
class BrochurePageText:
    page: int
    text: str
    trim_hint: bool = False
    table_lines: list[str] = field(default_factory=list)


@dataclass
class BrochureTextResult:
    ymm: BrochureYMM
    source_pdf: str
    page_count: int = 0
    pages: list[BrochurePageText] = field(default_factory=list)
    trim_hint_pages: list[int] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    all_pages: bool = False

    @property
    def combined_trim_pages_text(self) -> str:
        parts: list[str] = []
        for pg in self.pages:
            if not pg.trim_hint and self.trim_hint_pages:
                continue
            parts.append(f"--- page {pg.page} ---\n{pg.text}")
            if pg.table_lines:
                parts.append("\n".join(pg.table_lines))
        return "\n\n".join(parts)


def parse_brochure_filename(path: Path) -> BrochureYMM | None:
    m = _FILENAME_YMM.match(path.stem)
    if not m:
        return None
    return BrochureYMM(
        year=int(m.group(1)),
        make=m.group(2).replace("_", " ").strip(),
        model=m.group(3).replace("_", " ").strip(),
    )


def _page_table_lines(page: Any) -> list[str]:
    lines: list[str] = []
    try:
        tables = page.extract_tables() or []
    except Exception:
        return lines
    for table in tables:
        for row in table:
            if not row:
                continue
            line = " | ".join(str(c or "").strip() for c in row if c)
            if line.strip():
                lines.append(line)
    return lines


def extract_brochure_text_pdf(
    path: Path,
    *,
    all_pages: bool = False,
) -> BrochureTextResult | None:
    """
    Extract readable text from a brochure PDF for manual trim review.

    Default: all pages with text, flagged trim_hint on pages matching marketing/FCA patterns.
    Does not compute adds_by_trim or delete PDFs.
    """
    ymm = parse_brochure_filename(path)
    if not ymm:
        return None

    try:
        import pdfplumber
    except ImportError:
        return BrochureTextResult(
            ymm=ymm,
            source_pdf=str(path),
            warnings=["pdfplumber_not_installed"],
        )

    warnings: list[str] = []
    raw_pages: list[BrochurePageText] = []
    trim_pages: list[int] = []
    pages_out: list[BrochurePageText] = []
    page_count = 0

    try:
        with pdfplumber.open(str(path)) as pdf:
            page_count = len(pdf.pages)
            for i, page in enumerate(pdf.pages):
                page_num = i + 1
                try:
                    text = page.extract_text() or ""
                except Exception as exc:
                    warnings.append(f"page_{page_num}_extract_error:{exc}")
                    text = ""
                table_lines = _page_table_lines(page)
                trim_hint = bool(_TRIM_PAGE_HINT.search(text)) or any(
                    _TRIM_PAGE_HINT.search(line) for line in table_lines
                )
                if trim_hint:
                    trim_pages.append(page_num)
                if not text.strip() and not table_lines:
                    continue
                raw_pages.append(
                    BrochurePageText(
                        page=page_num,
                        text=text,
                        trim_hint=trim_hint,
                        table_lines=table_lines,
                    )
                )
            trim_pages = sorted(set(trim_pages))
            if all_pages or not trim_pages:
                pages_out = raw_pages
            else:
                pages_out = [p for p in raw_pages if p.trim_hint]
    except Exception as exc:
        warnings.append(f"pdf_open_failed:{exc}")
        return BrochureTextResult(
            ymm=ymm,
            source_pdf=str(path),
            warnings=warnings,
        )

    if not pages_out:
        warnings.append("no_text_extracted")
    elif not trim_pages:
        warnings.append("no_trim_hint_pages_used_all_text_pages")

    return BrochureTextResult(
        ymm=ymm,
        source_pdf=str(path),
        page_count=page_count,
        pages=pages_out,
        trim_hint_pages=trim_pages,
        warnings=warnings,
        all_pages=all_pages,
    )


def _brochure_text_path(ymm: BrochureYMM) -> Path:
    safe = ymm.catalog_key.replace("|", "__")
    return BROCHURE_TEXT_DIR / f"{safe}.json"


def persist_brochure_text(result: BrochureTextResult) -> Path:
    """Write extracted page text JSON under derived/brochure_text/."""
    BROCHURE_TEXT_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "catalog_key": result.ymm.catalog_key,
        "year": result.ymm.year,
        "make": result.ymm.make,
        "model": result.ymm.model,
        "source_pdf": result.source_pdf,
        "page_count": result.page_count,
        "all_pages": result.all_pages,
        "trim_hint_pages": result.trim_hint_pages,
        "warnings": result.warnings,
        "pages": [
            {
                "page": p.page,
                "trim_hint": p.trim_hint,
                "text": p.text,
                "table_lines": p.table_lines,
            }
            for p in result.pages
        ],
        "combined_trim_pages_text": result.combined_trim_pages_text,
    }
    out = _brochure_text_path(result.ymm)
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return out


def _extract_pdf_text(path: Path) -> tuple[str, list[int]]:
    """Return combined text and 1-based page numbers that contributed."""
    try:
        import pdfplumber
    except ImportError:
        logger.warning("pdfplumber not installed; cannot read %s", path.name)
        return "", []

    text_parts: list[str] = []
    pages_used: list[int] = []
    with pdfplumber.open(str(path)) as pdf:
        for i, page in enumerate(pdf.pages):
            try:
                t = page.extract_text() or ""
            except Exception:
                t = ""
            if not t.strip():
                continue
            is_guide = bool(re.search(r"BUYER.?S\s*GUIDE", t, re.I))
            has_standard = bool(re.search(r"STANDARD\s*[-–]", t, re.I))
            has_equipment = bool(_EQUIPMENT_PAGE.search(t))
            if is_guide or has_standard or has_equipment:
                text_parts.append(t)
                pages_used.append(i + 1)
                for table in page.extract_tables() or []:
                    for row in table:
                        line = " | ".join(str(c or "").strip() for c in row if c)
                        if re.search(r"STANDARD\s*[-–]", line, re.I):
                            text_parts.append(line)

    if text_parts:
        return "\n".join(text_parts), pages_used

    # Fallback: last pages (legacy importer behavior).
    fallback: list[str] = []
    fb_pages: list[int] = []
    with pdfplumber.open(str(path)) as pdf:
        start = max(0, len(pdf.pages) - 6)
        for i in range(start, len(pdf.pages)):
            page = pdf.pages[i]
            try:
                t = page.extract_text() or ""
            except Exception:
                t = ""
            if t.strip():
                fallback.append(t)
                fb_pages.append(i + 1)
    return "\n".join(fallback), fb_pages


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


def extract_brochure_pdf(path: Path) -> BrochureExtractResult | None:
    ymm = parse_brochure_filename(path)
    if not ymm:
        return None

    text, pages = _extract_pdf_text(path)
    warnings: list[str] = []
    if not text.strip():
        warnings.append("no_text_extracted")
        return BrochureExtractResult(
            ymm=ymm,
            source_pdf=str(path),
            pages_used=pages,
            warnings=warnings,
        )

    ladder_id, ladder_order = _ladder_trim_order(ymm)
    allowed = set(ladder_order) if ladder_order else None
    sections = _find_trim_sections(
        text, allowed_trims=allowed, make=ymm.make, model=ymm.model
    )
    if not sections and ladder_order:
        sections = _find_trim_sections(
            text, allowed_trims=None, make=ymm.make, model=ymm.model
        )
    extracted: dict[str, list[str]] = {}
    for i, (start, trim_name, _header) in enumerate(sections):
        end = sections[i + 1][0] if i + 1 < len(sections) else len(text)
        block = text[start:end]
        feats = _split_features(block)
        if feats:
            canon = _canonical_trim_name(trim_name, make=ymm.make, model=ymm.model)
            if canon in extracted:
                seen = {f.lower() for f in extracted[canon]}
                for f in feats:
                    if f.lower() not in seen:
                        extracted[canon].append(f)
                        seen.add(f.lower())
            else:
                extracted[canon] = feats

    matrix_column_order: list[str] = []
    if not extracted:
        matrix_extracted, matrix_pages, matrix_column_order = _extract_equipment_matrix(
            path, ymm, ladder_order=ladder_order
        )
        if matrix_extracted:
            extracted = matrix_extracted
            pages = sorted(set(pages) | set(matrix_pages))
            warnings.append("extracted_via_equipment_matrix")

    if ladder_order:
        # Base → top for delta math.
        ladder_base_top = list(reversed(ladder_order))
        mapped = _map_extracted_to_ladder(extracted, ladder_base_top, make=ymm.make, model=ymm.model)
        trims_available = [t for t in ladder_base_top if mapped.get(t)]
        if len(trims_available) < 2 and matrix_column_order:
            ladder_base_top = matrix_column_order
            mapped = extracted
            trims_available = [t for t in matrix_column_order if mapped.get(t)]
        elif len(trims_available) < 2:
            ladder_base_top = list(extracted.keys())
            mapped = extracted
            trims_available = ladder_base_top
        adds_by_trim = _compute_adds_over_lower(ladder_base_top, mapped)
        trims_available = [
            t for t in trims_available if adds_by_trim.get(t) or mapped.get(t)
        ]
    else:
        ladder_id = None
        trims_available = list(extracted.keys())
        order = trims_available
        adds_by_trim = _compute_adds_over_lower(order, extracted)
        mapped = extracted

    if not mapped:
        warnings.append("no_trim_sections_found")
    elif "extracted_via_equipment_matrix" in warnings and not adds_by_trim:
        warnings.append("matrix_found_no_adds_delta")

    return BrochureExtractResult(
        ymm=ymm,
        ladder_id=ladder_id,
        trims_available=trims_available,
        adds_by_trim={k: v for k, v in adds_by_trim.items() if v},
        standard_by_trim=mapped,
        source_pdf=str(path),
        pages_used=pages,
        warnings=warnings,
    )


def _overlay_path(ymm: BrochureYMM) -> Path:
    safe = ymm.catalog_key.replace("|", "__")
    return TRIM_ADDS_BY_YEAR_DIR / f"{safe}.json"


def _facts_path(ymm: BrochureYMM) -> Path:
    make_dir = BROCHURE_FACTS_DIR / ymm.make.replace(" ", "_")
    slug = ymm.model.replace(" ", "_").lower()
    return make_dir / f"{ymm.year}_{slug}.json"


def persist_brochure_extract(result: BrochureExtractResult) -> Path | None:
    """Write brochure_facts + trim_adds_by_year JSON. Returns overlay path if written."""
    if not result.adds_by_trim and not result.standard_by_trim:
        return None

    TRIM_ADDS_BY_YEAR_DIR.mkdir(parents=True, exist_ok=True)
    BROCHURE_FACTS_DIR.mkdir(parents=True, exist_ok=True)

    overlay = {
        "catalog_key": result.ymm.catalog_key,
        "year": result.ymm.year,
        "make": result.ymm.make,
        "model": result.ymm.model,
        "ladder_id": result.ladder_id,
        "trims_available": result.trims_available,
        "adds_by_trim": result.adds_by_trim,
        "source_pdf": result.source_pdf,
        "pages_used": result.pages_used,
        "warnings": result.warnings,
    }
    op = _overlay_path(result.ymm)
    op.write_text(json.dumps(overlay, indent=2), encoding="utf-8")

    facts = {
        "catalog_key": result.ymm.catalog_key,
        "standard_by_trim": result.standard_by_trim,
        "adds_by_trim": result.adds_by_trim,
        "warnings": result.warnings,
    }
    fp = _facts_path(result.ymm)
    fp.parent.mkdir(parents=True, exist_ok=True)
    fp.write_text(json.dumps(facts, indent=2), encoding="utf-8")

    _write_marketing_trim_csv_rows(result)
    return op


def _write_marketing_trim_csv_rows(result: BrochureExtractResult) -> None:
    """Append/update marketing trim rows in Complete_Options for dictionary_adds."""
    import csv

    ymm = result.ymm
    make_dir = OPTIONS_RAW_DIR / ymm.make.replace(" ", "_")
    make_dir.mkdir(parents=True, exist_ok=True)
    slug = ymm.model.replace(" ", "_")
    csv_path = make_dir / f"{ymm.year}_{ymm.make}_{slug}_Complete_Options.csv"

    fieldnames = list(CANONICAL_CSV_COLUMNS)
    rows: list[dict[str, str]] = []
    if csv_path.is_file():
        with csv_path.open(encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            fieldnames = list(reader.fieldnames or fieldnames)
            rows = list(reader)

    # Remove prior brochure marketing rows for this YMM.
    rows = [
        r
        for r in rows
        if not (r.get("Trim") or "").strip().startswith("[Brochure")
        and (r.get("Trim") or "").strip() not in result.adds_by_trim
    ]

    existing_trims = {(r.get("Trim") or "").strip() for r in rows}
    for trim_name, adds in result.adds_by_trim.items():
        if not adds:
            continue
        opt = "; ".join(adds)
        detail = " | ".join(f"[Brochure] {a}" for a in adds)
        if trim_name in existing_trims:
            for row in rows:
                if (row.get("Trim") or "").strip() == trim_name:
                    row["Options"] = opt
                    row["optionDetails"] = detail
                    row["Year"] = str(ymm.year)
                    row["Make"] = ymm.make
                    row["Model"] = ymm.model
            continue
        row = {col: "" for col in fieldnames}
        row["Year"] = str(ymm.year)
        row["Make"] = ymm.make
        row["Model"] = ymm.model
        row["Trim"] = trim_name
        row["Options"] = opt
        row["optionDetails"] = detail
        rows.append(row)

    with csv_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def brochure_overlay_is_usable(data: dict[str, Any]) -> bool:
    """Reject auto-promoted overlays with junk trim tokens or brochure footer lines."""
    if not isinstance(data, dict):
        return False
    make = str(data.get("make") or "")
    model = str(data.get("model") or "")
    src = str(data.get("source") or "").lower()
    if "manual" in src or "curated" in src:
        return bool(data.get("adds_by_trim"))

    from backend.enrichment.trim_ladder import _is_valid_trim_name
    from backend.enrichment.trim_ladder_knowledge import is_generic_trim_add

    trims = list(data.get("trims_available") or [])
    adds = data.get("adds_by_trim") or {}
    if not isinstance(adds, dict):
        return False
    valid_trims = [
        str(t).strip()
        for t in trims
        if str(t).strip() and _is_valid_trim_name(str(t).strip(), make=make, model=model)
    ]
    if len(valid_trims) < 2:
        return False

    substantive_trims = 0
    for trim_name, bullets in adds.items():
        if not _is_valid_trim_name(str(trim_name).strip(), make=make, model=model):
            continue
        if any(
            str(b).strip()
            and not is_generic_trim_add(str(b), str(trim_name))
            for b in (bullets or [])
        ):
            substantive_trims += 1
    return substantive_trims >= max(1, len(valid_trims) // 2)


def _read_brochure_overlay_file(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(data, dict) or not data.get("adds_by_trim"):
        return None
    if not brochure_overlay_is_usable(data):
        return None
    return data


def load_brochure_trim_overlay(year: Any, make: str, model: str) -> dict[str, Any] | None:
    try:
        y = int(year)
    except (TypeError, ValueError):
        return None
    for delta in (0, -1, 1, -2, 2):
        candidate = y + delta
        if candidate < 2010:
            continue
        path = TRIM_ADDS_BY_YEAR_DIR / f"{catalog_key(candidate, make, model).replace('|', '__')}.json"
        data = _read_brochure_overlay_file(path)
        if data:
            from backend.enrichment.brochure_overlay_backfill import enrich_brochure_overlay

            return enrich_brochure_overlay(data)
    return None


def process_brochure_file(
    path: Path,
    *,
    delete_after: bool = False,
    dry_run: bool = False,
) -> BrochureExtractResult | None:
    result = extract_brochure_pdf(path)
    if not result:
        logger.warning("skip unparseable filename: %s", path.name)
        return None
    if dry_run:
        return result
    overlay = persist_brochure_extract(result)
    if not overlay:
        logger.warning("no adds persisted for %s", path.name)
        return result
    if delete_after:
        try:
            path.unlink()
        except OSError as exc:
            logger.error("failed to delete %s: %s", path, exc)
            result.warnings.append(f"delete_failed:{exc}")
    return result


def iter_brochure_pdfs(directory: Path | None = None) -> list[Path]:
    root = directory or BROCHURES_DIR
    if not root.is_dir():
        return []
    return sorted(root.glob("*.pdf"), key=lambda p: p.name)
