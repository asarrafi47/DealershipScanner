"""Extract and promote brochure text into trim_adds_by_year overlays (GC quality bar)."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from backend.enrichment.brochure_extract import (
    BrochureYMM,
    _canonical_trim_name,
    _compute_adds_over_lower,
    _find_trim_sections,
    _ladder_trim_order,
    _normalize_feature,
    _split_features,
)
from backend.enrichment.brochure_trim_candidates import (
    load_brochure_text_json,
    parse_trim_names_from_text,
)
from backend.enrichment.dictionary_catalog import catalog_key
from backend.enrichment.dictionary_paths import TRIM_ADDS_BY_YEAR_DIR
from backend.enrichment.trim_ladder_knowledge import luxury_rank, sanitize_brochure_trim_adds

_PAGE_SPLIT = re.compile(r"---\s*page\s+\d+\s*---", re.I)
_MODELS_LINE = re.compile(r"^\s*MODELS\s*$", re.I)
_SPECS_TRIM_ROW = re.compile(
    r"^Mechanical/Performance\s+((?:[A-Z][A-Z0-9®™'\-]{1,12})(?:\s+[A-Z][A-Z0-9®™'\-]{1,12}){1,7})\s*$",
    re.M,
)
_ADDS_TO_LINE = re.compile(
    r"Adds to or replaces features (?:off\s*ered|on)\s+([A-Za-z0-9®™'\-\s]{2,28})",
    re.I,
)
_SOLO_TRIM_LINE = re.compile(r"^[A-Z][A-Z0-9®™'\-]{1,12}$")
_SECTION_REPEAT = re.compile(
    r"^(?:Mechanical/Performance|Exterior Features|Interior Features|"
    r"Audio Multimedia|Safety and Convenience Features|Packages/Options)\s*$",
    re.I,
)

_DOT_BULLET = re.compile(r"[•●]")
_MATRIX_SECTION = re.compile(
    r"^(?:Safety|Seating|Audio|Instrumentation|Exterior|Comfort|Convenience|"
    r"Mechanical|Performance|Fuel|Economy)\b",
    re.I,
)
_MATRIX_HEADER_CELL = re.compile(
    r"(?:[A-Z]{1,4}(?:-[A-Z0-9]+)?(?:\s+V-6)?"
    r"(?:/[A-Z]{1,4}(?:-[A-Z0-9]+)?(?:\s+V-6)?)*)"
)

_JUNK_ADD_PHRASES = (
    "exterior features exterior",
    "mechanical/performance mechanical",
    "includes these key features adds",
    "adds to or replaces features off",
    "transmission (ecvt) with sequential shift mode transmission",
    "normal, eco, ev",
    "connected services12 trials",
    "4g network dependent",
    "see numbered footnotes",
    "packages/options",
)


def add_line_quality_ok(line: str) -> bool:
    low = re.sub(r"\s+", " ", (line or "").strip().lower())
    if len(low) < 18 or len(low) > 240:
        return False
    if any(p in low for p in _JUNK_ADD_PHRASES):
        return False
    if low.count(" features ") >= 2:
        return False
    if sum(1 for c in line if c.isupper()) > len(line) * 0.85 and len(line) > 40:
        return False
    return len(low.split()) >= 4


def split_parallel_columns(line: str, n: int) -> list[str]:
    line = re.sub(r"\s+", " ", (line or "").strip())
    if not line or n <= 1:
        return [line]
    parts = re.split(r"\s{2,}", line)
    if len(parts) == n:
        return [p.strip() for p in parts]
    for width in range(8, min(72, len(line) // max(n, 1)) + 1):
        for start in range(0, min(24, len(line) - width)):
            chunk = line[start : start + width].strip()
            if len(chunk) < 6:
                continue
            hits = [m.start() for m in re.finditer(re.escape(chunk), line)]
            if len(hits) >= n:
                segments: list[str] = []
                for i in range(n):
                    seg_start = hits[i]
                    seg_end = hits[i + 1] if i + 1 < len(hits) else len(line)
                    segments.append(line[seg_start:seg_end].strip())
                if len(segments) == n and all(len(s) > 4 for s in segments):
                    return segments
    return [line]


def _trim_tokens_from_line(line: str, make: str, model: str) -> list[str]:
    from backend.enrichment.trim_ladder import _is_valid_trim_name

    tokens = re.split(r"\s+", line.strip())
    out: list[str] = []
    for tok in tokens:
        t = tok.strip("®™,")
        if not t or len(t) > 20:
            continue
        if not _SOLO_TRIM_LINE.match(t):
            continue
        if _is_valid_trim_name(t, make=make, model=model):
            out.append(_canonical_trim_name(t, make=make, model=model))
    return out


def _line_is_trim_header(line: str, make: str, model: str) -> list[str] | None:
    """Line that is only trim tokens (e.g. 'LE SE XSE')."""
    raw = line.strip()
    if not raw or re.search(r"\bMODELS\b", raw, re.I):
        return None
    parts = raw.split()
    if len(parts) < 2 or len(parts) > 8:
        return None
    toks = _trim_tokens_from_line(raw, make, model)
    if len(toks) != len(parts):
        return None
    return toks


def _discover_trim_order(text: str, make: str, model: str) -> list[str]:
    m = _SPECS_TRIM_ROW.search(text)
    if m:
        toks = _trim_tokens_from_line(m.group(1), make, model)
        if len(toks) >= 2:
            return toks
    found: list[str] = []
    for line in text.splitlines():
        if _MODELS_LINE.match(line.strip()):
            continue
        if re.search(r"\bMODELS\b", line, re.I):
            toks = _trim_tokens_from_line(re.sub(r".*MODELS\s*", "", line, flags=re.I), make, model)
            if len(toks) >= 2:
                return toks
            continue
    found = parse_trim_names_from_text(text, make=make, model=model)
    if len(found) >= 2:
        found.sort(key=lambda name: luxury_rank(name, make, model))
    return found


def _parse_models_column_standards(
    text: str,
    trim_order: list[str],
    *,
    make: str,
    model: str,
) -> dict[str, list[str]]:
    n = len(trim_order)
    if n < 2:
        return {}
    acc: dict[str, list[str]] = {t: [] for t in trim_order}
    in_models = False
    for line in text.splitlines():
        raw = line.strip()
        if not raw:
            continue
        if raw.startswith("--- page"):
            in_models = False
            continue
        if re.search(r"\bMODELS\b", raw, re.I) and "SPECIFICATIONS" not in raw.upper():
            in_models = True
            toks = _trim_tokens_from_line(re.sub(r".*MODELS\s*", "", raw, flags=re.I), make, model)
            if len(toks) >= 2:
                trim_order = toks
                n = len(trim_order)
                acc = {t: [] for t in trim_order}
            continue
        if "SPECIFICATIONS" in raw.upper():
            in_models = False
            continue
        if not in_models:
            continue
        header_toks = _line_is_trim_header(raw, make, model)
        if header_toks:
            trim_order = header_toks
            n = len(trim_order)
            acc = {t: [] for t in trim_order}
            continue
        if _SECTION_REPEAT.match(raw) or raw.lower().startswith("includes these key"):
            continue
        if _ADDS_TO_LINE.search(raw) and raw.lower().count("adds to") >= 2:
            continue
        parts = split_parallel_columns(raw, n)
        if len(parts) != n:
            continue
        for trim_name, part in zip(trim_order, parts):
            feat = _normalize_feature(part)
            if not feat:
                continue
            bucket = acc.setdefault(trim_name, [])
            if feat.lower() not in {x.lower() for x in bucket}:
                bucket.append(feat)
    return acc


def _parse_single_trim_pages(
    text: str,
    *,
    make: str,
    model: str,
) -> dict[str, list[str]]:
    acc: dict[str, list[str]] = {}
    for block in _PAGE_SPLIT.split(text):
        lines = [ln.strip() for ln in block.splitlines() if ln.strip()]
        if len(lines) < 4:
            continue
        if re.search(r"\bMODELS\b", block, re.I):
            solo_header_trims = sum(
                1
                for ln in lines[1:8]
                if _SOLO_TRIM_LINE.match(ln) and "MODELS" not in ln.upper()
            )
            if solo_header_trims >= 2:
                continue
        trim_name: str | None = None
        for ln in lines[:6]:
            if _SOLO_TRIM_LINE.match(ln) and "MODELS" not in ln.upper():
                trim_name = _canonical_trim_name(ln, make=make, model=model)
                break
        if not trim_name or not _ADDS_TO_LINE.search(block):
            continue
        feats: list[str] = []
        after_adds = False
        for ln in lines:
            if _ADDS_TO_LINE.search(ln):
                after_adds = True
                continue
            if not after_adds:
                continue
            if _SECTION_REPEAT.match(ln) and len(ln) < 50:
                continue
            if ln.startswith("See numbered footnotes"):
                break
            feat = _normalize_feature(ln)
            if feat and add_line_quality_ok(feat):
                feats.append(feat)
        if len(feats) >= 3:
            prev = acc.get(trim_name) or []
            if len(feats) > len(prev):
                acc[trim_name] = feats
    return acc


def _split_matrix_header_cells(rest: str) -> list[str]:
    cells = [m.group(0).strip() for m in _MATRIX_HEADER_CELL.finditer(rest)]
    return [c for c in cells if c and "/" in c or re.search(r"[A-Z]{2}", c)]


def _expand_trim_cell(cell: str, *, make: str, model: str) -> list[str]:
    from backend.enrichment.trim_ladder import _is_valid_trim_name

    out: list[str] = []
    for part in re.split(r"/", cell):
        part = re.sub(r"\s+", " ", part.strip())
        if not part:
            continue
        canon = _canonical_trim_name(part, make=make, model=model)
        if _is_valid_trim_name(canon, make=make, model=model):
            out.append(canon)
    return out


def _parse_inline_trim_bullet_rows(
    text: str,
    *,
    make: str,
    model: str,
) -> dict[str, list[str]]:
    """Honda rows like 'Leather-Trimmed Seats … SE EX-L EX-L V-6 EX-L •'."""
    from backend.enrichment.trim_ladder import _is_valid_trim_name

    acc: dict[str, list[str]] = {}
    trim_tail = re.compile(r"^([A-Z][A-Z0-9\-]+(?:\s+V-6)?)$")

    for line in text.splitlines():
        line = line.strip()
        if not line.endswith("•"):
            continue
        left = line[:-1].strip()
        if not left:
            continue
        trim_tokens: list[str] = []
        while True:
            m = re.search(r"\s((?:[A-Z][A-Z0-9\-]+(?:\s+V-6)?))$", left)
            if not m or not trim_tail.match(m.group(1).strip()):
                break
            trim_tokens.insert(0, m.group(1).strip())
            left = left[: m.start()].strip()
        if not trim_tokens:
            continue
        feat = _normalize_feature(left)
        if not feat:
            continue
        if not add_line_quality_ok(feat) and (
            len(feat) < 10 or len(feat.split()) < 2
        ):
            continue
        for raw_t in trim_tokens:
            t = _canonical_trim_name(raw_t, make=make, model=model)
            if not _is_valid_trim_name(t, make=make, model=model):
                continue
            bucket = acc.setdefault(t, [])
            if feat.lower() not in {x.lower() for x in bucket}:
                bucket.append(feat)
    return acc


def _parse_dot_feature_matrix(
    text: str,
    *,
    make: str,
    model: str,
) -> dict[str, list[str]]:
    """
    Honda/Acura-style grids: header row lists trim columns, feature rows end with • per column.
    """
    acc: dict[str, list[str]] = {}
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    i = 0
    while i < len(lines):
        line = lines[i]
        if not _MATRIX_SECTION.match(line):
            i += 1
            continue
        header_rest = re.sub(r"^[^\s]+\s+", "", line, count=1).strip()
        header_cells = _split_matrix_header_cells(header_rest)
        if not header_cells and i + 1 < len(lines) and "/" in lines[i + 1] and "•" not in lines[i + 1]:
            if not _MATRIX_SECTION.match(lines[i + 1]):
                header_cells = _split_matrix_header_cells(lines[i + 1].strip())
                i += 1
        column_trims = [_expand_trim_cell(c, make=make, model=model) for c in header_cells if c.strip()]
        column_trims = [c for c in column_trims if c]
        if not column_trims or sum(len(c) for c in column_trims) < 2:
            i += 1
            continue
        ncol = len(column_trims)
        i += 1
        while i < len(lines):
            row = lines[i]
            if _MATRIX_SECTION.match(row) or row.startswith("All 2012") or row.startswith("Whether you"):
                break
            if row.startswith("GENUINE ACCESSORIES") or len(row) > 400:
                i += 1
                continue
            m = re.search(r"^(.+?)\s+((?:[•●]\s*)+)$", row)
            if not m:
                i += 1
                continue
            feat = _normalize_feature(m.group(1))
            if not feat:
                i += 1
                continue
            bullets = re.findall(r"[•●]", m.group(2))
            if not bullets or len(bullets) > ncol:
                i += 1
                continue
            for ci, trim_list in enumerate(column_trims):
                if ci >= len(bullets):
                    continue
                for t in trim_list:
                    bucket = acc.setdefault(t, [])
                    if feat.lower() not in {x.lower() for x in bucket}:
                        bucket.append(feat)
            i += 1
    return acc


def _parse_fca_standard_blocks(
    text: str,
    ymm: BrochureYMM,
    *,
    ladder_order: list[str],
) -> dict[str, list[str]]:
    allowed = set(ladder_order) if ladder_order else None
    sections = _find_trim_sections(
        text, allowed_trims=allowed, make=ymm.make, model=ymm.model
    )
    if not sections and ladder_order:
        sections = _find_trim_sections(text, allowed_trims=None, make=ymm.make, model=ymm.model)
    extracted: dict[str, list[str]] = {}
    for i, (start, trim_name, _header) in enumerate(sections):
        end = sections[i + 1][0] if i + 1 < len(sections) else len(text)
        feats = _split_features(text[start:end])
        if feats:
            canon = _canonical_trim_name(trim_name, make=ymm.make, model=ymm.model)
            extracted[canon] = feats
    return extracted


def extract_promotable_overlay(
    data: dict[str, Any],
) -> dict[str, Any] | None:
    """Return overlay fields if extraction passes GC quality bar."""
    year = int(data.get("year") or 0)
    make = str(data.get("make") or "")
    model = str(data.get("model") or "")
    if not year or not make or not model:
        return None

    text = str(data.get("combined_trim_pages_text") or "")
    if not text:
        for pg in data.get("pages") or []:
            if isinstance(pg, dict):
                text += "\n" + str(pg.get("text") or "")

    ymm = BrochureYMM(year=year, make=make, model=model)
    ladder_id, ladder_order = _ladder_trim_order(ymm)
    warnings: list[str] = []

    standard: dict[str, list[str]] = {}
    fca = _parse_fca_standard_blocks(text, ymm, ladder_order=ladder_order)
    if fca:
        standard.update(fca)
        warnings.append("fca_standard_blocks")

    trim_order = _discover_trim_order(text, make, model)
    if len(trim_order) >= 2:
        col = _parse_models_column_standards(text, trim_order, make=make, model=model)
        for t, feats in col.items():
            if len(feats) > len(standard.get(t) or []):
                standard[t] = feats
        if col:
            warnings.append("models_column_matrix")

    solo = _parse_single_trim_pages(text, make=make, model=model)
    for t, feats in solo.items():
        if len(feats) > len(standard.get(t) or []):
            standard[t] = feats
    if solo:
        warnings.append("single_trim_pages")

    dot = _parse_dot_feature_matrix(text, make=make, model=model)
    for t, feats in dot.items():
        if len(feats) > len(standard.get(t) or []):
            standard[t] = feats
    if dot:
        warnings.append("dot_feature_matrix")

    inline = _parse_inline_trim_bullet_rows(text, make=make, model=model)
    for t, feats in inline.items():
        merged = list(standard.get(t) or [])
        for f in feats:
            if f.lower() not in {x.lower() for x in merged}:
                merged.append(f)
        if merged:
            standard[t] = merged
    if inline:
        warnings.append("inline_trim_bullet_rows")

    if len(standard) < 2:
        return None

    # luxury_rank: lower = more luxurious; adds math needs base → top.
    order = sorted(
        standard.keys(),
        key=lambda t: luxury_rank(t, make, model),
        reverse=True,
    )
    if ladder_order:
        from backend.enrichment.brochure_trim_candidates import filter_spurious_brochure_trims

        ladder_order = filter_spurious_brochure_trims(
            ladder_order, make=make, model=model
        )
        order = [t for t in reversed(ladder_order) if t in standard]
        for t in sorted(
            standard.keys(),
            key=lambda t: luxury_rank(t, make, model),
            reverse=True,
        ):
            if t not in order:
                order.append(t)

    if len(order) < 2:
        return None

    adds_raw = _compute_adds_over_lower(order, standard)
    adds_by_trim: dict[str, list[str]] = {}
    for trim_name, bullets in adds_raw.items():
        cleaned = []
        for b in sanitize_brochure_trim_adds(bullets, trim_name):
            if add_line_quality_ok(b) or (len(b) >= 10 and len(b.split()) >= 2):
                cleaned.append(b)
        if cleaned:
            adds_by_trim[trim_name] = cleaned[:12]

    from backend.enrichment.trim_ladder import _is_valid_trim_name

    trims_available = [
        t
        for t in order
        if (adds_by_trim.get(t) or standard.get(t))
        and _is_valid_trim_name(t, make=make, model=model)
    ]
    adds_by_trim = {
        k: v for k, v in adds_by_trim.items() if _is_valid_trim_name(k, make=make, model=model)
    }
    if len(trims_available) < 2:
        return None
    # trims_available is base-to-top from `order`; sort ascending by luxury_rank so most
    # luxurious is first.  When luxury_rank ties (all 9999 for unknown trims), use the
    # reversed position in `order` as tiebreaker so the top trim stays first.
    _order_pos = {t: i for i, t in enumerate(trims_available)}
    trims_available = sorted(
        trims_available,
        key=lambda t: (luxury_rank(t, make, model), -_order_pos.get(t, 0)),
    )  # most luxurious first for UI
    if not meets_gc_quality_bar(order, adds_by_trim):
        return None
    if not any(
        add_line_quality_ok(b)
        for bullets in adds_by_trim.values()
        for b in bullets
        if not re.search(r"\bauthorized\b.*\bcenter\b|\bbmwusa\.com\b", str(b), re.I)
    ):
        return None

    return {
        "catalog_key": catalog_key(year, make, model),
        "year": year,
        "make": make,
        "model": model,
        "ladder_id": ladder_id,
        "source": "promoted_brochure_auto",
        "trims_available": trims_available,
        "adds_by_trim": adds_by_trim,
        "warnings": warnings,
        "promotion_quality": score_overlay_quality(trims_available, adds_by_trim),
    }


def score_overlay_quality(
    trims_available: list[str],
    adds_by_trim: dict[str, list[str]],
) -> dict[str, Any]:
    upper = trims_available[1:] if len(trims_available) > 1 else []
    per_trim: dict[str, int] = {}
    for t in upper:
        per_trim[t] = len(adds_by_trim.get(t) or [])
    return {
        "trim_count": len(trims_available),
        "upper_trims_with_adds": sum(1 for c in per_trim.values() if c >= 4),
        "avg_adds_upper": (
            sum(per_trim.values()) / len(per_trim) if per_trim else 0.0
        ),
        "per_trim_add_counts": per_trim,
    }


def meets_gc_quality_bar(
    ladder_base_to_top: list[str],
    adds_by_trim: dict[str, list[str]],
) -> bool:
    """Align with 2016 Grand Cherokee bar: 2+ trims, multiple real equipment deltas."""
    if len(ladder_base_to_top) < 2:
        return False
    upper = ladder_base_to_top[1:]
    if not upper:
        return False
    strong = 0
    for t in upper:
        n = len(adds_by_trim.get(t) or [])
        if n >= 4:
            strong += 1
        elif n >= 3 and any("replaces" in a.lower() or "—" in a for a in adds_by_trim.get(t) or []):
            strong += 1
    need = max(1, min(3, len(upper) // 2 + (1 if len(upper) >= 4 else 0)))
    if strong >= need:
        return True
    from backend.enrichment.trim_ladder_knowledge import is_generic_trim_add

    substantive = 0
    for t in upper:
        bullets = adds_by_trim.get(t) or []
        if any(
            str(b).strip()
            and not is_generic_trim_add(str(b), t)
            for b in bullets
        ):
            substantive += 1
    return substantive >= max(1, len(upper) // 2)


def existing_overlay_blocks_promotion(catalog_key_str: str) -> bool:
    path = TRIM_ADDS_BY_YEAR_DIR / f"{catalog_key_str.replace('|', '__')}.json"
    if not path.is_file():
        return False
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    src = str(data.get("source") or "").lower()
    if "manual" in src or "curated" in src:
        return True
    abt = data.get("adds_by_trim") or {}
    if isinstance(abt, dict) and any(abt.get(k) for k in abt):
        if src == "promoted_brochure_auto":
            return False
        return True
    return False


def promote_from_brochure_text(
    catalog_key_str: str,
    *,
    overwrite: bool = False,
) -> Path | None:
    if existing_overlay_blocks_promotion(catalog_key_str) and not overwrite:
        return None
    data = load_brochure_text_json(catalog_key_str)
    if not data:
        return None
    payload = extract_promotable_overlay(data)
    if not payload:
        return None
    TRIM_ADDS_BY_YEAR_DIR.mkdir(parents=True, exist_ok=True)
    out = TRIM_ADDS_BY_YEAR_DIR / f"{catalog_key_str.replace('|', '__')}.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return out
