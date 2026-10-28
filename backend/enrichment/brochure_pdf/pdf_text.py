"""PDF text and layout capture, brochure_text JSON payload and persistence.

Split out of ``brochure_extract`` (P17C.4); ``brochure_extract`` re-exports every name.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from backend.enrichment.brochure_pdf.archive import BrochureYMM, parse_brochure_filename, sha256_file
from backend.enrichment.dictionary_paths import BROCHURE_TEXT_DIR

__all__ = [
    "BROCHURE_TEXT_SCHEMA_VERSION",
    "BrochurePageText",
    "BrochureTextResult",
    "_EQUIPMENT_PAGE",
    "_LAYOUT_GAP_CHAR_MULTIPLE",
    "_LAYOUT_MIN_CELL_GAP",
    "_LAYOUT_ROW_TOLERANCE",
    "_TRIM_PAGE_HINT",
    "_brochure_text_path",
    "_capture_status",
    "_extract_pdf_text",
    "_page_layout_lines",
    "_page_table_lines",
    "brochure_text_payload",
    "brochure_text_rejection",
    "dumps_brochure_text",
    "extract_brochure_text_pdf",
    "persist_brochure_text",
]

logger = logging.getLogger(__name__)


# Bumped when the shape of a derived/brochure_text JSON changes.
#   (absent) v1  only pages whose text matched the trim-hint regex, text only
#   2            every page of the PDF, plus per-line cell x-coordinates
BROCHURE_TEXT_SCHEMA_VERSION = 2

# Layout capture. Words are bucketed into rows by their ``top`` and split into
# cells wherever the horizontal gap exceeds a space-ish width, so a spec-grid row
# survives as "label at x=52, value at x=229, value at x=430" instead of one
# flattened string. Column→trim attribution is the single biggest failure class
# in this corpus and these x-positions are what make it decidable.
_LAYOUT_ROW_TOLERANCE = 2.5
_LAYOUT_MIN_CELL_GAP = 4.0
_LAYOUT_GAP_CHAR_MULTIPLE = 1.2


_EQUIPMENT_PAGE = re.compile(
    r"Standard\s+equipment|Specifications|Features\s+\w+\s+\w+",
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
class BrochurePageText:
    page: int
    text: str
    trim_hint: bool = False
    table_lines: list[str] = field(default_factory=list)
    # v2 layout signal. ``lines`` rows are {"top": float, "cells": [[x0, x1, text], ...]}.
    width: float = 0.0
    height: float = 0.0
    lines: list[dict[str, Any]] = field(default_factory=list)
    has_text_layer: bool = False


@dataclass
class BrochureTextResult:
    ymm: BrochureYMM
    source_pdf: str
    page_count: int = 0
    pages: list[BrochurePageText] = field(default_factory=list)
    trim_hint_pages: list[int] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    all_pages: bool = True
    source_pdf_sha256: str = ""
    source_pdf_bytes: int = 0
    layout_captured: bool = False

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


def _page_layout_lines(page: Any) -> list[dict[str, Any]]:
    """
    Word rows with cell x-extents, in reading order.

    Returns ``[{"top": 91.5, "cells": [[267.6, 276.7, "2.5 S"], ...]}, ...]``.
    Empty list when the page has no extractable words or pdfplumber raises.
    """
    try:
        words = page.extract_words(use_text_flow=False, keep_blank_chars=False) or []
    except Exception:
        return []

    rows: dict[int, list[dict[str, Any]]] = {}
    for w in words:
        try:
            top = float(w["top"])
        except (KeyError, TypeError, ValueError):
            continue
        rows.setdefault(int(round(top / _LAYOUT_ROW_TOLERANCE)), []).append(w)

    out: list[dict[str, Any]] = []
    for key in sorted(rows):
        ws = sorted(rows[key], key=lambda w: float(w["x0"]))
        groups: list[list[dict[str, Any]]] = [[ws[0]]]
        for prev, cur in zip(ws, ws[1:]):
            prev_text = str(prev.get("text") or "")
            char_w = (float(prev["x1"]) - float(prev["x0"])) / max(1, len(prev_text))
            gap = float(cur["x0"]) - float(prev["x1"])
            if gap > max(_LAYOUT_MIN_CELL_GAP, char_w * _LAYOUT_GAP_CHAR_MULTIPLE):
                groups.append([cur])
            else:
                groups[-1].append(cur)
        cells = [
            [
                round(float(g[0]["x0"]), 1),
                round(float(g[-1]["x1"]), 1),
                " ".join(str(w.get("text") or "") for w in g),
            ]
            for g in groups
        ]
        out.append(
            {
                "top": round(min(float(w["top"]) for w in ws), 1),
                "cells": cells,
            }
        )
    return out


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
    all_pages: bool = True,
    capture_layout: bool = True,
) -> BrochureTextResult | None:
    """
    Capture every page of a brochure PDF: text, table lines and cell x-extents.

    Capture is lossless — one entry per page of the PDF, in page order, including
    pages with no text layer (recorded with ``text=""`` and
    ``has_text_layer=False`` so a later parser can tell "grid is an image" from
    "grid was thrown away"). ``trim_hint`` is still flagged per page for the
    readers that only want the marketing/FCA pages, but it no longer decides what
    is written to disk.

    ``all_pages`` is accepted for callers that still pass it and is ignored:
    filtering pages at capture time is the bug this function stopped doing. It
    threw away the equipment grids for 1,368 of 3,204 brochures, and those PDFs
    were then deleted.

    ``capture_layout=False`` skips word-position extraction (~4x smaller JSON,
    and no column→trim attribution is possible from the result).

    Does not compute adds_by_trim and does not delete the PDF.
    """
    ymm = parse_brochure_filename(path)
    if not ymm:
        return None

    digest = ""
    size = 0
    try:
        digest = sha256_file(path)
        size = path.stat().st_size
    except OSError:
        pass

    try:
        import pdfplumber
    except ImportError:
        return BrochureTextResult(
            ymm=ymm,
            source_pdf=str(path),
            warnings=["pdfplumber_not_installed"],
            source_pdf_sha256=digest,
            source_pdf_bytes=size,
        )

    warnings: list[str] = []
    pages_out: list[BrochurePageText] = []
    trim_pages: list[int] = []
    page_count = 0
    layout_rows = 0

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
                lines = _page_layout_lines(page) if capture_layout else []
                layout_rows += len(lines)
                trim_hint = bool(_TRIM_PAGE_HINT.search(text)) or any(
                    _TRIM_PAGE_HINT.search(line) for line in table_lines
                )
                if trim_hint:
                    trim_pages.append(page_num)
                pages_out.append(
                    BrochurePageText(
                        page=page_num,
                        text=text,
                        trim_hint=trim_hint,
                        table_lines=table_lines,
                        width=round(float(page.width or 0), 1),
                        height=round(float(page.height or 0), 1),
                        lines=lines,
                        has_text_layer=bool(text.strip()) or bool(lines),
                    )
                )
            trim_pages = sorted(set(trim_pages))
    except Exception as exc:
        warnings.append(f"pdf_open_failed:{exc}")
        return BrochureTextResult(
            ymm=ymm,
            source_pdf=str(path),
            warnings=warnings,
            source_pdf_sha256=digest,
            source_pdf_bytes=size,
        )

    if not any(p.has_text_layer for p in pages_out):
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
        all_pages=True,
        source_pdf_sha256=digest,
        source_pdf_bytes=size,
        layout_captured=capture_layout and layout_rows > 0,
    )


def _brochure_text_path(ymm: BrochureYMM) -> Path:
    safe = ymm.catalog_key.replace("|", "__")
    return BROCHURE_TEXT_DIR / f"{safe}.json"


def _capture_status(result: BrochureTextResult) -> str:
    """``lossless`` only when there is one persisted entry per page of the PDF."""
    if not result.page_count:
        return "failed"
    return "lossless" if len(result.pages) == result.page_count else "partial"


def brochure_text_payload(result: BrochureTextResult) -> dict[str, Any]:
    """
    The derived/brochure_text JSON document for one brochure.

    Every v1 key keeps its name and meaning, so existing readers
    (``brochure_trim_candidates``, ``brochure_promote``, the LLM scripts) need no
    change: they read ``pages[].page/.text/.table_lines``, ``page_count``,
    ``trim_hint_pages``, ``warnings`` and ``combined_trim_pages_text``, and
    ``combined_trim_pages_text`` still contains only the trim-hint pages. What is
    new is additive: ``schema_version``, per-page ``lines``/``width``/``height``/
    ``has_text_layer``, and the source-PDF identity fields.

    Deterministic: the same PDF produces byte-identical JSON on every run.
    """
    return {
        "schema_version": BROCHURE_TEXT_SCHEMA_VERSION,
        "catalog_key": result.ymm.catalog_key,
        "year": result.ymm.year,
        "make": result.ymm.make,
        "model": result.ymm.model,
        "source_pdf": result.source_pdf,
        "source_pdf_name": Path(result.source_pdf).name if result.source_pdf else "",
        "source_pdf_sha256": result.source_pdf_sha256,
        "source_pdf_bytes": result.source_pdf_bytes,
        "page_count": result.page_count,
        "pages_captured": len(result.pages),
        "capture": _capture_status(result),
        "layout_captured": result.layout_captured,
        "all_pages": result.all_pages,
        "trim_hint_pages": result.trim_hint_pages,
        "warnings": result.warnings,
        "pages": [
            {
                "page": p.page,
                "trim_hint": p.trim_hint,
                "has_text_layer": p.has_text_layer,
                "width": p.width,
                "height": p.height,
                "text": p.text,
                "table_lines": p.table_lines,
                "lines": p.lines,
            }
            for p in result.pages
        ],
        "combined_trim_pages_text": result.combined_trim_pages_text,
    }


def dumps_brochure_text(payload: dict[str, Any]) -> str:
    """
    ``json.dumps(payload, indent=2)`` with each layout row kept on one line.

    Indenting the coordinate arrays the same way as the rest of the document
    costs ~2.6x the bytes (measured: 499KB vs 193KB on the 2024 Mazda CX-90) for
    output nobody reads a token at a time. One row per line stays greppable.
    """
    sentinel = "\x00"
    rows: list[str] = []

    def _stash(row: Any) -> str:
        rows.append(json.dumps(row, separators=(",", ":")))
        return f"{sentinel}LAYOUT{len(rows) - 1}{sentinel}"

    pages = payload.get("pages") or []
    shallow = dict(payload)
    shallow["pages"] = [
        {**p, "lines": [_stash(row) for row in (p.get("lines") or [])]} for p in pages
    ]
    text = json.dumps(shallow, indent=2)
    for i, row in enumerate(rows):
        token = json.dumps(f"{sentinel}LAYOUT{i}{sentinel}")
        if token not in text:
            # A NUL in the extracted text would make the substitution ambiguous.
            return json.dumps(payload, indent=2)
        text = text.replace(token, row)
    return text


def brochure_text_rejection(result: BrochureTextResult) -> str:
    """
    Why this capture may not go into the live corpus, or ``""`` if it may.

    The corpus gates live in ``brochure_sources``: ``assess_text_quality`` (does
    the text decode, is there enough of it, did a font subset remap a character
    class into the Private Use Area, does it carry the document's numbers) and
    ``assess_nameplate_dominance`` (is the document about this nameplate or
    about one derived from it, the M3-filed-as-3-Series case).

    WHY IT IS CALLED FROM HERE and not left to the caller. Checked 2026-08-02,
    :func:`persist_brochure_text` has three callers and only one of them ran
    those gates first:

    * ``backend/scripts/fetch_oem_brochures.py`` (code in
      ``backend/enrichment/brochure_acquisition/download.py``) — checks both,
      then persists. The check here repeats its verdict and changes nothing.
    * ``backend/scripts/reingest_brochures.py`` — checks neither. It consults
      ``brochure_sources.is_quarantined`` so it cannot resurrect a file already
      moved aside, but a document it extracts for the first time was written
      with no quality or subject test at all.
    * ``backend/scripts/extract_brochure_text.py`` — checks neither.

    A gate enforced on one of three write paths is not a gate on the class, and
    the extraction phase this corpus is about to go through runs on the other
    two. Imported lazily because ``brochure_sources`` imports this module.

    TWO READINGS OF THE PAGE, both checked, because the two existing lanes do
    not agree on what a page's text is: ``brochure_acquisition.corpus.page_texts``
    appends the layout table lines, ``brochure_acquisition.corpus.corpus_page_texts``
    (the re-validation sweep) reads ``page.text`` alone. A document can pass one
    and fail the other — the table lines carry most of a spec grid's digits.
    Failing either one is a rejection here, so this never admits something the
    later sweep would move aside anyway.
    """
    from backend.enrichment.brochure_sources import (
        assess_nameplate_dominance,
        assess_text_quality,
    )

    plain = [p.text or "" for p in result.pages]
    with_tables = [
        "\n".join([p.text or ""] + (["\n".join(p.table_lines)] if p.table_lines else []))
        for p in result.pages
    ]
    for pages in (with_tables, plain):
        subject = assess_nameplate_dominance(pages, result.ymm.make, result.ymm.model)
        if not subject.ok:
            return subject.reason
        verdict = assess_text_quality("\n".join(pages))
        if not verdict.ok:
            return verdict.reason
    return ""


def persist_brochure_text(result: BrochureTextResult) -> Path:
    """
    Write the lossless page-capture JSON under derived/brochure_text/.

    A capture that fails :func:`brochure_text_rejection` is written to
    ``derived/brochure_text_quarantine/`` instead, and that path is returned.

    QUARANTINED, NOT REFUSED, and not deleted. The extraction is expensive and
    the reason a document is rejected can turn out to be wrong, so the bytes are
    kept where they can be moved back — the same promise
    ``fetch_oem_brochures.py --quarantine-unidentified`` makes. Writing aside
    rather than raising also means the two callers that do no checking of their
    own keep running and simply do not add the document to the corpus; raising
    would abort a batch inside ``reingest_brochures``, which catches only
    ``OSError``.
    """
    reason = brochure_text_rejection(result)
    if reason:
        from backend.enrichment.brochure_sources import BROCHURE_TEXT_QUARANTINE_DIR

        BROCHURE_TEXT_QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
        out = BROCHURE_TEXT_QUARANTINE_DIR / _brochure_text_path(result.ymm).name
        logger.warning("quarantined %s: %s", result.ymm.catalog_key, reason)
    else:
        BROCHURE_TEXT_DIR.mkdir(parents=True, exist_ok=True)
        out = _brochure_text_path(result.ymm)
    out.write_text(dumps_brochure_text(brochure_text_payload(result)), encoding="utf-8")
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


