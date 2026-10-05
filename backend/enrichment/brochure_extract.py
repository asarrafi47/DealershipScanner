"""Extract trim-level standard equipment from OEM brochure PDFs."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import shutil
from dataclasses import dataclass, field
from datetime import datetime, timezone
from functools import lru_cache
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

_REPO_ROOT = Path(__file__).resolve().parents[2]

# ---------------------------------------------------------------------------
# Durable source-PDF archive
# ---------------------------------------------------------------------------
#
# ``BROCHURES_DIR`` (backend/data/brochures) is a STAGING lane: the fetcher drops
# PDFs there and ``backend/scripts/delete_brochure_pdfs.py --confirm`` empties it
# once text has been extracted. That deletion is why the corpus cannot be
# re-parsed today — for 1,368 of 3,204 brochures the extractor kept only the
# pages that matched a trim-hint regex, and the PDF holding the discarded
# equipment grid is gone.
#
# The archive below is the copy that is never deleted. It is a separate tree
# from the staging lane, it is written before any delete, and re-archiving a file
# that is already there is a no-op. Point ``BROCHURE_ARCHIVE_DIR`` at durable
# storage (a mounted volume, an S3-synced directory) in production; the default
# is repo-local and carries its own ``.gitignore`` so the binaries are never
# committed.
#
#   backend/data/brochure_archive/
#     .gitignore                        # "*" — written on first archive write
#     mazda/2024_Mazda_CX-5_Brochure.pdf
#     mazda/2024_Mazda_CX-5_Brochure.pdf.meta.json
#
# The sidecar records sha256 + byte size + where the file came from, so a bullet
# quoted out of the text JSON can be tied back to the exact bytes it was read
# from. A name collision whose sha256 differs is stored beside the incumbent as
# ``<stem>__<sha12>.pdf`` — an archive write never overwrites another document.

BROCHURE_ARCHIVE_ENV = "BROCHURE_ARCHIVE_DIR"
DEFAULT_BROCHURE_ARCHIVE_DIR = _REPO_ROOT / "backend" / "data" / "brochure_archive"

_ARCHIVE_GITIGNORE = (
    "# Durable OEM brochure archive: source documents, never deleted, never committed.\n"
    "*\n"
)

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


def parse_brochure_filename(path: Path) -> BrochureYMM | None:
    m = _FILENAME_YMM.match(path.stem)
    if not m:
        return None
    return BrochureYMM(
        year=int(m.group(1)),
        make=m.group(2).replace("_", " ").strip(),
        model=m.group(3).replace("_", " ").strip(),
    )


def sha256_file(path: Path, *, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        while True:
            block = fh.read(chunk)
            if not block:
                break
            h.update(block)
    return h.hexdigest()


def brochure_archive_dir() -> Path:
    """Directory holding the source PDFs. Read from the env on every call."""
    raw = (os.environ.get(BROCHURE_ARCHIVE_ENV) or "").strip()
    return Path(raw).expanduser().resolve() if raw else DEFAULT_BROCHURE_ARCHIVE_DIR


def _archive_make_dir(ymm: BrochureYMM | None) -> str:
    make = (ymm.make if ymm else "").strip().lower()
    slug = re.sub(r"[^a-z0-9]+", "_", make).strip("_")
    return slug or "_unparsed"


def archived_pdf_path(path: Path, *, ymm: BrochureYMM | None = None) -> Path:
    """Where ``path`` belongs in the archive (does not touch the filesystem)."""
    return brochure_archive_dir() / _archive_make_dir(ymm or parse_brochure_filename(path)) / path.name


def archive_source_pdf(
    path: Path,
    *,
    ymm: BrochureYMM | None = None,
    source_url: str = "",
    dry_run: bool = False,
) -> Path | None:
    """
    Copy a brochure PDF into the durable archive and return its archived path.

    Never deletes, never overwrites: a same-named file whose sha256 differs is
    stored as ``<stem>__<sha12>.pdf`` beside it. Re-archiving identical bytes is
    a no-op that still returns the archived path, so callers can use a non-None
    return as "this document is safe on disk".
    """
    if not path.is_file():
        return None
    ymm = ymm or parse_brochure_filename(path)
    try:
        digest = sha256_file(path)
        size = path.stat().st_size
    except OSError as exc:
        logger.error("archive: cannot read %s: %s", path, exc)
        return None

    target = archived_pdf_path(path, ymm=ymm)
    if target.is_file():
        try:
            if sha256_file(target) == digest:
                if not dry_run:
                    _write_archive_meta(target, path, ymm, digest, size, source_url)
                return target
        except OSError:
            return None
        target = target.with_name(f"{target.stem}__{digest[:12]}{target.suffix}")
        if target.is_file():
            if not dry_run:
                _write_archive_meta(target, path, ymm, digest, size, source_url)
            return target

    if dry_run:
        return target

    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        gitignore = brochure_archive_dir() / ".gitignore"
        if not gitignore.is_file():
            gitignore.write_text(_ARCHIVE_GITIGNORE, encoding="utf-8")
        tmp = target.with_name(f".{target.name}.partial")
        shutil.copyfile(path, tmp)
        tmp.replace(target)
    except OSError as exc:
        logger.error("archive: failed to store %s: %s", path, exc)
        return None
    _write_archive_meta(target, path, ymm, digest, size, source_url)
    return target


def _write_archive_meta(
    target: Path,
    origin: Path,
    ymm: BrochureYMM | None,
    digest: str,
    size: int,
    source_url: str,
) -> None:
    meta_path = target.with_name(target.name + ".meta.json")
    if meta_path.is_file():
        try:
            prior = json.loads(meta_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            prior = {}
        if isinstance(prior, dict) and prior.get("sha256") == digest:
            return
    meta = {
        "sha256": digest,
        "bytes": size,
        "archived_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "archived_from": str(origin),
        "original_name": origin.name,
        "catalog_key": ymm.catalog_key if ymm else "",
        "year": ymm.year if ymm else None,
        "make": ymm.make if ymm else "",
        "model": ymm.model if ymm else "",
        "source_url": source_url,
    }
    try:
        meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    except OSError as exc:
        logger.error("archive: failed to write %s: %s", meta_path, exc)


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


# --- provenance gate: no citation, no bullet -----------------------------
#
# The overlays under ``derived/trim_adds_by_year`` were written by several
# generations of extractor. Only some of them can say WHERE a bullet came from:
#
#   brochure_text_quoted    per-bullet {text, source file, page} in adds_provenance
#   manual_brochure_review / brochure_llm / brochure_llm_web /
#   brochure_llm_knowledge / promoted_brochure_auto
#                           no citation of any kind is recorded, per bullet or
#                           per file (see the note on ADMISSIBLE_OVERLAY_SOURCES)
#
# A shopper cannot tell those apart on the page, so the uncited ones do not
# render. ``ADMISSIBLE_OVERLAY_SOURCES`` is the list of overlay sources allowed
# through, and it is derived from ``LADDER_BULLET_STORES`` below — that table is
# the single place a later phase widens the policy, for overlays and for every
# other store that can put a bullet on a rung.

_OFF_VALUES = ("0", "false", "no", "off")

# --- every store that can put a bullet on a rung, and the rule it must satisfy ---
#
# The overlays are not the only producer. A rung's ``adds`` are assembled in
# ``trim_ladder._build_ladder_result`` from all of these:
#
#   brochure_text_quoted   overlay bullets carrying {text, source file, page}
#   brochure_trim_walk     an engine sentence BUILT around a line off a brochure
#                          "<TRIM> / Adds to <LOWER>" page (see the revocation
#                          note below — it composes, so it is uncited now)
#   epa_csv                backend/dictionary/epa/**/*_EPA.csv — addressable as
#                          file + row, but the cell we would quote is a sentence
#                          OUR importer composed (see the revocation note below)
#   manual_brochure_review /
#   brochure_llm           overlay bullets with no citation (see the note below)
#   curated_trim_ladder    hand-typed adds in curated/trim_ladders.json
#   generated_trim_ladder  curated/trim_ladders_generated.json, machine-written
#                          from the Complete_Options CSVs
#   complete_options_csv   options/raw/**/*_Complete_Options.csv
#   trim_spec_sheet        derived/trim_spec_sheets/*.json
#   oem_knowledge_prose    trim_ladder_knowledge position copy ("Mid-level trim
#                          with added convenience features…")
#   inventory_prose        price notes derived from our own active listings
#
# ``LADDER_BULLET_STORES`` is the ONE place that says which of those a shopper
# may see. Everything else — the overlay gate, the ladder-def step adds, the
# spec sheets, the CSV scrapes — reads its verdict from here, so widening the
# policy later is a single-row edit rather than a hunt through call sites.
LADDER_BULLET_STORES: dict[str, str] = {
    "brochure_text_quoted": "verified_per_bullet_citation",
    # Revoked 2026-07-31 — see the note below. Was "per_bullet_citation".
    "brochure_trim_walk": "uncited",
    # Revoked 2026-07-31 — see the two grounds below. Was "per_bullet_citation".
    "epa_csv": "uncited",
    "manual_brochure_review": "uncited",
    "brochure_llm": "uncited",
    "curated_trim_ladder": "uncited",
    "generated_trim_ladder": "uncited",
    "complete_options_csv": "uncited",
    "trim_spec_sheet": "uncited",
    "oem_knowledge_prose": "uncited",
    "inventory_prose": "uncited",
}

ADMISSIBLE_LADDER_BULLET_STORES = frozenset(
    store for store, rule in LADDER_BULLET_STORES.items() if rule != "uncited"
)

# Stores whose bullets additionally have to be signed off by
# ``backend/scripts/verify_trim_citations.py`` BEFORE a request can render them.
# The script re-opens the source PDF and checks the bullet is printed there; the
# render path then reads its verdict out of the data instead of re-deciding.
VERIFIED_LADDER_BULLET_STORES = frozenset(
    store
    for store, rule in LADDER_BULLET_STORES.items()
    if rule == "verified_per_bullet_citation"
)

# ``brochure_trim_walk`` WAS admitted on the reasoning that a brochure page
# headed "<TRIM> / Adds to <LOWER>" states the engine step-up itself. Revoked
# 2026-07-31: the store is the same defect class as ``epa_csv``, on two counts.
#
#  1. IT COMPOSES ITS BULLET. ``trim_ladder._brochure_engine_bullet_by_step``
#     builds ``f"{engine} — added over the {base_name}"``. Only ``engine`` is
#     printed in the brochure; the em dash, the words "added over the" and
#     ``base_name`` (taken from OUR ladder definition, not from the page) are
#     ours. The composed string was then handed to the citation register
#     (``citations.add_exact(engine_claim[0], …)``), so the gate was certifying
#     text no document contains — the register said yes because we had just told
#     it to.
#  2. THE TRIM IS INFERRED POSITIONALLY, NOT READ FROM A LABEL.
#     ``_brochure_engine_step_ups`` scans back up to 3 lines from the "Adds to
#     <LOWER>" marker and takes the first non-marker line as the trim name. That
#     is a guess about page layout, and it misreads: it is how a heading, a
#     footnote or a stray caption becomes the trim a claim is attributed to.
#
# Re-admitting it needs BOTH fixed: a bullet that is one printed line quoted
# whole, and a trim name read off a labelled field. It would then also have to
# pass ``verify_trim_citations.py`` like every other admitted store.

# Why ``complete_options_csv`` is blocked rather than cited like the EPA rows:
# backend/dictionary/README.md describes options/raw as "Wikipedia/NHTSA/brochure"
# and nothing in the row records WHICH of the three a given cell came from. A
# citation that cannot name the document is not a citation, and Wikipedia is not
# an admissible source here in any case. ``generated_trim_ladder`` is machine
# output of the same CSVs (its ``source`` is the CSV filename) and inherits the
# same problem, on top of re-ingesting its own prior output — the 2025 Tonale
# Premium rung reads "Upgrades Engine Options: Upgrades Engine Options: L GME T4
# turbo I4 … (was Layout Front-engine, front-wheel-drive …)".
#
# ``trim_spec_sheet`` is derived FROM the two blocked stores (see
# ``trim_spec_extractor.build_spec_sheet``), so admitting it would reintroduce
# them one level down; 1,259 of the 1,260 sheets are marked "generated" and none
# of the 1,260 carries a provenance field of any kind.
#
# ``epa_csv`` WAS admitted, on the reasoning that an EPA row is our own catalog
# data rather than invented prose and can be named as file + row. That reasoning
# was wrong on two independent grounds and the store is revoked (2026-07-31).
# Do not re-admit it on either of them:
#
#  1. THE CELL IS A SENTENCE WE COMPOSED, NOT ONE WE QUOTE.
#     ``backend/scripts/import_epa_to_dictionary.py:build_engine_desc`` BUILDS
#     the ``engineOptions`` string out of separate FuelEconomy.gov columns:
#     ``parts.append(f"{displ}L")``, ``parts.append("I%d"/"V%d" % cyl)``,
#     ``parts.append(f"({eng_dscr})")``. The 2021 Jeep Grand Cherokee Trackhawk
#     row reads "6.2L V8 (Hellcat engine)" — three of our own concatenations
#     around one quoted fragment. Citing it cites our own synthesis, which is
#     the circularity this gate exists to stop, and the V/I bank letter is a
#     guess from the cylinder count (the 2026 Ram 2500 Laramie row pairs
#     engineOptions "6.7L Cummins Turbo Diesel I6" with engineDisplay
#     "6.7L V6 Turbo").
#
#  2. THE FILE IS NOT ALWAYS THIS VEHICLE'S FILE. ``find_epa_csv`` used to fall
#     back to a sibling model's CSV. Measured 2026-07-31 over the 5,020 active
#     (year, make, model) groups that resolved to an EPA file: 1,439 groups /
#     11,121 active cars resolved to a file whose ``Model`` column is a
#     different model (2025 Chevrolet "Blazer EV" → the gas 2025 Blazer file,
#     2026 Ford "F-250SD" → ``1999_Ford_F250_EPA.csv``). That fallback is now
#     removed in ``dictionary_catalog.find_epa_csv``, but a store may not depend
#     on a resolver being right about which vehicle it opened.
#
# Cost of the revocation, measured on the live fleet the same day by resolving
# the ladder for all 14,465 active (year, make, model, trim) groups: cars showing
# at least one ladder bullet fall 20,851 → 2,118 of 72,504 active cars, because
# 18,699 of those cars had nothing but EPA-cited bullets. Silence is the correct
# output for them.
#
# Two rules that were recorded here while the store was admitted still hold for
# anyone re-reading the EPA rows for a different purpose: ``drivetrainOptions``
# is not a trim-ladder add (the row describes the one configuration it was filed
# for, so "All-Wheel Drive" as something a rung ADDS is our inference —
# ``trim_spec_extractor._extract_epa_trim_fields`` refuses it for the same
# reason), and ``engineDisplay`` is our synthesized label, not source text.
#
# --- 2026-07-31, second pass: narrow to one store and verify it at build time --
#
# Three more defects were found live with the gate ON, and all three are fixed
# above. Measured the same day by resolving the ladder for all 14,465 active
# (year, make, model, trim) groups, on 72,504 active cars, one change at a time:
#
#   stage                                       groups   cars ≥1 bullet  bullets
#   gate on, all three defects live                177          2,119      2,698
#   + brochure_trim_walk revoked                   155          2,032      2,658
#   + citations must be the car's OWN model year   130          1,969      2,340
#   + citations verified against the PDF            57          1,760      1,105
#
# so the three cost 87 / 63 / 209 cars and 40 / 318 / 1,235 bullets respectively.
# 895 active cars had been quoting a brochure from a NEIGHBOURING model year —
# 154 of them a 2026 RAV4 book read as if it described a 2024 car.
#
# The build-time run behind the last row (``verify_trim_citations.py``, which
# re-opens the PDF and re-extracts the cited page) checked all 614 provenance
# entries in the 26 admissible overlays:
#
#   verified                        322   (12 brochures we still hold)
#   source_pdf_not_held             287   (14 brochures lost off disk)
#   text_not_printed_on_cited_page    5
#
# The 5 are real: "Front tow hooks (4x only)" where the 2022 Titan XD brochure
# prints "(4x4 only)", and four cases where a footnote marker was dropped out of
# the middle of a printed line. Re-checked with a second, independent PDF reader
# (PyMuPDF rather than pdfplumber): it agrees on all 327 checkable entries.
#
# The number is meant to be small. Growing it means ACQUIRING BROCHURES — the
# 287 lost citations come back the moment their PDFs are on disk again and the
# verifier is re-run — not widening this table.

# Overlay ``source`` strings live in their own namespace; map them onto the
# stores above so the admissibility verdict still comes from one table.
_OVERLAY_SOURCE_STORES: dict[str, str] = {
    "brochure_text_quoted": "brochure_text_quoted",
    "manual_brochure_review": "manual_brochure_review",
    "brochure_llm": "brochure_llm",
    "brochure_llm_web": "brochure_llm",
    "brochure_llm_knowledge": "brochure_llm",
    "promoted_brochure_auto": "brochure_llm",
}

ADMISSIBLE_OVERLAY_SOURCES = frozenset(
    source
    for source, store in _OVERLAY_SOURCE_STORES.items()
    if store in ADMISSIBLE_LADDER_BULLET_STORES
)


def ladder_bullet_store_admissible(store: Any) -> bool:
    """True when bullets from this store may be shown, gate permitting."""
    return str(store or "").strip().lower() in ADMISSIBLE_LADDER_BULLET_STORES


def ladder_bullet_store_for_source(source: Any) -> str:
    """
    A ladder definition's ``source`` string → the store id its step ``adds`` came from.

    Ladder defs label themselves inconsistently: ``"curated"``, ``"inventory"``,
    ``"oem_knowledge"``, the literal ``"EPA CSV"``, or a bare filename
    (``"2022_Toyota_RAV4_EPA.csv"``, ``"2022_Jeep_Grand_Cherokee_Complete_Options.csv"``).
    Anything unrecognised maps to an uncited store, so a new producer is silent
    until it is added to :data:`LADDER_BULLET_STORES` on purpose.
    """
    text = str(source or "").strip().lower()
    if text == "curated":
        return "curated_trim_ladder"
    if text == "inventory":
        return "inventory_prose"
    if text == "oem_knowledge":
        return "oem_knowledge_prose"
    if text == "brochure":
        return "brochure_llm"
    if text == "epa csv" or text.endswith("_epa.csv") or text.endswith("_epa"):
        return "epa_csv"
    if "complete_options" in text:
        return "complete_options_csv"
    return "generated_trim_ladder"


# --- every store that can put a RUNG NAME on the ladder, and its rule ---------
#
# ``LADDER_BULLET_STORES`` above governs the BULLETS. It said nothing about the
# card they sit on. The rung names — which trims exist for this vehicle, and in
# what order — were asserted with no provenance at all: a 2023 Ram 1500 Limited
# listed nine rungs (TRX, Limited, Laramie Longhorn, Laramie, Rebel, Lone Star…)
# and zero bullets, and a shopper reads that list as "these are the trims of this
# vehicle". The producers were the hand-written ``curated/trim_ladders.json``,
# the machine-written ``trim_ladders_generated.json``, the Complete_Options and
# EPA CSVs, LLM brochure overlays, and — when nothing else matched — a
# hard-coded ``generic_fallback_steps`` table in ``trim_ladder_knowledge``.
#
# Same discipline as the bullets: a rung renders only if we can point at
# something that is not our own synthesis.
#
#   active_inventory        at least one ACTIVE row in our own ``cars`` table,
#                           same make + model + EXACT model year, whose ``trim``
#                           string EQUALS the rung's displayed name once both
#                           are case-folded and stripped of non-alphanumerics
#                           ("Tremor®" == "Tremor"; "Sport Utility" != "Sport").
#                           The claim this makes is narrow and checkable:
#                           "dealers are listing this trim, spelled this way,
#                           for this year of this vehicle." ``name_provenance``
#                           carries the literal spellings, so we can name the
#                           rows.
#   brochure_text_quoted    a rung of a ``brochure_text_quoted`` overlay FOR THIS
#                           MODEL YEAR that carries at least one bullet the
#                           build-time verifier confirmed is printed in the PDF
#                           (``verify_trim_citations.py``). The trim heading is
#                           the label that bullet was filed under in a document
#                           we re-opened, and it is matched to the rung name by
#                           the same exact rule.
#
# NEITHER STORE MAY BE MATCHED FUZZILY, and that is the whole point rather than
# an implementation detail. The first version of the gate scored inventory trims
# against rung names and admitted anything >= 80, which is a prefix match: the 3
# active "Sport Utility" listings of the 2026 Ford Explorer justified a rung
# named "Sport" — a string that appears on none of the 13 distinct trim values
# on active 2026 Explorer rows and under no verified 2026 citation — and the
# gate stamped it ``active_inventory``, so the provenance label asserted an
# observation that had never been made. Measured 2026-08-01 across the 14,343
# active (year, make, model, trim) groups: 3,703 groups rendered at least one
# rung whose name matched nothing we hold, 29,777 car-by-rung impressions.
# A rung with no provenance is unsupported; a fabricated rung
# carrying a provenance label is laundered, which is strictly worse. Matching is
# therefore exact equality on ``trim_ladder._rung_evidence_key``, name against
# name, aliases excluded (an alias table is our own synthesis, so resolving
# an observed "Ltd" to a printed "Limited" would be us renaming the evidence).
#
# Everything else is uncited by the same reasoning already recorded above for the
# bullets, and additionally:
#
#   epa_csv                 ``epa_master.trim`` holds body/drive descriptors
#                           ("Pickup 4WD", "RAPTOR R 4WD"), not marketing trims,
#                           and the file a vehicle resolves to has been the wrong
#                           vehicle's file (measured 2026-07-31: 1,439 groups).
#   oem_knowledge_generic   ``generic_fallback_steps`` is a hand-typed table. It
#                           is the source of the "always shows a ladder" promise
#                           in this module's docstring, which is exactly the
#                           promise that had to go.
#   brochure_neighbour_year a ±1/±2 model-year overlay. Trims change between
#                           years; a 2022 book is not evidence about a 2024 car.
LADDER_RUNG_STORES: dict[str, str] = {
    "active_inventory": "observed_active_listing",
    "brochure_text_quoted": "verified_per_bullet_citation",
    "epa_csv": "uncited",
    "curated_trim_ladder": "uncited",
    "generated_trim_ladder": "uncited",
    "complete_options_csv": "uncited",
    "brochure_llm": "uncited",
    "manual_brochure_review": "uncited",
    "brochure_neighbour_year": "uncited",
    "oem_knowledge_generic": "uncited",
    "inventory_prose": "uncited",
}

ADMISSIBLE_LADDER_RUNG_STORES = frozenset(
    store for store, rule in LADDER_RUNG_STORES.items() if rule != "uncited"
)

TRIM_RUNGS_PROVENANCE_ENV = "TRIM_RUNGS_REQUIRE_PROVENANCE"


def ladder_rung_store_admissible(store: Any) -> bool:
    """True when a rung NAME justified by this store may be listed."""
    return str(store or "").strip().lower() in ADMISSIBLE_LADDER_RUNG_STORES


# --- every store that can assert the ORDER of the rungs, and its rule --------
#
# ``LADDER_BULLET_STORES`` governs the bullets, ``LADDER_RUNG_STORES`` the rung
# names. Neither said anything about the SEQUENCE, and the ladder is drawn as a
# hierarchy — a shopper reads the top card as the better vehicle. Until
# 2026-08-02 that sequence came from ``trim_ladder_knowledge.luxury_rank``, a
# hand-maintained table, applied by ``resolve_trim_ladder`` ON TOP OF a brochure
# order that had already been derived from the document and then discarded.
# Measured live by the main thread on 2026-08-01: the 2020 Durango rendered
# Citadel above R/T above GT, and the 2026 RAV4 rendered XLE Premium above XSE.
# Both contradict the vehicles' own books.
#
#   brochure_adds_to_edge      a page of the brochure headed "<TRIM> / Adds to
#                              <LOWER>" (or "<TRIM> INCLUDES <LOWER> EQUIPMENT
#                              PLUS"). The OEM states the relation itself, in
#                              its own words, on a page we hold and can name.
#                              This is the only store that PROVES an order.
#   brochure_printed_sequence  the order the document printed the grades in —
#                              successive trim-walk pages, or the columns of one
#                              equipment grid. The sequence is the document's,
#                              but "printed earlier means lower" is a habit of
#                              the format rather than a statement, and for a
#                              grid the direction is read off mark counts by
#                              ``brochure_trim_candidates._cell_grid_direction``
#                              — our inference. CORROBORATING ONLY: it may order
#                              rungs the edges do not reach and break ties the
#                              edges leave, and it may never move a rung an edge
#                              placed.
#   luxury_rank_table          ``trim_ladder_knowledge.luxury_rank`` / the
#                              per-model ordered trim lists it reads. Hand
#                              typed, no document behind any entry. It is still
#                              the last resort when the document says nothing,
#                              and every rung it places is labelled
#                              ``unproven`` so the caller can decline to draw a
#                              hierarchy at all.
#   curated_trim_ladder        the step order in ``curated/trim_ladders.json``.
#                              Hand typed, same objection.
#   inventory                  the order rungs happened to arrive in from our
#                              own ``cars`` rows. Not evidence of anything.
LADDER_ORDER_STORES: dict[str, str] = {
    "brochure_adds_to_edge": "quoted_edge_with_page",
    "brochure_printed_sequence": "printed_sequence_in_document",
    "luxury_rank_table": "uncited",
    "curated_trim_ladder": "uncited",
    "inventory": "uncited",
}

#: Stores whose ordering may be shown as a hierarchy at all.
ADMISSIBLE_LADDER_ORDER_STORES = frozenset(
    store for store, rule in LADDER_ORDER_STORES.items() if rule != "uncited"
)

#: The subset that PROVES a position rather than corroborating one.
PROVEN_LADDER_ORDER_STORES = frozenset(
    store for store, rule in LADDER_ORDER_STORES.items() if rule == "quoted_edge_with_page"
)

#: ``brochure_trim_candidates`` basis vocabulary -> the store it belongs to.
_ORDER_BASIS_STORES: dict[str, str] = {
    "adds_to_edge": "brochure_adds_to_edge",
    "page_sequence": "brochure_printed_sequence",
    "printed_sequence": "brochure_printed_sequence",
}

#: Overlay sources that record ordering evidence at all. Every other generation
#: of overlay writer stored a bare trim list with nothing behind its sequence.
ORDER_BEARING_OVERLAY_SOURCES = frozenset({"brochure_text_quoted"})


def ladder_order_store_admissible(store: Any) -> bool:
    """True when an order attributed to this store may be drawn as a hierarchy."""
    return str(store or "").strip().lower() in ADMISSIBLE_LADDER_ORDER_STORES


def order_basis_store(basis: Any) -> str:
    """The store a ``brochure_trim_candidates`` order basis belongs to ('' if none)."""
    return _ORDER_BASIS_STORES.get(str(basis or "").strip().lower(), "")


def _order_basis_entry_is_citable(entry: Any) -> bool:
    """True when one ``order_basis`` entry names where its claim can be checked.

    Same shape of test the bullets get: an ordering claim renders only if it can
    point at a file and a page. An ``adds_to_edge`` additionally has to carry the
    two printed lines it was read off and the trim it names, so a reviewer can
    re-open that page and find the sentence.
    """
    if not isinstance(entry, dict):
        return False
    basis = str(entry.get("basis") or "").strip().lower()
    store = order_basis_store(basis)
    if not store or not ladder_order_store_admissible(store):
        return False
    if basis == "adds_to_edge":
        edge = entry.get("edge")
        if not isinstance(edge, dict):
            return False
        try:
            page = int(edge.get("page") or 0)
        except (TypeError, ValueError):
            return False
        return bool(
            page > 0
            and str(edge.get("source") or "").strip()
            and str(edge.get("trim") or "").strip()
            and str(edge.get("below") or "").strip()
            and str(edge.get("trim_quote") or "").strip()
            and str(edge.get("below_quote") or "").strip()
        )
    if basis == "page_sequence":
        try:
            return int(entry.get("page") or 0) > 0
        except (TypeError, ValueError):
            return False
    if basis == "printed_sequence":
        pages = entry.get("pages")
        return bool(
            str(entry.get("source") or "").strip()
            and isinstance(pages, list)
            and any(isinstance(p, int) and p > 0 for p in pages)
        )
    return False


def overlay_rung_order(
    overlay: dict[str, Any] | None,
    *,
    for_year: Any = None,
) -> tuple[list[str], dict[str, dict[str, Any]]]:
    """The base→top rung order this overlay can account for, and why.

    Returns ``([], {})`` unless every one of these holds:

    * the overlay's source is one that records ordering evidence;
    * ``for_year`` is given and the overlay IS that model year's book — a
      neighbouring year's brochure is not evidence about this car's lineup, the
      same rule ``overlay_citable_for_year`` applies to the bullets;
    * at least two rungs carry an ``order_basis`` entry that names a file and a
      page.

    Rungs whose basis cannot be checked are dropped from the returned order
    rather than carried along unexplained.
    """
    if not isinstance(overlay, dict):
        return [], {}
    if str(overlay.get("source") or "").strip().lower() not in ORDER_BEARING_OVERLAY_SOURCES:
        return [], {}
    if not overlay_citable_for_year(overlay, for_year):
        return [], {}
    basis_raw = overlay.get("order_basis")
    if not isinstance(basis_raw, dict):
        return [], {}
    order = [
        str(t).strip()
        for t in (overlay.get("rung_order") or overlay.get("trims_available") or [])
        if str(t).strip()
    ]
    kept: list[str] = []
    basis: dict[str, dict[str, Any]] = {}
    for trim in order:
        entry = basis_raw.get(trim)
        if not _order_basis_entry_is_citable(entry):
            continue
        kept.append(trim)
        basis[trim] = {
            **entry,
            "store": order_basis_store(entry.get("basis")),
            "proven": order_basis_store(entry.get("basis")) in PROVEN_LADDER_ORDER_STORES,
        }
    if len(kept) < 2:
        return [], {}
    return kept, basis


def trim_rungs_provenance_required() -> bool:
    """
    Kill switch for the rung-name gate (see ``trim_ladder._justified_ladder_steps``).

    Default ON. ``TRIM_RUNGS_REQUIRE_PROVENANCE=0`` restores the previous
    behaviour of listing whatever rungs a ladder definition happened to carry.
    Read per call, not cached, so it can be flipped without a restart.
    """
    return (
        os.environ.get(TRIM_RUNGS_PROVENANCE_ENV) or "1"
    ).strip().lower() not in _OFF_VALUES


# ``manual_brochure_review`` is deliberately NOT in that set, though its name
# claims a human read the brochure. Measured 2026-07-31 by looking every overlay
# bullet up in the brochure text we still hold for that same catalog key
# (punctuation/case/whitespace-insensitive substring):
#
#   brochure_text_quoted     266 / 270    verbatim  (98.5%; the 4 misses are a ™
#                                         ligature and a mid-phrase line wrap)
#   brochure_llm          19,714 / 57,442 verbatim  (34.3%)
#   manual_brochure_review   100 / 2,018  verbatim  (5.0%)
#
# The 2011 Jeep Grand Cherokee Laredo rung that reported "3.6L Pentastar V6
# engine with 360 horsepower" — the 2011 Pentastar makes 290 (our own
# epa_extended_specs says 293); 360 is the 5.7 HEMI V8 — is a
# manual_brochure_review file. The label records that someone looked at the file,
# not that the bullets are the brochure's words, and none of the 125 such files
# carries a citation of any kind. Re-admit the source by flipping its row in
# ``LADDER_BULLET_STORES`` only once those files quote something.

# Sources whose bullets are checked one at a time: a bullet from one of these
# files renders only if adds_provenance names a file AND a page for that exact
# bullet text. An admissible source that is NOT listed here would be admitted
# whole-file; today every admissible source is per-bullet, so the two sets match.
PER_BULLET_PROVENANCE_SOURCES = frozenset({"brochure_text_quoted"})

TRIM_ADDS_PROVENANCE_ENV = "TRIM_ADDS_REQUIRE_PROVENANCE"


def trim_adds_provenance_required() -> bool:
    """
    Kill switch for the overlay provenance gate (see :func:`admissible_overlay_adds`).

    Default ON. Set ``TRIM_ADDS_REQUIRE_PROVENANCE=0`` to restore the previous
    behaviour of rendering every overlay bullet regardless of citation.
    """
    return (
        os.environ.get(TRIM_ADDS_PROVENANCE_ENV) or "1"
    ).strip().lower() not in _OFF_VALUES


# --- the pre-verified flag the render path trusts -------------------------
#
# ``backend/scripts/verify_trim_citations.py`` re-opens the source PDF named by
# each provenance entry, re-extracts the cited page, and stamps the entry with
# ``verified: true`` only when the bullet text is printed on that page. It is a
# build step; nothing at render time re-derives that verdict.
#
# FAIL CLOSED, TWICE OVER: an entry with no ``verified`` key at all (an overlay
# written before the verifier existed, or one the verifier could not check
# because we no longer hold the PDF) is NOT renderable. Only the literal value
# ``True`` passes. That is why the key is read with ``is True`` rather than
# truth-tested — a string "false" is truthy.
CITATION_VERIFIED_KEY = "verified"

#: Where ``verify_trim_citations.py`` records the run that produced those flags.
CITATION_VERIFICATION_KEY = "citation_verification"


#: The three Unicode Private Use Area blocks, duplicated from
#: ``brochure_sources._PRIVATE_USE_AREA`` because that module imports from this
#: one and the dependency may not run the other way.
#:
#: A codepoint in these blocks means whatever the PDF's own embedded font says
#: it means and nothing outside that font, so a bullet holding one is text we
#: cannot read being shown as if we could. ``brochure_sources`` refuses a whole
#: DOCUMENT when a character CLASS was remapped
#: (``MAX_PRIVATE_USE_CODEPOINTS``); this refuses a single BULLET for any PUA
#: character at all, which is the residue that gate deliberately leaves behind.
#:
#: WHY BOTH. Measured 2026-08-02 over ``derived/brochure_text``, before the
#: document gate swept anything: 122 live files held at least one PUA character,
#: and only 12 of them had had a class remapped. The other 110 are sound
#: documents with a dingbat or a punctuation glyph in a subsetted font, and are
#: still live — ``2012__cadillac__cts`` prints
#: ``DIRECT<U+F6BA>INJECTION``, ``2012__jaguar__xf`` prints ``18<U+E044>`` for an
#: 18-inch wheel, ``2020__buick__encoregx`` glues a tick mark onto the next
#: token as ``<U+F0A2>20``. Refusing those documents would throw away 110 books
#: over a hyphen. Refusing the individual bullet costs only the bullet, and a
#: bullet is exactly the unit that reaches a shopper.
#:
#: MEASURED COST at the time of writing: **zero renderable bullets**. None of
#: the 26 live ``brochure_text_quoted`` overlays carries a PUA character in any
#: bullet text. Four live overlays do — ``2013__bmw__5series``,
#: ``2015__bmw__7series`` (``promoted_brochure_auto``), ``2014__bmw__5series``
#: (``brochure_llm``) — and all four are already refused a step earlier by
#: ``LADDER_BULLET_STORES``. This check is what stops that text if any of those
#: stores is ever re-admitted, and what stops it arriving from the corpus-wide
#: extraction run this store is about to be filled by.
#: Built from ordinals rather than written as a literal character class. A raw
#: ``[-...]`` in source is invisible to a reviewer and survives
#: copy/paste badly — the first draft of this constant lost its BMP block in
#: transit and silently matched nothing but the two supplementary planes, which
#: is a gate that passes everything. ``test_bullet_pua_pattern_covers_bmp_block``
#: pins it.
_PUA_BLOCKS: tuple[tuple[int, int], ...] = (
    (0xE000, 0xF8FF),      # BMP Private Use Area — where subsetted fonts land
    (0xF0000, 0xFFFFD),    # Supplementary Private Use Area-A
    (0x100000, 0x10FFFD),  # Supplementary Private Use Area-B
)
_BULLET_PRIVATE_USE_AREA = re.compile(
    "[" + "".join(f"{chr(lo)}-{chr(hi)}" for lo, hi in _PUA_BLOCKS) + "]"
)


def bullet_text_is_readable(text: Any) -> bool:
    """
    False when a bullet holds a character we cannot actually read.

    Today that means a Unicode Private Use Area codepoint (see
    :data:`_BULLET_PRIVATE_USE_AREA`) or a U+FFFD replacement character — both
    mean a font's glyph did not resolve to a character, so the string is not the
    document's words even though it was copied out of the document.

    Fails closed: a bullet that cannot be read is not shown, and the rung simply
    has one less thing to say.
    """
    string = str(text or "")
    if "�" in string:
        return False
    return not _BULLET_PRIVATE_USE_AREA.search(string)


def _cited_bullet_texts(entries: Any, *, require_verified: bool) -> set[str]:
    """
    Bullet texts in one trim's ``adds_provenance`` that this overlay can account for.

    A text counts only when its entry names a document AND a page inside it, and
    — for stores in :data:`VERIFIED_LADDER_BULLET_STORES` — only when the
    build-time verifier has confirmed the text is printed there.
    """
    out: set[str] = set()
    if not isinstance(entries, list):
        return out
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        text = str(entry.get("text") or "").strip()
        source = str(entry.get("source") or "").strip()
        page = entry.get("page")
        if not text or not source or page is None:
            continue
        if not bullet_text_is_readable(text):
            # The citation may be perfectly sound — the text really is printed
            # on that page — but what we extracted from it is not the words. See
            # :data:`_BULLET_PRIVATE_USE_AREA`.
            continue
        if require_verified and entry.get(CITATION_VERIFIED_KEY) is not True:
            continue
        out.add(text)
    return out


def overlay_source_admissible(source: Any) -> bool:
    """True when this overlay's ``source`` may put bullets in front of a shopper."""
    return str(source or "").strip().lower() in ADMISSIBLE_OVERLAY_SOURCES


def _overlay_year(overlay: dict[str, Any] | None) -> int | None:
    try:
        return int((overlay or {}).get("year"))
    except (TypeError, ValueError):
        return None


def overlay_citable_for_year(overlay: dict[str, Any] | None, year: Any) -> bool:
    """
    True when this overlay is evidence about ``year`` — i.e. it IS that year's book.

    ``load_brochure_trim_overlay`` still reaches ±2 model years to learn which
    rungs a lineup had, because a trim list moves slowly. A CITATION may not: a
    2019 brochure does not say what a 2022 car has, and equipment moves between
    trims every refresh. Measured 2026-07-31 over the 26 ``brochure_text_quoted``
    overlays, the ±2 reach was answering for years those books never covered.
    Asked with ``year=None`` this returns False, so a caller that cannot say
    which car it is rendering gets no citations rather than the benefit of the
    doubt.
    """
    y = _overlay_year(overlay)
    if y is None:
        return False
    try:
        want = int(year)
    except (TypeError, ValueError):
        return False
    return y == want


def admissible_overlay_adds(
    overlay: dict[str, Any],
    *,
    for_year: Any = None,
) -> dict[str, list[str]]:
    """
    ``adds_by_trim`` reduced to the bullets this overlay can account for.

    Uncited source → ``{}``. Per-bullet-citation source → only the bullets whose
    exact text appears in ``adds_provenance`` with a file and a page, and (for a
    ``verified_per_bullet_citation`` store) only those the build-time verifier
    confirmed against the document. Idempotent, so it is safe to re-apply
    anywhere an overlay is consumed. With the kill switch off it returns
    ``adds_by_trim`` untouched.

    ``for_year`` is the model year of the car being rendered. When it is given,
    an overlay for a different year keeps its ``trims_available`` but contributes
    no bullets (see :func:`overlay_citable_for_year`). It defaults to None —
    meaning "not being rendered against a particular car", as when a script
    audits an overlay file — and in that case the year check does not apply;
    :func:`load_brochure_trim_overlay` always passes the real year.
    """
    raw = (overlay or {}).get("adds_by_trim") or {}
    if not isinstance(raw, dict):
        return {}
    # ``bullet_text_is_readable`` is applied HERE, before the provenance gate,
    # rather than only inside ``_cited_bullet_texts``. Two returns below leave
    # this function without consulting a citation at all — the kill switch, and
    # an admissible source that is not per-bullet — and an unreadable bullet is
    # wrong on every one of those paths, not only on the cited one.
    normalized = {
        str(k).strip(): [
            str(a).strip()
            for a in v
            if str(a).strip() and bullet_text_is_readable(a)
        ]
        for k, v in raw.items()
        if isinstance(v, list)
    }
    if not trim_adds_provenance_required():
        return normalized
    source = str((overlay or {}).get("source") or "").strip().lower()
    if not overlay_source_admissible(source):
        return {}
    if for_year is not None and not overlay_citable_for_year(overlay, for_year):
        return {}
    if source not in PER_BULLET_PROVENANCE_SOURCES:
        return normalized
    provenance = (overlay or {}).get("adds_provenance") or {}
    if not isinstance(provenance, dict):
        return {}
    require_verified = _OVERLAY_SOURCE_STORES.get(source) in VERIFIED_LADDER_BULLET_STORES
    out: dict[str, list[str]] = {}
    for trim, bullets in normalized.items():
        cited = _cited_bullet_texts(
            provenance.get(trim), require_verified=require_verified
        )
        kept = [b for b in bullets if b in cited]
        if kept:
            out[trim] = kept
    return out


def verified_overlay_trim_names(
    overlay: dict[str, Any] | None,
    *,
    for_year: Any,
) -> list[str]:
    """
    Trim names this overlay can justify LISTING as rungs — not merely name.

    A name qualifies only when the overlay is this car's own model year, its
    source is admissible for rung names, and the trim carries at least one
    ``adds_provenance`` entry the build-time verifier stamped ``verified: true``
    — i.e. we re-opened that PDF, re-extracted that page, and found the bullet
    printed on it under this trim.

    ``trims_available`` on its own is NOT enough and is deliberately ignored
    here: on an LLM-written overlay it is a list nobody checked, and even on a
    ``brochure_text_quoted`` overlay it is the extractor's reading of the lineup
    rather than a location in the document. Returns [] whenever any of that is
    missing, so the failure mode is a shorter ladder.
    """
    if not isinstance(overlay, dict):
        return []
    if not overlay_citable_for_year(overlay, for_year):
        return []
    source = str(overlay.get("source") or "").strip().lower()
    store = _OVERLAY_SOURCE_STORES.get(source)
    if not store or not ladder_rung_store_admissible(store):
        return []
    provenance = overlay.get("adds_provenance") or {}
    if not isinstance(provenance, dict):
        return []
    require_verified = store in VERIFIED_LADDER_BULLET_STORES
    out: list[str] = []
    for trim, entries in provenance.items():
        name = str(trim or "").strip()
        if not name:
            continue
        if _cited_bullet_texts(entries, require_verified=require_verified):
            out.append(name)
    return out


def apply_overlay_provenance_gate(
    overlay: dict[str, Any] | None,
    *,
    for_year: Any = None,
) -> dict[str, Any] | None:
    """
    Copy of ``overlay`` whose ``adds_by_trim`` holds only admissible bullets.

    The overlay object itself is kept even when every bullet is dropped: its
    ``trims_available`` still tells the ladder which rungs this model year had,
    and its presence is what stops the ladder substituting Complete_Options CSV
    lines or generated position copy for the bullets just removed. A rung with
    nothing left to say renders no bullets at all.
    """
    if not isinstance(overlay, dict):
        return overlay
    return {
        **overlay,
        "adds_by_trim": admissible_overlay_adds(overlay, for_year=for_year),
    }


def brochure_overlay_is_usable(data: dict[str, Any]) -> bool:
    """Reject auto-promoted overlays with junk trim tokens or brochure footer lines."""
    if not isinstance(data, dict):
        return False
    make = str(data.get("make") or "")
    model = str(data.get("model") or "")
    src = str(data.get("source") or "").lower()
    if "manual" in src or "curated" in src:
        return bool(data.get("adds_by_trim"))

    if src == "brochure_text_quoted":
        # Every bullet in this overlay is quoted from a named page of a named
        # brochure and carries that citation in ``adds_provenance``. The overlay
        # deliberately lists the whole lineup while claiming adds only for the
        # rungs the brochure's layout could be read for, so the "adds on half
        # the listed trims" rule below does not apply — two cited rungs is a
        # trim walk.
        quoted = data.get("trims_quoted") or [
            t for t, v in (data.get("adds_by_trim") or {}).items() if v
        ]
        provenance = data.get("adds_provenance") or {}
        cited = [
            t
            for t in quoted
            if (data.get("adds_by_trim") or {}).get(t) and provenance.get(t)
        ]
        return len(cited) >= 2

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


def _brochure_text_file(catalog_key_value: str) -> Path:
    return BROCHURE_TEXT_DIR / f"{str(catalog_key_value).replace('|', '__')}.json"


@lru_cache(maxsize=512)
def _rung_order_from_brochure_text(
    catalog_key_value: str, make: str, model: str, year: int
) -> tuple[tuple[str, ...], str]:
    """Re-derive (order, order_basis JSON) straight from the brochure text file.

    The ordering evidence lives in the document, not in the overlay: the overlay
    is only a cache of it. This re-reads the document so a rebuilt overlay that
    lost the fields (see :func:`backfill_overlay_rung_order`) silently regains
    them instead of silently falling back to the hand-typed rank table.

    Returns ``((), "{}")`` when the document yields no order. JSON is returned
    rather than a dict so the result stays hashable/immutable under the cache.
    """
    path = _brochure_text_file(catalog_key_value)
    if not path.is_file():
        return (), "{}"
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return (), "{}"
    if not isinstance(data, dict):
        return (), "{}"
    from backend.enrichment.brochure_trim_candidates import extract_trim_walk

    try:
        extract = extract_trim_walk(data, make=make, model=model, year=year)
    except Exception:  # a malformed brochure text file must not break a VDP
        logger.exception("rung order re-derivation failed for %s", catalog_key_value)
        return (), "{}"
    if not extract.order_basis:
        return (), "{}"
    return tuple(extract.trims_available), json.dumps(extract.order_basis)


def attach_rung_order(overlay: dict[str, Any]) -> dict[str, Any]:
    """Fill ``rung_order``/``order_basis``/``rung_edges`` from the cited document.

    A no-op when the overlay already carries them, or when its source is not one
    that records ordering evidence.
    """
    if not isinstance(overlay, dict):
        return overlay
    if str(overlay.get("source") or "").strip().lower() not in ORDER_BEARING_OVERLAY_SOURCES:
        return overlay
    if isinstance(overlay.get("order_basis"), dict) and overlay.get("order_basis"):
        return overlay
    ck = str(overlay.get("catalog_key") or "")
    try:
        year = int(overlay.get("year") or 0)
    except (TypeError, ValueError):
        return overlay
    if not ck or not year:
        return overlay
    order, basis_json = _rung_order_from_brochure_text(
        ck, str(overlay.get("make") or ""), str(overlay.get("model") or ""), year
    )
    if not order:
        return overlay
    basis = json.loads(basis_json)
    return {
        **overlay,
        "rung_order": list(order),
        "order_basis": basis,
        "rung_edges": [
            entry["edge"]
            for trim in order
            for entry in [basis.get(trim) or {}]
            if isinstance(entry.get("edge"), dict)
            and str(entry["edge"].get("trim") or "") == trim
        ],
    }


def backfill_overlay_rung_order(*, dry_run: bool = False) -> dict[str, int]:
    """Write the document-derived rung order into every overlay that can hold one.

    Data build step, run the same way ``build_trim_spec_sheets.py`` is: the
    render path does not depend on it (``attach_rung_order`` re-derives on read
    when the fields are missing), it just saves that work per process.

    Bullets, provenance and every other field are left exactly as they were.
    """
    counts = {"scanned": 0, "written": 0, "no_order_in_document": 0, "wrong_source": 0}
    if not TRIM_ADDS_BY_YEAR_DIR.is_dir():
        return counts
    for path in sorted(TRIM_ADDS_BY_YEAR_DIR.glob("*.json")):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(data, dict):
            continue
        counts["scanned"] += 1
        if str(data.get("source") or "").strip().lower() not in ORDER_BEARING_OVERLAY_SOURCES:
            counts["wrong_source"] += 1
            continue
        stripped = {k: v for k, v in data.items() if k not in ("rung_order", "order_basis", "rung_edges")}
        filled = attach_rung_order(stripped)
        if not filled.get("order_basis"):
            counts["no_order_in_document"] += 1
            continue
        if not dry_run:
            path.write_text(
                json.dumps(filled, indent=2, ensure_ascii=False), encoding="utf-8"
            )
        counts["written"] += 1
    return counts


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
    return attach_rung_order(data)


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
            if trim_adds_provenance_required():
                # Enrichment fills empty rungs from a NEIGHBOUR YEAR's overlay or
                # from a hard-coded table, so the bullets it adds are not the ones
                # this overlay cites — they would enter uncited. Skipped while the
                # gate is on.
                #
                # ``for_year=y`` is the car's year, not ``candidate``. When this
                # loop had to walk out to a neighbouring model year, the file it
                # found is still useful for its trim LIST but is not evidence
                # about this car, so every bullet in it drops here.
                return apply_overlay_provenance_gate(data, for_year=y)
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
        # Never drop the staging copy until the document is in the archive: the
        # derived text is not a substitute for the PDF, and losing the PDF is
        # what made this corpus unre-parseable.
        archived = archive_source_pdf(path, ymm=result.ymm)
        if not archived:
            logger.error("keeping %s: could not archive it first", path)
            result.warnings.append("delete_skipped_not_archived")
            return result
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
