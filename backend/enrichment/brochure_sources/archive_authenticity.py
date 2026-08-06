"""Archive-tier authenticity: compare an archive PDF against the OEM original."""
from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from backend.enrichment.dictionary_paths import (
    BROCHURES_DIR,
)

logger = logging.getLogger(__name__)

from .http import (
    looks_like_pdf,
)
from .storage import (
    sha256_bytes,
    sha256_file,
)
from .tiers import (
    TIER_ARCHIVE,
)

# --------------------------------------------------------------------------
# Archive-tier authenticity
# --------------------------------------------------------------------------
#
# An OEM document is its own provenance: it came from the manufacturer's host,
# over a connection we made, honouring rules that host published. An archive
# document has none of that. What follows is what can actually be established
# about a third party's copy, and -- as importantly -- what cannot.

#: Where an archive PDF that fails :func:`assess_archive_document` is moved.
#:
#: A **sibling** of the brochures directory, not a subdirectory, so every
#: ``BROCHURES_DIR.glob("*.pdf")`` in the codebase (including
#: :meth:`ContentIndex.scan_directory` and the re-ingestion lane) stops seeing
#: it. Moved, never deleted: deleting acquired documents is what made this
#: corpus unrecoverable once already, and a quarantine rule that turns out to be
#: wrong has to be undoable by moving the files back.
ARCHIVE_QUARANTINE_DIR = BROCHURES_DIR.parent / "brochures_archive_quarantine"

#: Where a comparison copy is written by the OEM-vs-archive measurement.
#:
#: Separate from :data:`BROCHURES_DIR` on purpose. These documents are fetched
#: to be *compared*, for vehicles we already hold an OEM original for. They are
#: evidence about the archive, not corpus documents, and nothing extracts text
#: from this directory into ``derived/brochure_text``.
ARCHIVE_COMPARISON_DIR = BROCHURES_DIR.parent / "brochures_archive_comparison"


def read_pdf_metadata(payload: bytes) -> dict[str, object]:
    """
    The PDF's own document-information dictionary, plus what it says structurally.

    Returns ``{producer, creator, title, author, subject, creation_date,
    mod_date, page_count, encrypted, error}``. Every field is reported as found,
    including empty; nothing is normalised away, because the point is to build
    an evidence base about what archive copies look like.

    **This is recorded, not enforced.** Measured 2026-08-01 over the 135 PDFs
    this project fetched from manufacturer hosts, ``/Producer`` takes 25 distinct
    values -- 29 "Microsoft(R) Word for Microsoft 365", 25 "Adobe PDF Library
    17.0", 12 "Microsoft(R) Excel(R) for Microsoft 365", 10 "Creo Normalizer
    JTP", 3 "Microsoft: Print To PDF", 2 "Skia/PDF m138" (a Chrome print), and 7
    with no Producer at all. There is no toolchain signature that distinguishes
    a genuine OEM brochure from anything else, so hard-failing on an unfamiliar
    Producer would reject real documents and admit tampered ones. What the field
    is good for is spotting a *pattern* across many archive documents, which
    needs the evidence base first.
    """
    out: dict[str, object] = {
        "producer": "",
        "creator": "",
        "title": "",
        "author": "",
        "subject": "",
        "creation_date": "",
        "mod_date": "",
        "page_count": 0,
        "encrypted": False,
        "error": "",
    }
    try:
        import io

        from pypdf import PdfReader

        reader = PdfReader(io.BytesIO(payload))
        out["encrypted"] = bool(reader.is_encrypted)
        try:
            out["page_count"] = len(reader.pages)
        except Exception as exc:  # noqa: BLE001 - a broken page tree is a finding
            out["error"] = f"page tree unreadable: {type(exc).__name__}"
        meta = reader.metadata or {}
        for key, field_name in (
            ("/Producer", "producer"),
            ("/Creator", "creator"),
            ("/Title", "title"),
            ("/Author", "author"),
            ("/Subject", "subject"),
            ("/CreationDate", "creation_date"),
            ("/ModDate", "mod_date"),
        ):
            value = meta.get(key)
            out[field_name] = "" if value is None else str(value)[:300]
    except Exception as exc:  # noqa: BLE001 - an unparseable PDF is a finding
        out["error"] = f"{type(exc).__name__}: {str(exc)[:160]}"
    return out


def pdf_text_layer(payload: bytes) -> tuple[str, int, int]:
    """
    ``(text, pages_with_text, page_count)`` for a PDF held in memory.

    Used for the no-text-layer count and for the similarity comparison, both of
    which need a cheap whole-document read. The corpus extraction lane
    (``brochure_extract``) still does the real capture with pdfplumber; this is
    not a second extractor and its output never reaches ``derived/``.
    """
    try:
        import io

        from pypdf import PdfReader

        reader = PdfReader(io.BytesIO(payload))
        parts: list[str] = []
        with_text = 0
        for page in reader.pages:
            try:
                text = page.extract_text() or ""
            except Exception:  # noqa: BLE001
                text = ""
            if text.strip():
                with_text += 1
            parts.append(text)
        return "\n".join(parts), with_text, len(parts)
    except Exception:  # noqa: BLE001
        return "", 0, 0


def _similarity_tokens(text: str) -> list[str]:
    return re.sub(r"[^a-z0-9]+", " ", (text or "").lower()).split()


#: Shingle length for :func:`text_similarity`. Five words is long enough that
#: two unrelated brochures for the same make share almost no shingles, and short
#: enough to survive a re-save that reflows a line.
SIMILARITY_SHINGLE = 5


def text_similarity(left: str, right: str) -> float:
    """
    Jaccard similarity of the two texts' 5-word shingles, in ``[0, 1]``.

    Word shingles rather than a character diff because a PDF re-save changes
    whitespace and line breaks everywhere while leaving the words alone; and
    Jaccard rather than a ratio because it is symmetric and does not reward one
    document simply for being longer.

    ``0.0`` when either side has no text -- a scanned copy with no text layer is
    not "different from" the OEM original, it is *unmeasurable*, and
    :func:`compare_archive_to_oem` reports that as its own outcome rather than
    as a mismatch.
    """
    left_tokens, right_tokens = _similarity_tokens(left), _similarity_tokens(right)
    if len(left_tokens) < SIMILARITY_SHINGLE or len(right_tokens) < SIMILARITY_SHINGLE:
        return 0.0
    def shingles(tokens: list[str]) -> set[tuple[str, ...]]:
        return {
            tuple(tokens[i : i + SIMILARITY_SHINGLE])
            for i in range(len(tokens) - SIMILARITY_SHINGLE + 1)
        }
    a, b = shingles(left_tokens), shingles(right_tokens)
    union = a | b
    return len(a & b) / len(union) if union else 0.0


#: Shingle overlap at or above which an archive copy is labelled the same
#: document as the OEM original we hold.
#:
#: Calibrated on the one same-publication pair the 2026-08-01 comparison run
#: produced: ``2018|nissan|roguesport``, where our OEM copy and the archive's are
#: both the 8-page consumer brochure with the same opening paragraph, and scored
#: **0.798**. Everything else scored 0.005-0.015 -- see
#: :data:`DIFFERENT_DOCUMENT_SIMILARITY` for why. One calibration point is one
#: calibration point, which is exactly why nothing is admitted or rejected on
#: this number; it labels a measurement.
SAME_DOCUMENT_SIMILARITY = 0.80

#: Below this the two documents share almost no text.
#:
#: IMPORTANT, and the opposite of what the first version of this code assumed:
#: this is **not** evidence of tampering, and ``different_document`` is **not** a
#: failure. Measured 2026-08-01 over 10 vehicles where we hold an OEM original
#: and the archive holds a copy, 9 landed here with similarities of 0.005-0.015,
#: and inspecting them showed why: for ``2014|mazda|mazda3`` our "OEM original"
#: is an 11-page Mazda press release titled "PRESS RELEASE", while the archive's
#: is the 23-page consumer sales brochure. Both are genuine Mazda documents about
#: the same vehicle; they are different publications. Failing the archive copy on
#: that would quarantine real brochures for the crime of not being the press
#: release we happen to hold.
DIFFERENT_DOCUMENT_SIMILARITY = 0.20


@dataclass
class OemArchiveComparison:
    """One archive copy measured against the OEM original we already hold."""

    catalog_key: str
    oem_path: str
    oem_sha256: str
    archive_url: str
    archive_sha256: str
    #: ``"identical_bytes"`` -- same sha256: the archive is serving the OEM file.
    #:     The only outcome that is *positive evidence*.
    #: ``"same_document"``   -- different bytes, >= SAME_DOCUMENT_SIMILARITY.
    #: ``"partial_overlap"`` -- in between.
    #: ``"different_document"`` -- < DIFFERENT_DOCUMENT_SIMILARITY. Means "the
    #:     OEM file we hold is a different publication", not "this is fake" --
    #:     see :data:`DIFFERENT_DOCUMENT_SIMILARITY`. Carries no verdict.
    #: ``"unmeasurable"``    -- one side has no text layer, so nothing was compared.
    outcome: str = "unmeasurable"
    similarity: float = 0.0
    detail: str = ""

    def to_json(self) -> dict:
        return {
            "catalog_key": self.catalog_key,
            "oem_path": self.oem_path,
            "oem_sha256": self.oem_sha256,
            "archive_url": self.archive_url,
            "archive_sha256": self.archive_sha256,
            "outcome": self.outcome,
            "similarity": round(self.similarity, 4),
            "detail": self.detail,
        }


def compare_archive_to_oem(
    archive_payload: bytes,
    oem_pdf_path: Path,
    *,
    catalog_key_str: str = "",
    archive_url: str = "",
) -> OemArchiveComparison:
    """
    Compare an archive copy against an OEM original on disk: sha256, then text.

    sha256 first because it is the only test that settles the question. Equal
    hashes mean the archive is serving the manufacturer's own file byte for
    byte, which is the strongest evidence this source can produce and needs no
    threshold to interpret.

    Where the hashes differ this is a **measurement and not a verdict**, and the
    2026-08-01 run is why that wording is emphatic. The first version of this
    code failed an archive copy whose text overlap with "the OEM original" was
    low. Run over 10 real pairs, 9 came back at 0.005-0.015 -- because the
    document we hold from the OEM is frequently a different publication about the
    same car (an 11-page press release, where the archive has the 23-page
    consumer brochure). Nothing here is a failure; the label and the number go on
    the record and :func:`assess_archive_document` reports them.
    """
    result = OemArchiveComparison(
        catalog_key=catalog_key_str,
        oem_path=str(oem_pdf_path),
        oem_sha256="",
        archive_url=archive_url,
        archive_sha256=sha256_bytes(archive_payload),
    )
    if not oem_pdf_path.is_file():
        result.detail = "no OEM original on disk to compare against"
        return result
    result.oem_sha256 = sha256_file(oem_pdf_path)

    if result.oem_sha256 == result.archive_sha256:
        result.outcome = "identical_bytes"
        result.similarity = 1.0
        result.detail = "archive serves the OEM file byte for byte"
        return result

    archive_text, archive_pages_with_text, _ = pdf_text_layer(archive_payload)
    oem_text, oem_pages_with_text, _ = pdf_text_layer(oem_pdf_path.read_bytes())
    if not archive_pages_with_text or not oem_pages_with_text:
        result.outcome = "unmeasurable"
        result.detail = (
            f"no text layer to compare (archive pages with text="
            f"{archive_pages_with_text}, OEM={oem_pages_with_text})"
        )
        return result

    result.similarity = text_similarity(archive_text, oem_text)
    if result.similarity >= SAME_DOCUMENT_SIMILARITY:
        result.outcome = "same_document"
    elif result.similarity < DIFFERENT_DOCUMENT_SIMILARITY:
        result.outcome = "different_document"
    else:
        result.outcome = "partial_overlap"
    result.detail = f"{result.similarity:.3f} 5-word-shingle overlap with the OEM copy"
    return result


@dataclass
class ArchiveAuthenticity:
    """The full authenticity record for one archive document."""

    ok: bool
    reason: str = ""
    metadata: dict[str, object] = field(default_factory=dict)
    pages_with_text: int = 0
    page_count: int = 0
    comparison: OemArchiveComparison | None = None

    @property
    def has_text_layer(self) -> bool:
        return self.pages_with_text > 0

    def to_json(self) -> dict:
        payload: dict[str, object] = {
            "ok": self.ok,
            "reason": self.reason,
            "tier": TIER_ARCHIVE,
            "pdf_metadata": self.metadata,
            "pages_with_text": self.pages_with_text,
            "page_count": self.page_count,
            "has_text_layer": self.has_text_layer,
        }
        if self.comparison is not None:
            payload["oem_comparison"] = self.comparison.to_json()
        return payload


def assess_archive_document(
    payload: bytes,
    *,
    oem_pdf_path: Path | None = None,
    catalog_key_str: str = "",
    archive_url: str = "",
) -> ArchiveAuthenticity:
    """
    Every authenticity check an archive document gets, in one record.

    Three things happen, and only one of them can fail the document:

    1. **Metadata is read and recorded** -- always, for every document, never as
       a pass/fail. See :func:`read_pdf_metadata` for the measurement that says
       why a Producer allowlist would be worse than useless.
    2. **Structural sanity fails closed.** A payload that is not a PDF, that is
       encrypted, or whose page tree will not open is refused: it is not a
       document we can verify a quotation against, so it cannot back a bullet.
       This is the only failure this function issues.
    3. **Comparison against the OEM original, where we hold one** -- recorded,
       never decisive. This was written the other way first, failing a copy whose
       text overlap with the OEM file was low, and the 2026-08-01 measurement
       showed that rule would quarantine genuine brochures: 9 of 10 pairs scored
       0.005-0.015 because the OEM file we hold is a press release and the
       archive's is the sales brochure. ``identical_bytes`` is real positive
       evidence; the rest is context for a human.

    What this does NOT establish, stated so nobody reads more into the record
    than is in it: for the vehicles where we hold no OEM original -- which is
    every vehicle the archive tier is actually *for* -- there is nothing to
    compare against, and a passing record means only "this is a structurally
    sound PDF whose metadata we wrote down". The load-bearing gates for an
    archive document are the ones that come after it, on the extracted text:
    :func:`verify_document_identity` (the document must print this model's name
    as a declaration) and :func:`assess_text_quality`. Those are what the fetch
    lane quarantines an archive document on. And the tier is recorded on the
    document precisely so this weaker provenance is never mistaken for an OEM
    one.
    """
    record = ArchiveAuthenticity(ok=True)
    if not looks_like_pdf(payload):
        record.ok = False
        record.reason = f"not a PDF: first bytes {payload[:16]!r}"
        return record

    record.metadata = read_pdf_metadata(payload)
    if record.metadata.get("error"):
        record.ok = False
        record.reason = f"unreadable PDF: {record.metadata['error']}"
        return record
    if record.metadata.get("encrypted"):
        record.ok = False
        record.reason = "PDF is encrypted; its text cannot be verified against a quotation"
        return record

    _, record.pages_with_text, record.page_count = pdf_text_layer(payload)

    if oem_pdf_path is not None:
        # Recorded, not decisive. See the docstring and
        # DIFFERENT_DOCUMENT_SIMILARITY: a low score here means the OEM file we
        # hold is a different publication, which is not a finding about the
        # archive's copy.
        record.comparison = compare_archive_to_oem(
            payload,
            oem_pdf_path,
            catalog_key_str=catalog_key_str,
            archive_url=archive_url,
        )

    # A scanned document with no text layer is NOT a failure. It is a document
    # we hold and cannot quote from -- silence, which is always acceptable. The
    # existing quality gate refuses its (empty) text further down the lane; the
    # count is reported by --archive-audit so the cost of this tier is visible.
    return record


def quarantine_archive_pdf(pdf_path: Path, reason: str, *, applied: bool = True) -> Path:
    """
    Move a failed archive PDF into :data:`ARCHIVE_QUARANTINE_DIR` with a reason.

    Moved, never deleted, and the reason is appended to a cumulative record
    beside the files -- cumulative because this runs again on the next batch, and
    overwriting would leave the earlier batch quarantined with no recorded
    reason.
    """
    destination = ARCHIVE_QUARANTINE_DIR / pdf_path.name
    if not applied:
        return destination
    ARCHIVE_QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
    if pdf_path.is_file():
        pdf_path.replace(destination)

    record_path = ARCHIVE_QUARANTINE_DIR / "_why_these_are_here.json"
    rows: dict[str, dict[str, object]] = {}
    if record_path.is_file():
        try:
            previous = json.loads(record_path.read_text(encoding="utf-8"))
            for row in previous.get("files") or []:
                rows[str(row.get("file"))] = row
        except (OSError, json.JSONDecodeError):
            logger.warning("could not read %s; starting a fresh record", record_path)
    rows[destination.name] = {
        "file": destination.name,
        "tier": TIER_ARCHIVE,
        "reason": reason,
        "quarantined_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
    }
    record_path.write_text(
        json.dumps(
            {
                "check": "backend.enrichment.brochure_sources.assess_archive_document",
                "count": len(rows),
                "files": [rows[k] for k in sorted(rows)],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return destination
