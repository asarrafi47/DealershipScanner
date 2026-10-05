"""
Corpus text helpers, the fetch ledger, the content index and the corpus text audit.

Moved verbatim out of ``backend/scripts/fetch_oem_brochures.py`` (audit
datascripts.md F7); that script is still the CLI and re-exports these names.
"""
from __future__ import annotations

import json
from typing import Any

from backend.enrichment.brochure_acquisition.paths import (
    CONTENT_INDEX_PATH,
    FETCH_LOG_PATH,
)
from backend.enrichment.brochure_sources import (
    ContentIndex,
    assess_nameplate_dominance,
    assess_text_quality,
    verify_document_identity,
)
from backend.enrichment.dictionary_paths import (
    BROCHURE_TEXT_DIR,
    BROCHURES_DIR,
)


def full_document_text(result: Any) -> str:
    """
    Every captured page's text and table lines, in page order.

    ``combined_trim_pages_text`` is only the trim-hint pages. Since capture went
    lossless that is a subset, so gating on it would judge a document by the
    pages one regex happened to flag.
    """
    return "\n".join(page_texts(result))


def page_texts(result: Any) -> list[str]:
    """
    One string per captured page: its text plus its table lines.

    Kept separate from :func:`full_document_text` because a check that asks
    "which vehicle presides over this document" has to see the page boundaries.
    Flattening first turns nine identical running headers into nine occurrences
    in one blob, which is exactly the count that cannot tell a combined A4/S4
    brochure apart from an M3 brochure filed as a 3 Series.
    """
    out: list[str] = []
    for page in result.pages:
        parts = [page.text or ""]
        if page.table_lines:
            parts.append("\n".join(page.table_lines))
        out.append("\n".join(parts))
    return out


def _append_fetch_log(record: dict[str, Any]) -> None:
    """
    One line per stored document. ``text_status`` says whether the derived text
    was persisted, so a rejected extraction is visible rather than looking like
    a document we never fetched.

    ``tier`` and, for the archive tier, ``authenticity`` are written here as well
    as into the content index. The ledger is append-only and the index is
    rewritten, so the ledger is the record that survives an index rebuild.
    """
    FETCH_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    entry = {
        "catalog_key": record["primary_catalog_key"].replace("__", "|"),
        "alias_catalog_keys": record.get("alias_catalog_keys", []),
        "year": record["year"],
        "make": record["make"],
        "model": record["model"],
        "tier": record.get("tier") or "",
        "authenticity": record.get("authenticity") or None,
        "source_url": record["doc_url"],
        "discovery_url": record.get("discovery_url"),
        "fetched_at": record["fetched_at"],
        "sha256": record["sha256"],
        "bytes": record["bytes"],
        "pdf_path": record["pdf_path"],
        "text_status": "persisted" if record.get("json_path") else "rejected",
        "text_rejected_reason": None if record.get("json_path") else record.get("detail"),
        "brochure_text_path": record.get("json_path"),
        "page_count": record.get("page_count"),
        "pages_saved": record.get("pages_saved"),
        "user_agent": record.get("user_agent"),
        "fetcher": "backend.scripts.fetch_oem_brochures",
    }
    with FETCH_LOG_PATH.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry) + "\n")


def open_content_index() -> tuple[ContentIndex, dict[str, list[str]]]:
    """Index seeded from the ledger and from whatever is already on disk."""
    index = ContentIndex(CONTENT_INDEX_PATH).load()
    index.absorb_ledger(FETCH_LOG_PATH)
    duplicates = index.scan_directory(BROCHURES_DIR)
    return index, duplicates


def audit_corpus_text() -> dict[str, Any]:
    """
    How much of ``derived/brochure_text`` is a usable document.

    A file existing is what the gap list counts as coverage, but a file whose
    pages hold only a disclosures page is not a brochure. Reports, per corpus
    file: no text at all / does not name its own model / fails the decode and
    length gate. Read-only.
    """
    summary: dict[str, Any] = {
        "files": 0,
        "usable": 0,
        "no_text": 0,
        "model_not_named": 0,
        "wrong_vehicle": 0,
        "failed_quality": 0,
        "failed_digit_density": 0,
        "worst_examples": [],
    }
    for path in sorted(BROCHURE_TEXT_DIR.glob("*.json")):
        if path.name.startswith("_"):
            continue
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        summary["files"] += 1
        pages = corpus_page_texts(data)
        text = "\n".join(pages)
        if not text.strip():
            summary["no_text"] += 1
            continue
        named, why = verify_document_identity(
            text, data.get("year"), data.get("make") or "", data.get("model") or ""
        )
        subject = assess_nameplate_dominance(
            pages, data.get("make") or "", data.get("model") or ""
        )
        quality = assess_text_quality(text)
        if not named:
            summary["model_not_named"] += 1
        if not subject.ok:
            summary["wrong_vehicle"] += 1
        if not quality.ok:
            summary["failed_quality"] += 1
            if "digit" in quality.reason:
                summary["failed_digit_density"] += 1
        if named and subject.ok and quality.ok:
            summary["usable"] += 1
        elif len(summary["worst_examples"]) < 10:
            summary["worst_examples"].append(
                {
                    "file": path.stem,
                    "chars": len(text),
                    "reason": why or subject.reason or quality.reason,
                }
            )
    return summary


def corpus_page_texts(data: dict[str, Any]) -> list[str]:
    """Per-page text of a stored ``derived/brochure_text`` document."""
    return [(page.get("text") or "") for page in data.get("pages") or []]
