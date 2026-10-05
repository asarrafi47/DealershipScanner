"""
Download one resolved document and run the extraction lane over it.

Moved verbatim out of ``backend/scripts/fetch_oem_brochures.py`` (audit
datascripts.md F7); that script is still the CLI and re-exports these names.
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend.enrichment.brochure_acquisition.corpus import (
    _append_fetch_log,
    full_document_text,
    page_texts,
)
from backend.enrichment.brochure_acquisition.sources import (
    ArchiveResolver,
)
from backend.enrichment.brochure_extract import (
    extract_brochure_text_pdf,
    persist_brochure_text,
)
from backend.enrichment.brochure_sources import (
    TIER_ARCHIVE,
    ContentIndex,
    PacedFetcher,
    RobotsPolicy,
    assess_archive_document,
    assess_nameplate_dominance,
    assess_text_quality,
    brochure_pdf_filename,
    is_allowed_source_url,
    looks_like_pdf,
    quarantine_archive_pdf,
    source_tier_for_url,
    verify_document_identity,
)
from backend.enrichment.dictionary_paths import (
    BROCHURES_DIR,
)


def oem_pdf_for_catalog_key(catalog_key_str: str, index: ContentIndex) -> Path | None:
    """
    A stored OEM-tier PDF for this vehicle, if we hold one.

    This is what makes the OEM-vs-archive comparison possible during an ordinary
    fetch rather than only in a dedicated pass. Documents whose tier was never
    recorded (everything fetched before 2026-08-01) count as OEM here **only**
    because every one of them came from :data:`OFFICIAL_HOSTS` -- the archive
    tier did not exist when they were fetched, so there is nothing else they
    could be. That reasoning is stated rather than assumed, and it stops being
    true the moment an untiered archive document could exist, which is why
    :meth:`ContentIndex.register` records the tier on every new document.
    """
    for doc in index.by_sha.values():
        if doc.tier == TIER_ARCHIVE:
            continue
        if catalog_key_str in doc.catalog_keys and Path(doc.path).is_file():
            return Path(doc.path)
    return None


def download_and_extract(
    resolved: dict[str, Any],
    fetcher: PacedFetcher,
    robots: RobotsPolicy,
    index: ContentIndex,
    *,
    archive: ArchiveResolver | None = None,
    apply_quarantine: bool = True,
) -> dict[str, Any]:
    """
    Download one resolved document and run the existing extraction lane.

    Content-hash dedupe happens here, in two places:

    1. Before the request, if this exact URL is already in the index (resume:
       a repeat run costs no bandwidth).
    2. After the response, on the sha256 of the bytes. A hash already stored is
       not written again; the existing file is used and the outcome is reported
       as ``duplicate_content``.

    An archive-tier document takes the archive's own fetcher (12s pacing) and,
    once downloaded, must clear :func:`assess_archive_document` before its text
    is extracted. A failure there stores the PDF and then moves it to
    :data:`ARCHIVE_QUARANTINE_DIR` with the reason -- the document is kept, its
    text is not derived, and the corpus never sees it.
    """
    year, make, model = resolved["year"], resolved["make"], resolved["model"]
    doc_url = resolved["doc_url"]
    tier = resolved.get("tier") or source_tier_for_url(doc_url, make)
    out: dict[str, Any] = dict(resolved)
    out["tier"] = tier
    ck = resolved["primary_catalog_key"].replace("__", "|")

    if tier == TIER_ARCHIVE:
        if archive is None:
            out["status"] = "rejected_archive_not_enabled"
            out["detail"] = "archive-tier URL resolved without --allow-archive"
            return out
        fetcher, robots = archive.fetcher, archive.robots
    out["user_agent"] = fetcher.user_agent

    if not is_allowed_source_url(doc_url, make, allow_archive=archive is not None):
        out["status"] = "rejected_non_official_host"
        out["detail"] = f"{doc_url} is on no tier we admit for {make}"
        return out
    if not robots.allows(doc_url):
        out["status"] = "robots_disallowed"
        out["detail"] = f"{doc_url} disallowed by robots.txt"
        return out

    known = index.seen_url(doc_url)
    if known is not None and Path(known.path).is_file():
        out["status"] = "already_downloaded"
        out["sha256"] = known.sha256
        out["pdf_path"] = known.path
        out["detail"] = f"URL already fetched -> {Path(known.path).name}"
        return out

    filename = brochure_pdf_filename(year, make, model)
    if filename is None:
        out["status"] = "unmappable_filename"
        out["detail"] = "no filename round-trips to the expected catalog_key"
        return out

    try:
        status, payload, content_type = fetcher.get_bytes(doc_url)
    except Exception as exc:  # noqa: BLE001
        out["status"] = "download_failed"
        out["detail"] = f"{type(exc).__name__}: {str(exc)[:160]}"
        return out

    if status != 200:
        out["status"] = "download_failed"
        out["detail"] = f"HTTP {status}"
        return out
    if not looks_like_pdf(payload):
        # An HTML error/consent page served with a 200 is the usual cause.
        out["status"] = "not_a_pdf"
        out["detail"] = f"content-type={content_type} first-bytes={payload[:16]!r}"
        return out

    # ------------------------------------------------------------------
    # Archive tier: authenticity BEFORE anything is derived from the bytes.
    # ------------------------------------------------------------------
    authenticity: dict[str, Any] = {}
    archive_failure = ""
    if tier == TIER_ARCHIVE:
        record = assess_archive_document(
            payload,
            oem_pdf_path=oem_pdf_for_catalog_key(ck, index),
            catalog_key_str=ck,
            archive_url=doc_url,
        )
        authenticity = record.to_json()
        out["authenticity"] = authenticity
        if not record.ok:
            archive_failure = record.reason

    fetched_at = datetime.now(timezone.utc).isoformat()
    doc, is_new = index.register(
        payload,
        preferred_path=BROCHURES_DIR / filename,
        source_url=doc_url,
        catalog_key_str=ck,
        fetched_at=fetched_at,
        tier=tier,
        authenticity=authenticity,
    )
    pdf_path = Path(doc.path)

    if archive_failure and is_new:
        # Stored first, then moved: the document is evidence about this source
        # and is never deleted. Its text is not extracted, so nothing from it can
        # reach a car page.
        #
        # `and is_new` is not belt-and-braces. Without it, an archive URL that
        # returns bytes we already hold would move the EXISTING file -- which may
        # be the OEM copy, since register() dedupes across tiers -- out of the
        # corpus on the strength of a check about a different source. A repeat
        # hash is reported as a duplicate below and nothing is moved.
        destination = quarantine_archive_pdf(
            pdf_path, archive_failure, applied=apply_quarantine
        )
        doc.path = str(destination)
        index.save()
        out.update(
            status="archive_quarantined",
            sha256=doc.sha256,
            pdf_path=str(destination),
            bytes=len(payload),
            fetched_at=fetched_at,
            detail=archive_failure,
        )
        _append_fetch_log(out)
        return out

    if not is_new:
        # Same bytes as a document we already hold, reached by a different URL
        # or a different spelling of the model. Do not store or count it twice.
        index.save()
        out.update(
            status="duplicate_content",
            sha256=doc.sha256,
            pdf_path=str(pdf_path),
            bytes=len(payload),
            detail=(
                f"sha256 {doc.sha256[:12]} already stored as {pdf_path.name} "
                f"(keys: {', '.join(sorted(set(doc.catalog_keys)))})"
            ),
        )
        return out

    def reject_text(detail: str, page_count: int | None = None) -> dict[str, Any]:
        """
        The single exit for "PDF kept, derived text refused".

        Four call sites reach it -- unparseable filename, an extraction that
        produced no pages, a document that will not name its own model, and a
        document that fails the decode/length gate -- and they all used to be
        four copies of the same block. They are one block now because the
        archive tier had to change what happens here, and changing it in three
        of four places would have been a fix that was not a fix.

        For an OEM document the behaviour is exactly what it was: the PDF stays
        in the corpus directory, the text is not persisted. For an ARCHIVE
        document the PDF is moved to ARCHIVE_QUARANTINE_DIR with the reason.

        Why the difference is not arbitrary: for an OEM document a failed
        identity check means "we mis-picked a link on a manufacturer's site",
        and the file itself is still a manufacturer document worth keeping where
        the re-ingestion lane can see it. For an archive document, "this file
        does not print the name of the vehicle it is filed under" is the
        strongest tamper-or-misfiling signal this tier can produce, and it is the
        one authenticity test with real teeth. It is the failure that
        assess_archive_document deliberately does NOT issue on its own -- see its
        docstring for why the OEM-vs-archive comparison cannot.
        """
        final_path = pdf_path
        status = "stored_text_rejected"
        if tier == TIER_ARCHIVE:
            final_path = quarantine_archive_pdf(
                pdf_path, detail, applied=apply_quarantine
            )
            doc.path = str(final_path)
            status = "archive_quarantined"
        index.save()
        out.update(
            status=status,
            sha256=doc.sha256,
            pdf_path=str(final_path),
            bytes=len(payload),
            fetched_at=fetched_at,
            detail=detail,
        )
        if page_count is not None:
            out["page_count"] = page_count
        _append_fetch_log(out)
        return out

    result = extract_brochure_text_pdf(pdf_path)
    if result is None:
        return reject_text("filename not parseable by brochure_extract")
    if result.warnings and not result.pages:
        return reject_text(";".join(result.warnings)[:200])

    # Gate BEFORE persisting text, over the WHOLE captured document rather than
    # the trim-hint pages: capture is lossless now, so "did this PDF decode" is
    # a document-level question. The PDF is kept either way -- deleting acquired
    # source PDFs after a lossy capture is the mistake that cost this project
    # 1,368 brochures, and the content index means a re-run never re-downloads
    # it. What a failed gate withholds is the derived text, not the document.
    # (An archive document is moved aside rather than left in the corpus
    # directory; see reject_text. It is moved, never deleted.)
    document_text = full_document_text(result)

    # The document must say which vehicle it is about. This is the only check
    # that survives an OEM serving documents behind an opaque id, it is what
    # makes the page-scoped fallback in resolve_source safe, and it is the
    # load-bearing authenticity test for the archive tier.
    identified, why_not = verify_document_identity(
        document_text,
        year,
        make,
        model,
        require_year=resolved.get("page_scoped", False),
    )
    if not identified:
        return reject_text(why_not, result.page_count)

    # Naming the vehicle is not being about it. A brochure for a derived
    # nameplate (the M3 book for the 3 Series) names the base model in its body
    # copy and passes the check above; this one asks which nameplate presides
    # over the document's pages. Page texts, not the flattened string: the
    # signal is a running header, which is per page by construction.
    subject = assess_nameplate_dominance(page_texts(result), make, model)
    if not subject.ok:
        return reject_text(subject.reason, result.page_count)

    verdict = assess_text_quality(document_text)
    if not verdict.ok:
        return reject_text(verdict.reason, result.page_count)

    json_path = persist_brochure_text(result)
    index.save()

    out.update(
        status="fetched",
        pdf_path=str(pdf_path),
        json_path=str(json_path),
        bytes=len(payload),
        sha256=doc.sha256,
        page_count=result.page_count,
        pages_saved=len(result.pages),
        trim_hint_pages=result.trim_hint_pages,
        text_chars=len(document_text),
        warnings=result.warnings,
        fetched_at=fetched_at,
    )
    _append_fetch_log(out)
    return out
