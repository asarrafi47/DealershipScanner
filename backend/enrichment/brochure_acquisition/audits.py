"""
Read-mostly reports: --archive-verify-overlap, --archive-audit, --audit-corpus.

Moved verbatim out of ``backend/scripts/fetch_oem_brochures.py`` (audit
datascripts.md F7); that script is still the CLI and re-exports these names.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend.enrichment.brochure_acquisition.corpus import (
    audit_corpus_text,
    open_content_index,
)
from backend.enrichment.brochure_acquisition.paths import (
    ARTIFACT_DIR,
)
from backend.enrichment.brochure_acquisition.sources import (
    ArchiveResolver,
    _resolve_from_archive,
)
from backend.enrichment.brochure_sources import (
    ARCHIVE_COMPARISON_DIR,
    ARCHIVE_DELAY_SECONDS,
    ARCHIVE_QUARANTINE_DIR,
    ARCHIVE_SOURCE_NOTE,
    TIER_ARCHIVE,
    compare_archive_to_oem,
    document_tier_for_citation,
    looks_like_pdf,
    pdf_text_layer,
    read_pdf_metadata,
    sha256_bytes,
)


def archive_verify_overlap(
    limit: int, delay: float, user_agent: str, brands: set[str] | None
) -> int:
    """
    Fetch the archive's copy of documents we ALREADY hold from the OEM, and compare.

    This is the single best evidence available about whether this source can be
    trusted at scale, and it is the reason the mode exists separately from
    acquisition: it deliberately spends requests on vehicles we do not need,
    because those are the only vehicles where the answer is checkable.

    The comparison copies land in :data:`ARCHIVE_COMPARISON_DIR`, NOT in the
    corpus. Nothing extracts text from there, so a run of this cannot put a
    single character on a car page. It is measurement, and it is kept as
    measurement.

    Byte-identical to the OEM original is the strong result. Different bytes are
    expected on their own -- re-saving a PDF changes them -- so the fallback is
    a 5-word-shingle overlap of the extracted text, reported as a number.
    """
    index, _ = open_content_index()
    candidates: list[tuple[str, Path]] = []
    seen_keys: set[str] = set()
    for doc in index.by_sha.values():
        if doc.tier == TIER_ARCHIVE or not Path(doc.path).is_file():
            continue
        for key in sorted(set(doc.catalog_keys)):
            parts = key.split("|")
            if len(parts) != 3 or key in seen_keys:
                continue
            if brands and parts[1].lower() not in brands:
                continue
            seen_keys.add(key)
            candidates.append((key, Path(doc.path)))
    candidates.sort()
    candidates = candidates[:limit]

    print(
        f"OEM-vs-archive comparison over {len(candidates)} vehicle(s) we already hold "
        f"an OEM original for\n(paced {max(delay, ARCHIVE_DELAY_SECONDS):g}s, sequential; "
        f"copies -> {ARCHIVE_COMPARISON_DIR})\n"
    )

    archive = ArchiveResolver(delay=delay, user_agent=user_agent)
    index_pages = archive.make_index_pages()
    print(f"archive make index pages read from its own navigation: {len(index_pages)}\n")

    rows: list[dict[str, Any]] = []
    for key, oem_path in candidates:
        year_str, make, model_token = key.split("|")
        year = int(year_str)
        row: dict[str, Any] = {
            "catalog_key": key,
            "oem_pdf": oem_path.name,
            "outcome": "no_archive_copy",
            "detail": "",
        }
        resolved = {
            "year": year,
            "make": make,
            "model": model_token,
            "status": "seed",
            "detail": "",
        }
        resolved = _resolve_from_archive(resolved, archive)
        if resolved.get("status") != "found":
            row["detail"] = resolved.get("archive_detail", "")
            rows.append(row)
            print(f"  {key:<34} no archive copy  {row['detail'][:70]}")
            continue

        url = resolved["doc_url"]
        try:
            status, payload, _ = archive.fetcher.get_bytes(url)
        except Exception as exc:  # noqa: BLE001
            row.update(outcome="download_failed", detail=f"{type(exc).__name__}: {exc}")
            rows.append(row)
            continue
        if status != 200 or not looks_like_pdf(payload):
            row.update(outcome="download_failed", detail=f"HTTP {status}")
            rows.append(row)
            continue

        comparison = compare_archive_to_oem(
            payload, oem_path, catalog_key_str=key, archive_url=url
        )
        metadata = read_pdf_metadata(payload)
        _, pages_with_text, page_count = pdf_text_layer(payload)
        ARCHIVE_COMPARISON_DIR.mkdir(parents=True, exist_ok=True)
        copy_path = ARCHIVE_COMPARISON_DIR / f"{key.replace('|', '__')}__archive.pdf"
        copy_path.write_bytes(payload)

        row.update(
            outcome=comparison.outcome,
            similarity=round(comparison.similarity, 4),
            archive_url=url,
            archive_sha256=comparison.archive_sha256,
            oem_sha256=comparison.oem_sha256,
            bytes=len(payload),
            pages_with_text=pages_with_text,
            page_count=page_count,
            producer=metadata.get("producer"),
            creator=metadata.get("creator"),
            title=metadata.get("title"),
            creation_date=metadata.get("creation_date"),
            comparison_copy=str(copy_path),
            detail=comparison.detail,
        )
        rows.append(row)
        print(
            f"  {key:<34} {comparison.outcome:<20} sim={comparison.similarity:.3f}  "
            f"text {pages_with_text}/{page_count}p  producer={str(metadata.get('producer'))[:34]!r}"
        )

    by_outcome: dict[str, int] = {}
    for row in rows:
        by_outcome[str(row["outcome"])] = by_outcome.get(str(row["outcome"]), 0) + 1
    print("\nOutcome counts:")
    for outcome, count in sorted(by_outcome.items(), key=lambda kv: -kv[1]):
        print(f"  {outcome:<24} {count}")
    no_text = sum(
        1 for r in rows if r.get("page_count") and not r.get("pages_with_text")
    )
    fetched = sum(1 for r in rows if r.get("page_count"))
    print(f"\narchive documents with NO text layer: {no_text} of {fetched} fetched")

    timings = archive.request_timings
    if len(timings) > 1:
        print(f"archive requests made: {archive.fetcher.request_count}")

    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    path = ARTIFACT_DIR / "archive_oem_comparison.json"
    path.write_text(
        json.dumps(
            {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "policy": ARCHIVE_SOURCE_NOTE,
                "delay_seconds": archive.fetcher.delay,
                "requests": archive.fetcher.request_count,
                "counts": by_outcome,
                "documents_without_text_layer": no_text,
                "rows": rows,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    print(f"\nreport -> {path}")
    return 0


def archive_audit() -> int:
    """
    What the archive tier has actually put on disk, read back off the disk.

    Deliberately does NOT ask the content index what it recorded and print that
    back: it re-opens every archive PDF, re-hashes it, re-reads its metadata and
    re-counts its text layer. A register confirming its own entries is not
    evidence.
    """
    index, _ = open_content_index()
    archive_docs = [d for d in index.by_sha.values() if d.tier == TIER_ARCHIVE]
    print(f"archive-tier documents in the content index: {len(archive_docs)}")

    on_disk = 0
    quarantined = 0
    no_text = 0
    producers: dict[str, int] = {}
    tier_from_citation: dict[str, int] = {}
    for doc in sorted(archive_docs, key=lambda d: d.path):
        path = Path(doc.path)
        if not path.is_file():
            print(f"  MISSING  {path}")
            continue
        on_disk += 1
        is_quarantined = ARCHIVE_QUARANTINE_DIR in path.parents
        if is_quarantined:
            quarantined += 1
        payload = path.read_bytes()
        digest = sha256_bytes(payload)
        metadata = read_pdf_metadata(payload)
        _, pages_with_text, page_count = pdf_text_layer(payload)
        if not pages_with_text:
            no_text += 1
        producer = str(metadata.get("producer") or "(none)")
        producers[producer] = producers.get(producer, 0) + 1
        for key in sorted(set(doc.catalog_keys)):
            citation = f"derived/brochure_text/{key.replace('|', '__')}.json"
            tier = document_tier_for_citation(citation)
            tier_from_citation[tier or "(unresolved)"] = (
                tier_from_citation.get(tier or "(unresolved)", 0) + 1
            )
        print(
            f"  {path.name:<52} {'QUARANTINED ' if is_quarantined else ''}"
            f"sha256 {digest[:12]}{' MISMATCH' if digest != doc.sha256 else ''}  "
            f"text {pages_with_text}/{page_count}p  producer={producer[:40]!r}"
        )

    print(
        f"\non disk {on_disk}  quarantined {quarantined}  "
        f"no text layer {no_text}"
    )
    if producers:
        print("\nPDF Producer values seen (evidence base, not a gate):")
        for producer, count in sorted(producers.items(), key=lambda kv: -kv[1]):
            print(f"  {count:>4}  {producer[:70]!r}")
    if tier_from_citation:
        print("\ntier resolved back from a brochure_text citation:")
        for tier, count in sorted(tier_from_citation.items(), key=lambda kv: -kv[1]):
            print(f"  {tier:<16} {count}")
    return 0


def audit_corpus() -> int:
    index, duplicates = open_content_index()
    print(f"stored PDFs indexed: {len(index.by_sha)} distinct sha256")
    if not duplicates:
        print("no duplicate content on disk")
    else:
        wasted = 0
        print(f"\n{len(duplicates)} document(s) stored more than once:")
        for sha, paths in sorted(duplicates.items()):
            size = Path(paths[0]).stat().st_size
            wasted += size * (len(paths) - 1)
            print(f"  sha256 {sha[:16]}  {size} bytes  x{len(paths)}")
            for path in paths:
                print(f"    {Path(path).name}")
        print(f"\n{wasted} redundant bytes on disk.")
        print(
            "Not deleted here: the derived text under each spelling is still what "
            "the exact-match lookup in dictionary_derived reads. Removing a copy is "
            "only safe once that lookup folds model spellings."
        )

    text = audit_corpus_text()
    print(
        f"\nbrochure_text corpus health ({text['files']} files):\n"
        f"  usable (right vehicle, decodes, has its numbers)  {text['usable']}\n"
        f"  no extracted text at all                         {text['no_text']}\n"
        f"  never names its own model                        {text['model_not_named']}\n"
        f"  is about a DERIVED nameplate, not this one       {text['wrong_vehicle']}\n"
        f"  fails the decode/length/digit gate               {text['failed_quality']}\n"
        f"    of which: numbers missing from the text layer  "
        f"{text['failed_digit_density']}"
    )
    print(
        "\nThe gap list counts a file's existence as coverage, so anything above "
        "that is not 'usable' is coverage this backlog does not really have."
    )
    for example in text["worst_examples"][:6]:
        print(f"  {example['file']:<36} {example['chars']:>7} chars  {example['reason']}")
    return 0
