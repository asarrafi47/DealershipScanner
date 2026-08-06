#!/usr/bin/env python3
"""
Re-extract brochure PDFs into derived/brochure_text/ losslessly, and archive the PDFs.

Why this exists
---------------
The first pass over this corpus persisted only the pages whose text matched a
trim-hint regex. For 1,368 of 3,204 brochures that threw the equipment grid away
at capture time (the 2020 Kia Sportage kept pages 13 and 18 of 18), and the
source PDFs were then deleted, so the discarded text cannot be recovered from
anything on disk. ``extract_brochure_text_pdf`` no longer filters pages; this
script re-runs it over every PDF we still hold or can re-fetch, and copies each
PDF into the durable archive so the next parser generation is not stuck the same
way.

What it does per PDF
--------------------
  1. read the existing derived/brochure_text JSON (if any) for the before counts
  2. copy the PDF into the archive (``BROCHURE_ARCHIVE_DIR``) unless --no-archive
     -- first, before any step that can skip or fail, so no outcome leaves a
     source document unarchived
  3. re-extract EVERY page: text, table lines, and per-line cell x-extents
  4. write the new JSON and one report row: pages before -> pages after

Idempotent and resumable: a brochure whose JSON is already schema_version 2 and
whose recorded source_pdf_sha256 matches the PDF on disk has its *re-extraction*
skipped, so an interrupted run can simply be re-run. Re-extracting the same PDF
produces byte-identical JSON. Archiving is **not** skipped with it: the resume
check asks whether the derived text is current, which says nothing about whether
the source document is safe on disk, and those are the PDFs a later
``delete_brochure_pdfs.py --confirm`` would destroy.

Dry run is the DEFAULT. Nothing is written without --apply.

Durable PDF storage
-------------------
Source PDFs live in TWO places and only one of them is disposable:

  backend/data/brochures/          STAGING. The fetcher writes here.
                                   ``delete_brochure_pdfs.py --confirm`` empties
                                   it. Gitignored. Treat as scratch.
  backend/data/brochure_archive/   DURABLE. Never deleted, never overwritten.
                                   Override with the BROCHURE_ARCHIVE_DIR env var
                                   to put it on a mounted volume or an S3-synced
                                   directory in production.

Archive layout, one directory per make:

    backend/data/brochure_archive/
      .gitignore                                     # "*", written on first use
      mazda/2024_Mazda_CX-5_Brochure.pdf             # original filename, unchanged
      mazda/2024_Mazda_CX-5_Brochure.pdf.meta.json   # sha256, bytes, source_url,
                                                     # catalog_key, archived_at

The filename is the fetcher's ``{year}_{Make}_{Model}_Brochure.pdf``, which is
what ``parse_brochure_filename`` reads the catalog key out of, so an archived PDF
can be re-ingested with no other metadata. A second document that lands on the
same name with a different sha256 is stored as ``<stem>__<sha12>.pdf`` beside the
incumbent; an archive write never replaces a document.

Anything that acquires a brochure should call
``backend.enrichment.brochure_extract.archive_source_pdf`` on the file before it
does anything else with it. ``process_brochure_file(delete_after=True)`` already
archives first and keeps the PDF if archiving fails.

Cost: full-page capture with coordinates is roughly 4x the JSON of text alone
(measured: 52KB -> 193KB compact on the 13-page 2024 Mazda CX-90) and about 1s
of CPU per brochure. ``--no-layout`` drops the coordinates and the 4x, at the
price of making column->trim attribution impossible.

Usage:
    python -m backend.scripts.reingest_brochures                      # dry run, all PDFs
    python -m backend.scripts.reingest_brochures --report /tmp/r.jsonl
    python -m backend.scripts.reingest_brochures --apply
    python -m backend.scripts.reingest_brochures --apply --year 2020 --make Kia
    python -m backend.scripts.reingest_brochures --apply --path backend/data/brochures/x.pdf
    python -m backend.scripts.reingest_brochures --apply --overwrite   # ignore resume state
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from backend.enrichment.brochure_extract import (  # noqa: E402
    BROCHURE_TEXT_SCHEMA_VERSION,
    BrochureYMM,
    archive_source_pdf,
    brochure_archive_dir,
    extract_brochure_text_pdf,
    parse_brochure_filename,
    persist_brochure_text,
    sha256_file,
)
from backend.enrichment.brochure_sources import is_quarantined  # noqa: E402
from backend.enrichment.dictionary_paths import (  # noqa: E402
    BROCHURES_DIR,
    BROCHURE_TEXT_DIR,
)

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("reingest_brochures")


def _text_json_path(ymm: BrochureYMM) -> Path:
    return BROCHURE_TEXT_DIR / f"{ymm.catalog_key.replace('|', '__')}.json"


def _read_existing(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def _before_counts(data: dict[str, Any]) -> tuple[int, int, int]:
    """(pages_persisted, page_count_of_pdf, schema_version) from an existing JSON."""
    pages = data.get("pages")
    persisted = len(pages) if isinstance(pages, list) else 0
    try:
        page_count = int(data.get("page_count") or 0)
    except (TypeError, ValueError):
        page_count = 0
    try:
        version = int(data.get("schema_version") or 1)
    except (TypeError, ValueError):
        version = 1
    return persisted, page_count, version


def _collect_pdfs(args: argparse.Namespace) -> list[Path]:
    if args.path:
        return [Path(args.path)]
    seen: dict[str, Path] = {}
    roots: list[Path] = [Path(args.brochures_dir)]
    if args.include_archive:
        roots.append(brochure_archive_dir())
    for root in roots:
        if not root.is_dir():
            continue
        for pdf in sorted(root.rglob("*.pdf")):
            if pdf.name.startswith("."):
                continue
            seen.setdefault(pdf.name.lower(), pdf)
    return [seen[k] for k in sorted(seen)]


def _wanted(ymm: BrochureYMM | None, args: argparse.Namespace) -> bool:
    if not ymm:
        return False
    if args.year and ymm.year != args.year:
        return False
    if args.make and ymm.make.strip().lower() != args.make.strip().lower():
        return False
    return True


def main() -> int:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--apply", action="store_true", help="Write JSON and archive PDFs (default: dry run)")
    p.add_argument("--path", type=Path, help="Single PDF instead of scanning directories")
    p.add_argument("--brochures-dir", type=Path, default=BROCHURES_DIR)
    p.add_argument(
        "--no-include-archive",
        dest="include_archive",
        action="store_false",
        help="Do not also scan the durable archive for PDFs",
    )
    p.add_argument("--year", type=int, default=0, help="Only this model year")
    p.add_argument("--make", default="", help="Only this make")
    p.add_argument("--limit", type=int, default=0, help="Stop after N PDFs (0 = all)")
    p.add_argument(
        "--overwrite",
        action="store_true",
        help="Re-extract even when the existing JSON is already current (disables resume)",
    )
    p.add_argument("--no-layout", dest="layout", action="store_false", help="Skip word x-positions")
    p.add_argument("--no-archive", dest="archive", action="store_false", help="Do not copy PDFs to the archive")
    p.add_argument("--report", type=Path, help="Write one JSON row per PDF here (works in dry run)")
    args = p.parse_args()

    pdfs = _collect_pdfs(args)
    if not pdfs:
        logger.info(
            "No brochure PDFs found under %s%s",
            args.brochures_dir,
            f" or {brochure_archive_dir()}" if args.include_archive else "",
        )
        return 0

    mode = "APPLY" if args.apply else "DRY RUN (pass --apply to write)"
    logger.info("%s — %d PDF(s); archive=%s", mode, len(pdfs), brochure_archive_dir())

    rows: list[dict[str, Any]] = []
    done = skipped = failed = 0
    pages_before_total = pages_after_total = 0

    for pdf in pdfs:
        if args.limit and done + skipped >= args.limit:
            break
        ymm = parse_brochure_filename(pdf)
        if ymm is None:
            # Cannot be extracted -- there is no catalog key to file the text
            # under -- but it is still an acquired OEM document, and these are
            # the ones most at risk: with no derived JSON, nothing downstream
            # knows they exist. Archive, then move on. (archive_source_pdf
            # files a nameless PDF under the archive's unknown-make directory.)
            archived = archive_source_pdf(
                pdf, dry_run=not (args.apply and args.archive)
            )
            skipped += 1
            logger.warning(
                "%s: filename does not parse to year/make/model — archived only",
                pdf.name,
            )
            rows.append(
                {
                    "catalog_key": None,
                    "pdf": pdf.name,
                    "status": "unparseable_filename_archived_only",
                    "archived_to": str(archived) if archived else "",
                }
            )
            continue
        if not _wanted(ymm, args):
            continue

        out_path = _text_json_path(ymm)
        existing = _read_existing(out_path)
        pages_before, page_count_before, version_before = _before_counts(existing)

        try:
            digest = sha256_file(pdf)
        except OSError as exc:
            failed += 1
            logger.error("%s: unreadable (%s)", pdf.name, exc)
            continue

        # Archive FIRST, before anything that can skip, throw or return early.
        #
        # This used to sit after extraction, and the resume branch below
        # `continue`d straight past it -- so on a resumed run (the normal way
        # this script is used, since it is idempotent and interruptible) every
        # PDF whose derived text was already current stayed unarchived. Three
        # other paths had the same hole: an extractor exception, an unparseable
        # result, and a failed JSON write all left the document in the staging
        # directory only. That directory is what
        # ``delete_brochure_pdfs.py --confirm`` empties, and a source PDF that
        # never reached backend/data/brochure_archive/ is gone for good -- the
        # exact loss that cost this corpus 1,368 brochures. Whether the derived
        # text is current says nothing about whether the document is safe on
        # disk, so the archive step must not be downstream of that question.
        #
        # archive_source_pdf is idempotent (identical bytes re-archive to a
        # no-op returning the same path), so hoisting it costs nothing.
        archived = archive_source_pdf(
            pdf, ymm=ymm, dry_run=not (args.apply and args.archive)
        )
        if args.archive and archived is None:
            failed += 1
            logger.error("%s: could not be archived", pdf.name)

        # A quarantined vehicle keeps its PDF (we never delete acquired
        # documents), so without this check re-extraction would write the text
        # straight back into derived/brochure_text and undo the quarantine
        # without saying so. Archiving above still happened, which is the point:
        # the document stays safe, only the derived text stays withheld.
        if is_quarantined(ymm.catalog_key):
            skipped += 1
            logger.warning(
                "%s: catalog key %s is quarantined — archived, text not re-extracted",
                pdf.name,
                ymm.catalog_key,
            )
            rows.append(
                {
                    "catalog_key": ymm.catalog_key,
                    "pdf": pdf.name,
                    "status": "skipped_quarantined",
                    "archived_to": str(archived) if archived else "",
                }
            )
            continue

        current = (
            version_before >= BROCHURE_TEXT_SCHEMA_VERSION
            and str(existing.get("source_pdf_sha256") or "") == digest
        )
        if current and not args.overwrite:
            skipped += 1
            rows.append(
                {
                    "catalog_key": ymm.catalog_key,
                    "pdf": pdf.name,
                    "status": "skipped_already_current",
                    "pages_before": pages_before,
                    "pages_after": pages_before,
                    "page_count": page_count_before,
                    "archived_to": str(archived) if archived else "",
                }
            )
            continue

        try:
            result = extract_brochure_text_pdf(pdf, capture_layout=args.layout)
        except Exception as exc:  # noqa: BLE001 — one bad PDF must not stop the run
            failed += 1
            logger.exception("%s: extract failed (%s)", pdf.name, exc)
            continue
        if result is None:
            skipped += 1
            logger.warning("%s: unparseable filename", pdf.name)
            continue

        pages_after = len(result.pages)
        layout_rows = sum(len(pg.lines) for pg in result.pages)
        recovered = max(0, pages_after - pages_before) if existing else pages_after
        pages_before_total += pages_before
        pages_after_total += pages_after

        row = {
            "catalog_key": ymm.catalog_key,
            "pdf": pdf.name,
            "status": "extracted",
            "schema_version_before": version_before if existing else None,
            "pages_before": pages_before,
            "pages_after": pages_after,
            "page_count": result.page_count,
            "pages_recovered": recovered,
            "layout_rows": layout_rows,
            "trim_hint_pages": result.trim_hint_pages,
            "warnings": result.warnings,
            "sha256": result.source_pdf_sha256,
            "archived_to": str(archived) if archived else "",
        }
        rows.append(row)
        logger.info(
            "%-46s pages %3d -> %3d of %3d  (+%d)  layout_rows=%d%s",
            pdf.name,
            pages_before,
            pages_after,
            result.page_count,
            recovered,
            layout_rows,
            "  warnings=" + ",".join(result.warnings) if result.warnings else "",
        )

        if args.apply:
            try:
                persist_brochure_text(result)
            except OSError as exc:
                failed += 1
                logger.error("%s: write failed (%s)", pdf.name, exc)
                continue
        done += 1

    if args.report and rows:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        with args.report.open("w", encoding="utf-8") as fh:
            for row in rows:
                fh.write(json.dumps(row) + "\n")
        logger.info("report: %s (%d rows)", args.report, len(rows))

    logger.info(
        "done: extracted=%d skipped=%d failed=%d | pages persisted %d -> %d (+%d recovered)",
        done,
        skipped,
        failed,
        pages_before_total,
        pages_after_total,
        pages_after_total - pages_before_total,
    )
    if not args.apply:
        logger.info("DRY RUN — nothing was written. Re-run with --apply.")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
