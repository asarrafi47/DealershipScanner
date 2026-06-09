#!/usr/bin/env python3
"""
Extract text from brochure PDFs for manual trim review (no overlay generation).

Output: backend/dictionary/derived/brochure_text/{catalog_key}.json

After a full extract + dictionary rebuild, PDFs may be removed via
``python -m backend.scripts.delete_brochure_pdfs --confirm`` (text JSON is canonical).

Usage:
    python -m backend.scripts.extract_brochure_text
    python -m backend.scripts.extract_brochure_text --year 2023
    python -m backend.scripts.extract_brochure_text --limit 20
    python -m backend.scripts.extract_brochure_text --all-pages
    python -m backend.scripts.extract_brochure_text --path backend/data/brochures/2016_Jeep_Grand_Cherokee_Brochure.pdf
    python -m backend.scripts.extract_brochure_text --dry-run
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from backend.enrichment.brochure_extract import (  # noqa: E402
    extract_brochure_text_pdf,
    iter_brochure_pdfs,
    parse_brochure_filename,
    persist_brochure_text,
)
from backend.enrichment.dictionary_paths import BROCHURES_DIR, BROCHURE_TEXT_DIR  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


def _filter_paths(paths: list[Path], year: int | None) -> list[Path]:
    if year is None:
        return paths
    out: list[Path] = []
    for p in paths:
        ymm = parse_brochure_filename(p)
        if ymm and ymm.year == year:
            out.append(p)
    return out


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--limit", type=int, default=0, help="Max PDFs (0 = all)")
    p.add_argument("--year", type=int, default=0, help="Only this model year (0 = all)")
    p.add_argument("--all-pages", action="store_true", help="Save every page with text, not just trim-hint pages")
    p.add_argument("--dry-run", action="store_true", help="Extract only; do not write JSON")
    p.add_argument("--overwrite", action="store_true", help="Re-write JSON even if output exists")
    p.add_argument("--path", type=Path, help="Single PDF path")
    p.add_argument("--brochures-dir", type=Path, default=BROCHURES_DIR)
    args = p.parse_args()

    if args.path:
        paths = [args.path]
    else:
        paths = iter_brochure_pdfs(args.brochures_dir)

    year = args.year if args.year > 0 else None
    paths = _filter_paths(paths, year)

    if args.limit > 0:
        paths = paths[: args.limit]

    if not paths:
        logger.info("No brochure PDFs found in %s", args.brochures_dir)
        return 0

    ok = 0
    skipped = 0
    failed = 0
    index_rows: list[dict] = []

    for i, pdf in enumerate(paths, 1):
        logger.info("[%d/%d] %s", i, len(paths), pdf.name)
        try:
            ymm = parse_brochure_filename(pdf)
            if not ymm:
                skipped += 1
                logger.warning("  skip: unparseable filename")
                continue

            out_path = BROCHURE_TEXT_DIR / f"{ymm.catalog_key.replace('|', '__')}.json"
            if out_path.is_file() and not args.overwrite and not args.dry_run:
                skipped += 1
                logger.info("  skip: exists (use --overwrite)")
                continue

            result = extract_brochure_text_pdf(pdf, all_pages=args.all_pages)
            if not result:
                skipped += 1
                continue

            logger.info(
                "  pages_saved=%d trim_hint_pages=%s warnings=%s",
                len(result.pages),
                result.trim_hint_pages or "none",
                result.warnings or "none",
            )

            index_rows.append(
                {
                    "catalog_key": result.ymm.catalog_key,
                    "source_pdf": pdf.name,
                    "pages_saved": len(result.pages),
                    "trim_hint_pages": result.trim_hint_pages,
                    "warnings": result.warnings,
                }
            )

            if args.dry_run:
                ok += 1
                continue

            written = persist_brochure_text(result)
            logger.info("  wrote %s", written.relative_to(_REPO))
            ok += 1
        except Exception as exc:
            failed += 1
            logger.exception("  failed: %s", exc)

    if not args.dry_run and index_rows:
        BROCHURE_TEXT_DIR.mkdir(parents=True, exist_ok=True)
        index_path = BROCHURE_TEXT_DIR / "_index.jsonl"
        with index_path.open("w", encoding="utf-8") as fh:
            for row in index_rows:
                fh.write(json.dumps(row) + "\n")
        logger.info("index: %s (%d rows)", index_path.relative_to(_REPO), len(index_rows))

    logger.info("done: ok=%d skipped=%d failed=%d", ok, skipped, failed)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
