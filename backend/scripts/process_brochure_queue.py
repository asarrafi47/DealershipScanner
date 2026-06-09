#!/usr/bin/env python3
"""
FCA-style trim overlay extraction (BUYER'S GUIDE / STANDARD – blocks). Often fails on modern brochures.

For text-only extraction (recommended first step), use:
    python -m backend.scripts.extract_brochure_text

PDFs are never deleted unless --delete-after is passed.

Usage:
    python -m backend.scripts.process_brochure_queue --dry-run
    python -m backend.scripts.process_brochure_queue --limit 50
    python -m backend.scripts.process_brochure_queue --delete-after
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from backend.enrichment.brochure_extract import (  # noqa: E402
    extract_brochure_pdf,
    iter_brochure_pdfs,
    process_brochure_file,
)
from backend.enrichment.dictionary_paths import BROCHURES_DIR  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--limit", type=int, default=0, help="Max PDFs to process (0 = all)")
    p.add_argument("--dry-run", action="store_true", help="Extract only; do not write or delete")
    p.add_argument(
        "--delete-after",
        action="store_true",
        help="Delete each PDF after adds are saved (default: keep all PDFs)",
    )
    p.add_argument("--path", type=Path, help="Single PDF path")
    p.add_argument("--brochures-dir", type=Path, default=BROCHURES_DIR)
    args = p.parse_args()

    if args.path:
        paths = [args.path]
    else:
        paths = iter_brochure_pdfs(args.brochures_dir)

    if args.limit > 0:
        paths = paths[: args.limit]

    if not paths:
        logger.info("No brochure PDFs found in %s", args.brochures_dir)
        return 0

    ok = 0
    skipped = 0
    failed = 0

    for i, pdf in enumerate(paths, 1):
        logger.info("[%d/%d] %s", i, len(paths), pdf.name)
        try:
            if args.dry_run:
                result = extract_brochure_pdf(pdf)
                if not result:
                    skipped += 1
                    continue
                logger.info(
                    "  trims=%s adds_trims=%d warnings=%s",
                    result.trims_available,
                    len(result.adds_by_trim),
                    result.warnings,
                )
                for trim, adds in result.adds_by_trim.items():
                    logger.info("    %s: %d bullets", trim, len(adds))
                ok += 1
                continue

            result = process_brochure_file(
                pdf,
                delete_after=args.delete_after,
                dry_run=False,
            )
            if not result:
                skipped += 1
                continue
            if result.adds_by_trim:
                ok += 1
                logger.info(
                    "  saved %d trims, pdf_deleted=%s",
                    len(result.adds_by_trim),
                    args.delete_after,
                )
            else:
                skipped += 1
                logger.warning("  no adds extracted: %s", result.warnings)
        except Exception as exc:
            failed += 1
            logger.exception("  failed: %s", exc)

    logger.info("done: ok=%d skipped=%d failed=%d", ok, skipped, failed)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
