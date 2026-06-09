#!/usr/bin/env python3
"""
Delete brochure PDFs after text extraction is verified.

Requires every PDF under brochures-dir to have a matching derived/brochure_text JSON
(or explicit no_text_extracted in index).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from backend.enrichment.brochure_extract import iter_brochure_pdfs, parse_brochure_filename  # noqa: E402
from backend.enrichment.dictionary_paths import BROCHURES_DIR, BROCHURE_TEXT_DIR  # noqa: E402


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--brochures-dir", type=Path, default=BROCHURES_DIR)
    p.add_argument("--confirm", action="store_true", help="Actually delete files")
    args = p.parse_args()

    pdfs = list(iter_brochure_pdfs(args.brochures_dir))
    missing_json: list[str] = []
    for pdf in pdfs:
        ymm = parse_brochure_filename(pdf)
        if not ymm:
            missing_json.append(pdf.name)
            continue
        out = BROCHURE_TEXT_DIR / f"{ymm.catalog_key.replace('|', '__')}.json"
        if not out.is_file():
            missing_json.append(pdf.name)

    if missing_json:
        print(f"Refusing: {len(missing_json)} PDFs lack brochure_text JSON")
        for name in missing_json[:20]:
            print(f"  {name}")
        return 1

    print(f"Verified {len(pdfs)} PDFs have brochure_text JSON")
    if not args.confirm:
        print("Dry run — pass --confirm to delete PDFs")
        return 0

    deleted = 0
    for pdf in pdfs:
        pdf.unlink(missing_ok=True)
        deleted += 1
    print(f"Deleted {deleted} PDFs from {args.brochures_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
