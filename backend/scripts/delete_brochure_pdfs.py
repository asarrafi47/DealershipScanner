#!/usr/bin/env python3
"""
Delete brochure PDFs after text extraction is verified.  **DISABLED BY DEFAULT.**

This script is why the trim feature had to be rebuilt. Its guard only asks whether a
``brochure_text`` JSON exists -- not whether that JSON captured the whole document. The
extractor at the time persisted only pages matching a trim-hint regex, so for 1,368 of
3,204 brochures the equipment grids were dropped, this script then deleted the source
PDFs, and the data became permanently unrecoverable. Re-ingesting the 8 PDFs that
happened to survive recovered 38 -> 86 pages: 44% of the content had been thrown away.

"We extracted it" is not the same as "we extracted all of it", and extraction keeps
improving -- coordinate-aware grid parsing needs the original bytes that text extraction
discards. A source document is cheap to keep and impossible to recreate.

Deleting therefore now requires ``--i-have-archived-these-elsewhere`` in addition to
``--confirm``. Prefer archiving over deleting; see ``backend/scripts/reingest_brochures.py``.
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
    p.add_argument(
        "--i-have-archived-these-elsewhere",
        action="store_true",
        dest="archived",
        help="Required second confirmation; see the module docstring for why.",
    )
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
    if not args.archived:
        # The existence check above proves only that *some* text was extracted, which is
        # exactly the assumption that destroyed the corpus once. Make the operator assert
        # the originals survive somewhere before the bytes go away.
        print(
            "Refusing to delete: a brochure_text JSON proves text was extracted, not that\n"
            "the whole document was captured, and re-extraction needs the original bytes.\n"
            "Pass --i-have-archived-these-elsewhere as well if the PDFs are archived."
        )
        return 1

    deleted = 0
    for pdf in pdfs:
        pdf.unlink(missing_ok=True)
        deleted += 1
    print(f"Deleted {deleted} PDFs from {args.brochures_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
