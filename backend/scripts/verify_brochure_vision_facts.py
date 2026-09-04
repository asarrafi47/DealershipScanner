#!/usr/bin/env python3
"""
Independent review pass for the 24/7 local-model brochure lane.

``local_brochure_vision_extract.py`` deliberately never stamps
``review_verdict='confirmed'`` (see its own docstring) -- a single unreviewed
pass is not the bar this codebase promotes facts on
(``feed_package_registry_from_brochure_vision.py`` only reads confirmed rows).
This script is that independent pass.

It reuses the anchored, whole-printed-cell match from ``verify_trim_citations.py``
rather than a substring test: re-running that script's OWN 27 quoted overlays
under anchoring (vs. the older unanchored substring rule) found 21% of
previously "verified" citations were actually interior fragments of a longer
printed line. A plain ``needle in haystack`` test would repeat that mistake
here, so it is not repeated -- ``PageReading.prints`` requires the quoted text
to equal one or more WHOLE consecutive printed cells.

THREE verdicts, not two
------------------------
A binary confirm/reject would misclassify every fact that came off the VISION
path (page rasterized because its cached text layer was missing, suspect, or
the model was asked to also check for a graphic-only chart) -- pdfplumber's
``extract_words()`` only sees the PDF's embedded text layer, so a page with no
usable layer legitimately has nothing to check a quote against. Stamping those
``rejected`` would be a false negative, not a caught hallucination. See
[[feedback: verification passes need cannot_assess]] -- a prior recheck that
could only confirm-or-delete mass-deleted 158 of 161 MSRPs for exactly this
shape of gap. So:

  * ``confirmed``     -- quoted_text equals a whole printed cell (or a
                          consecutive run of them) on the cited page. Also
                          counted here: the same match after stripping a
                          leading isolated "l " token -- measured on this
                          corpus, several OEM brochures render their bullet
                          glyph as the literal letter "l" in the text layer
                          (font substitution with no embedded symbol font),
                          glued to the following cell on some lines and split
                          onto its own cell on others. ``_is_mark_cell`` in
                          ``verify_trim_citations.py`` was written against a
                          different citation corpus and does not know this
                          glyph; this script's own bullet-strip step is
                          exactly that accommodation, applied only here.
  * ``rejected``       -- REPORTED ONLY, never written to the row. The page
                          has extractable text cells and at least one WORD of
                          the quote (bag-of-words, unordered) does not appear
                          anywhere on the page. This sounds like a
                          hallucination signal and sometimes is, but manual
                          review of this bucket also found real printed
                          sentences here -- e.g. pdfplumber gluing "251
                          lb.-ft." into the single word "251lb" with no space,
                          which an exact-token bag-of-words check treats as
                          "251" being absent. Trusting this bucket needs
                          fuzzy/character-level matching, not exact tokens;
                          that is not built here, so it is surfaced for a
                          human to look at and never auto-applied.
  * ``cannot_assess``  -- everything else that isn't a clean confirm: the
                          source PDF is missing/changed, the page is out of
                          range, the page prints no extractable text cells at
                          all (image-only page), OR every word of the quote
                          IS present somewhere on the page but not as one
                          anchored run. That last case is common and
                          deliberately NOT stamped rejected: many of these
                          are spec-table rows where the model correctly read
                          a row LABEL from one column and a VALUE from an
                          adjacent column and joined them (measured: e.g. a
                          2006 911 brochure prints "anti lock braking
                          system" and "8.0" as separate cells in different
                          column bands of the same table row) -- genuinely
                          printed, just not a single contiguous run this
                          single-column anchored matcher can span. Confirming
                          it would risk re-legitimizing a wrong row/column
                          pairing; rejecting it would discard real facts this
                          method simply cannot judge. It needs a table-aware
                          checker (row-band reconstruction, not column-band),
                          not built here.

Nothing is ever deleted or downgraded from 'confirmed' here. Only CONFIRMED
rows are written; REJECTED and CANNOT_ASSESS both leave ``review_verdict``
NULL, so a fact this pass could not clear is left checkable again later
(by a smarter checker, or a rerun after a bug fix here) rather than being
forced into a verdict this method cannot actually back up.

Usage
-----
    .venv/bin/python -m backend.scripts.verify_brochure_vision_facts             # report only
    .venv/bin/python -m backend.scripts.verify_brochure_vision_facts --apply     # stamp the rows
    .venv/bin/python -m backend.scripts.verify_brochure_vision_facts --limit 200 --apply
"""
from __future__ import annotations

import argparse
import hashlib
import logging
import os
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from backend.scripts.verify_trim_citations import PageReading, normalize, read_page  # noqa: E402

#: See the module docstring's CONFIRMED bullet -- a bullet glyph rendered as a
#: literal "l" in the text layer, measured on this corpus (2011 Mazda2 and
#: others), glued to or split from the following cell inconsistently line to
#: line.
_LEAD_BULLET_L = re.compile(r"^l\s+")


def _strip_bullet_glyph(reading: PageReading) -> PageReading:
    cols = tuple(
        tuple(_LEAD_BULLET_L.sub("", c) for c in col if c != "l")
        for col in reading.cells
    )
    return PageReading(cells=cols)


def _page_word_bag(reading: PageReading) -> set[str]:
    bag: set[str] = set()
    for col in reading.cells:
        for cell in col:
            bag.update(cell.split())
    return bag

_log = logging.getLogger("verify_brochure_vision_facts")

SOURCE_TAG = "qwen3.8-27b-local"
VERIFIER_VERSION = "2026-08-29.1"

CONFIRMED = "confirmed"
REJECTED = "rejected"
CANNOT_ASSESS = "cannot_assess"

#: Reported-reject rate above which the summary flags the run loudly. REJECTED
#: is diagnostic only (never written -- see the module docstring), so this
#: gates nothing; it is purely a "go look at this bucket" signal.
_HIGH_REJECT_ALARM = 0.30


def _connect():
    import psycopg
    from dotenv import load_dotenv

    load_dotenv()
    return psycopg.connect(os.environ["INVENTORY_DATABASE_URL"])


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _select_pending(cur, limit: int | None) -> list[tuple]:
    sql = """
        SELECT id, source_pdf_path, source_pdf_sha256, page_number, quoted_text
        FROM brochure_vision_facts
        WHERE extracted_by = %s AND review_verdict IS NULL
        ORDER BY source_pdf_sha256, page_number
    """
    if limit:
        sql += " LIMIT %s"
        cur.execute(sql, (SOURCE_TAG, limit))
    else:
        cur.execute(sql, (SOURCE_TAG,))
    return cur.fetchall()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true",
                     help="write review_verdict back into brochure_vision_facts")
    ap.add_argument("--limit", type=int, default=None,
                     help="cap on rows examined this run")
    ap.add_argument("--show-rejects", type=int, default=15,
                     help="how many rejected facts to print (0 for none)")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    import pdfplumber

    conn = _connect()
    cur = conn.cursor()
    rows = _select_pending(cur, args.limit)
    _log.info("%d pending row(s) from %s to check", len(rows), SOURCE_TAG)

    outcomes: Counter = Counter()
    reject_examples: list[dict[str, Any]] = []
    updates: list[tuple[str, int]] = []

    open_pdfs: dict[str, Any] = {}
    sha_ok: dict[str, bool] = {}
    page_cache: dict[tuple[str, int], PageReading] = {}
    bullet_stripped_cache: dict[tuple[str, int], PageReading] = {}
    word_bag_cache: dict[tuple[str, int], set[str]] = {}

    def pdf_for(path_str: str, recorded_sha: str) -> Any | None:
        if path_str in open_pdfs:
            return open_pdfs[path_str] if sha_ok.get(path_str) else None
        path = Path(path_str)
        ok = path.is_file() and (not recorded_sha or _sha256(path) == recorded_sha)
        sha_ok[path_str] = ok
        if not ok:
            open_pdfs[path_str] = None
            return None
        try:
            pdf = pdfplumber.open(str(path))
        except Exception as exc:  # pragma: no cover - corrupt/unreadable PDF
            _log.warning("could not open %s: %s", path.name, exc)
            open_pdfs[path_str] = None
            return None
        open_pdfs[path_str] = pdf
        return pdf

    try:
        for fact_id, path_str, sha, page_number, quoted_text in rows:
            path_str = path_str or ""
            pdf = pdf_for(path_str, sha or "")
            if pdf is None:
                outcomes[CANNOT_ASSESS] += 1
                continue
            if not isinstance(page_number, int) or page_number < 1 or page_number > len(pdf.pages):
                outcomes[CANNOT_ASSESS] += 1
                continue

            key = (path_str, page_number)
            if key not in page_cache:
                page_cache[key] = read_page(pdf.pages[page_number - 1])
            reading = page_cache[key]

            if not reading.cells:
                # No embedded text layer to check against -- most likely how a
                # vision-path fact was read in the first place. Not evidence
                # either way.
                outcomes[CANNOT_ASSESS] += 1
                continue

            if reading.prints(quoted_text):
                outcomes[CONFIRMED] += 1
                updates.append((CONFIRMED, fact_id))
                continue

            if key not in bullet_stripped_cache:
                bullet_stripped_cache[key] = _strip_bullet_glyph(reading)
            if bullet_stripped_cache[key].prints(quoted_text):
                outcomes[CONFIRMED] += 1
                updates.append((CONFIRMED, fact_id))
                continue

            if key not in word_bag_cache:
                word_bag_cache[key] = _page_word_bag(reading)
            needle_words = set(normalize(quoted_text).split())
            if needle_words and needle_words.issubset(word_bag_cache[key]):
                # Every word is on the page somewhere, just not as one
                # anchored run -- most likely a table row split across
                # columns. Genuinely unverifiable by this method; see the
                # module docstring. Left NULL, not stamped either way.
                outcomes[CANNOT_ASSESS] += 1
                continue

            # A missing word here is NOT trusted as a hallucination signal.
            # Manual review of this bucket found real printed sentences
            # rejected because pdfplumber glued a number to its unit with no
            # space ("251 lb.-ft." on the page as the single word "251lb",
            # normalize()'s independent tokenisation expects "251" alone) --
            # a token-boundary artifact, not absent content. A reliable
            # reject verdict needs fuzzy/character-level matching, not exact
            # bag-of-words; that is not built here, so REJECTED is reported
            # for visibility but deliberately never written to the row.
            outcomes[REJECTED] += 1
            if len(reject_examples) < args.show_rejects:
                reject_examples.append(
                    {"id": fact_id, "page": page_number, "pdf": Path(path_str).name,
                     "quoted_text": quoted_text}
                )
    finally:
        for pdf in open_pdfs.values():
            if pdf is not None:
                try:
                    pdf.close()
                except Exception:  # pragma: no cover
                    pass

    total_checked = sum(outcomes.values())
    assessed = outcomes[CONFIRMED] + outcomes[REJECTED]
    reject_rate = (outcomes[REJECTED] / assessed) if assessed else 0.0

    print(f"checked:              {total_checked}")
    print(f"  confirmed (WRITTEN):{outcomes[CONFIRMED]}")
    print(f"  rejected (reported only, not written): {outcomes[REJECTED]}")
    print(f"  cannot_assess (not written):{outcomes[CANNOT_ASSESS]}")
    if assessed:
        print(f"reported reject rate: {reject_rate:.1%}")
    if reject_rate > _HIGH_REJECT_ALARM:
        print(f"** HIGH REPORTED REJECT RATE (> {_HIGH_REJECT_ALARM:.0%}) -- worth a look, but see the docstring: this bucket is not a reliable hallucination signal yet **")

    if reject_examples:
        print("\nsample reported-rejected facts (NOT written to the row):")
        for ex in reject_examples:
            print(f"  #{ex['id']} {ex['pdf']} p{ex['page']}: {ex['quoted_text']!r}")

    if args.apply and updates:
        cur.executemany(
            "UPDATE brochure_vision_facts SET review_verdict = %s, "
            "reviewed_by = %s, reviewed_at = now() WHERE id = %s",
            [(verdict, VERIFIER_VERSION, fact_id) for verdict, fact_id in updates],
        )
        conn.commit()
        print(f"\napplied: stamped {len(updates)} row(s) ({SOURCE_TAG} reviewed_by={VERIFIER_VERSION!r})")
    elif args.apply:
        print("\napplied: nothing to stamp (cannot_assess rows are left NULL)")
    else:
        print("\ndry run -- pass --apply to write review_verdict")

    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
