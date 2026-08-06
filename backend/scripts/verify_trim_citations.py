#!/usr/bin/env python3
"""
Re-open the brochure PDF behind every ladder-bullet citation and check the bullet is in it.

WHY THIS IS A BUILD STEP AND NOT A RENDER-TIME CHECK
----------------------------------------------------
The render path used to decide, per request, whether a bullet was citable — by
asking a register that the same request had just filled in. Two stores were
caught certifying themselves that way (``epa_csv``, then ``brochure_trim_walk``,
which handed the register a sentence it had composed one line earlier). Asking a
citation register whether it issued a citation proves nothing about a document.

So the decision moves here, where it can be made against the document itself,
once, and written into the data. This script:

  1. reads every overlay under ``derived/trim_adds_by_year`` whose ``source`` is
     an admissible store (today: ``brochure_text_quoted``);
  2. for each ``adds_provenance`` entry — one per rendered bullet — resolves the
     ORIGINAL PDF the cited ``derived/brochure_text/*.json`` was extracted from,
     confirms the file still hashes to the sha256 recorded at extraction time,
     re-extracts the cited page with pdfplumber, and checks the bullet text is
     printed on it;
  3. with ``--apply``, stamps each entry ``verified: true`` or ``verified:
     false``.

``brochure_extract._cited_bullet_texts`` and
``trim_ladder._register_overlay_citations`` then require ``verified is True``.
An entry this script has never seen has no such key and does not render, so a
new overlay is silent until it has been checked. Fail closed.

WHAT "VERBATIM" MEANS HERE, AND WHAT IT REFUSES
----------------------------------------------
Normalisation touches case, whitespace, and quote/dash glyph variants, and
nothing else. Two token sequences match only when they are IDENTICAL.

The page is rebuilt into CELLS — the printed units of a page — and the bullet
must equal one whole cell, or a run of CONSECUTIVE whole cells joined in
printing order. It is anchored at both ends: the bullet must start where a
printed cell starts and end where a printed cell ends.

Cell reconstruction is necessary and is not a loosening. Brochure equipment
grids print one feature label per row, wrapped over several lines, with the
per-trim S/O/- marks landing between the wrapped halves; two-column trim walks
interleave unrelated text line by line. Reading ``page.extract_text()`` finds 5
of 33 bullets on a page whose text really does contain all 33. So the page is
rebuilt from word geometry — words to lines, lines to cells on horizontal gaps,
cells to columns on x position — and mark-only cells are dropped. Joining
consecutive cells is what re-assembles a label the printer wrapped; it is the
one accommodation made, and it never reorders anything.

WHAT IT REFUSES, precisely:

* a dropped or added character — "(4x only)" against a printed "(4x4 only)";
* a dropped footnote digit — "Rear Park Assist" against "Rear Park Assist5";
* a reordered token — "sunroof Panorama" against "Panorama sunroof";
* a bag of words that is present but not adjacent — "Heated front seats Power
  tailgate" off a row printing "…seats Panorama sunroof Power tailgate";
* AND an INTERIOR FRAGMENT of a longer printed cell. "Tex Leatherette seating
  surfaces" is printed inside "perforated V-Tex Leatherette seating surfaces",
  but it is not what the page says; the head has been chopped off it. A
  substring test accepts it. This one does not.

That last refusal is the reason this docstring was rewritten. Until
2026-08-02 the comparison was ``" needle " in " haystack "`` — a contiguous
run, but unanchored, so any interior fragment of a printed line passed while
the docstring claimed fuzzy and partial matches were refused. Re-running the
27 quoted overlays on disk under the anchored rule: of 581 citations then
stamped ``verified: true``, 122 (21.0%) were interior fragments and now fail.
Nothing that failed before passes now — the change is strictly a tightening.

The cost of anchoring is false negatives, and they are accepted deliberately.
When a brochure prints two features on one row with less than ``cell_gap``
between them, they become one cell and neither feature can be quoted on its
own. Fail closed: an unquotable true feature is silent, which is fine; a
fragment that changes the claim is not.

RESIDUAL, not fixed here: ``read_page`` appends a whole-page reading in
(top, x0) order as a last column so single-column pages work. Consecutive
cells in that ordering can come from different printed columns of the same
line, so a bullet could in principle be assembled across a column boundary.
Anchoring makes that far harder than a substring test did, but it does not
make it impossible.

Usage
-----
    python backend/scripts/verify_trim_citations.py            # report only
    python backend/scripts/verify_trim_citations.py --apply    # stamp the data
    python backend/scripts/verify_trim_citations.py --json out.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from backend.enrichment.brochure_extract import (  # noqa: E402
    ADMISSIBLE_OVERLAY_SOURCES,
    CITATION_VERIFICATION_KEY,
    CITATION_VERIFIED_KEY,
)
from backend.enrichment.dictionary_paths import (  # noqa: E402
    BROCHURES_DIR,
    BROCHURE_TEXT_DIR,
    DERIVED_DIR,
    DICTIONARY_ROOT,
    TRIM_ADDS_BY_YEAR_DIR,
)

#: Bumped when the ADMISSION RULE changes, so a stamp records which rule made
#: it. ``2026-08-02.1`` is the first anchored (whole-printed-cell) rule; every
#: stamp older than it was made by the unanchored substring test.
VERIFIER_VERSION = "2026-08-02.1"

#: Extra places a brochure PDF may live besides ``BROCHURES_DIR``.
_PDF_SEARCH_DIRS = (
    BROCHURES_DIR,
    _REPO / "backend" / "data" / "brochure_archive",
)

REPORT_PATH = DERIVED_DIR / "trim_citation_verification.json"


# --- normalisation: case, whitespace, quote glyphs. Nothing else. ---------

_GLYPHS = str.maketrans(
    {
        "’": "'", "‘": "'", "“": '"', "”": '"',
        "–": "-", "—": "-", "−": "-", " ": " ",
    }
)
_NON_ALNUM = re.compile(r"[^0-9a-z]+")


def normalize(text: Any) -> str:
    """Alphanumeric token sequence of ``text``, lowercased, single-spaced.

    Everything a PDF renderer can legitimately vary — curly vs straight quotes,
    en/em dashes, bullet glyphs, line-wrap hyphens, run of spaces — collapses.
    Words and digits do not. ``"6-way manual"`` and ``"6-\\nway manual"`` both
    become ``"6 way manual"``; ``"(4x4 only)"`` and ``"(4x only)"`` do not.
    """
    return " ".join(_NON_ALNUM.sub(" ", str(text or "").translate(_GLYPHS).lower()).split())


def matches_printed_run(needle: str, cells: Sequence[str]) -> bool:
    """True when ``needle`` EQUALS a run of consecutive printed cells.

    ``needle`` and ``cells`` are already ``normalize``d. The needle must equal
    ``cells[i]`` joined through ``cells[j]`` for some ``i <= j`` — so it starts
    at the start of a printed cell and ends at the end of one.

    Joining consecutive cells is the line-wrap accommodation and only that: a
    label the printer wrapped across rows, with the equipment-grid marks
    dropped from between its halves, is re-assembled in printing order.

    What this does NOT do, all of which a plain ``needle in haystack`` test did
    or would do:

    * accept an interior fragment of a cell. ``"tow hooks"`` does not match a
      cell printing ``"front tow hooks 4x4 only"``.
    * accept a longer needle that merely contains a cell.
    * reorder, skip, or gap over a cell. The run is consecutive.
    * accept a needle whose tokens are all present but not adjacent.

    Empty needles and empty cells never match.
    """
    if not needle or not cells:
        return False
    for i in range(len(cells)):
        run = ""
        for j in range(i, len(cells)):
            cell = cells[j]
            if not cell:
                continue
            run = cell if not run else f"{run} {cell}"
            if run == needle:
                return True
            # The run can only grow. Once it is no longer a token-aligned
            # prefix of the needle, no longer j can rescue it.
            if not needle.startswith(f"{run} "):
                break
    return False


# --- rebuilding a printed page from word geometry -------------------------

#: A cell holding only equipment-grid marks (S / O / P / — / N/A) carries no
#: prose and sits BETWEEN the wrapped halves of a feature label, so it is
#: dropped rather than allowed to break the label in two.
_MARK_TOKEN = re.compile(r"^(?:[sopn]|na|std|opt|[\-–—•●■])+$", re.I)


def _is_mark_cell(text: str) -> bool:
    tokens = str(text).split()
    return bool(tokens) and all(_MARK_TOKEN.match(t.strip("/")) for t in tokens)


#: A superscript footnote call lands on almost the same baseline as the word it
#: follows, so pdfplumber emits "Rear Park Assist5" as one word, or leaves the
#: call standing as its own short numeric token. Scrubbing those is used for
#: DIAGNOSIS ONLY -- see ``FAIL_FOOTNOTE_GLUED``. It is not part of ``prints``.
#: Guards, so a real number is never mistaken for a footnote call: a glued call
#: is at most two digits, ends the token, and follows at least three letters --
#: which leaves "4x4" (starts with a digit) and "F150" (three digits) alone.
_GLUED_FOOTNOTE = re.compile(r"^([a-z]{3,}[a-z0-9]*?)\d{1,2}$")
_LONE_FOOTNOTE = re.compile(r"^\d{1,2}$")


def _scrub_footnote_calls(cell: str) -> str:
    """``cell`` with footnote calls removed. Diagnosis only -- never in ``prints``."""
    out: list[str] = []
    for token in cell.split():
        if _LONE_FOOTNOTE.match(token):
            continue
        m = _GLUED_FOOTNOTE.match(token)
        out.append(m.group(1) if m else token)
    return " ".join(out)


@dataclass
class PageReading:
    """One page of a PDF, rebuilt as the columns of printed CELLS it prints.

    ``cells[c][k]`` is the k-th printed cell of column ``c``, already
    normalised, in printing order. The field is deliberately NOT called
    ``columns`` any more and no longer holds flattened per-column strings:
    flattening is what allowed an interior fragment of one printed line to pass
    as a quote, and a caller that hands this class a flat string would silently
    get character-by-character iteration instead of cells. The rename makes
    that a loud ``TypeError``.
    """

    cells: tuple[tuple[str, ...], ...]

    def prints(self, text: str) -> bool:
        """True when the page prints ``text`` as whole printed cell(s). Anchored."""
        needle = normalize(text)
        return any(matches_printed_run(needle, col) for col in self.cells)

    def prints_but_for_a_footnote_call(self, text: str) -> bool:
        """True when the ONLY thing between us and a match is a footnote call.

        Reported, never accepted. The bullet is still stamped ``verified: false``
        and still does not render; this only separates "our text is wrong about
        the car" from "the text extractor ran a superscript into the word before
        it". The scrub is applied to the PAGE, never to our stored text, and the
        match is the same anchored whole-cell match ``prints`` uses.

        Measured 2026-08-02 over the 27 quoted overlays: 9 of the 30 refusals
        under the pre-anchoring rule were this, and PyMuPDF -- a different text
        engine -- found all of those strings printed in reading order on the
        same page. The fix belongs upstream, where the bullet text is
        normalised, not here in the check.
        """
        needle = normalize(text)
        return any(
            matches_printed_run(needle, tuple(_scrub_footnote_calls(c) for c in col))
            for col in self.cells
        )


def read_page(page: Any, *, cell_gap: float = 14.0, line_tol: float = 3.0) -> PageReading:
    """Rebuild ``page`` as columns of normalised printed cells.

    ``cell_gap`` is the horizontal white space (in points) that separates two
    printed cells on the same visual line; ``line_tol`` is the vertical
    tolerance for calling two words the same line. Both are layout constants —
    they decide where the printed units are, not how similar a match has to be.
    The match itself is exact and anchored to those units.
    """
    try:
        words = page.extract_words(use_text_flow=False, keep_blank_chars=False)
    except Exception:  # pragma: no cover - pdfplumber page-level failure
        return PageReading(cells=())
    if not words:
        return PageReading(cells=())

    lines: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for w in words:
        lines[round(float(w["top"]) / line_tol)].append(w)

    cells: list[tuple[float, float, str]] = []
    for key in sorted(lines):
        row = sorted(lines[key], key=lambda w: float(w["x0"]))
        run = [row[0]]
        for w in row[1:]:
            if float(w["x0"]) - float(run[-1]["x1"]) > cell_gap:
                cells.append((float(run[0]["top"]), float(run[0]["x0"]),
                              " ".join(x["text"] for x in run)))
                run = [w]
            else:
                run.append(w)
        cells.append((float(run[0]["top"]), float(run[0]["x0"]),
                      " ".join(x["text"] for x in run)))

    cells = [c for c in cells if not _is_mark_cell(c[2])]
    if not cells:
        return PageReading(cells=())

    # Cluster cell left edges into columns; 30pt apart starts a new column.
    edges = sorted({round(c[1]) for c in cells})
    bands: list[list[int]] = []
    for x in edges:
        if bands and x - bands[-1][-1] <= 30:
            bands[-1].append(x)
        else:
            bands.append([x])

    def band_of(x: float) -> int:
        rx = round(x)
        for i, band in enumerate(bands):
            if band[0] - 1 <= rx <= band[-1] + 1:
                return i
        return 0

    by_band: dict[int, list[tuple[float, float, str]]] = defaultdict(list)
    for cell in cells:
        by_band[band_of(cell[1])].append(cell)

    def in_printing_order(group: list[tuple[float, float, str]]) -> tuple[str, ...]:
        ordered = sorted(group, key=lambda c: (c[0], c[1]))
        return tuple(n for n in (normalize(t) for _, _, t in ordered) if n)

    columns = [in_printing_order(v) for _, v in sorted(by_band.items())]
    # Single-column pages: the whole page in printing order is the one column.
    # See the RESIDUAL note in the module docstring — consecutive cells here can
    # straddle a column boundary, so this reading is the loosest one we keep.
    columns.append(in_printing_order(cells))
    return PageReading(cells=tuple(c for c in columns if c))


# --- resolving the document a citation names ------------------------------

FAIL_NO_BROCHURE_TEXT = "no_brochure_text_for_citation"
FAIL_CITATION_OTHER_VEHICLE = "citation_names_another_vehicle"
FAIL_NO_PDF = "source_pdf_not_held"
FAIL_PDF_CHANGED = "source_pdf_sha256_mismatch"
FAIL_NO_SHA = "source_pdf_sha256_not_recorded"
FAIL_BAD_PAGE = "cited_page_out_of_range"
FAIL_NOT_PRINTED = "text_not_printed_on_cited_page"
#: A FAILURE, like every other FAIL_* code -- the bullet is stamped
#: ``verified: false`` and does not render. It is split out from
#: FAIL_NOT_PRINTED only so the report distinguishes a wrong quote from a
#: text-extractor artefact. See ``PageReading.prints_but_for_a_footnote_call``.
FAIL_FOOTNOTE_GLUED = "text_printed_but_footnote_digit_glued_to_word"
FAIL_BAD_ENTRY = "citation_incomplete"
OK = "verified"

#: Every code that is not ``OK``. ``verified`` is stamped True for OK alone.
FAILURE_CODES = frozenset(
    {
        FAIL_NO_BROCHURE_TEXT,
        FAIL_CITATION_OTHER_VEHICLE,
        FAIL_NO_PDF,
        FAIL_PDF_CHANGED,
        FAIL_NO_SHA,
        FAIL_BAD_PAGE,
        FAIL_NOT_PRINTED,
        FAIL_FOOTNOTE_GLUED,
        FAIL_BAD_ENTRY,
    }
)


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _resolve_citation_doc(source: str) -> Path | None:
    """``derived/brochure_text/x.json`` (as cited) → the file on disk."""
    text = str(source or "").strip()
    if not text:
        return None
    candidate = Path(text)
    if candidate.is_absolute():
        return candidate if candidate.is_file() else None
    for root in (DICTIONARY_ROOT, _REPO):
        p = (root / candidate).resolve()
        if p.is_file():
            return p
    p = BROCHURE_TEXT_DIR / candidate.name
    return p if p.is_file() else None


def _find_pdf(recorded: str) -> Path | None:
    """The brochure PDF, at its recorded path or by filename in our archives."""
    text = str(recorded or "").strip()
    if not text:
        return None
    direct = Path(text)
    if direct.is_file():
        return direct
    name = direct.name.lower()
    for root in _PDF_SEARCH_DIRS:
        if not root.is_dir():
            continue
        for p in root.rglob("*.pdf"):
            if p.name.lower() == name:
                return p
    return None


@dataclass
class OverlayReport:
    path: Path
    catalog_key: str
    source: str
    outcomes: Counter = field(default_factory=Counter)
    failures: list[dict[str, Any]] = field(default_factory=list)
    pdf: str | None = None

    @property
    def total(self) -> int:
        return sum(self.outcomes.values())

    @property
    def verified(self) -> int:
        return self.outcomes[OK]


def verify_overlay(path: Path, *, apply: bool) -> OverlayReport | None:
    """Check every provenance entry in one overlay file against its document."""
    import pdfplumber

    try:
        overlay = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(overlay, dict):
        return None
    source = str(overlay.get("source") or "").strip().lower()
    if source not in ADMISSIBLE_OVERLAY_SOURCES:
        return None
    provenance = overlay.get("adds_provenance")
    if not isinstance(provenance, dict):
        return None

    report = OverlayReport(
        path=path,
        catalog_key=str(overlay.get("catalog_key") or path.stem),
        source=source,
    )
    o_year, o_make, o_model = (
        overlay.get("year"),
        normalize(overlay.get("make")),
        normalize(overlay.get("model")),
    )

    # Resolve, once per cited document, the PDF and its page readings.
    doc_cache: dict[str, tuple[str | None, Any]] = {}
    open_pdfs: dict[str, Any] = {}
    page_cache: dict[tuple[str, int], PageReading] = {}

    def document_for(citation_source: str) -> tuple[str | None, Any]:
        """(failure code, pdfplumber PDF) for a cited brochure_text file."""
        if citation_source in doc_cache:
            return doc_cache[citation_source]
        result: tuple[str | None, Any] = (FAIL_NO_BROCHURE_TEXT, None)
        doc = _resolve_citation_doc(citation_source)
        if doc is not None:
            try:
                blob = json.loads(doc.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                blob = None
            if isinstance(blob, dict):
                # The cited transcript must be OF THIS VEHICLE. A citation that
                # names another year/make/model's book is not evidence here even
                # if the sentence happens to be printed in it.
                same_vehicle = (
                    str(blob.get("year")) == str(o_year)
                    and normalize(blob.get("make")) == o_make
                    and normalize(blob.get("model")) == o_model
                )
                if not same_vehicle:
                    result = (FAIL_CITATION_OTHER_VEHICLE, None)
                else:
                    pdf_path = _find_pdf(str(blob.get("source_pdf") or ""))
                    recorded_sha = str(blob.get("source_pdf_sha256") or "").strip()
                    if pdf_path is None:
                        result = (FAIL_NO_PDF, None)
                    elif not recorded_sha:
                        result = (FAIL_NO_SHA, None)
                    elif _sha256(pdf_path) != recorded_sha:
                        result = (FAIL_PDF_CHANGED, None)
                    else:
                        pdf = pdfplumber.open(str(pdf_path))
                        open_pdfs[citation_source] = pdf
                        report.pdf = pdf_path.name
                        result = (None, pdf)
        doc_cache[citation_source] = result
        return result

    try:
        for trim, entries in provenance.items():
            if not isinstance(entries, list):
                continue
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                text = str(entry.get("text") or "").strip()
                cite_src = str(entry.get("source") or "").strip()
                page_no = entry.get("page")
                verdict: str
                if not text or not cite_src or not isinstance(page_no, int):
                    verdict = FAIL_BAD_ENTRY
                else:
                    fail, pdf = document_for(cite_src)
                    if fail:
                        verdict = fail
                    elif page_no < 1 or page_no > len(pdf.pages):
                        verdict = FAIL_BAD_PAGE
                    else:
                        key = (cite_src, page_no)
                        if key not in page_cache:
                            page_cache[key] = read_page(pdf.pages[page_no - 1])
                        reading = page_cache[key]
                        if reading.prints(text):
                            verdict = OK
                        elif reading.prints_but_for_a_footnote_call(text):
                            verdict = FAIL_FOOTNOTE_GLUED
                        else:
                            verdict = FAIL_NOT_PRINTED
                report.outcomes[verdict] += 1
                if verdict != OK:
                    report.failures.append(
                        {"trim": trim, "page": page_no, "text": text, "why": verdict}
                    )
                if apply:
                    entry[CITATION_VERIFIED_KEY] = verdict == OK
                    entry["verified_why"] = verdict
    finally:
        for pdf in open_pdfs.values():
            try:
                pdf.close()
            except Exception:  # pragma: no cover
                pass

    if apply:
        overlay[CITATION_VERIFICATION_KEY] = {
            "verifier": Path(__file__).name,
            "version": VERIFIER_VERSION,
            "checked_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "source_pdf": report.pdf,
            "verified": report.verified,
            "total": report.total,
        }
        # Same shape the writer of these files uses
        # (``build_trim_spec_sheets._write_quoted_overlays``), so a verify run
        # and an extract run do not fight over the encoding of every ® and ™.
        # The only overlays reachable here are ``brochure_text_quoted`` ones —
        # ``ADMISSIBLE_OVERLAY_SOURCES`` holds nothing else — so this is the
        # writer to match, not ``persist_brochure_extract``.
        path.write_text(
            json.dumps(overlay, indent=2, ensure_ascii=False), encoding="utf-8"
        )
    return report


def iter_overlays(paths: Iterable[Path] | None = None) -> list[Path]:
    if paths:
        return [Path(p) for p in paths]
    return sorted(TRIM_ADDS_BY_YEAR_DIR.glob("*.json"))


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true",
                    help="write verified true/false back into the overlay files")
    ap.add_argument("--json", dest="json_out", default=None,
                    help="write the full report here (default: derived/trim_citation_verification.json)")
    ap.add_argument("--show-failures", type=int, default=25,
                    help="how many failing bullets to print (0 for none)")
    ap.add_argument("overlays", nargs="*", help="specific overlay files (default: all)")
    args = ap.parse_args(argv)

    reports: list[OverlayReport] = []
    for path in iter_overlays(args.overlays):
        r = verify_overlay(path, apply=args.apply)
        if r is not None:
            reports.append(r)

    totals: Counter = Counter()
    for r in reports:
        totals.update(r.outcomes)
    checked = sum(totals.values())

    print(f"overlays with an admissible source : {len(reports)}")
    print(f"citations checked                  : {checked}")
    for code, n in sorted(totals.items(), key=lambda kv: (-kv[1], kv[0])):
        print(f"  {code:<34} {n:>6}")
    print(f"files fully verified               : "
          f"{sum(1 for r in reports if r.total and r.verified == r.total)}")
    print(f"files with nothing renderable      : "
          f"{sum(1 for r in reports if not r.verified)}")

    if args.show_failures:
        shown = 0
        print("\n-- failing citations")
        for r in reports:
            for f in r.failures:
                if shown >= args.show_failures:
                    break
                print(f"  {r.catalog_key:<28} p{f['page']!s:<4} {f['why']:<32} "
                      f"{f['text'][:70]!r}")
                shown += 1
            if shown >= args.show_failures:
                break

    out = Path(args.json_out) if args.json_out else REPORT_PATH
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(
        json.dumps(
            {
                "verifier_version": VERIFIER_VERSION,
                "checked_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "applied": bool(args.apply),
                "totals": dict(totals),
                "overlays": [
                    {
                        "file": r.path.name,
                        "catalog_key": r.catalog_key,
                        "source": r.source,
                        "source_pdf": r.pdf,
                        "outcomes": dict(r.outcomes),
                        "failures": r.failures,
                    }
                    for r in reports
                ],
            },
            indent=1,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    print(f"\nreport: {out}")
    # Non-zero only when the run could not check anything at all; failing
    # citations are an expected, recorded outcome, not a script error.
    return 0 if checked else 1


if __name__ == "__main__":
    raise SystemExit(main())
