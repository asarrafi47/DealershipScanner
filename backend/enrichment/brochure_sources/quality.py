"""Extraction quality gate on the derived text layer."""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass

logger = logging.getLogger(__name__)

from .identity import (
    MIN_TRIM_TEXT_CHARS,
)

@dataclass
class QualityVerdict:
    ok: bool
    reason: str = ""
    chars: int = 0
    replacement_chars: int = 0
    digits: int = 0
    letters: int = 0
    private_use_chars: int = 0
    #: How many DISTINCT Private Use Area codepoints the text uses. This, not
    #: the raw count, is what tells a remapped character class apart from a
    #: dingbat -- see :data:`MAX_PRIVATE_USE_CODEPOINTS`.
    private_use_codepoints: int = 0
    #: Longest run of consecutive PUA codepoints. Reported so a human reading
    #: the quarantine record can see *which* class was remapped: 10 consecutive
    #: is the digits, 26 is the lowercase alphabet.
    private_use_run: int = 0

    @property
    def digit_density(self) -> float:
        """Digits per 100 letters; 0.0 when there are no letters."""
        return (self.digits / self.letters * 100.0) if self.letters else 0.0


#: Digits per 100 letters, below which the text layer cannot be carrying the
#: document's numbers and is refused.
#:
#: WHY IT EXISTS. ``derived/brochure_text/2015__bmw__x1.json`` holds 170,126
#: characters and 362 digits -- 0.27 per 100 letters -- and passed every gate.
#: Re-opening ``backend/data/brochures/2015_BMW_X1_Brochure.pdf`` on 2026-08-02
#: shows why, and it is not a pdfplumber artefact: **pypdf extracts the same 362
#: digits**. The PDF's fonts map their digit glyphs into the Unicode Private Use
#: Area. Where the page prints "2.0-liter inline four-cylinder" the text layer
#: reads ``<U+EA02>.<U+EA0A>-liter inline four-cylinder``; "131-hp electric
#: motor" reads ``<U+EA01><U+EA03><U+EA01>-hp electric motor``. One PUA
#: codepoint per digit, nothing else changed. Every engine size, output,
#: capacity and dimension in the book is absent from what we can quote, while
#: the marketing prose around
#: them extracts perfectly -- so the file looks like a rich document and is
#: silently missing exactly the facts a spec is made of. This is the same class
#: of failure as the U+FFFD check above (a font whose glyphs did not decode),
#: reaching us through a codepoint range that is not U+FFFD.
#:
#: CALIBRATION, and it is measured rather than picked -- but read the scope.
#: Over the documents in ``derived/brochure_fetch_log.jsonl`` that this lane
#: fetched itself and whose text is live in the corpus -- documents acquired
#: under the current controls and therefore the closest thing we have to a
#: known-good set -- the digit density runs from 1.206
#: (``2016__volkswagen__golfsportwagen``) to 9.644 (``2022__gmc__sierra1500``),
#: median 3.486. The one exception below that floor is the defective X1, at
#: 0.270. The band between 0.270 and 1.206 is empty *in that set*, and this
#: threshold is set inside it.
#:
#: RE-MEASURED 2026-08-02 against the whole corpus we hold, which is the wider
#: set the gate actually runs on. That self-fetched set is now 166 documents
#: (2 have since been quarantined) and reproduces exactly: min 1.206, median
#: 3.486, max 9.644. The full live corpus does NOT reproduce the empty band --
#: over its 2,827 files the density runs 1.005 (``2020__mini__mini``) to 38.007,
#: median 3.335, and **64 sit inside 1.005-1.206**, the interval the paragraph
#: above calls empty. The band is a property of the 166-document sample, not of
#: the corpus, and the threshold therefore has about a 0.5% margin under the
#: nearest live document rather than the wide gap the sample suggests. Left at
#: 1.0 because moving it up would start refusing held documents on a sample that
#: does not represent them; recorded here so nobody re-derives comfort from the
#: paragraph above.
#:
#: MEASURED COST, re-run 2026-08-02 with the production function over the 2,827
#: files then live in ``derived/brochure_text``: **0 fail this check**. The 78
#: that failed when it was introduced had already been moved to
#: ``brochure_text_quarantine`` by
#: ``fetch_oem_brochures.py --quarantine-unidentified --apply``.
#:
#: WHAT IT DOES NOT CATCH, measured the same day: all 12 documents refused by
#: :data:`MAX_PRIVATE_USE_CODEPOINTS` below pass this check. The font remap that
#: motivated this constant is per-font, not per-document -- a book whose spec
#: tables use a subsetted font and whose body copy does not still carries plenty
#: of real digits (``2015__bmw__3series``: density 3.74, and every one of its 10
#: distinct PUA codepoints is a digit glyph). A density floor sees the symptom
#: only when the remapped font covers the whole document.
MIN_DIGIT_DENSITY_PER_100_LETTERS = 1.0

#: Below this many letters the density ratio is noise, so it is not applied.
#: Nothing this short survives :data:`MIN_TRIM_TEXT_CHARS` anyway; the guard
#: exists so the ratio is never computed on a handful of characters.
MIN_DIGIT_DENSITY_LETTERS = 500

#: Unicode Private Use Area ranges: the three blocks a font is allowed to map
#: its own glyphs into, which by definition carry no meaning outside that font.
#:
#: The comment that stood here said these were "not a rejection rule of their
#: own" because "135 corpus files contain at least one while extracting their
#: numbers perfectly well". Checked 2026-08-02, and it is wrong twice. The count
#: is 122 live files, not 135. And of those, 11 do NOT extract their numbers
#: perfectly well -- they are the same defect as the X1, still live, still
#: counted as coverage. Presence alone is indeed not a rejection rule; the shape
#: of what is present is, and that is :data:`MAX_PRIVATE_USE_CODEPOINTS`.
_PRIVATE_USE_AREA = re.compile(
    "[\ue000-\uf8ff\U000f0000-\U000ffffd\U00100000-\U0010fffd]"
)


def _longest_consecutive_run(codepoints: "list[int] | set[int]") -> tuple[int, int]:
    """
    ``(start, length)`` of the longest run of consecutive integers.

    ``(0, 0)`` for an empty input. Ties go to the lowest start, which only
    affects which block gets named in a rejection reason.
    """
    best_start = best_len = 0
    start = previous = None
    for value in sorted(set(codepoints)):
        if previous is None or value != previous + 1:
            start = value
        previous = value
        length = value - (start or value) + 1
        if length > best_len:
            best_start, best_len = start or value, length
    return best_start, best_len


#: Distinct Private Use Area codepoints at or above which the document is
#: refused as having had a CHARACTER CLASS remapped into the PUA.
#:
#: THIS IS THE ROOT CAUSE, and the digit-density floor above is only its most
#: visible symptom. When a PDF embeds a subsetted font, the subset may map its
#: glyphs to PUA codepoints instead of to the characters they draw. Extraction
#: then yields codepoints with no meaning, and every character that font drew is
#: gone from what we can quote. Re-opened 2026-08-02, this is what the corpus
#: actually holds:
#:
#: * ``2015__bmw__z4`` and six more BMW books use ``U+EA01..U+EA0A`` -- ten
#:   consecutive codepoints, the ten digits. "Z4" extracts as ``Z<U+EA04>``,
#:   "sDrive28i" as ``sDrive<U+EA02><U+EA08>i``, "1968" as four PUA characters.
#: * ``2018__volvo__s90`` and ``2019__volvo__s90`` use ``U+F02E..U+F039``:
#:   ASCII ``. / 0 1 2 3 4 5 6 7 8 9`` shifted by 0xF000. "39.6" extracts as
#:   ``<U+F033><U+F039><U+F02E><U+F036>``.
#: * ``2012__bentley__mulsanne`` uses ``U+F643..U+F64C`` (digits, shifted 0xF600)
#:   AND ``U+F761..U+F779`` (lowercase letters, shifted 0xF700). "the" extracts
#:   as ``<U+F774><U+F768><U+F765>``; 125 whole words in that book are gone.
#:
#: WHY DISTINCT CODEPOINTS AND NOT A COUNT. A brochure that uses a Wingdings
#: bullet or a standard/optional tick mark also lands in the PUA, thousands of
#: times over -- ``2014__audi__r8`` has 208 PUA characters and they are all one
#: codepoint, ``U+F06E``, the filled square in its equipment grid. Raw count
#: cannot separate that from the X1. What separates them is that a remapped
#: class needs one codepoint per character in the class, so it is broad; a
#: dingbat is one or two codepoints used over and over.
#:
#: CALIBRATION, over every document we hold rather than a sample, taken
#: 2026-08-02 *before* this constant swept anything: 2,827 live files in
#: ``derived/brochure_text`` plus the 555 already in
#: ``brochure_text_quarantine``. Distinct-PUA-codepoint histogram, live corpus:
#:
#: ===========  =====
#: distinct     files
#: ===========  =====
#: 0            2,705
#: 1               91
#: 2               16
#: 3                3
#: 4..7             0
#: 8                1
#: 11               5
#: 12               3
#: 17               1
#: 19               1
#: 32               1
#: ===========  =====
#:
#: The quarantined set has the same shape (0: 511, 1: 3, 2: 3, 3: 5, then
#: nothing until 9) and every one of the 166 self-fetched known-good documents
#: that holds any PUA at all holds at most 3 distinct. **The band 4-7 is empty
#: across all 3,382 documents we hold**, and this threshold is set inside it --
#: the same discipline the digit floor claimed and, unlike that one, checked
#: against the whole corpus rather than a subset.
#:
#: MEASURED COST: **12 live files failed and have been quarantined**. The
#: durable per-file record, in the repo, is
#: ``derived/brochure_text_quarantine/_why_these_are_here.json``; a fuller local
#: dump with each file's codepoint blocks, digit density and sample tokens is
#: ``workspace/brochure_pua_quarantine_2026_08_02.json`` (``workspace/`` is
#: gitignored, so that one is evidence for this run, not a committed artifact).
#: Every one of the 12
#: passes all three gates that existed before this one, including the digit
#: floor. Nothing being rendered today is lost: none of the 26 live
#: ``brochure_text_quoted`` overlays cites any of the 12. Quarantined, never
#: deleted, by ``fetch_oem_brochures.py --quarantine-unidentified --apply``;
#: the corpus is 2,827 -> 2,815 live and every remaining file passes every gate.
#:
#: CROSS-CHECK: ``2015__bmw__x1``, the document this whole class was discovered
#: from, is at 11 distinct codepoints with a run of 10. The gate built for the
#: class catches its founding member without being told about it.
#:
#: WHAT THIS DOES NOT DO. It does not clean a document with one to three
#: remapped codepoints, and 110 live files have those: ``2012__cadillac__cts``
#: prints ``DIRECT<U+F6BA>INJECTION`` (a hyphen glyph), ``2012__jaguar__xf`` prints
#: ``18<U+E044>`` for an 18-inch wheel. Those documents are not defective as
#: documents -- their prose and their numbers are intact -- but a bullet quoted
#: out of one would render a replacement box to a shopper. Refusing them here
#: would discard 110 sound books over a punctuation glyph, so they are kept and
#: the residue is refused one bullet at a time, in
#: ``brochure_extract._cited_bullet_texts``.
MAX_PRIVATE_USE_CODEPOINTS = 6


def assess_text_quality(text: str) -> QualityVerdict:
    """
    Decide whether extracted brochure text is safe to add to the corpus.

    Fails closed. Four rejections, all observed on real OEM PDFs:

    * **Decode failure.** A U+FFFD replacement character means pdfplumber could
      not map the font's glyphs, which yields strings like ``PREM<?>i<?> PLUS``.
      Any occurrence rejects the document. Every document this lane has fetched
      contains zero; ``2010__chevrolet__impala`` in the existing corpus is
      19,823 characters that include them and never names the car.
    * **A character class remapped into the Private Use Area.**
      :data:`MAX_PRIVATE_USE_CODEPOINTS` distinct PUA codepoints or more. This
      is the same decode failure as U+FFFD arriving silently: the font subset
      mapped its glyphs to codepoints that mean nothing outside it, so the text
      layer *looks* fine and is missing every character that font drew. See the
      constant for the three remap patterns the corpus actually holds and for
      why the count of PUA characters cannot be used in its place.
    * **Too thin.** Fewer than :data:`MIN_TRIM_TEXT_CHARS` characters means the
      equipment pages did not extract, leaving a disclosures page that reads
      like data -- ``2026__toyota__crown`` is 24 pages and 2,392 characters of
      nothing but footnotes.
    * **No numbers.** Fewer than :data:`MIN_DIGIT_DENSITY_PER_100_LETTERS`
      digits per 100 letters. A brochure is dense with numbers; text this
      digit-poor either lost them to an unmapped font (see the constant's
      docstring for the 2015 BMW X1, whose digits are Private Use Area
      codepoints) or never carried a spec worth quoting. Either way every claim
      that has a number in it is missing, and a document cannot be counted as
      coverage for facts it cannot supply.

    Rejecting here is cheap. A garbled spec rendered on a car page is the exact
    failure this feature already had to be walked back from once.
    """
    text = text or ""
    digits = sum(char.isdigit() for char in text)
    letters = sum(char.isalpha() for char in text)
    private_use_hits = _PRIVATE_USE_AREA.findall(text)
    private_use = len(private_use_hits)
    private_use_points = {ord(char) for char in private_use_hits}
    private_use_start, private_use_run = _longest_consecutive_run(private_use_points)
    stats = {
        "chars": len(text),
        "digits": digits,
        "letters": letters,
        "private_use_chars": private_use,
        "private_use_codepoints": len(private_use_points),
        "private_use_run": private_use_run,
    }

    replacements = text.count("�")
    if replacements:
        return QualityVerdict(
            ok=False,
            reason=(
                f"{replacements} U+FFFD replacement char(s): the PDF's glyphs did not "
                "decode, so the text is unreliable"
            ),
            replacement_chars=replacements,
            **stats,
        )
    if len(private_use_points) >= MAX_PRIVATE_USE_CODEPOINTS:
        block = ""
        if private_use_run >= 2:
            block = (
                f"; the longest block is U+{private_use_start:04X}.."
                f"U+{private_use_start + private_use_run - 1:04X}, "
                f"{private_use_run} consecutive"
            )
        return QualityVerdict(
            ok=False,
            reason=(
                f"{len(private_use_points)} distinct Private Use Area codepoint(s) over "
                f"{private_use} character(s) (limit "
                f"{MAX_PRIVATE_USE_CODEPOINTS}): a font subset mapped a whole character "
                f"class into the PUA, so every character it drew is missing from this "
                f"text layer{block}"
            ),
            **stats,
        )
    if len(text) < MIN_TRIM_TEXT_CHARS:
        return QualityVerdict(
            ok=False,
            reason=f"only {len(text)} chars of extracted text (< {MIN_TRIM_TEXT_CHARS})",
            **stats,
        )
    if letters >= MIN_DIGIT_DENSITY_LETTERS:
        density = digits / letters * 100.0
        if density < MIN_DIGIT_DENSITY_PER_100_LETTERS:
            cause = (
                f"; {private_use} Private Use Area codepoint(s) are present, so the "
                "font's digit glyphs did not decode"
                if private_use
                else ""
            )
            return QualityVerdict(
                ok=False,
                reason=(
                    f"only {digits} digit(s) against {letters} letters "
                    f"({density:.2f} per 100, under "
                    f"{MIN_DIGIT_DENSITY_PER_100_LETTERS:.2f}): a brochure's numbers "
                    f"are missing from this text layer{cause}"
                ),
                **stats,
            )
    return QualityVerdict(ok=True, **stats)
