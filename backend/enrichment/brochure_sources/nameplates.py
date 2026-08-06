"""Derived nameplates: is the document about the variant or about the nameplate?"""
from __future__ import annotations

import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)

from .identity import (
    IDENTITY_MAX_CONTEXT_ALNUM,
    _declaration_context,
    find_model_name,
)
from .naming import (
    _make_token,
    model_token_variants,
)

# --------------------------------------------------------------------------
# Derived nameplates: is the document about the variant or about the nameplate?
# --------------------------------------------------------------------------
#
# WHAT WENT WRONG. ``backend/data/brochures/2018_BMW_3_Series_Brochure.pdf`` is
# the **M3** brochure. Re-opened with pdfplumber on 2026-08-02, all nine of its
# pages carry the running header ``BMW M3 SEDAN EXTERIOR COLORS UPHOLSTERY
# INTERIOR TRIMS WHEELS / TIRES PACKAGES TECHNICAL DATA``, and the only line in
# the document that says "3 Series" is body copy on page 2: "The 3 Series is the
# best-selling BMW sedan. Add M to the equation and its personality transforms".
# That one line is a short line block, so :func:`verify_document_identity` -- a
# NAMING test, as its own docstring says -- passed it, and the file has been
# counted as 3 Series coverage ever since. The M3 is a separately stocked model
# in this fleet (49 active cars on 2026-08-02), so this is the wrong vehicle,
# not a trim of the right one.
#
# WHY NO EXISTING GATE SEES IT. Every other variant control keys off a token
# that is *present* in the name: ``_VARIANT_QUALIFIERS`` compares the qualifiers
# on the link's filename with those on the requested model, and
# :func:`find_model_name` is word-bounded, so it catches ``Rogue`` vs ``Rogue
# Plug-In Hybrid``. "M3" contains no substring of "3 Series" and the file is
# named ``3_Series``: there is nothing for either control to match on.
#
# WHAT IS ENFORCED NOW. :func:`assess_nameplate_dominance` counts PAGES, not
# occurrences, and asks whether the base nameplate or a derived one is the
# document's subject. Calibration, measured 2026-08-02 over all 2,986 files then
# live in ``derived/brochure_text``: 47 of them belong to a make/model in
# :data:`DERIVED_NAMEPLATES` and declare a derived nameplate on at least one
# page. Ranked by ``derived_pages / base_pages`` they run 0.20, 0.25, ... 1.25,
# 1.33, 1.40 -- and then one document at **8.00**, which is the M3 file. The 46
# at or below 1.40 are genuine combined books (Audi ships one A4/S4, A5/S5,
# TT/TTS volume: page 1 of ``2010__audi__a4`` reads "A4 | S4 Premium Plus" and
# its spec grid has an A4 column and an S4 column), so they really are about the
# base nameplate and are admitted unchanged.
# :data:`VARIANT_PAGE_DOMINANCE_RATIO` is set in the middle of the empty band
# between 1.40 and 8.00.
#
# A second document of the same kind sits in ``brochure_text_quarantine``:
# ``2018__bmw__4series`` declares M4 on 13 pages and the 4 Series on none. An
# earlier lane had already quarantined it, for "document text never mentions the
# model", so it is not a new catch -- but it is independent evidence that the
# M-car family is a recurring filing error rather than one bad file.

# Occurrence counting was tried first and does not separate the two cases: the
# M3 book names "M3" 8 times and "3 Series" once, and ``2010__audi__a4`` names
# "S4" 28 times and "A4" 23 -- and that second one is a genuine A4 book.
#
# A general "is some other nameplate of this make more prominent?" test was also
# tried, using every model this fleet stocks per make. It flagged 369 of 2,986
# documents and was abandoned as unusable: "IS" matched the English word in
# every Lexus document, "SS" every Chevrolet one, "Jeep" every Jeep one, and
# trim-level nameplates the fleet stocks separately (``330i``, ``535i``,
# ``f350``) out-declared the model whose brochure they were in. That is why the
# table below is narrow and hand-checked rather than derived automatically.
#
# WHAT THIS DOES NOT DO, stated because the name of the check would otherwise
# over-promise exactly as ``verify_document_identity`` did. It refuses a
# document whose subject is a derived nameplate. It does NOT resolve which
# column of a combined book a given line belongs to, so the 46 combined books
# still carry base and variant equipment in one text file; anything quoting them
# is quoting a document that describes two vehicles. That is a real, unclosed
# attribution risk, recorded here rather than argued away.
#
# RE-CHECKED 2026-08-02 by re-opening the document, not by asking the register.
# ``backend/data/brochures/2018_BMW_3_Series_Brochure.pdf`` still parses to nine
# pages, eight of which carry the ``M3 SEDAN`` running header, with "3 Series"
# printed exactly once. ``assess_nameplate_dominance`` refuses it (M3 on 8 pages,
# 3 Series on 1, ratio 8.0) and ``derived/brochure_text/2018__bmw__3series.json``
# is in ``brochure_text_quarantine``. The overlay
# ``trim_adds_by_year/2018__bmw__3series.json`` is still live but is a
# ``brochure_llm`` file that ``LADDER_BULLET_STORES`` does not admit, so it puts
# nothing on a page; the same is true of ``2018__bmw__4series``, whose LLM
# overlay describes the M4 Coupe and M4 Convertible under the 4 Series key.
#
# CAN A PERFORMANCE VARIANT STILL BE FILED UNDER ITS BASE NAMEPLATE? YES, three
# ways, all measured rather than supposed:
#
# 1. THE TABLE REACHES 4% OF THE CORPUS. ``DERIVED_NAMEPLATES`` lists two makes.
#    The live corpus holds 47 makes and 2,815 documents, of which **123 are in a
#    make this table covers at all**; for the other 2,692 the check returns
#    ``ok=True`` without looking at anything. Non-containing performance
#    nameplates outside the table are easy to name -- Golf/GTI, Impreza/WRX,
#    C-Class/C63, F-150/Raptor, Grand Cherokee/Trackhawk, 1500/TRX. Running this
#    same page-dominance arithmetic over a hand-written candidate table for
#    those families flagged **0 of the 213 live documents** it applies to, so no
#    held document is currently mis-filed that way -- but that is a fact about
#    today's corpus, not about the control. Adding a family stays a reviewed
#    edit for the reason given below the table.
# 2. A SHORT DOCUMENT SLIPS UNDER :data:`VARIANT_MIN_DERIVED_PAGES`. Truncating
#    the real M3 book to its first page and re-running the check returns
#    ``ok=True``: one declaring page is not a subject. A single-page M3 spec
#    sheet filed as a 3 Series would be admitted. Two pages is already enough to
#    refuse it, so the exposure is one-page documents.
# 3. A COMBINED BOOK IS ADMITTED WHOLE, as the paragraph above says. The base
#    nameplate holding its own keeps the ratio under
#    :data:`VARIANT_PAGE_DOMINANCE_RATIO`, and the M-car or S-car equipment in
#    that book is then quotable against the base car.

#: Nameplates that are a DIFFERENT vehicle from the base nameplate and whose
#: name contains no part of it, so no word-boundary or filename-token control
#: can see them. Make token -> base model token -> derived model tokens.
#:
#: Deliberately narrow. Only two families are listed, and both were checked
#: against active inventory on 2026-08-02 -- every derived token below is a
#: model this fleet stocks *separately* from its base (BMW M2/M3/M4/M5/M8; Audi
#: S3/S4/S5/S8, SQ5/SQ7/SQ8, RS 3/RS 5/RS 6/RS 7/RS Q8). Families where the
#: derived name *contains* the base name are deliberately absent because the
#: existing controls already see them: ``X5 M`` matches ``x5``, ``CT5-V``
#: matches ``ct5``, ``Civic Type R`` matches ``civic``, ``911 Turbo`` matches
#: ``911``, and ``Rogue NISMO`` is caught by ``_VARIANT_QUALIFIERS``.
#:
#: Adding a make here is a reviewed edit, like :data:`OFFICIAL_HOSTS`. Guessing
#: at a family we have not checked against inventory would be inventing a
#: control, and a control that fires on invented facts is worse than none.
DERIVED_NAMEPLATES: dict[str, dict[str, tuple[str, ...]]] = {
    "bmw": {
        "1series": ("m1",),
        "2series": ("m2",),
        "3series": ("m3",),
        "4series": ("m4",),
        "5series": ("m5",),
        "6series": ("m6",),
        "7series": ("m7",),
        "8series": ("m8",),
    },
    "audi": {
        "a3": ("s3", "rs3"),
        "a4": ("s4", "rs4"),
        "a5": ("s5", "rs5"),
        "a6": ("s6", "rs6"),
        "a7": ("s7", "rs7"),
        "a8": ("s8", "rs8"),
        "q3": ("sq3", "rsq3"),
        "q5": ("sq5", "rsq5"),
        "q7": ("sq7", "rsq7"),
        "q8": ("sq8", "rsq8"),
        "tt": ("tts", "ttrs"),
    },
}

#: ``derived_pages / base_pages`` at or above which the document is refused as
#: being about the derived vehicle. See the calibration note above: the corpus
#: puts every genuine combined book at or below 1.40 and the one wrong-vehicle
#: document at 8.00, so 2.0 sits inside an empty band rather than on a guess.
VARIANT_PAGE_DOMINANCE_RATIO = 2.0

#: A derived nameplate must be declared on at least this many pages before the
#: ratio is consulted. One stray mention -- a comparison table, a footnote, a
#: "see also" -- is not a document's subject, and on a short document it would
#: otherwise be enough to refuse a correct filing.
VARIANT_MIN_DERIVED_PAGES = 2


@dataclass
class SubjectVerdict:
    """Which vehicle the document is *about*, as opposed to which it names."""

    ok: bool
    reason: str = ""
    base_pages: int = 0
    derived_pages: int = 0
    derived_name: str = ""


def derived_nameplates_for(make: str, model: str) -> frozenset[str]:
    """Nameplates derived from this vehicle's, per :data:`DERIVED_NAMEPLATES`."""
    family = DERIVED_NAMEPLATES.get(_make_token(make))
    if not family:
        return frozenset()
    tokens = {v for v in model_token_variants(make, model) if len(v) > 1}
    out: set[str] = set()
    for base, derived in family.items():
        if base in tokens:
            out.update(derived)
    return frozenset(out - tokens)


def declares_nameplate(page_text: str, token: str) -> bool:
    """
    True if ``page_text`` names ``token`` as a declaration rather than as prose.

    Same two tests :func:`verify_document_identity` applies -- whole-name match
    plus a line block within :data:`IDENTITY_MAX_CONTEXT_ALNUM` -- applied one
    page at a time, which is what makes a running header count once per page
    instead of once per document.
    """
    text = page_text or ""
    if not text:
        return False
    return any(
        _declaration_context(text, match) <= IDENTITY_MAX_CONTEXT_ALNUM
        for match in find_model_name(text, token)
    )


def assess_nameplate_dominance(
    page_texts: "list[str] | tuple[str, ...]", make: str, model: str
) -> SubjectVerdict:
    """
    Refuse a document whose subject is a nameplate *derived* from this one.

    Page-level on purpose. Occurrence counts do not separate the two cases: the
    M3 book names "M3" 8 times and "3 Series" once, but ``2010__audi__a4`` names
    "S4" 28 times and "A4" 23 -- and that one is a genuine A4 book. What does
    separate them is how much of the document each nameplate presides over,
    which is a per-page question.

    ``ok=True`` when the make/model has no known derived nameplate, when none is
    declared on enough pages (:data:`VARIANT_MIN_DERIVED_PAGES`), or when the
    base nameplate holds its own (:data:`VARIANT_PAGE_DOMINANCE_RATIO`).

    Fails closed: a base nameplate declared on **no** page while a derived one
    presides over several is refused, not treated as a division by zero.
    """
    derived = derived_nameplates_for(make, model)
    if not derived:
        return SubjectVerdict(ok=True)

    pages = [p or "" for p in page_texts or []]
    if not any(p.strip() for p in pages):
        return SubjectVerdict(ok=True, reason="no page text to attribute")

    base_tokens = sorted(v for v in model_token_variants(make, model) if len(v) > 1)
    base_pages = sum(
        1 for page in pages if any(declares_nameplate(page, t) for t in base_tokens)
    )

    worst: tuple[int, str] = (0, "")
    for token in sorted(derived):
        count = sum(1 for page in pages if declares_nameplate(page, token))
        if count > worst[0]:
            worst = (count, token)
    derived_pages, derived_name = worst

    if derived_pages < VARIANT_MIN_DERIVED_PAGES:
        return SubjectVerdict(
            ok=True, base_pages=base_pages, derived_pages=derived_pages,
            derived_name=derived_name,
        )

    ratio = derived_pages / base_pages if base_pages else float("inf")
    if ratio < VARIANT_PAGE_DOMINANCE_RATIO:
        return SubjectVerdict(
            ok=True, base_pages=base_pages, derived_pages=derived_pages,
            derived_name=derived_name,
        )
    return SubjectVerdict(
        ok=False,
        reason=(
            f"the document's subject is {derived_name.upper()}, not this vehicle: "
            f"{derived_name.upper()} is declared on {derived_pages} of {len(pages)} "
            f"page(s) and the requested nameplate "
            f"({'/'.join(base_tokens) or model}) on {base_pages}"
        ),
        base_pages=base_pages,
        derived_pages=derived_pages,
        derived_name=derived_name,
    )
