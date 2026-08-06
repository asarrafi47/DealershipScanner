"""Does the document declare the model it claims to be about?"""
from __future__ import annotations

import logging
import re

logger = logging.getLogger(__name__)

from .naming import (
    model_token_variants,
)

# --------------------------------------------------------------------------
# Extraction quality gate
# --------------------------------------------------------------------------

#: Below this many characters a document cannot contain a usable trim walk.
#: Measured 2026-07-31 over the whole captured text of documents this lane
#: fetched: Mazda spec decks 47,187-107,654 chars, Toyota/Nissan brochures
#: 31,626-65,675. Across the 3,210 files already in ``derived/brochure_text``,
#: 116 fall below this line and 33 have no text layer at all.
MIN_TRIM_TEXT_CHARS = 2000


#: An occurrence of the model name whose enclosing line block runs longer than
#: this many alphanumeric characters is prose, not a declaration. 300 is about
#: four printed lines: no cover title, section heading, spec-grid row or photo
#: caption in this corpus reaches it, and every footnote block exceeds it.
#: Calibrated 2026-07-31 over the 2,890 ``derived/brochure_text`` documents that
#: contain the name at all -- 2,885 name it somewhere under 300, and all 5 that
#: do not name it *only* inside a trademark notice, an EPA-estimate footnote or
#: a vehicle-class disclaimer.
IDENTITY_MAX_CONTEXT_ALNUM = 300


#: Longest run of non-alphanumerics allowed *between* two characters of a model
#: name. Covers every real spelling seen in this corpus -- ``R O G U E`` (one
#: space), ``CX-5`` (one hyphen), ``4\nSERIES`` (one newline), ``Plug-\nIn``
#: (two) -- while stopping a two-character token from stitching itself together
#: out of two unrelated words: with an unbounded run, ``SC`` matched the
#: "S (C" of "ESTIMATES (CITY/HIGHWAY)".
MAX_NAME_SEPARATOR_RUN = 3


def model_name_pattern(token: str) -> re.Pattern[str]:
    """
    Loose regex for ``token``, tolerating the separators OEM PDFs put inside a
    model name. Callers must still check word boundaries -- see
    :func:`find_model_name`, which is what production uses.

    Up to :data:`MAX_NAME_SEPARATOR_RUN` non-alphanumerics may sit between the
    token's characters, so ``R O G U E`` (cover letter-spacing), ``CX-5``,
    ``4\\nSERIES`` and a name hyphenated across a line break all match. A run of
    *alphanumerics* always breaks it, so ``MX 5`` matches ``mx5`` while
    ``MAX 5`` does not.
    """
    body = rf"[^a-z0-9]{{0,{MAX_NAME_SEPARATOR_RUN}}}".join(
        re.escape(char) for char in token
    )
    return re.compile(body, re.I)


def _starts_a_word(text: str, index: int) -> bool:
    """True if ``text[index]`` begins a word."""
    if index <= 0:
        return True
    previous = text[index - 1]
    if not previous.isalnum():
        return True
    # pdfplumber routinely glues a caption onto the name before it:
    # "Mazda3Specifications", "CX-5Specifications", "11AVALANCHE-0211". The new
    # word is marked only by the case change, so treat that as a boundary.
    return text[index].isupper() and (previous.islower() or previous.isdigit())


def _ends_a_word(text: str, index: int) -> bool:
    """True if the character before ``text[index]`` ends a word."""
    if index >= len(text):
        return True
    following = text[index]
    if not following.isalnum():
        return True
    return following.isupper() and (text[index - 1].islower() or text[index - 1].isdigit())


def find_model_name(text: str, token: str):
    """
    Yield every occurrence of ``token`` in ``text`` that is a **whole name**.

    This is the part the old check did not do at all: it stripped every
    non-alphanumeric from the whole document, joined it into one string and
    asked whether the token appeared anywhere in it. That made ``gx`` match
    ``GX460``, ``m`` match "Infiniti Mobile", and ``rogue`` match the middle of
    a longer nameplate.

    A word boundary is a non-alphanumeric character, the start/end of the text,
    or a lowercase/digit-to-uppercase transition (the glued-caption case above).
    Note what that last allowance costs: ``Rogue`` would still match a
    hypothetical ``RogueSport``. Real OEM copy spaces those names, and a
    space-separated sibling was never distinguishable here -- that is what the
    variant-qualifier symmetry in :func:`score_document_link` is for.
    """
    for match in model_name_pattern(token).finditer(text):
        if _starts_a_word(text, match.start()) and _ends_a_word(text, match.end()):
            yield match


def _declaration_context(text: str, match: re.Match[str]) -> int:
    """Alphanumeric length of the line block a match sits in."""
    start = text.rfind("\n", 0, match.start()) + 1
    end = text.find("\n", match.end())
    if end == -1:
        end = len(text)
    return sum(char.isalnum() for char in text[start:end])


def verify_document_identity(
    text: str, year: int, make: str, model: str, *, require_year: bool = False
) -> tuple[bool, str]:
    """
    Check that the document **prints this model's name somewhere that reads as a
    declaration** rather than as incidental prose.

    Be precise about what this establishes, because the name of this function
    over-promises and the previous implementation over-promised further -- it
    stripped every non-alphanumeric from the whole document, joined it into one
    string and asked whether the model token appeared anywhere in it. Under that
    test ``rogue`` matched inside ``roguesport``, and a name occurring once in a
    9,000-character legal footnote counted the same as a name on the cover.
    That is how ``2026__nissan__roguepluginhybrid`` entered the corpus: the
    document is the base Rogue brochure, and its only occurrence of "Rogue
    Plug-In Hybrid" is footnote 30 on the disclaimer page, hyphenated across a
    line break.

    What is enforced now:

    1. the model name matches as a **whole name** -- see
       :func:`find_model_name`, which is word-bounded and separator tolerant;
    2. at least one match sits in a line block of at most
       :data:`IDENTITY_MAX_CONTEXT_ALNUM` alphanumeric characters, i.e. a title,
       heading, caption or table row rather than a paragraph of disclaimers;
    3. ``require_year``: the printed model year, for links whose URL carried no
       year to score against.

    What is **not** established: that the document's subject is this vehicle. A
    full-line-up brochure that gives one model a heading passes, and so does a
    single-model brochure that names a sibling in a comparison table. This
    narrows a wrong-vehicle attribution; it does not eliminate it. The controls
    that do more work are the per-make host allowlist and the variant-qualifier
    symmetry in :func:`score_document_link`.

    ``require_year`` is not the default: measured over the 3,210 files in
    ``derived/brochure_text`` on 2026-07-31, 1,044 (33%) never print their model
    year in the text layer while still carrying real equipment text.

    Returns ``(ok, reason)``; fails closed on empty text and on a model name too
    short to identify anything.
    """
    if not (text or "").strip():
        return False, "no extracted text to verify identity against"

    if require_year:
        compact = re.sub(r"[^a-z0-9]+", "", text.lower())
        if str(year) not in compact and f"{year % 100}my" not in compact:
            return False, f"document text never mentions the model year {year}"

    variants = sorted(v for v in model_token_variants(make, model) if len(v) > 1)
    if not variants:
        return False, (
            f"no model name long enough to identify a document (make={make!r} "
            f"model={model!r})"
        )

    best_context: int | None = None
    for variant in variants:
        for match in find_model_name(text, variant):
            context = _declaration_context(text, match)
            if context <= IDENTITY_MAX_CONTEXT_ALNUM:
                return True, ""
            if best_context is None or context < best_context:
                best_context = context

    names = "/".join(variants)
    if best_context is None:
        return False, f"document text never mentions the model ({names})"
    return False, (
        f"document text names the model ({names}) only inside running prose -- "
        f"the shortest line block holding it is {best_context} characters, over "
        f"the {IDENTITY_MAX_CONTEXT_ALNUM}-character declaration limit"
    )
