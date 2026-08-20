"""
One spelling per make and model, because dealer feeds cannot agree on any of them.

Every field a dealer types by hand arrives several ways. This corpus holds ``RAM`` and
``Ram`` (2,324 rows on the minority spelling), ``LEXUS`` and ``Lexus`` (3,698), a bare
``Mercedes`` and a bare ``Land``, plus 218 model families that differ only in case or
punctuation -- ``Camry``/``CAMRY``, ``RAV4``/``Rav4``/``rav 4``, ``F-150``/``F150``.

Left alone this splits every kind of evidence keyed on those fields: the option price book
counted a Ram accessory seen on 3 ``RAM`` cars and 2 ``Ram`` cars as corroborated by
neither, and browse facets list the same marque twice.

What this module deliberately does NOT do
-----------------------------------------
It does not strip words that look like trim. That instinct is wrong here and would destroy
real distinctions:

* ``CR-V Hybrid`` is a distinct Honda model, not a CR-V with a trim word attached
* ``Bronco Sport`` is a different vehicle from a ``Bronco``
* ``RAV4 Plug-in Hybrid`` is a different car from a ``RAV4``

12,211 cars carry a model containing such a word, and merging them would be a much worse
error than leaving them alone. Only genuinely redundant BODY-STYLE suffixes are candidates
(``Accord Sedan`` -> ``Accord``), and even those are left to an explicit list rather than a
pattern, because ``Civic Coupe`` and ``Civic Sedan`` really were different cars in some
years.

The rule is: merge only what is unambiguously the SAME string typed differently. Anything
that might be a different vehicle stays separate.
"""

from __future__ import annotations

import re

# Marque spellings the feeds get wrong, keyed on the name with all non-letters removed.
#
# Two problems, one map. Case and punctuation ("RAM"/"Ram", "Mercedes-benz") fall out of
# the squash. ALIASES do not -- no amount of case folding joins "Mercedes" to
# "Mercedes-Benz" -- so shorthands are listed explicitly, including ones this corpus has
# not produced yet. An unused entry costs nothing; a missing one loses evidence silently.
_MAKE_CANON: dict[str, str] = {
    # stylised by the manufacturer
    "bmw": "BMW", "gmc": "GMC", "mini": "MINI", "infiniti": "INFINITI",
    "ineos": "INEOS", "brightdrop": "BrightDrop", "mg": "MG",
    # intercaps -- a naive title-case turns these into "Mclaren"/"Rawmaxx", which is
    # exactly what an earlier version of this did to 31 McLarens
    "mclaren": "McLaren", "rawmaxx": "RawMaxx", "canam": "Can-Am",
    # hyphens and spaces
    "mercedesbenz": "Mercedes-Benz", "landrover": "Land Rover",
    "alfaromeo": "Alfa Romeo", "rollsroyce": "Rolls-Royce",
    "astonmartin": "Aston Martin", "harleydavidson": "Harley-Davidson",
    "harleydavid": "Harley-Davidson",
    # aliases and truncations
    "mercedes": "Mercedes-Benz", "merc": "Mercedes-Benz", "benz": "Mercedes-Benz",
    "mb": "Mercedes-Benz",
    "chevy": "Chevrolet", "chev": "Chevrolet",
    "vw": "Volkswagen", "volkswagon": "Volkswagen",
    "land": "Land Rover", "range": "Land Rover", "rangerover": "Land Rover",
    "ram": "Ram", "dodgeram": "Ram",
    "vette": "Chevrolet", "caddy": "Cadillac",
}


def canonical_make(make: str) -> str:
    """One spelling per marque. Safe to call repeatedly; the result is a fixed point."""
    raw = (make or "").strip()
    if not raw:
        return raw
    squashed = re.sub(r"[^a-z]", "", raw.lower())
    if squashed in _MAKE_CANON:
        return _MAKE_CANON[squashed]
    # Already mixed case? The feed styled it deliberately -- leave it. Skipping this test
    # is how "McLaren" became "Mclaren".
    if raw != raw.upper() and raw != raw.lower():
        return raw
    return " ".join(
        "-".join(part.capitalize() for part in word.split("-")) for word in raw.split()
    )


def model_key(make: str, model: str) -> tuple[str, str]:
    """
    Identity of a model for MERGING purposes only.

    Collapses case, spacing and punctuation so ``F-150``/``F150``, ``RAV4``/``Rav4``/
    ``rav 4`` and ``Camry``/``CAMRY`` land on one key. Never used for display -- it is
    deliberately lossy, and the human-readable spelling is chosen separately by
    :func:`preferred_spelling`.
    """
    return (
        canonical_make(make).lower(),
        re.sub(r"[^a-z0-9]", "", (model or "").lower()),
    )


def preferred_spelling(variants: dict[str, int]) -> str:
    """
    Pick the display spelling for a family of variants, given each one's row count.

    Majority wins, because the feeds that publish the most cars are the ones that bothered
    to type it properly -- with two corrections drawn from real failures here:

    * an ALL-CAPS or all-lower spelling never beats a mixed-case one, however common.
      ``LEXUS`` outnumbered ``Lexus`` 2,656 to 2,063 and is still not how anyone writes it.
    * a spelling containing a digit-letter break like ``F-150`` beats ``F150``; the
      hyphenated form is the manufacturer's.
    """
    if not variants:
        return ""

    def rank(item: tuple[str, int]) -> tuple:
        name, count = item
        mixed = name != name.upper() and name != name.lower()
        hyphenated = bool(re.search(r"[A-Za-z]-\d|\d-[A-Za-z]", name))
        return (mixed, hyphenated, count)

    return max(variants.items(), key=rank)[0]
