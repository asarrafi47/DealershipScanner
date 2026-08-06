"""Exterior-color extraction from listing text.

Vendored replacement for the legacy ``extract_keffer_colors`` script, which
``enrich_from_dictionary`` imported behind a ``try/except ImportError`` and
which no longer exists anywhere in the repo — so the fallback had been ``None``
on every machine and the description-based color fill (plus its three tests in
``test_enrich_dictionary_heuristics.py``) was permanently dead. This module
restores the function under a path that is always importable.

Heuristics, in priority order:

1. A labeled field — ``Exterior color: Agate Black Metallic`` /
   ``Paint: Pearl White Tri-Coat``. The label is the dealer saying which string
   is the paint, so it wins outright.
2. An unlabeled compound color phrase — up to two capitalized OEM modifier
   words in front of a base color noun, plus finish suffixes
   (``Magnetic Gray Metallic``). The longest phrase wins so a marketing name is
   preferred over the bare base color it contains ("Pearl White Tri-Coat", not
   "White").

Returns ``None`` when nothing color-like is found; never invents a value.
"""

from __future__ import annotations

import re

__all__ = ["extract_color_from_description"]

_BASE_COLOR = (
    r"(?:Black|White|Gray|Grey|Silver|Blue|Red|Green|Brown|Beige|Tan|Gold|"
    r"Orange|Yellow|Purple|Bronze|Burgundy|Maroon|Charcoal|Graphite|Pearl|"
    r"Ivory|Cream|Champagne|Granite|Ruby|Sapphire|Onyx)"
)
_FINISH = (
    r"(?:Metallic|Pearl|Pearlcoat|Tri-?Coat|Clearcoat|Clear\s?Coat|Mica|"
    r"Tintcoat|Tinted\s?Clearcoat)"
)

_LABELED_RE = re.compile(
    r"(?:exterior\s+colou?r|paint(?:\s+colou?r)?|ext\.?\s+colou?r)"
    r"\s*[:\-]\s*(?P<value>[^\n\r]{3,60})",
    re.I,
)

# "Magnetic Gray Metallic", "Pearl White Tri-Coat", "Agate Black" — one leading
# capitalized modifier at most (two would swallow the model name in
# "2023 Explorer Magnetic Gray Metallic"), a base color noun, then any number
# of finish words.
_COMPOUND_RE = re.compile(
    rf"\b(?P<value>(?:[A-Z][a-z]+\s+)?{_BASE_COLOR}(?:\s+{_FINISH})*)\b"
)

# Words that read like a modifier but are vehicle text, not paint.
_NOT_A_MODIFIER = frozenset(
    {"interior", "leather", "cloth", "seats", "seat", "trim", "wheels", "wheel"}
)


def _clean(value: str) -> str:
    value = re.sub(r"\s+", " ", value).strip(" .,;:-")
    # A labeled value can run into the next sentence; keep the leading
    # color-cased words only.
    words = []
    for word in value.split(" "):
        if not re.match(r"^[A-Z][A-Za-z\-]*$", word):
            break
        words.append(word)
    return " ".join(words) if words else value


def extract_color_from_description(text: str | None) -> str | None:
    """Best-effort exterior color phrase from free listing text, or ``None``."""
    blob = str(text or "")
    if not blob.strip():
        return None

    labeled = _LABELED_RE.search(blob)
    if labeled:
        value = _clean(labeled.group("value"))
        if len(value) >= 3:
            return value

    best: str | None = None
    for match in _COMPOUND_RE.finditer(blob):
        value = match.group("value").strip()
        first = value.split(" ", 1)[0].lower()
        if first in _NOT_A_MODIFIER:
            continue
        if best is None or len(value.split()) > len(best.split()):
            best = value
    return best
