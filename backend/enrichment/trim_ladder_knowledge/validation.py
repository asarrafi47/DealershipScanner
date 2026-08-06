"""What is not a trim name: junk tokens, spec fragments, and the key normalisers."""
from __future__ import annotations

import re


_DIMENSION_TAIL_RE = re.compile(
    r"\s*:?\s*\d+(?:\.\d+)?\s*(?:in|mm|inches)\b.*$",
    re.I,
)
_PAREN_DIMENSION_RE = re.compile(
    r"\(\s*\d+(?:\.\d+)?\s*(?:in|mm|inches)?[^)]*\)",
    re.I,
)
_COLON_SUFFIX_RE = re.compile(r"\s*:\s*.+$")
_NON_TRIM_NAME_RE = re.compile(
    r"^(?:wheelbase|trim levels?|engine|transmission|varies\b|model\b|standard\b|optional\b|and\b|or\b|the\b)",
    re.I,
)
_ENGINE_TRIM_RE = re.compile(
    r"\b(?:\d+\.\d+\s*l\b|\d+\s*l\b|v\d|v-\d|turbo|diesel|cummins|hemi|pentastar|"
    r"ecoboost|hybrid|electric|i\d|tfsi|tsi|dohc|sohc|ohv|hp\b|kw\b|nm\b|"
    r"cylinder|liter|litre)\b",
    re.I,
)
_TRANSMISSION_TRIM_RE = re.compile(
    r"\b(?:\d+[\s-]*speed|automatic|manual|cvt|torqueflite|aisin|zf\b|"
    r"transmission|dual[\s-]*clutch|dct|e-cvt)\b",
    re.I,
)
_FEATURE_FRAGMENT_RE = re.compile(
    r"\b(?:grille|headlamp|head\s*lamp|taillamp|tail\s*lamps?|bumper|fender|"
    r"seat|seats|vinyl|cloth|decal|mirror|wiper|exhaust|muffler|spoiler|"
    r"running\s*boards?|step\s*bars?|plastic|chrome|aluminum|steel\s*wheel|"
    r"incandescent|halogen|projector)\b",
    re.I,
)
_SENTENCE_FRAGMENT_RE = re.compile(
    r"^(?:a|an|and|or|the|with|for|from|includes?|featuring)\s+",
    re.I,
)
_JUNK_TRIM_TOKENS = frozenset(
    {
        "fwd",
        "4wd",
        "2wd",
        "awd",
        "rwd",
        "4x4",
        "4x2",
        "hybrid",
        "electric",
        "gasoline",
        "diesel",
    }
)
# EPA / wiki CSV section headers and prose fragments — never trim levels.
_JUNK_TRIM_EXACT = frozenset(
    {
        "an",
        "and",
        "or",
        "the",
        "na",
        "n/a",
        "bmw",
        "none",
        "unknown",
        "package",
        "packages",
        "options",
        "option",
        "badges",
        "badge",
        "wheels",
        "wheel",
        "tires",
        "tire",
        "engines",
        "engine",
        "transmission",
        "transmissions",
        "layout",
        "body",
        "bodym",
        "body-m",
        "specialinteriors",
        "specialinterior",
        "interiors",
        "exterior",
        "upholstery",
        "paint",
        "accessories",
        "medseats",
        "seats",
        "note",
        "includes",
        "offered",
        "available",
        "discontinued",
    }
)
_JUNK_TRIM_SUBSTR_RE = re.compile(
    r"\b(?:internet\s+movie|imcdb|movie\s+cars\s+database|authority\s+control|"
    r"wikipedia|wikidata|carbuzz|discontinued|database|databases|"
    r"bronze\s+package|co-?pilot|assist\+|sunroof|innovative\s+technologies|"
    r"mazda-?based|served\s+as|accessories\s+were|and\s+options|"
    r"now\s+available|had\s+an\s+available|med\s+seats|special\s+interiors?|"
    r"body-?m\b|trim\s+levels?)\b",
    re.I,
)
_SHORT_PERFORMANCE_TRIM_RE = re.compile(
    r"^(?:[A-Z]{1,4}\d{0,2}|[A-Z]/[A-Z]|GT\d*|ST\d*|RS|SS|SE|LE|LX|EX|LT|LS|XL|XLT|"
    r"SRT|R/T|TRX|TRD|FX\d*|STX|GTD|HFE)(?:\s+[A-Z][a-z]+){0,2}$"
)


def _norm_make_key(make: str) -> str:
    t = re.sub(r"[^a-z0-9]+", "", (make or "").lower())
    if t in ("chevrolet", "chevy"):
        return "chevrolet"
    if t in ("mercedesbenz", "mercedes"):
        return "mercedesbenz"
    return t


def _norm_model_key(model: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", "", (model or "").lower())
