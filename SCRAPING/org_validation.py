"""
Strict validation and normalization for dealer / automotive group names.
"""
from __future__ import annotations

import re
from typing import Iterable

# Hard reject if substring appears (case-insensitive)
NEGATIVE_SUBSTRINGS = (
    "privacy policy",
    "terms of use",
    "terms of service",
    "terms of service",
    "cookie policy",
    "highlight the best aspects",
    "charlotte community",
    "the charlotte community",
    "posting updates and changes",
    "all rights reserved",
    "schedule service",
    "view inventory",
    "parts center",
    "model lineup",
    "contact us",
    "reviews",
    "specials",
    "payment calculator",
    "this privacy policy",
    "the terms of use",
    "these terms",
    "our job to",
    "your information",
    "we collect",
    "we may",
    "you agree",
)

# Entity / org shape signals
ENTITY_WORDS = frozenset(
    {
        "automotive",
        "auto",
        "group",
        "motors",
        "motorcars",
        "holdings",
        "family",
        "corporation",
        "corp",
        "inc",
        "llc",
        "lp",
        "company",
        "dealerships",
        "dealership",
        "dealers",
        "enterprises",
    }
)

# Known public / group families (substring match boosts plausibility)
KNOWN_GROUP_FAMILIES = (
    "hendrick",
    "penske",
    "lithia",
    "sonic",
    "autonation",
    "group 1",
    "group1",
    "asbury",
    "tuttle-click",
    "tuttle click",
    "fletcher jones",
    "morgan",
    "holman",
    "galpin",
    "sewell",
    "bermuda",
    "ken garff",
    "larry h miller",
    "priority",
    "flow",
)

# Demote-only starts: reject sentence fragments / policy junk.
# Leading articles (the, a, an) are stripped before this check — see strip_leading_articles.
GENERIC_START_WORDS = frozenset(
    {
        "this",
        "these",
        "that",
        "our",
        "your",
        "we",
        "you",
        "their",
        "its",
    }
)

# Common English stopwords — if too many relative to length, reject
_STOPWORDS = frozenset(
    """
    the a an and or but if in on at to for of as is was are were be been being
    with by from that this these those we you our your they their its not no
    so such any all each both few more most other some such than too very can
    will just also only own same than when while which who whom how about into
    through during before after above below between under again further then once
    here there when where why how both each few many other some such
    """.split()
)

def strip_leading_articles(s: str) -> str:
    """Remove leading English articles so 'the Tuttle-Click automotive group' can validate as an org name."""
    t = s.strip()
    while True:
        low = t.lower()
        for art in ("the ", "a ", "an "):
            if low.startswith(art):
                t = t[len(art) :].lstrip()
                break
        else:
            break
    return t.strip()


def has_negative_substring(s: str) -> bool:
    sl = s.lower()
    return any(n in sl for n in NEGATIVE_SUBSTRINGS)


def _word_count(s: str) -> int:
    return len(re.findall(r"[A-Za-z0-9]+", s))


def _stopword_ratio(s: str) -> float:
    words = re.findall(r"[A-Za-z]+", s.lower())
    if not words:
        return 1.0
    sw = sum(1 for w in words if w in _STOPWORDS)
    return sw / len(words)


def has_entity_word(s: str) -> bool:
    tokens = re.findall(r"[A-Za-z]+", s.lower())
    return any(t in ENTITY_WORDS for t in tokens)


def matches_known_family(s: str) -> bool:
    sl = s.lower()
    return any(k in sl for k in KNOWN_GROUP_FAMILIES)


def is_title_like(s: str) -> bool:
    """Most significant words should be capitalized or known acronyms."""
    parts = re.findall(r"[A-Za-z][a-z]+|[A-Z]{2,}", s)
    if len(parts) < 2:
        return bool(re.match(r"^[A-Z][a-z]+(?:\s+[A-Z][a-z]+)+", s.strip()))
    caps = sum(1 for p in parts if p[:1].isupper() and len(p) > 1)
    return caps >= max(1, len(parts) // 2)


def is_plausible_org_name(raw: str) -> tuple[bool, str]:
    """
    Returns (ok, reason_if_not).

    A candidate should look like an organization name, not a sentence fragment.
    """
    s = strip_leading_articles(raw.strip())
    if not s:
        return False, "empty"
    if len(s) < 4:
        return False, "too_short"
    if len(s) > 120:
        return False, "too_long"
    if s.count(" ") > 14:
        return False, "too_many_words"

    low = s.lower()
    first = low.split()[0] if low.split() else ""
    if first in GENERIC_START_WORDS:
        return False, "generic_start"

    if has_negative_substring(s):
        return False, "negative_phrase"

    if _stopword_ratio(s) > 0.42 and not has_entity_word(s):
        return False, "too_many_stopwords"

    # Sentence-like (multiple clauses)
    if s.count(",") >= 2:
        return False, "comma_clause"
    if re.search(r"\b(?:which|that|because|although|while|where)\b", s, re.I):
        return False, "subordinate_clause"

    # Must look like a name: entity word, known family, or strong Title Case
    if not (has_entity_word(s) or matches_known_family(s) or is_title_like(s)):
        return False, "not_entity_shaped"

    if not has_entity_word(s) and not matches_known_family(s):
        # Still allow short proper-name stacks without "Group" if clearly titled
        if _word_count(s) > 8:
            return False, "long_without_entity_word"

    return True, ""


def normalize_group_name(raw: str) -> str:
    """Trim punctuation, collapse whitespace, fix common glue typos."""
    s = strip_leading_articles(raw.strip())
    s = re.sub(r"^[\s\-\|:;,.]+|[\s\-\|:;,.]+$", "", s)
    s = re.sub(r"\s+", " ", s)
    # Broken concatenations from HTML
    s = re.sub(r"(?i)groupplaywright", "Group", s)
    s = re.sub(r"(?i)automotivegroup", "Automotive Group", s)
    s = re.sub(r"(?i)autogroup", "Auto Group", s)
    return s.strip()


def canonicalize_group_name(normalized: str) -> str:
    """Map known variants to a canonical display string (delegates to alias table)."""
    from SCRAPING.canonical_groups import canonical_group_display

    return canonical_group_display(normalized)


def finalize_status(confidence: float, best_normalized: str | None = None) -> str:
    """Map numeric confidence to final_status (before fetch/redirect overrides)."""
    has_candidate = bool(best_normalized and str(best_normalized).strip())
    if confidence >= 0.80:
        return "assigned"
    if 0.55 <= confidence < 0.80:
        return "manual_review"
    if confidence < 0.55:
        if has_candidate:
            return "manual_review_low_confidence"
        return "unknown"
    return "unknown"


def apply_status_for_fetch(
    homepage_loaded: bool,
    redirect_mismatch: bool,
    confidence: float,
    best_normalized: str | None,
    vendor_only: bool,
) -> str:
    """
    Never assign high confidence from an unrelated redirect target; surface for human review.
    """
    if not homepage_loaded:
        return "fetch_failed"
    if redirect_mismatch:
        return "manual_review"
    if vendor_only:
        return "unknown"
    return finalize_status(confidence, best_normalized)
