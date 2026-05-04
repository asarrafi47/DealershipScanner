"""HTTP identity, regex patterns, vendor lists, and page-weight weights."""
from __future__ import annotations

import re

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

_YEAR_OR_RANGE = r"(?:\d{4}-\d{4}|\d{4})"
_HOLD = r"[A-Za-z0-9\s,\.&'\-]{1,120}?"
COPYRIGHT_RE = re.compile(
    rf"(?:©|Copyright|&copy;)\s*(?:{_YEAR_OR_RANGE})?\s*\.?\s*({_HOLD})(?=\||All rights|\Z)",
    re.IGNORECASE | re.DOTALL,
)

BOILERPLATE_HINTS = (
    "all rights reserved",
    "privacy policy",
    "terms of use",
    "cookie",
    "do not sell",
)

VENDOR_SUBSTRINGS = (
    "dealer.com",
    "dealeron",
    "dealer on",
    "shift digital",
    "cdk global",
    "cdk ",
    "reynolds and reynolds",
    "reynolds & reynolds",
    "dealer inspire",
    "team velocity",
    "foxdealer",
    "sincro",
)

# Thresholds for final_status (after normalization; see org_validation.finalize_status)
THRESH_ASSIGNED = 0.80
THRESH_MANUAL_REVIEW = 0.55

PAGE_WEIGHT: dict[str, float] = {
    "about": 1.0,
    "privacy": 0.55,
    "terms": 0.55,
    "legal": 0.6,
    "careers": 0.65,
    "footer": 0.5,
    "homepage": 0.42,
    "company": 0.72,
    # Linked from dealer site; often parent/legal copy
    "cross_domain_legal": 0.82,
    "cross_domain_group": 0.78,
}

# (regex after phrase boundary, signal_name, base_score)
# Match is case-insensitive; extraction uses a short window after the match end.
# Stronger "part of" / family phrases first (higher base_score).
OWNERSHIP_ANCHOR_PATTERNS: list[tuple[re.Pattern[str], str, float]] = [
    (
        re.compile(r"\bis\s+part\s+of\s+the\b", re.I),
        "explicit_is_part_of_the",
        0.96,
    ),
    (
        re.compile(
            r"\b(?:a\s+)?member\s+of\s+the\s+\S",
            re.I,
        ),
        "member_of_the",
        0.94,
    ),
    (
        re.compile(
            r"\bpart\s+of\s+the\s+\S",
            re.I,
        ),
        "part_of_the",
        0.93,
    ),
    (
        re.compile(r"\bowned\s+by\b", re.I),
        "owned_by",
        0.92,
    ),
    (
        re.compile(r"\bparent\s+company\b", re.I),
        "parent_company",
        0.88,
    ),
    (
        re.compile(r"\b(?:part of|a member of|member of)\b", re.I),
        "part_of",
        0.88,
    ),
    (
        re.compile(r"\b(?:operated|managed)\s+by\b", re.I),
        "operated_by",
        0.86,
    ),
    (
        re.compile(
            r"\b(?:a\s+)?family\s+of\s+dealerships?\b",
            re.I,
        ),
        "family_dealerships",
        0.65,
    ),
]

# Standalone org line (no anchor) — tight patterns
STANDALONE_ORG_PATTERNS: list[tuple[re.Pattern[str], str, float]] = [
    (
        re.compile(
            r"\b([A-Z][A-Za-z0-9'\-]*(?:\s+[A-Z][A-Za-z0-9'\-]*){0,4}\s+Automotive\s+Group)\b",
        ),
        "automotive_group",
        0.8,
    ),
    (
        re.compile(
            r"\b([A-Z][A-Za-z0-9'\-]*(?:\s+[A-Z][A-Za-z0-9'\-]*){0,4}\s+Auto\s+Group)\b",
        ),
        "auto_group",
        0.76,
    ),
    (
        re.compile(
            r"\b((?:Hendrick|Penske|Lithia|Sonic|Asbury|AutoNation|Group\s+1|"
            r"Tuttle-Click|Tuttle Click|Fletcher Jones|Holman|Morgan)\b"
            r"[A-Za-z0-9\s,\'\-]{0,50}?(?:\s+Automotive\s+)?Group)\b",
            re.I,
        ),
        "known_family_group",
        0.85,
    ),
    (
        re.compile(r"\b(Sonic\s+Automotive(?:\s+Group)?)\b", re.I),
        "sonic_corporate",
        0.86,
    ),
]
