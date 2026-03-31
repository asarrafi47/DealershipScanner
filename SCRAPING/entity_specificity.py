"""
Entity specificity: parent / dealer-group names vs department or generic business-unit labels.
Used for scoring, ranking, and rejection (recall is handled elsewhere).
"""
from __future__ import annotations

import re

from SCRAPING.org_validation import matches_known_family

# Substrings that indicate a department / service line, not a parent org, when standing alone
_DEPARTMENT_UNIT_EXACT = frozenset(
    {
        "collision group",
        "collision center",
        "service center",
        "finance center",
        "parts center",
        "body shop",
        "detail center",
    }
)

# If the normalized name equals or ends with these (after stripping "the"), treat as unit-like
_DEPARTMENT_TAIL_PHRASES = (
    "collision group",
    "collision center",
    "service center",
    "finance center",
    "parts center",
)

_STRONG_OWNERSHIP_SIGNALS = frozenset(
    {
        "explicit_is_part_of_the",
        "part_of_the",
        "member_of_the",
        "owned_by",
        "parent_company",
        "part_of",
        "operated_by",
        "family_dealerships",
        "automotive_group",
        "auto_group",
        "known_family_group",
        "sonic_corporate",
        "copyright",
    }
)

_OWNERSHIP_SNIPPET_MARKERS = (
    "part of ",
    "owned by ",
    "member of ",
    "parent company",
    "affiliated with ",
    "subsidiary of ",
)


def snippet_has_ownership_language(snippet: str) -> bool:
    sl = (snippet or "").lower()
    return any(m in sl for m in _OWNERSHIP_SNIPPET_MARKERS)


def is_department_or_unit_like(name: str) -> bool:
    """True if the string looks like a store department / business unit, not a parent group."""
    s = re.sub(r"\s+", " ", (name or "").strip().lower())
    if not s:
        return False
    if s in _DEPARTMENT_UNIT_EXACT:
        return True
    for tail in _DEPARTMENT_TAIL_PHRASES:
        if s == tail or s.endswith(" " + tail):
            return True
    return False


def has_distinctive_owner_token(name: str) -> bool:
    """
    True if the name carries a recognizable family or brand token (not only generic department words).
    """
    if matches_known_family(name):
        return True
    # Hyphenated or multi-token proper names: Tuttle-Click, No. 1 Foo — still may be weak
    if re.search(r"[A-Za-z]+-[A-Za-z]+", name):
        return True
    tokens = re.findall(r"[A-Za-z0-9]+(?:'[A-Za-z]+)?", name)
    if len(tokens) < 2:
        return False
    generic = {
        "group",
        "center",
        "centre",
        "automotive",
        "auto",
        "collision",
        "service",
        "finance",
        "parts",
        "body",
        "motor",
        "motors",
        "dealership",
        "dealerships",
        "no",
        "1",
    }
    distinctive = [t for t in tokens if t.lower() not in generic and len(t) > 1]
    return len(distinctive) >= 2


def entity_specificity_score(name: str) -> float:
    """
    0..1 heuristic: parent / corporate dealer-group vs department / generic unit.
    """
    if not (name or "").strip():
        return 0.0
    if is_department_or_unit_like(name) and not has_distinctive_owner_token(name):
        return 0.15
    if matches_known_family(name):
        return 0.95
    low = name.lower()
    if any(
        x in low
        for x in (
            "automotive group",
            "auto group",
            "motorcars",
            "holdings",
            "dealerships",
        )
    ):
        return 0.85
    if has_distinctive_owner_token(name) and not is_department_or_unit_like(name):
        return 0.72
    if is_department_or_unit_like(name) and has_distinctive_owner_token(name):
        return 0.42
    return 0.55


def ownership_signal_strong(signal: str, snippet: str) -> bool:
    sig = (signal or "").lower()
    if any(
        x in sig
        for x in (
            "explicit_is_part_of",
            "part_of_the",
            "member_of_the",
            "owned_by",
            "parent_company",
            "second_pass_partof",
            "second_pass_member",
            "second_pass_owned",
        )
    ):
        return True
    if signal in _STRONG_OWNERSHIP_SIGNALS and snippet_has_ownership_language(snippet):
        return True
    return signal in _STRONG_OWNERSHIP_SIGNALS


def evidence_source_tier(page_kind: str, cross_domain: bool, signal: str) -> str:
    """strong | medium | weak — for weighting and artifacts."""
    pk = (page_kind or "").lower()
    sig = (signal or "").lower()
    if cross_domain and ("cross_domain" in pk or "legal" in pk or "privacy" in pk):
        return "strong"
    if cross_domain and pk.startswith("cross_domain"):
        return "strong"
    if "explicit_is_part_of" in sig or "part_of_the" in sig or "part_of" in sig:
        if pk in ("homepage", "about", "footer"):
            return "strong"
    if sig in ("owned_by", "parent_company", "member_of_the", "sonic_corporate", "known_family_group"):
        return "strong"
    if pk == "careers":
        return "medium"
    if pk == "company" or "collision" in pk:
        return "weak"
    if pk in ("about", "homepage", "legal", "privacy", "terms"):
        return "medium"
    return "medium"


def evidence_source_multiplier(
    page_kind: str,
    cross_domain: bool,
    signal: str,
    snippet: str,
) -> float:
    """
    Multiplier applied to raw rule confidence (before specificity merge).
    """
    tier = evidence_source_tier(page_kind, cross_domain, signal)
    pk = (page_kind or "").lower()
    sig = (signal or "").lower()
    if tier == "strong":
        m = 1.18
    elif tier == "medium":
        m = 1.0
    else:
        m = 0.82
    # Related-business pages: weak unless ownership language in snippet
    if pk == "company" or "collision" in pk:
        if snippet_has_ownership_language(snippet) or ownership_signal_strong(signal, snippet):
            m *= 1.05
        else:
            m *= 0.72
    if cross_domain and "cross_domain_legal" in pk:
        m *= 1.06
    return max(0.35, min(1.35, m))


def department_unit_penalty(name: str, signal: str, snippet: str) -> float:
    """
    0..1 multiplier applied after other factors. Near-zero kills generic units without ownership proof.
    """
    if not is_department_or_unit_like(name):
        return 1.0
    if ownership_signal_strong(signal, snippet):
        return 0.72
    if snippet_has_ownership_language(snippet) and has_distinctive_owner_token(name):
        return 0.55
    return 0.06


def composite_candidate_score(
    raw_conf: float,
    name: str,
    page_kind: str,
    cross_domain: bool,
    signal: str,
    snippet: str,
) -> float:
    """Single combined score for ranking candidates."""
    spec = entity_specificity_score(name)
    spec_w = 0.35 + 0.65 * spec
    src = evidence_source_multiplier(page_kind, cross_domain, signal, snippet)
    dept = department_unit_penalty(name, signal, snippet)
    return min(1.0, max(0.0, raw_conf * spec_w * src * dept))


def rank_candidate_entries(
    entries: list[tuple[str, float, object, str, str, bool, str]],
) -> list[tuple[str, float, object, str, float, str]]:
    """
    entries: (name, raw_conf, ev, raw_orig, page_kind, cross_domain, snippet).
    Returns rows (name, raw_conf, ev, raw_orig, composite, tier) sorted best-first.
    """
    ranked: list[tuple[str, float, object, str, float, str]] = []
    for name, raw_conf, ev, raw_orig, page_kind, cross_domain, snippet in entries:
        sig = getattr(ev, "signal", "") or ""
        composite = composite_candidate_score(
            raw_conf, name, page_kind, cross_domain, sig, snippet
        )
        tier = evidence_source_tier(page_kind, cross_domain, sig)
        ranked.append((name, raw_conf, ev, raw_orig, composite, tier))

    def sort_key(t: tuple[str, float, object, str, float, str]) -> tuple[float, int, float]:
        name, _raw_conf, _ev, _raw_orig, composite, tier = t
        tier_i = 2 if tier == "strong" else (1 if tier == "medium" else 0)
        spec = entity_specificity_score(name)
        return (composite, tier_i, spec)

    ranked.sort(key=sort_key, reverse=True)
    return ranked

