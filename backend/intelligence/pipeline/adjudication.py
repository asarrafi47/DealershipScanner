"""Hybrid rule-merge helpers and thresholds (the LLM call was removed, D-DC6)."""
from __future__ import annotations

from backend.scraping.models import SiteResult
from backend.scraping.org_validation import finalize_status
from backend.schemas.adjudication_result import AdjudicationResult

RULE_BYPASS_CONFIDENCE = 0.80


def compute_adjudication_skip_reason(sr: SiteResult) -> str:
    """
    Why no adjudication ran (kept for the saved run format; debugging rules_only).
    """
    if not sr.homepage_loaded:
        return "insufficient_evidence"
    if sr.fetch_error and not (sr.pages_checked or []):
        return "insufficient_evidence"
    if sr.redirect_mismatch:
        return "redirect_mismatch"
    if sr.confidence_score >= RULE_BYPASS_CONFIDENCE and sr.best_candidate_normalized:
        return "strong_rule_reject"
    if not sr.candidate_group_names and not sr.evidence_snippets:
        if len(sr.pages_checked or []) > 1:
            return "low_signal_pages_only"
        return "no_plausible_candidates"
    return "low_signal_pages_only"


def compute_manual_review_reason(
    sr: SiteResult,
    merged_st: str,
    source: str,
    rule_disagrees: bool,
) -> str | None:
    """Human-facing reason when status is manual_review or manual_review_low_confidence."""
    if merged_st not in ("manual_review", "manual_review_low_confidence"):
        return None
    if rule_disagrees or source == "hybrid_disagree":
        return "ai_rule_disagreement"
    if float(sr.entity_specificity_score or 0) < 0.42:
        return "low_specificity_entity"
    sig_l = (sr.best_supporting_signal or "").lower()
    if any(
        x in sig_l
        for x in (
            "part_of",
            "owned",
            "member_of",
            "explicit_is_part_of",
            "parent_company",
            "sonic_corporate",
            "known_family",
        )
    ):
        return "ownership_phrase_found"
    for e in sr.evidence_snippets or []:
        s = (e.get("signal") or "").lower()
        if any(
            x in s
            for x in (
                "part_of",
                "owned_by",
                "member_of",
                "explicit_is_part_of",
                "parent_company",
                "sonic_corporate",
            )
        ):
            return "ownership_phrase_found"
    if any(e.get("cross_domain_evidence") for e in (sr.evidence_snippets or [])):
        return "cross_domain_parent_signal"
    return "insufficient_store_family_evidence"


def merge_rule_and_ai(
    sr: SiteResult,
    ai: AdjudicationResult | None,
) -> tuple[str | None, float, str, str]:
    """
    Combine rule-based SiteResult with optional AdjudicationResult.

    Returns (best_name, confidence, final_status, source_tag).
    """
    if not sr.homepage_loaded or sr.final_status == "fetch_failed":
        return (
            sr.best_candidate_normalized,
            sr.confidence_score,
            "fetch_failed",
            "rules_only",
        )

    if ai is None:
        return (
            sr.best_candidate_normalized,
            sr.confidence_score,
            sr.final_status,
            "rules_only",
        )

    r_n, r_c = sr.best_candidate_normalized, float(sr.confidence_score)
    a_n = ai.best_candidate_normalized
    a_c = float(ai.confidence_score)

    if r_n and a_n and r_n.strip().lower() != a_n.strip().lower() and max(r_c, a_c) >= 0.35:
        pick = a_n if a_c >= r_c else r_n
        conf = min(r_c, a_c) * 0.85
        return pick, conf, "manual_review", "hybrid_disagree"

    if r_c >= RULE_BYPASS_CONFIDENCE and r_n:
        st = finalize_status(r_c, r_n)
        return r_n, r_c, st, "rules_only"

    name = a_n or r_n
    conf = max(r_c, a_c * 0.92) if name else 0.0
    conf = min(1.0, conf)
    st = finalize_status(conf, name)
    return name, conf, st, "ai"

