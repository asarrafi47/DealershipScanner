"""Build EvidencePackage from deterministic SiteResult + dealer manifest fields."""
from __future__ import annotations

from typing import Any
from urllib.parse import urlparse

from SCRAPING.models import SiteResult
from schemas.evidence_package import EvidencePackage, FetchedPage, HeuristicScores


def _snippet_provenance_counts(evidence_snippets: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for ev in evidence_snippets:
        pk = ev.get("page_kind") or "unknown"
        if ev.get("cross_domain_evidence"):
            pk = f"cross_domain:{pk}"
        counts[pk] = counts.get(pk, 0) + 1
    return counts


def site_result_to_evidence_package(
    sr: SiteResult,
    *,
    dealer_id: str = "",
    dealer_name: str = "",
    brand: str | None = None,
) -> EvidencePackage:
    """Group evidence snippets by page URL into FetchedPage records."""
    by_url: dict[str, dict[str, Any]] = {}
    for ev in sr.evidence_snippets:
        u = ev.get("page_url") or ""
        if not u:
            continue
        if u not in by_url:
            by_url[u] = {
                "page_type": ev.get("page_kind", "unknown"),
                "url": u,
                "title": None,
                "snippets": [],
            }
        sn = ev.get("snippet") or ""
        if sn and sn not in by_url[u]["snippets"]:
            by_url[u]["snippets"].append(sn[:4000])

    fetched = [
        FetchedPage(
            page_type=meta["page_type"],
            url=meta["url"],
            title=meta["title"],
            relevant_snippets=meta["snippets"],
        )
        for meta in by_url.values()
    ]

    # Pages visited without snippets (still useful metadata)
    for u in sr.pages_checked:
        if u not in by_url:
            fetched.append(
                FetchedPage(
                    page_type="visited",
                    url=u,
                    title=None,
                    relevant_snippets=[],
                )
            )

    heuristic = HeuristicScores(
        rule_confidence=float(sr.confidence_score),
        best_candidate_score=sr.confidence_score,
        num_candidates=len(sr.candidate_group_names),
        num_rejected_rule=len(sr.rejected_candidates),
        entity_specificity_score=float(sr.entity_specificity_score or 0.0),
        best_evidence_tier=sr.best_evidence_tier or "",
    )

    return EvidencePackage(
        dealer_id=dealer_id,
        dealer_name=dealer_name,
        root_url=sr.url,
        original_url=sr.original_url or sr.url,
        final_url=sr.final_url,
        redirected=sr.redirected,
        redirect_mismatch=sr.redirect_mismatch,
        final_domain=sr.final_domain,
        brand=brand,
        candidate_group_names=list(sr.candidate_group_names),
        rejected_candidates_rule_based=list(sr.rejected_candidates),
        heuristic_scores=heuristic,
        flags=list(sr.flags),
        fetched_pages=fetched,
        crawl_metadata={
            "fetch_mode": sr.fetch_mode,
            "fetch_error": sr.fetch_error,
            "pages_checked": sr.pages_checked,
            "snippet_provenance_counts": _snippet_provenance_counts(list(sr.evidence_snippets)),
            "distinct_domains_checked": list(
                {urlparse(u).netloc.lower() for u in (sr.pages_checked or []) if u}
            ),
            "site_profile": sr.site_profile or {},
            "site_stack_family": sr.site_stack_family or "",
            "crawl_strategy": sr.crawl_strategy or "",
            "likely_vendor": sr.likely_vendor or "",
            "heavy_js": bool(sr.heavy_js),
            "site_ownership_hints": {
                "company_name": sr.ownership_hint_company_name or "",
                "about_text": (sr.ownership_hint_about_text or "")[:1200],
                "copyright": sr.ownership_hint_copyright or "",
            },
            "canonical_site_warning": sr.canonical_site_warning or "",
        },
    )
