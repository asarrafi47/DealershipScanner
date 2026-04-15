"""Dataclasses for dealer-group crawl results."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class Evidence:
    snippet: str
    page_url: str
    page_kind: str
    signal: str
    base_score: float
    weighted_score: float
    cross_domain_evidence: bool = False
    related_domain: str | None = None


@dataclass
class SiteResult:
    """One processed dealer root URL."""

    url: str
    fetch_mode: str
    homepage_loaded: bool
    final_url: str

    original_url: str = ""
    final_domain: str = ""
    redirected: bool = False
    redirect_mismatch: bool = False

    flags: list[str] = field(default_factory=list)
    redirect_chain: list[str] = field(default_factory=list)
    fetch_error: str | None = None

    candidate_group_names: list[str] = field(default_factory=list)
    rejected_candidates: list[dict[str, str]] = field(default_factory=list)
    rejection_reasons: list[str] = field(default_factory=list)

    best_candidate_raw: str | None = None
    best_candidate_normalized: str | None = None
    best_candidate_canonical: str | None = None
    best_candidate: str | None = None
    confidence_score: float = 0.0

    evidence_snippets: list[dict[str, Any]] = field(default_factory=list)
    pages_checked: list[str] = field(default_factory=list)
    final_status: str = "unknown"

    raw_candidates_pre_filter: list[str] = field(default_factory=list)
    normalized_candidates: list[str] = field(default_factory=list)
    skipped_cross_domain: list[dict[str, str]] = field(default_factory=list)
    second_pass_candidates: list[str] = field(default_factory=list)
    adjudication_skip_reason: str | None = None

    entity_specificity_score: float = 0.0
    best_evidence_tier: str = ""
    best_supporting_signal: str = ""
    manual_review_reason: str | None = None

    # Wappalyzer-style site profile (supporting signals; not sole truth)
    site_profile: dict[str, Any] = field(default_factory=dict)
    site_stack_family: str = ""
    crawl_strategy: str = ""
    likely_vendor: str = ""
    heavy_js: bool = False
    ownership_hint_company_name: str = ""
    ownership_hint_about_text: str = ""
    ownership_hint_copyright: str = ""
    canonical_site_warning: str = ""
