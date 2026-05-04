"""Structured evidence passed to the LLM adjudicator (no browsing)."""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class FetchedPage(BaseModel):
    """One page the deterministic crawler visited."""

    page_type: str = Field(description="homepage, footer, about, privacy, terms, etc.")
    url: str
    title: str | None = None
    relevant_snippets: list[str] = Field(default_factory=list)


class HeuristicScores(BaseModel):
    """Rule-based extraction scores for hybrid decisions."""

    rule_confidence: float = 0.0
    best_candidate_score: float | None = None
    num_candidates: int = 0
    num_rejected_rule: int = 0
    entity_specificity_score: float = 0.0
    best_evidence_tier: str = ""


class EvidencePackage(BaseModel):
    """
    Full evidence bundle for adjudication. Built only from scraper output.
    """

    dealer_id: str = ""
    dealer_name: str = ""
    root_url: str = ""
    original_url: str = ""
    final_url: str = ""
    redirected: bool = False
    redirect_mismatch: bool = False
    final_domain: str = ""
    brand: str | None = None

    candidate_group_names: list[str] = Field(default_factory=list)
    rejected_candidates_rule_based: list[dict[str, str]] = Field(default_factory=list)

    heuristic_scores: HeuristicScores = Field(default_factory=HeuristicScores)
    flags: list[str] = Field(default_factory=list)

    fetched_pages: list[FetchedPage] = Field(default_factory=list)
    crawl_metadata: dict[str, Any] = Field(default_factory=dict)

    model_config = {"extra": "forbid"}
