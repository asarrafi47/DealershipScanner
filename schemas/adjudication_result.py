"""LLM + hybrid adjudication output."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal

from pydantic import BaseModel, Field

FinalStatus = Literal["assigned", "manual_review", "unknown", "fetch_failed"]


class AdjudicationResult(BaseModel):
    dealer_id: str = ""
    best_candidate_raw: str | None = None
    best_candidate_normalized: str | None = None
    confidence_score: float = Field(ge=0.0, le=1.0, default=0.0)
    final_status: FinalStatus = "unknown"
    evidence_used: list[str] = Field(default_factory=list, description="Snippet ids or short labels")
    rejected_candidates: list[str] = Field(default_factory=list)
    reasoning_summary: str = ""
    model_name: str = ""
    prompt_version: str = "adjudicator_v1"
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    model_config = {"extra": "forbid"}
