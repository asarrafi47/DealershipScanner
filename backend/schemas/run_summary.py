"""Aggregate stats for a crawl/adjudication run (for overseer / dashboards)."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field


class RunSummary(BaseModel):
    run_id: str = ""
    started_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    finished_at: datetime | None = None

    n_processed: int = 0
    n_assigned: int = 0
    n_manual_review: int = 0
    n_manual_review_low_confidence: int = 0
    n_unknown: int = 0
    n_fetch_failed: int = 0

    top_redirect_issues: list[dict[str, Any]] = Field(default_factory=list)
    top_junk_patterns: list[dict[str, Any]] = Field(default_factory=list)
    domain_failures: dict[str, int] = Field(default_factory=dict)
    brand_breakdown: dict[str, int] = Field(default_factory=dict)

    adjudication_invoked: int = 0
    adjudication_errors: int = 0

    top_assigned_groups: list[dict[str, Any]] = Field(default_factory=list)
    top_unknown_domains: list[dict[str, Any]] = Field(default_factory=list)
    top_fetch_failure_domains: list[dict[str, Any]] = Field(default_factory=list)
    top_site_stack_families: list[dict[str, Any]] = Field(default_factory=list)

    notes: str = ""
