"""
Future: higher-level agent that ingests run summaries + review queues and suggests
parser/threshold changes. Stub only — no automation of code edits.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from schemas.run_summary import RunSummary


@dataclass
class OverseerInput:
    """Inputs for a future overseer LLM or analyst."""

    recent_summaries: list[RunSummary] = field(default_factory=list)
    review_queue_snapshot: list[dict[str, Any]] = field(default_factory=list)
    manual_flags: list[str] = field(default_factory=list)


@dataclass
class OverseerRecommendations:
    """Structured recommendations (placeholder)."""

    retry_domains: list[str] = field(default_factory=list)
    parser_hints: list[str] = field(default_factory=list)
    threshold_suggestions: list[str] = field(default_factory=list)
    recurring_false_positives: list[str] = field(default_factory=list)


class MasterOverseerAgent:
    """
    Design stub: wire an LLM later to analyze RunSummary + review_queue.jsonl
    and emit OverseerRecommendations. Does not modify the codebase.
    """

    def analyze(self, inp: OverseerInput) -> OverseerRecommendations:
        # Placeholder heuristics — replace with model call when ready
        domains = list(inp.review_queue_snapshot)
        hints: list[str] = []
        for s in inp.recent_summaries[-3:]:
            if s.n_fetch_failed > s.n_processed * 0.3:
                hints.append("High fetch failure rate — check Playwright/403 handling.")
        return OverseerRecommendations(
            retry_domains=[d.get("final_domain", "") for d in domains if d.get("final_domain")][:10],
            parser_hints=hints,
            threshold_suggestions=[],
            recurring_false_positives=[],
        )
