"""LLM adjudicator: infer dealer group only from EvidencePackage JSON."""
from __future__ import annotations

import json
import logging
from typing import Any

from pydantic import ValidationError

from llm.client import LLMClient, LLMResponseError
from schemas.adjudication_result import AdjudicationResult
from schemas.evidence_package import EvidencePackage

logger = logging.getLogger("agents.adjudicator")

SYSTEM_PROMPT = """You are a strict automotive industry analyst. Your task is to infer the legal operating / dealer GROUP name (parent organization) for a dealership website using ONLY the JSON evidence provided.

Rules:
- Do NOT invent ownership. If the evidence does not clearly support a real organization name, set final_status to "unknown" and confidence_score below 0.4.
- Phrases about Privacy Policy, Terms of Use, Terms of Service, cookies, marketing slogans, "Charlotte community", inventory, service scheduling, or generic website copy are NOT dealer groups. Reject them explicitly in rejected_candidates.
- A valid answer looks like a company name: e.g. "Hendrick Automotive Group", "Tuttle-Click Automotive Group", "Penske Automotive Group", "AutoNation", "Lithia Motors".
- If the site shows a redirect_mismatch flag true, you must not trust destination-specific text; prefer unknown unless the evidence clearly names a group independent of that issue.
- crawl_metadata.site_ownership_hints (company name, about snippet, copyright) and crawl_metadata.site_profile (tech/stack heuristics) are weak, secondary context only — similar to Wappalyzer-style guesses. Do not assign a dealer group from them alone; they may slightly reinforce or contradict page snippets when clearly consistent or inconsistent.
- Output valid JSON only matching the requested schema. No markdown outside JSON.

JSON schema for your response:
{
  "best_candidate_raw": string or null,
  "best_candidate_normalized": string or null,
  "confidence_score": number 0-1,
  "final_status": "assigned" | "manual_review" | "unknown",
  "evidence_used": [short strings citing which snippets you relied on],
  "rejected_candidates": [strings you reject as junk],
  "reasoning_summary": string (2-4 sentences)
}

Note: "fetch_failed" is reserved for when no page loaded; you usually return unknown/manual_review/assigned only."""

PROMPT_VERSION = "adjudicator_v1"


def _user_prompt(pkg: EvidencePackage) -> str:
    payload = pkg.model_dump(mode="json")
    return (
        "Analyze this EvidencePackage and return ONLY a JSON object.\n\n"
        + json.dumps(payload, indent=2, ensure_ascii=False)
    )


def adjudicate_evidence(
    pkg: EvidencePackage,
    client: LLMClient,
    *,
    model: str | None = None,
    dealer_id: str = "",
) -> AdjudicationResult:
    raw = client.complete_json(
        system=SYSTEM_PROMPT,
        user=_user_prompt(pkg),
        model=model,
        temperature=0.05,
    )
    model_name = model or ""
    return _to_result(raw, dealer_id=dealer_id or pkg.dealer_id, model=model_name)


def _to_result(raw: dict[str, Any], *, dealer_id: str, model: str) -> AdjudicationResult:
    raw = dict(raw)
    raw.setdefault("dealer_id", dealer_id)
    raw.setdefault("model_name", model)
    raw.setdefault("prompt_version", PROMPT_VERSION)
    # Normalize final_status - LLM must not emit fetch_failed here
    fs = raw.get("final_status", "unknown")
    if fs == "fetch_failed":
        fs = "unknown"
    raw["final_status"] = fs
    try:
        return AdjudicationResult.model_validate(raw)
    except ValidationError as e:
        logger.warning("Adjudication parse fallback: %s", e)
        return AdjudicationResult(
            dealer_id=dealer_id,
            best_candidate_raw=raw.get("best_candidate_raw"),
            best_candidate_normalized=raw.get("best_candidate_normalized"),
            confidence_score=float(raw.get("confidence_score") or 0),
            final_status="unknown",
            evidence_used=[],
            rejected_candidates=[str(raw.get("reasoning_summary", ""))[:200]],
            reasoning_summary="Model output failed validation; treating as unknown.",
            model_name=model or "unknown",
            prompt_version=PROMPT_VERSION,
        )


class AdjudicatorAgent:
    """Thin wrapper for testing and future tool wiring."""

    def __init__(self, client: LLMClient) -> None:
        self.client = client

    def run(self, pkg: EvidencePackage, *, model: str | None = None) -> AdjudicationResult:
        return adjudicate_evidence(pkg, self.client, model=model)
