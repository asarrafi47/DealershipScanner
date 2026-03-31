"""Hybrid crawl → rules → optional LLM adjudication → review queue → summary."""
from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from SCRAPING.canonical_groups import apply_sibling_canonical_reinforcement, canonical_group_display
from llm.client import LLMClient
from llm.providers.ollama_client import OpenAICompatibleClient
from pipeline.adjudication import (
    compute_adjudication_skip_reason,
    compute_manual_review_reason,
    merge_rule_and_ai,
    run_llm_adjudication,
    should_invoke_adjudicator,
)
from pipeline.evidence_builder import site_result_to_evidence_package
from pipeline.review_queue import ReviewQueue, ReviewQueueEntry
from schemas.adjudication_result import AdjudicationResult
from schemas.evidence_package import EvidencePackage
from schemas.run_summary import RunSummary

logger = logging.getLogger("pipeline.orchestrator")


@dataclass
class PipelineSiteOutput:
    site_result_dict: dict[str, Any]
    evidence_package: EvidencePackage | None
    adjudication: AdjudicationResult | None
    merged_status: str
    merged_confidence: float
    merged_best: str | None
    source: str
    adjudication_skip_reason: str | None = None
    manual_review_reason: str | None = None
    merged_best_canonical: str | None = None


@dataclass
class OrchestratorResult:
    sites: list[PipelineSiteOutput] = field(default_factory=list)
    summary: RunSummary = field(default_factory=RunSummary)
    assigned_evidence_patterns: list[dict[str, Any]] = field(default_factory=list)


def _should_enqueue_manual(
    merged_status: str,
    sr_flags: list[str],
    rule_disagrees: bool,
) -> bool:
    if merged_status in ("manual_review", "manual_review_low_confidence"):
        return True
    if "redirect_mismatch" in sr_flags:
        return True
    if rule_disagrees:
        return True
    return False


def run_hybrid_batch(
    crawl_fn: Callable[..., Any],
    urls_with_meta: list[dict[str, Any]],
    *,
    use_adjudicator: bool,
    llm_client: LLMClient | None,
    llm_model: str | None,
    review_queue: ReviewQueue | None = None,
) -> OrchestratorResult:
    """
    crawl_fn: callable (url, **kwargs) -> SiteResult — e.g. bound process_site_requests
    urls_with_meta: [{"url": "...", "dealer_id": "", "dealer_name": "", "brand": null}, ...]
    """
    rq = review_queue or ReviewQueue()
    run_id = str(uuid.uuid4())[:8]
    summary = RunSummary(run_id=run_id)
    out: list[PipelineSiteOutput] = []
    junk_patterns: dict[str, int] = {}
    domain_fail: dict[str, int] = {}
    staged: list[dict[str, Any]] = []

    for row in urls_with_meta:
        url = row["url"]
        dealer_id = row.get("dealer_id") or ""
        dealer_name = row.get("dealer_name") or ""
        brand = row.get("brand")

        try:
            sr = crawl_fn(url)
        except Exception as e:
            logger.exception("crawl failed %s: %s", url, e)
            summary.n_processed += 1
            summary.n_fetch_failed += 1
            continue

        summary.n_processed += 1
        rule_name_before = sr.best_candidate_normalized
        rule_conf_snapshot = float(sr.confidence_score)

        pkg = site_result_to_evidence_package(
            sr,
            dealer_id=dealer_id,
            dealer_name=dealer_name,
            brand=brand,
        )
        pkg.dealer_id = dealer_id
        pkg.dealer_name = dealer_name

        ai_res: AdjudicationResult | None = None
        skip_reason: str | None = None
        if use_adjudicator and llm_client and should_invoke_adjudicator(sr):
            summary.adjudication_invoked += 1
            ai_res = run_llm_adjudication(pkg, llm_client, model=llm_model)
            if ai_res is None:
                summary.adjudication_errors += 1
        if ai_res is None:
            skip_reason = compute_adjudication_skip_reason(sr)
            sr.adjudication_skip_reason = skip_reason

        merged_best, merged_conf, merged_st, source = merge_rule_and_ai(sr, ai_res)
        rule_disagrees = bool(
            ai_res
            and rule_name_before
            and ai_res.best_candidate_normalized
            and rule_name_before.strip().lower() != ai_res.best_candidate_normalized.strip().lower()
        )
        mrr = compute_manual_review_reason(sr, merged_st, source, rule_disagrees)
        sr.manual_review_reason = mrr

        sr.best_candidate_normalized = merged_best
        sr.best_candidate_canonical = canonical_group_display(merged_best) if merged_best else None
        sr.best_candidate = sr.best_candidate_canonical
        sr.confidence_score = merged_conf
        sr.final_status = merged_st
        sr.flags.append(f"adjudication_source:{source}")

        if sr.redirect_mismatch:
            summary.top_redirect_issues.append(
                {"root": url, "final_url": sr.final_url, "domain": sr.final_domain}
            )

        staged.append(
            {
                "sr": sr,
                "url": url,
                "dealer_id": dealer_id,
                "dealer_name": dealer_name,
                "brand": brand,
                "pkg": pkg,
                "ai_res": ai_res,
                "skip_reason": skip_reason,
                "merged_st": merged_st,
                "merged_conf": merged_conf,
                "merged_best": merged_best,
                "source": source,
                "mrr": mrr,
                "rule_disagrees": rule_disagrees,
                "rule_conf_snapshot": rule_conf_snapshot,
            }
        )

    apply_sibling_canonical_reinforcement([x["sr"] for x in staged])

    from dataclasses import asdict

    for x in staged:
        sr = x["sr"]
        url = x["url"]
        dealer_id = x["dealer_id"]
        brand = x["brand"]
        pkg = x["pkg"]
        ai_res = x["ai_res"]
        skip_reason = x["skip_reason"]
        merged_st = x["merged_st"]
        merged_conf = x["merged_conf"]
        merged_best = x["merged_best"]
        source = x["source"]
        mrr = x["mrr"]
        rule_disagrees = x["rule_disagrees"]
        rule_conf_snapshot = x["rule_conf_snapshot"]

        merged_canon = sr.best_candidate_canonical

        if merged_st == "assigned":
            summary.n_assigned += 1
        elif merged_st == "manual_review":
            summary.n_manual_review += 1
        elif merged_st == "manual_review_low_confidence":
            summary.n_manual_review_low_confidence += 1
        elif merged_st == "unknown":
            summary.n_unknown += 1
        elif merged_st == "fetch_failed":
            summary.n_fetch_failed += 1

        if merged_st == "fetch_failed" and sr.final_domain:
            domain_fail[sr.final_domain] = domain_fail.get(sr.final_domain, 0) + 1
        if brand:
            summary.brand_breakdown[brand] = summary.brand_breakdown.get(brand, 0) + 1

        for r in sr.rejection_reasons[:5]:
            junk_patterns[r[:80]] = junk_patterns.get(r[:80], 0) + 1

        if _should_enqueue_manual(merged_st, sr.flags, rule_disagrees):
            rq.enqueue(
                ReviewQueueEntry(
                    dealer_id=dealer_id,
                    root_url=url,
                    final_url=sr.final_url,
                    reason=merged_st,
                    flags=list(sr.flags),
                    rule_confidence=rule_conf_snapshot,
                    ai_confidence=float(ai_res.confidence_score) if ai_res else None,
                    best_candidate=merged_canon,
                    evidence_digest={
                        "candidates": sr.candidate_group_names[:10],
                        "redirect_mismatch": sr.redirect_mismatch,
                    },
                )
            )

        out.append(
            PipelineSiteOutput(
                site_result_dict=asdict(sr),
                evidence_package=pkg,
                adjudication=ai_res,
                merged_status=merged_st,
                merged_confidence=merged_conf,
                merged_best=merged_best,
                merged_best_canonical=merged_canon,
                source=source,
                adjudication_skip_reason=skip_reason,
                manual_review_reason=mrr,
            )
        )

    summary.top_junk_patterns = [
        {"pattern": k, "count": v} for k, v in sorted(junk_patterns.items(), key=lambda x: -x[1])[:20]
    ]
    summary.domain_failures = domain_fail
    from datetime import datetime, timezone

    summary.finished_at = datetime.now(timezone.utc)

    from pipeline.eval_report import build_assigned_evidence_patterns, enrich_run_summary_batch

    enrich_run_summary_batch(summary, out)
    patterns = build_assigned_evidence_patterns(out)

    return OrchestratorResult(sites=out, summary=summary, assigned_evidence_patterns=patterns)


def default_llm_client() -> OpenAICompatibleClient:
    return OpenAICompatibleClient()


def save_hybrid_run(result: OrchestratorResult, path: str | Path) -> None:
    """Persist sites + summary for overseer / debugging."""
    out_path = Path(path)
    payload: dict[str, Any] = {
        "summary": result.summary.model_dump(mode="json"),
        "assigned_evidence_patterns": getattr(
            result, "assigned_evidence_patterns", []
        ),
        "sites": [
            {
                "site": s.site_result_dict,
                "merged_status": s.merged_status,
                "merged_confidence": s.merged_confidence,
                "adjudication": s.adjudication.model_dump(mode="json") if s.adjudication else None,
                "source": s.source,
                "adjudication_skip_reason": s.adjudication_skip_reason,
                "manual_review_reason": s.manual_review_reason,
                "merged_best_canonical": s.merged_best_canonical,
                "best_candidate_raw": (s.site_result_dict or {}).get("best_candidate_raw"),
                "best_candidate_normalized": (s.site_result_dict or {}).get(
                    "best_candidate_normalized"
                ),
                "best_candidate_canonical": (s.site_result_dict or {}).get(
                    "best_candidate_canonical"
                ),
            }
            for s in result.sites
        ],
    }
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
