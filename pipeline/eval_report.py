"""Batch evaluation aggregates and CSV export for hybrid adjudication runs."""
from __future__ import annotations

import csv
import json
import logging
from collections import Counter
from pathlib import Path
from typing import Any

from schemas.run_summary import RunSummary

logger = logging.getLogger("pipeline.eval_report")

EVAL_CSV_FIELDS = [
    "site_url",
    "final_status",
    "best_candidate_canonical",
    "confidence_score",
    "source_mode",
    "final_domain",
    "redirected",
    "flags",
    "site_stack_family",
    "crawl_strategy",
    "likely_vendor",
    "heavy_js",
    "stack_fingerprint",
    "ownership_hint_company_name",
]


def enrich_run_summary_batch(summary: RunSummary, sites: list[Any]) -> None:
    """Populate top_* counters for dashboards and comparison runs."""
    assigned_groups: Counter[str] = Counter()
    unknown_domains: Counter[str] = Counter()
    stack_families: Counter[str] = Counter()

    for s in sites:
        sd = getattr(s, "site_result_dict", None) or {}
        st = getattr(s, "merged_status", "")
        fd = (sd.get("final_domain") or "").strip().lower()
        canon = getattr(s, "merged_best_canonical", None) or sd.get(
            "best_candidate_canonical"
        ) or ""

        if st == "assigned" and canon:
            assigned_groups[canon] += 1
        elif st == "unknown" and fd:
            unknown_domains[fd] += 1
        sf = (sd.get("site_stack_family") or "").strip()
        if sf:
            stack_families[sf] += 1

    summary.top_assigned_groups = [
        {"group": k, "count": v} for k, v in assigned_groups.most_common(50)
    ]
    summary.top_unknown_domains = [
        {"domain": k, "count": v} for k, v in unknown_domains.most_common(50)
    ]
    summary.top_fetch_failure_domains = [
        {"domain": k, "count": v}
        for k, v in sorted(
            (summary.domain_failures or {}).items(), key=lambda x: -x[1]
        )[:50]
    ]
    summary.top_site_stack_families = [
        {"family": k, "count": v} for k, v in stack_families.most_common(40)
    ]


def build_assigned_evidence_patterns(sites: list[Any]) -> list[dict[str, Any]]:
    """Compact evidence fingerprint for assigned sites (evaluation / tuning)."""
    out: list[dict[str, Any]] = []
    for s in sites:
        if getattr(s, "merged_status", "") != "assigned":
            continue
        sd = getattr(s, "site_result_dict", None) or {}
        evs = sd.get("evidence_snippets") or []
        page_types: set[str] = set()
        for e in evs:
            page_types.add(e.get("page_kind") or "unknown")
        cross = any(e.get("cross_domain_evidence") for e in evs)
        canon = getattr(s, "merged_best_canonical", None) or sd.get(
            "best_candidate_canonical"
        )
        prof = sd.get("site_profile") or {}
        out.append(
            {
                "site_url": sd.get("url"),
                "ownership_signal": sd.get("best_supporting_signal"),
                "source_page_types": sorted(page_types),
                "cross_domain_evidence": cross,
                "canonical_group": canon,
                "site_stack_family": sd.get("site_stack_family") or "",
                "stack_fingerprint": prof.get("stack_fingerprint") or "",
                "likely_vendor": sd.get("likely_vendor") or "",
            }
        )
    return out


def _source_mode(sd: dict[str, Any], adjudication_source: str) -> str:
    fetch = sd.get("fetch_mode") or ""
    return f"{fetch}|{adjudication_source}"


def write_hybrid_eval_csv(path: Path | str, result: Any) -> None:
    """Flatten hybrid run to a small CSV for spreadsheets and diffing."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=EVAL_CSV_FIELDS)
        w.writeheader()
        for s in result.sites:
            sd = getattr(s, "site_result_dict", None) or {}
            prof = sd.get("site_profile") or {}
            w.writerow(
                {
                    "site_url": sd.get("url") or "",
                    "final_status": getattr(s, "merged_status", ""),
                    "best_candidate_canonical": getattr(s, "merged_best_canonical", None)
                    or sd.get("best_candidate_canonical")
                    or "",
                    "confidence_score": getattr(s, "merged_confidence", 0.0),
                    "source_mode": _source_mode(sd, getattr(s, "source", "")),
                    "final_domain": sd.get("final_domain") or "",
                    "redirected": sd.get("redirected"),
                    "flags": json.dumps(sd.get("flags") or []),
                    "site_stack_family": sd.get("site_stack_family") or "",
                    "crawl_strategy": sd.get("crawl_strategy") or "",
                    "likely_vendor": sd.get("likely_vendor") or "",
                    "heavy_js": sd.get("heavy_js"),
                    "stack_fingerprint": prof.get("stack_fingerprint") or "",
                    "ownership_hint_company_name": sd.get("ownership_hint_company_name")
                    or "",
                }
            )
    logger.info("Wrote evaluation CSV: %s", p)


def print_batch_eval_summary(result: Any) -> None:
    """Human-readable batch evaluation block for logs."""
    sm = result.summary
    logger.info("--- batch evaluation ---")
    logger.info(
        "counts: processed=%s assigned=%s unknown=%s manual_review=%s "
        "manual_review_low=%s fetch_failed=%s ai_calls=%s",
        sm.n_processed,
        sm.n_assigned,
        sm.n_unknown,
        sm.n_manual_review,
        sm.n_manual_review_low_confidence,
        sm.n_fetch_failed,
        sm.adjudication_invoked,
    )
    if sm.top_assigned_groups:
        top = sm.top_assigned_groups[:10]
        logger.info("top assigned groups: %s", top)
    if sm.top_unknown_domains:
        logger.info("top unknown domains: %s", sm.top_unknown_domains[:10])
    if sm.top_fetch_failure_domains:
        logger.info("top fetch failure domains: %s", sm.top_fetch_failure_domains[:10])
