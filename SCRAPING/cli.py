"""CLI entry: dealer-group crawl orchestration, JSON/CSV output."""
from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sys
import time
import urllib3
from dataclasses import asdict
from pathlib import Path

from SCRAPING.adjudicate_crawl import build_adjudicate_crawl_one
from SCRAPING.crawler import process_site_hybrid
from SCRAPING.fetch_requests import fetch_requests_session
from SCRAPING.interrupt import install_sigint_handler, stop_requested
from SCRAPING.models import SiteResult
from SCRAPING.paths import (
    DEFAULT_DB,
    MANIFEST_DEFAULT,
    dated_hybrid_run_path,
    default_hybrid_run_path,
    default_json_results_path,
    default_known_group_aliases_path,
)
from SCRAPING.sources import (
    load_manifest_records,
    load_roots_from_db,
    load_roots_from_manifest,
)

logger = logging.getLogger("SCRAPING.cli")

CSV_FIELDS = [
    "original_url",
    "url",
    "final_url",
    "final_domain",
    "redirected",
    "redirect_mismatch",
    "fetch_mode",
    "homepage_loaded",
    "final_status",
    "confidence_score",
    "best_candidate_raw",
    "best_candidate_normalized",
    "best_candidate_canonical",
    "best_candidate",
    "candidate_group_names",
    "rejected_candidates",
    "rejection_reasons",
    "flags",
    "redirect_chain",
    "fetch_error",
    "pages_checked",
    "evidence_snippets",
    "site_stack_family",
    "crawl_strategy",
    "likely_vendor",
    "heavy_js",
    "ownership_hint_company_name",
    "ownership_hint_about_text",
    "ownership_hint_copyright",
    "canonical_site_warning",
    "site_profile",
]


def _empty_site_result(url: str, mode: str, err: str) -> SiteResult:
    return SiteResult(
        url=url,
        original_url=url,
        fetch_mode=mode,
        homepage_loaded=False,
        final_url=url,
        final_domain="",
        fetch_error=err,
        final_status="fetch_failed",
    )


def _run_adjudicate_mode(args: argparse.Namespace) -> int:
    """Hybrid crawl + rules + LLM adjudication + run summary artifact."""
    from SCRAPING.canonical_groups import load_alias_table_from_json
    from llm.providers.ollama_client import OpenAICompatibleClient
    from pipeline.eval_report import print_batch_eval_summary, write_hybrid_eval_csv
    from pipeline.orchestrator import run_hybrid_batch, save_hybrid_run

    alias_path = args.known_group_aliases or default_known_group_aliases_path()
    n_alias = load_alias_table_from_json(alias_path)
    if n_alias:
        logger.info("Loaded %s known-group alias entries from %s", n_alias, alias_path)

    if args.test:
        manifest = MANIFEST_DEFAULT
        if not manifest.is_file():
            logger.error("dealers.json not found for --test")
            return 1
        rows = load_manifest_records(manifest)[:5]
    elif args.manifest is not None:
        if not args.manifest.is_file():
            logger.error("Manifest not found: %s", args.manifest)
            return 1
        rows = load_manifest_records(args.manifest)
    else:
        if not args.db.is_file():
            logger.error("Database not found: %s — use --manifest for adjudication", args.db)
            return 1
        rows = [
            {"url": u, "dealer_id": "", "dealer_name": "", "brand": None}
            for u in load_roots_from_db(args.db)
        ]

    if args.limit > 0:
        rows = rows[: args.limit]

    session = fetch_requests_session(args.timeout, verify_ssl=not args.insecure_ssl)
    llm = OpenAICompatibleClient(base_url=args.llm_base_url)

    browser = None
    playwright = None

    try:
        if not args.use_requests:
            try:
                from playwright.sync_api import sync_playwright

                playwright = sync_playwright().start()
                browser = playwright.chromium.launch(headless=True)
                logger.info("Playwright ready for adjudicated crawl")
            except Exception as e:
                logger.warning("Playwright unavailable (%s); using requests", e)
                browser = None

        crawl_one = build_adjudicate_crawl_one(
            session=session,
            browser=browser,
            timeout_sec=args.timeout,
            max_extra_pages=args.max_extra_pages,
            use_requests_only=args.use_requests,
            insecure_ssl=args.insecure_ssl,
        )

        orch = run_hybrid_batch(
            crawl_one,
            rows,
            use_adjudicator=True,
            llm_client=llm,
            llm_model=args.llm_model,
        )
    finally:
        if browser:
            try:
                browser.close()
            except Exception as e:
                logger.debug("browser.close: %s", e)
        if playwright:
            try:
                playwright.stop()
            except Exception as e:
                logger.debug("playwright.stop: %s", e)

    out_path = args.json_out if args.json_out else default_hybrid_run_path()
    save_hybrid_run(orch, out_path)
    logger.info("Wrote hybrid run: %s", out_path)
    if args.dated_run:
        dated = dated_hybrid_run_path()
        save_hybrid_run(orch, dated)
        logger.info("Dated comparison artifact: %s", dated)
    if args.eval_csv:
        write_hybrid_eval_csv(args.eval_csv, orch)
    if args.batch_eval:
        print_batch_eval_summary(orch)

    logger.info(
        "Summary: processed=%s assigned=%s manual_review=%s manual_review_low=%s "
        "unknown=%s failed=%s ai_calls=%s",
        orch.summary.n_processed,
        orch.summary.n_assigned,
        orch.summary.n_manual_review,
        orch.summary.n_manual_review_low_confidence,
        orch.summary.n_unknown,
        orch.summary.n_fetch_failed,
        orch.summary.adjudication_invoked,
    )

    for s in orch.sites:
        sd = s.site_result_dict
        print(
            f"{sd.get('url')}\t{s.merged_status}\t{s.merged_confidence}\t"
            f"{s.merged_best_canonical or sd.get('best_candidate_canonical') or ''}\t{s.source}"
        )

    return 0


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    ap = argparse.ArgumentParser(
        description="Multi-signal dealer group inference (Playwright preferred)."
    )
    ap.add_argument(
        "--db",
        type=Path,
        default=Path(os.environ.get("INVENTORY_DB_PATH", str(DEFAULT_DB))),
    )
    ap.add_argument("--manifest", type=Path, nargs="?", const=MANIFEST_DEFAULT, default=None)
    ap.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Max URLs to process (0 = all). Use for larger validation samples.",
    )
    ap.add_argument("--delay", type=float, default=0.4)
    ap.add_argument("--timeout", type=int, default=35)
    ap.add_argument("--max-extra-pages", type=int, default=5)
    ap.add_argument(
        "--use-requests",
        action="store_true",
        help="Use requests only (no Playwright)",
    )
    ap.add_argument(
        "--insecure-ssl",
        action="store_true",
        help="Ignore HTTPS certificate errors (Playwright + requests)",
    )
    ap.add_argument("--csv", type=Path, default=None)
    ap.add_argument("--json-out", type=Path, default=None)
    ap.add_argument("--test", action="store_true", help="Run 5 URLs from dealers.json")
    ap.add_argument(
        "--fixture-test",
        action="store_true",
        help="Run offline validation tests (org extraction, redirects)",
    )
    ap.add_argument(
        "--adjudicate",
        action="store_true",
        help="After crawl, run LLM adjudicator on ambiguous cases (OpenAI-compatible API).",
    )
    ap.add_argument(
        "--llm-base-url",
        default=os.environ.get("LLM_BASE_URL", "http://127.0.0.1:11434/v1"),
        help="OpenAI-compatible base URL (e.g. Ollama http://127.0.0.1:11434/v1)",
    )
    ap.add_argument(
        "--llm-model",
        default=os.environ.get("LLM_MODEL", "llama3.2"),
        help="Model id for /v1/chat/completions",
    )
    ap.add_argument(
        "--known-group-aliases",
        type=Path,
        default=None,
        help="JSON alias table (default: data/known_group_aliases.json). Merge-only; expands over time.",
    )
    ap.add_argument(
        "--dated-run",
        action="store_true",
        help="Also write a UTC-timestamped JSON under data/hybrid_runs/ for comparing runs.",
    )
    ap.add_argument(
        "--eval-csv",
        type=Path,
        default=None,
        help="Write evaluation CSV (site_url, final_status, canonical, confidence, source_mode, ...).",
    )
    ap.add_argument(
        "--batch-eval",
        action="store_true",
        help="Log extended batch summary (top assigned groups, unknown domains, fetch failures).",
    )
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()

    if args.fixture_test:
        from SCRAPING.fixture_tests import main as fixture_main

        return fixture_main()

    if args.quiet:
        logging.getLogger().setLevel(logging.WARNING)

    if args.insecure_ssl:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    install_sigint_handler(logger)

    if args.adjudicate:
        return _run_adjudicate_mode(args)

    if args.test:
        manifest = MANIFEST_DEFAULT
        if not manifest.is_file():
            logger.error("dealers.json not found for --test")
            return 1
        urls = load_roots_from_manifest(manifest)[:5]
        logger.info("--test: using %d URL(s) from dealers.json", len(urls))
    elif args.manifest is not None:
        if not args.manifest.is_file():
            logger.error("Manifest not found: %s", args.manifest)
            return 1
        urls = load_roots_from_manifest(args.manifest)
        logger.info("Loaded %d root URL(s) from manifest", len(urls))
    else:
        if not args.db.is_file():
            logger.error("Database not found: %s", args.db)
            return 1
        urls = load_roots_from_db(args.db)
        logger.info("Loaded %d root URL(s) from DB", len(urls))

    if args.limit > 0:
        urls = urls[: args.limit]

    results: list[SiteResult] = []
    session = fetch_requests_session(args.timeout, verify_ssl=not args.insecure_ssl)

    browser = None
    playwright = None

    try:
        if not args.use_requests:
            try:
                from playwright.sync_api import sync_playwright

                playwright = sync_playwright().start()
                browser = playwright.chromium.launch(headless=True)
                logger.info("Playwright Chromium ready (preferred mode)")
            except Exception as e:
                logger.warning("Playwright unavailable (%s); falling back to requests", e)
                browser = None

        for i, site in enumerate(urls):
            if stop_requested():
                logger.warning("Stopping early due to interrupt")
                break
            logger.info("(%d/%d) %s", i + 1, len(urls), site)
            try:
                r = process_site_hybrid(
                    session,
                    browser,
                    site,
                    args.timeout,
                    args.max_extra_pages,
                    use_requests_only=args.use_requests,
                    ignore_https_errors=args.insecure_ssl,
                )
                logger.info(
                    "  result: loaded=%s strategy=%s stack=%s vendor=%s status=%s conf=%s best=%s redirect_mismatch=%s flags=%s",
                    r.homepage_loaded,
                    r.crawl_strategy,
                    r.site_stack_family,
                    r.likely_vendor,
                    r.final_status,
                    r.confidence_score,
                    r.best_candidate_normalized,
                    r.redirect_mismatch,
                    r.flags,
                )
                results.append(r)
            except KeyboardInterrupt:
                logger.warning("KeyboardInterrupt — exiting loop")
                break
            except Exception as e:
                logger.exception("Unexpected error for %s: %s", site, e)
                results.append(_empty_site_result(site, "error", str(e)))

            if args.delay > 0 and i < len(urls) - 1 and not stop_requested():
                time.sleep(args.delay)

    finally:
        if browser:
            try:
                browser.close()
            except Exception as e:
                logger.debug("browser.close: %s", e)
        if playwright:
            try:
                playwright.stop()
            except Exception as e:
                logger.debug("playwright.stop: %s", e)

    serializable = [asdict(r) for r in results]

    out_path = args.json_out if args.json_out else default_json_results_path()
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(serializable, f, indent=2, ensure_ascii=False)
    logger.info("Wrote JSON: %s", out_path)

    if args.csv:
        with open(args.csv, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
            w.writeheader()
            for r in results:
                row = asdict(r)
                for k in (
                    "candidate_group_names",
                    "rejected_candidates",
                    "rejection_reasons",
                    "flags",
                    "redirect_chain",
                    "pages_checked",
                    "evidence_snippets",
                    "site_profile",
                ):
                    row[k] = json.dumps(row.get(k) or [])
                w.writerow(row)
        logger.info("Wrote CSV: %s", args.csv)

    for r in results:
        print(
            f"{r.url}\t{r.final_status}\t{r.confidence_score}\t{r.best_candidate_normalized or ''}\t{r.fetch_mode}"
        )

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        logging.getLogger("SCRAPING.cli").warning("Exiting on KeyboardInterrupt")
        sys.exit(130)
