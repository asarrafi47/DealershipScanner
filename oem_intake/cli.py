"""CLI: BMW OEM intake, normalize/dedupe, enrichment, export, reporting."""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from SCRAPING.paths import ROOT

from oem_intake.bmw_pipeline import (
    build_report,
    export_normalized_csv,
    export_partial_rows_review,
    export_summary_json,
    export_zip_diagnostics_from_latest_debug,
    run_bmw_enrichment,
    run_bmw_ingest,
    run_bmw_locator_dom_inspect,
    run_bmw_normalize_dedupe_from_raw,
)

logger = logging.getLogger("oem_intake.cli")


def _cmd_bmw_ingest(args: argparse.Namespace) -> int:
    zip_seeds = tuple(z.strip() for z in (args.zip_seeds or "").split(",") if z.strip())
    bundle, stats = run_bmw_ingest(
        project_root=ROOT,
        use_fixture=args.fixture,
        prefer_playwright=args.playwright,
        timeout=args.timeout,
        verify_ssl=not args.insecure_ssl,
        debug_live=getattr(args, "debug_live", False),
        test_url=getattr(args, "test_url", None),
        zip_seeds=zip_seeds or ("92606", "90807"),
        manual_input_selector=getattr(args, "input_selector", None),
        manual_result_selector=getattr(args, "result_selector", None),
        ai_selector_assist=getattr(args, "ai_selector_assist", False),
        llm_base_url=getattr(args, "llm_base_url", None),
        llm_model=getattr(args, "llm_model", None),
    )
    out: dict = {
        "records_from_source": len(bundle.records),
        "raw_rows_inserted": stats.raw_inserted,
        "raw_duplicate_fingerprints": stats.raw_duplicate_fingerprints,
        "normalized_processed": stats.normalized_upserts,
        "duplicates_merged": stats.duplicates_merged,
        "new_unique_normalized": stats.new_unique_normalized,
        "notes": bundle.notes,
    }
    if bundle.debug_report is not None and isinstance(bundle.debug_report, dict):
        dr = bundle.debug_report
        pw = (
            (dr.get("playwright_phase") or {}).get("playwright_network_json", [])
            if dr.get("combined")
            else dr.get("playwright_network_json", [])
        )
        out["debug_summary"] = {
            "debug_mode": dr.get("mode"),
            "rows_extracted": dr.get("rows_extracted"),
            "candidate_url_count": dr.get("candidate_url_count"),
            "playwright_network_json_count": len(pw),
            "combined_requests_and_playwright": bool(dr.get("combined")),
            "shortlist_counts": (dr.get("shortlist") or {}).get("counts"),
            "json_parser_rows_found": dr.get("json_parser_rows_found"),
            "html_script_candidates": len((dr.get("html_script_inspection") or {}).get("script_candidates", [])),
        }
    print(json.dumps(out, indent=2))
    if not bundle.records and not args.fixture:
        logger.warning(
            "No dealer rows parsed. Add JSON URLs to data/oem/bmw_endpoint_overrides.json "
            "or run with --playwright (or --fixture for offline sample)."
        )
        return 2
    return 0


def _cmd_bmw_inspect_locator(args: argparse.Namespace) -> int:
    zip_seeds = tuple(z.strip() for z in (args.zip_seeds or "").split(",") if z.strip())
    out = run_bmw_locator_dom_inspect(
        project_root=ROOT,
        timeout=args.timeout,
        verify_ssl=not args.insecure_ssl,
        zip_seeds=zip_seeds or ("92606", "90807"),
        manual_input_selector=getattr(args, "input_selector", None),
        manual_result_selector=getattr(args, "result_selector", None),
        ai_selector_assist=getattr(args, "ai_selector_assist", False),
        llm_base_url=getattr(args, "llm_base_url", None),
        llm_model=getattr(args, "llm_model", None),
    )
    print(json.dumps(out, indent=2))
    return 0


def _cmd_bmw_normalize(args: argparse.Namespace) -> int:
    out = run_bmw_normalize_dedupe_from_raw(ROOT)
    print(json.dumps(out, indent=2))
    return 0


def _cmd_bmw_dedupe(args: argparse.Namespace) -> int:
    return _cmd_bmw_normalize(args)


def _cmd_bmw_enrich(args: argparse.Namespace) -> int:
    r = run_bmw_enrichment(
        limit=args.limit or 0,
        use_requests_only=args.use_requests,
        timeout=args.timeout,
        max_extra_pages=args.max_extra_pages,
        insecure_ssl=args.insecure_ssl,
        llm_base_url=args.llm_base_url,
        llm_model=args.llm_model,
        hybrid_out=args.hybrid_out,
    )
    print(json.dumps(r, indent=2, default=str))
    if r.get("error"):
        return 2
    return 0


def _cmd_bmw_report(args: argparse.Namespace) -> int:
    r = build_report(ROOT)
    print(json.dumps(r, indent=2, default=str))
    if args.json_out:
        export_summary_json(Path(args.json_out), r, ingest_extra=None)
        print("Wrote", args.json_out)
    return 0


def _cmd_bmw_export(args: argparse.Namespace) -> int:
    path = Path(args.out or (ROOT / "data" / "oem" / "bmw" / "bmw_normalized_export.csv"))
    n = export_normalized_csv(path, ROOT)
    print(f"Wrote {n} rows to {path}")
    return 0


def _cmd_bmw_export_partial(args: argparse.Namespace) -> int:
    out = export_partial_rows_review(Path(args.out) if args.out else None, ROOT)
    print(json.dumps(out, indent=2))
    return 0


def _cmd_bmw_export_zip_diag(args: argparse.Namespace) -> int:
    out = export_zip_diagnostics_from_latest_debug(
        Path(args.out) if args.out else None, ROOT
    )
    print(json.dumps(out, indent=2))
    return 0


def _cmd_bmw_run_all(args: argparse.Namespace) -> int:
    rc = _cmd_bmw_ingest(args)
    if rc != 0 and not args.continue_on_empty_ingest:
        return rc
    run_bmw_normalize_dedupe_from_raw(ROOT)
    if args.enrich:
        er = run_bmw_enrichment(
            limit=args.enrich_limit or 0,
            use_requests_only=getattr(args, "enrich_use_requests", False),
            timeout=getattr(args, "enrich_timeout", 35),
            max_extra_pages=getattr(args, "enrich_max_extra_pages", 5),
            insecure_ssl=args.insecure_ssl,
            llm_base_url=args.llm_base_url,
            llm_model=args.llm_model,
            hybrid_out=args.hybrid_out,
        )
        print("enrichment:", json.dumps(er, indent=2, default=str))
    export_normalized_csv(Path(args.export_csv or (ROOT / "data" / "oem" / "bmw" / "bmw_normalized_export.csv")))
    r = build_report(ROOT)
    print("report:", json.dumps(r, indent=2, default=str))
    if args.summary_json:
        export_summary_json(Path(args.summary_json), r, ingest_extra={"run_all": True})
    return 0


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    p = argparse.ArgumentParser(description="OEM dealership intake (BMW USA first)")
    sub = p.add_subparsers(dest="oem", required=True)

    bmw = sub.add_parser("bmw", help="BMW USA locator pipeline")
    bsub = bmw.add_subparsers(dest="bmw_cmd", required=True)

    pi = bsub.add_parser("ingest", help="Fetch from BMW USA locator and persist raw + normalized")
    pi.add_argument("--fixture", action="store_true", help="Use data/oem/fixtures/bmw_locator_sample.json")
    pi.add_argument("--playwright", action="store_true", help="Use Playwright capture only (skip requests-first)")
    pi.add_argument(
        "--debug-live",
        action="store_true",
        help="Write data/oem/bmw/debug/bmw_ingest_debug_*.json (locator + candidate URLs + parse traces)",
    )
    pi.add_argument(
        "--test-url",
        type=str,
        default=None,
        help="Fetch this JSON URL first (same as adding to data/oem/bmw_endpoint_overrides.json)",
    )
    pi.add_argument("--timeout", type=int, default=60)
    pi.add_argument(
        "--zip-seeds",
        type=str,
        default="92606,90807",
        help="Comma-separated ZIP seeds for interactive locator search (debug/live discovery).",
    )
    pi.add_argument("--insecure-ssl", action="store_true")
    pi.add_argument("--input-selector", type=str, default=None, help="Manual locator input selector override (debug mode).")
    pi.add_argument("--result-selector", type=str, default=None, help="Manual result-container selector override (debug mode).")
    pi.add_argument("--ai-selector-assist", action="store_true", help="Use LLM selector adjudicator fallback in debug/live Playwright flow.")
    pi.add_argument("--llm-base-url", default=None, help="LLM base URL for AI selector assist (OpenAI-compatible).")
    pi.add_argument("--llm-model", default=None, help="LLM model for AI selector assist.")
    pi.set_defaults(func=_cmd_bmw_ingest)

    pid = bsub.add_parser(
        "inspect-locator",
        help="Playwright locator DOM inspection/debug pass (no intake persistence)",
    )
    pid.add_argument("--timeout", type=int, default=60)
    pid.add_argument("--insecure-ssl", action="store_true")
    pid.add_argument(
        "--zip-seeds",
        type=str,
        default="92606,90807,10001,30301",
        help="Comma-separated ZIP seeds for interaction debug pass.",
    )
    pid.add_argument("--input-selector", type=str, default=None, help="Manual locator input selector override.")
    pid.add_argument("--result-selector", type=str, default=None, help="Manual result-container selector override.")
    pid.add_argument("--ai-selector-assist", action="store_true", help="Use LLM selector adjudicator fallback.")
    pid.add_argument("--llm-base-url", default=None, help="LLM base URL for AI selector assist.")
    pid.add_argument("--llm-model", default=None, help="LLM model for AI selector assist.")
    pid.set_defaults(func=_cmd_bmw_inspect_locator)

    bsub.add_parser("normalize", help="Rebuild normalized dealers from raw (dedupe keys + merge history)").set_defaults(
        func=_cmd_bmw_normalize
    )
    bsub.add_parser("dedupe", help="Alias for normalize").set_defaults(func=_cmd_bmw_dedupe)

    pe = bsub.add_parser(
        "enrich",
        help="Run hybrid inference (default: Playwright + requests fallback, same as SCRAPING --adjudicate)",
    )
    pe.add_argument("--limit", type=int, default=0, help="Max dealers (0 = all with websites)")
    pe.add_argument(
        "--use-requests",
        action="store_true",
        help="HTTP-only crawl (skip Playwright; matches SCRAPING --use-requests)",
    )
    pe.add_argument("--timeout", type=int, default=35, help="Per-site timeout seconds (adjudicate default: 35)")
    pe.add_argument(
        "--max-extra-pages",
        type=int,
        default=5,
        help="Extra pages to fetch per site (adjudicate default: 5)",
    )
    pe.add_argument("--insecure-ssl", action="store_true")
    pe.add_argument("--llm-base-url", default=None)
    pe.add_argument("--llm-model", default=None)
    pe.add_argument(
        "--hybrid-out",
        type=Path,
        default=None,
        help="Hybrid JSON output path (default data/oem/bmw/bmw_enrichment_hybrid.json)",
    )
    pe.set_defaults(func=_cmd_bmw_enrich)

    pr = bsub.add_parser("report", help="Counts + optional last enrichment summary")
    pr.add_argument("--json-out", type=str, default=None)
    pr.set_defaults(func=_cmd_bmw_report)

    px = bsub.add_parser("export", help="CSV export of normalized BMW dealers")
    px.add_argument("--out", type=str, default=None)
    px.set_defaults(func=_cmd_bmw_export)

    ppx = bsub.add_parser("export-partial", help="Export partial staging review with missing-field reason codes")
    ppx.add_argument("--out", type=str, default=None, help="Output path (.csv or .json)")
    ppx.set_defaults(func=_cmd_bmw_export_partial)

    pzd = bsub.add_parser("export-zip-diagnostics", help="Export per-ZIP diagnostics from latest ingest debug artifact")
    pzd.add_argument("--out", type=str, default=None, help="Output path (.csv or .json)")
    pzd.set_defaults(func=_cmd_bmw_export_zip_diag)

    pa = bsub.add_parser(
        "run-all",
        help="ingest → normalize → optional enrich → export + report",
    )
    pa.add_argument("--fixture", action="store_true")
    pa.add_argument("--playwright", action="store_true")
    pa.add_argument("--timeout", type=int, default=60)
    pa.add_argument("--insecure-ssl", action="store_true")
    pa.add_argument("--continue-on-empty-ingest", action="store_true")
    pa.add_argument("--enrich", action="store_true", help="Run hybrid enrichment after ingest")
    pa.add_argument("--enrich-limit", type=int, default=0)
    pa.add_argument(
        "--enrich-use-requests",
        action="store_true",
        help="Enrich with HTTP-only (no Playwright)",
    )
    pa.add_argument("--enrich-timeout", type=int, default=35)
    pa.add_argument("--enrich-max-extra-pages", type=int, default=5)
    pa.add_argument("--debug-live", action="store_true", help="Pass through to ingest")
    pa.add_argument("--test-url", type=str, default=None, help="Pass through to ingest")
    pa.add_argument(
        "--zip-seeds",
        type=str,
        default="92606,90807",
        help="Pass through to ingest",
    )
    pa.add_argument("--llm-base-url", default=None)
    pa.add_argument("--llm-model", default=None)
    pa.add_argument("--hybrid-out", type=Path, default=None)
    pa.add_argument(
        "--export-csv",
        type=str,
        default=None,
        help="Normalized CSV path",
    )
    pa.add_argument("--summary-json", type=str, default=None)
    pa.set_defaults(func=_cmd_bmw_run_all)

    args = p.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
