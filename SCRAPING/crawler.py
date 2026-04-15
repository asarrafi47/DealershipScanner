"""Playwright + requests site crawlers: multi-page text collection and inference."""
from __future__ import annotations

import logging
import time
from typing import Any
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from SCRAPING.constants import USER_AGENT
from SCRAPING.fetch_requests import (
    HomepageFetchResult,
    fetch_homepage_full,
    fetch_homepage_requests,
)
from SCRAPING.html_extract import (
    collect_cross_domain_evidence_links,
    collect_internal_links,
    html_to_blobs,
)
from SCRAPING.entity_specificity import evidence_source_tier
from SCRAPING.inference import run_inference_on_blobs
from SCRAPING.interrupt import stop_requested
from SCRAPING.models import Evidence, SiteResult
from SCRAPING.org_validation import apply_status_for_fetch
from SCRAPING.redirects import describe_redirect
from SCRAPING.site_profile import (
    apply_profile_to_site_result,
    apply_site_profile_from_html,
    build_site_profile,
    build_site_profile_for_failed_peek,
)
from SCRAPING.text_utils import (
    classify_cross_domain_page_kind,
    classify_page_kind,
    collapse_ws,
    dns_check,
    is_vendor_text,
)

logger = logging.getLogger("SCRAPING.crawler")


def _ensure_failed_site_profile(res: SiteResult, crawl_strategy_executed: str) -> None:
    if res.site_profile:
        return
    prof = build_site_profile_for_failed_peek(
        res.final_url or res.url,
        error=res.fetch_error,
        flags=list(res.flags),
    )
    apply_profile_to_site_result(res, prof, crawl_strategy_executed=crawl_strategy_executed)


def _evidence_snippets_with_tiers(evidence_list: list[Evidence]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for e in evidence_list:
        out.append(
            {
                "snippet": e.snippet,
                "page_url": e.page_url,
                "page_kind": e.page_kind,
                "signal": e.signal,
                "score": round(e.weighted_score, 4),
                "cross_domain_evidence": e.cross_domain_evidence,
                "related_domain": e.related_domain,
                "evidence_tier": evidence_source_tier(
                    e.page_kind, e.cross_domain_evidence, e.signal
                ),
            }
        )
    return out


def safe_page_content(page: Any, timeout_ms: int) -> str:
    """Wait for load, retry on Playwright navigation/content races."""
    last_err: Exception | None = None
    cap = min(15000, timeout_ms)
    for attempt in range(3):
        try:
            page.wait_for_load_state("domcontentloaded", timeout=cap)
        except Exception:
            pass
        try:
            page.wait_for_load_state("load", timeout=cap)
        except Exception:
            pass
        try:
            return page.content()
        except Exception as e:
            last_err = e
            err_s = str(e).lower()
            if "navigating" in err_s or "content" in err_s or "closed" in err_s:
                time.sleep(0.35 + 0.2 * attempt)
                continue
            raise
    if last_err:
        raise last_err
    return page.content()


def _apply_redirect_fields(res: SiteResult, start_url: str) -> None:
    rdir, mismatch, fd = describe_redirect(start_url, res.final_url)
    res.redirected = rdir
    res.redirect_mismatch = mismatch
    res.final_domain = fd or urlparse(res.final_url).netloc.lower()
    if rdir:
        res.flags.append("redirect")
    if mismatch:
        res.flags.append("redirect_mismatch")


def _finalize_result(res: SiteResult, homepage_ok: bool) -> None:
    label = res.best_candidate_canonical or res.best_candidate_normalized
    vendor_only = bool(label and is_vendor_text(label))
    res.final_status = apply_status_for_fetch(
        homepage_ok,
        res.redirect_mismatch,
        res.confidence_score,
        label,
        vendor_only,
    )


def process_site_playwright(
    browser: Any,
    start_url: str,
    timeout_ms: int,
    max_extra_pages: int,
    ignore_https_errors: bool,
    max_cross_domain: int = 5,
    *,
    crawl_strategy_executed: str = "playwright_first",
) -> SiteResult:
    from playwright.sync_api import TimeoutError as PWTimeout

    res = SiteResult(
        url=start_url,
        original_url=start_url,
        fetch_mode="playwright",
        homepage_loaded=False,
        final_url=start_url,
    )
    host = urlparse(start_url).netloc
    ok, dns_msg = dns_check(host)
    if not ok:
        res.flags.append("dns_failure")
        res.fetch_error = dns_msg
        res.final_status = "fetch_failed"
        _ensure_failed_site_profile(res, crawl_strategy_executed)
        return res

    context = browser.new_context(
        user_agent=USER_AGENT,
        viewport={"width": 1280, "height": 900},
        ignore_https_errors=ignore_https_errors,
        locale="en-US",
    )
    page = context.new_page()
    all_blobs: list[tuple[str, str, str]] = []
    pages_checked: list[str] = []

    resp_headers: dict[str, str] = {}
    try:
        try:
            resp = page.goto(
                start_url, wait_until="domcontentloaded", timeout=timeout_ms
            )
        except PWTimeout:
            res.flags.append("timeout")
            res.fetch_error = "navigation_timeout"
            res.final_status = "fetch_failed"
            _ensure_failed_site_profile(res, crawl_strategy_executed)
            return res
        except Exception as e:
            msg = str(e)
            res.fetch_error = msg
            if "SSL" in msg or "ERR_CERT" in msg:
                res.flags.append("ssl_failure")
            elif "403" in msg or "Forbidden" in msg:
                res.flags.append("http_403")
            res.final_status = "fetch_failed"
            _ensure_failed_site_profile(res, crawl_strategy_executed)
            return res

        res.final_url = page.url
        _apply_redirect_fields(res, start_url)

        if resp:
            try:
                resp_headers = {str(k): str(v) for k, v in resp.headers.items()}
            except Exception:
                resp_headers = {}

        if resp and resp.status >= 400:
            res.fetch_error = f"http_{resp.status}"
            if resp.status == 403:
                res.flags.append("http_403")
            res.final_status = "fetch_failed"
            _ensure_failed_site_profile(res, crawl_strategy_executed)
            return res

        res.homepage_loaded = True
        pages_checked.append(res.final_url)

        try:
            html = safe_page_content(page, timeout_ms)
        except Exception as e:
            logger.info("  page.content failed after retries: %s", e)
            res.fetch_error = str(e)
            res.flags.append("content_read_failed")
            res.final_status = "fetch_failed"
            _ensure_failed_site_profile(res, crawl_strategy_executed)
            return res

        apply_site_profile_from_html(
            res,
            html,
            res.final_url,
            resp_headers,
            crawl_strategy_executed=crawl_strategy_executed,
        )

        blobs, soup = html_to_blobs(html, res.final_url, "homepage")
        all_blobs.extend(blobs)

        try:
            foot_el = page.query_selector("footer")
            if foot_el:
                ft = collapse_ws(foot_el.inner_text())
                if ft:
                    all_blobs.append((res.final_url, "footer", ft))
        except Exception:
            pass

        skip_evidence = res.redirect_mismatch
        links = collect_internal_links(res.final_url, soup)
        logger.info("  pages discovered (evidence links): %d", len(links))
        for extra in links[:max_extra_pages]:
            if stop_requested():
                break
            try:
                logger.info("  fetching %s", extra)
                page.goto(extra, wait_until="domcontentloaded", timeout=timeout_ms)
                time.sleep(0.25)
                kind = classify_page_kind(extra)
                txt = collapse_ws(page.inner_text("body"))
                all_blobs.append((page.url, kind, txt))
                pages_checked.append(page.url)
            except Exception as e:
                logger.info("  skip %s: %s", extra, e)

        xlinks, xskip = collect_cross_domain_evidence_links(
            res.final_url, soup, max_links=max_cross_domain
        )
        res.skipped_cross_domain = xskip[:200]
        for xu in xlinks:
            if stop_requested():
                break
            try:
                logger.info("  fetching cross-domain %s", xu)
                page.goto(xu, wait_until="domcontentloaded", timeout=timeout_ms)
                time.sleep(0.25)
                kind = classify_cross_domain_page_kind(page.url)
                txt = collapse_ws(page.inner_text("body"))
                all_blobs.append((page.url, kind, txt))
                pages_checked.append(page.url)
            except Exception as e:
                logger.info("  skip cross-domain %s: %s", xu, e)

        dealer_dom = urlparse(res.final_url).netloc.lower()
        raw_pre: list[str] = []
        cands, braw, bnorm, conf, ev, rej, sp2, spec_sc, ev_tier, sup_sig, bcanon = (
            run_inference_on_blobs(
                all_blobs,
                skip_evidence=skip_evidence,
                dealer_root_domain=dealer_dom,
                out_raw_pre_filter=raw_pre,
            )
        )
        res.candidate_group_names = cands
        res.entity_specificity_score = round(float(spec_sc), 4)
        res.best_evidence_tier = ev_tier
        res.best_supporting_signal = sup_sig
        res.raw_candidates_pre_filter = raw_pre[:200]
        res.normalized_candidates = list(cands)
        res.second_pass_candidates = list(dict.fromkeys(sp2))[:20]
        res.rejected_candidates = rej[:200]
        res.rejection_reasons = [
            f"{(r.get('text') or '')[:100]} :: {r.get('reason', '')}" for r in rej[:120]
        ]
        res.best_candidate_raw = braw
        res.best_candidate_normalized = bnorm
        res.best_candidate_canonical = bcanon
        res.best_candidate = bcanon
        res.confidence_score = round(conf, 4)
        res.evidence_snippets = _evidence_snippets_with_tiers(ev[:40])
        res.pages_checked = pages_checked

        _finalize_result(res, True)
        return res

    finally:
        try:
            context.close()
        except Exception as e:
            logger.debug("context.close: %s", e)


def process_site_requests(
    session: Any,
    start_url: str,
    timeout: int,
    max_extra_pages: int,
    max_cross_domain: int = 5,
    prefetch: HomepageFetchResult | None = None,
    *,
    crawl_strategy_executed: str = "requests_first",
) -> SiteResult:
    res = SiteResult(
        url=start_url,
        original_url=start_url,
        fetch_mode="requests",
        homepage_loaded=False,
        final_url=start_url,
    )
    hdrs: dict[str, str] = {}
    if prefetch is not None and prefetch.html and not prefetch.error:
        html = prefetch.html
        err = None
        chain = list(prefetch.redirect_chain)
        final = prefetch.final_url
        flags = list(prefetch.flags)
        hdrs = dict(prefetch.response_headers)
    else:
        fr = fetch_homepage_full(session, start_url, timeout)
        html, err, chain, final, flags = fr.html, fr.error, fr.redirect_chain, fr.final_url, fr.flags
        hdrs = dict(fr.response_headers)

    res.flags.extend(flags)
    res.redirect_chain = chain
    res.final_url = final
    _apply_redirect_fields(res, start_url)

    if err or not html:
        res.fetch_error = err or "no_body"
        res.final_status = "fetch_failed"
        _ensure_failed_site_profile(res, crawl_strategy_executed)
        return res

    res.homepage_loaded = True
    all_blobs: list[tuple[str, str, str]] = []
    pages_checked = [final]

    blobs, soup = html_to_blobs(html, final, "homepage")
    apply_site_profile_from_html(
        res,
        html,
        final,
        hdrs,
        crawl_strategy_executed=crawl_strategy_executed,
    )
    all_blobs.extend(blobs)
    skip_evidence = res.redirect_mismatch
    links = collect_internal_links(final, soup)
    logger.info("  pages discovered (evidence links): %d", len(links))

    for extra in links[:max_extra_pages]:
        if stop_requested():
            break
        h, e2, _, fin2, f2 = fetch_homepage_requests(session, extra, timeout)
        res.flags.extend(f2)
        if not h:
            logger.info("  skip %s: %s", extra, e2)
            continue
        logger.info("  fetched %s", fin2)
        kind = classify_page_kind(fin2)
        sp = BeautifulSoup(h, "html.parser")
        txt = collapse_ws(sp.get_text(separator=" "))
        all_blobs.append((fin2, kind, txt))
        pages_checked.append(fin2)

    xlinks, xskip = collect_cross_domain_evidence_links(final, soup, max_links=max_cross_domain)
    res.skipped_cross_domain = xskip[:200]
    for xu in xlinks:
        if stop_requested():
            break
        h, e2, _, fin2, f2 = fetch_homepage_requests(session, xu, timeout)
        res.flags.extend(f2)
        if not h:
            logger.info("  skip cross-domain %s: %s", xu, e2)
            continue
        logger.info("  fetched cross-domain %s", fin2)
        kind = classify_cross_domain_page_kind(fin2)
        sp = BeautifulSoup(h, "html.parser")
        txt = collapse_ws(sp.get_text(separator=" "))
        all_blobs.append((fin2, kind, txt))
        pages_checked.append(fin2)

    dealer_dom = urlparse(final).netloc.lower()
    raw_pre: list[str] = []
    cands, braw, bnorm, conf, ev, rej, sp2, spec_sc, ev_tier, sup_sig, bcanon = (
        run_inference_on_blobs(
            all_blobs,
            skip_evidence=skip_evidence,
            dealer_root_domain=dealer_dom,
            out_raw_pre_filter=raw_pre,
        )
    )
    res.candidate_group_names = cands
    res.entity_specificity_score = round(float(spec_sc), 4)
    res.best_evidence_tier = ev_tier
    res.best_supporting_signal = sup_sig
    res.raw_candidates_pre_filter = raw_pre[:200]
    res.normalized_candidates = list(cands)
    res.second_pass_candidates = list(dict.fromkeys(sp2))[:20]
    res.rejected_candidates = rej[:200]
    res.rejection_reasons = [
        f"{(r.get('text') or '')[:100]} :: {r.get('reason', '')}" for r in rej[:120]
    ]
    res.best_candidate_raw = braw
    res.best_candidate_normalized = bnorm
    res.best_candidate_canonical = bcanon
    res.best_candidate = bcanon
    res.confidence_score = round(conf, 4)
    res.evidence_snippets = _evidence_snippets_with_tiers(ev[:40])
    res.pages_checked = pages_checked

    _finalize_result(res, True)
    return res


def process_site_hybrid(
    session: Any,
    browser: Any | None,
    start_url: str,
    timeout: int,
    max_extra_pages: int,
    max_cross_domain: int = 5,
    *,
    use_requests_only: bool = False,
    ignore_https_errors: bool = False,
) -> SiteResult:
    """
    Profile-first crawl routing: requests for light/static stacks, Playwright when
    HTML suggests heavy JS / SPA or when requests fails (e.g. 403).
    """
    if use_requests_only:
        return process_site_requests(
            session,
            start_url,
            timeout,
            max_extra_pages,
            max_cross_domain,
            crawl_strategy_executed="requests_only",
        )

    prefetch = fetch_homepage_full(session, start_url, timeout)
    if prefetch.html and not prefetch.error:
        peek_prof = build_site_profile(
            prefetch.html, prefetch.final_url, prefetch.response_headers
        )
    else:
        peek_prof = build_site_profile_for_failed_peek(
            prefetch.final_url,
            error=prefetch.error,
            flags=prefetch.flags,
        )

    prefer_pw = (
        browser is not None
        and peek_prof.get("recommended_crawl_strategy") == "playwright_first"
    )

    if prefer_pw:
        r = process_site_playwright(
            browser,
            start_url,
            timeout * 1000,
            max_extra_pages,
            ignore_https_errors,
            max_cross_domain=max_cross_domain,
            crawl_strategy_executed="playwright_first",
        )
        if r.homepage_loaded:
            return r
        if prefetch.html and not prefetch.error:
            r2 = process_site_requests(
                session,
                start_url,
                timeout,
                max_extra_pages,
                max_cross_domain,
                prefetch=prefetch,
                crawl_strategy_executed="playwright_then_requests",
            )
            if r2.homepage_loaded:
                return r2
        r2 = process_site_requests(
            session,
            start_url,
            timeout,
            max_extra_pages,
            max_cross_domain,
            prefetch=None,
            crawl_strategy_executed="playwright_then_requests",
        )
        if r2.homepage_loaded:
            return r2
        return r

    r = process_site_requests(
        session,
        start_url,
        timeout,
        max_extra_pages,
        max_cross_domain,
        prefetch=prefetch if (prefetch.html and not prefetch.error) else None,
        crawl_strategy_executed="requests_first",
    )
    if r.homepage_loaded:
        return r
    if browser is not None:
        r3 = process_site_playwright(
            browser,
            start_url,
            timeout * 1000,
            max_extra_pages,
            ignore_https_errors,
            max_cross_domain=max_cross_domain,
            crawl_strategy_executed="requests_then_playwright",
        )
        if r3.homepage_loaded:
            return r3
    return r
