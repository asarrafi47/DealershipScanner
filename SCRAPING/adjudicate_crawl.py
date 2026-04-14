"""
Shared crawl function for hybrid adjudication runs.

Keeps SCRAPING.cli adjudicate mode and OEM enrichment aligned (Playwright-first,
requests fallback when homepage does not load, same timeouts/extra pages).
"""
from __future__ import annotations

from collections.abc import Callable
from typing import Any

from SCRAPING.models import SiteResult


def build_adjudicate_crawl_one(
    *,
    session: Any,
    browser: Any | None,
    timeout_sec: int,
    max_extra_pages: int,
    use_requests_only: bool,
    insecure_ssl: bool,
) -> Callable[[str], SiteResult]:
    """
    Match SCRAPING.cli _run_adjudicate_mode crawl_one behavior.

    - If browser is set and not use_requests_only: Playwright first, then requests
      if homepage did not load (unless use_requests_only is True for the outer flow).
    - If no browser or use_requests_only: requests only.
    """
    from SCRAPING.crawler import process_site_playwright, process_site_requests

    def crawl_one(url: str) -> SiteResult:
        if browser and not use_requests_only:
            r = process_site_playwright(
                browser,
                url,
                timeout_ms=timeout_sec * 1000,
                max_extra_pages=max_extra_pages,
                ignore_https_errors=insecure_ssl,
            )
            if not r.homepage_loaded and not use_requests_only:
                r2 = process_site_requests(session, url, timeout_sec, max_extra_pages)
                if r2.homepage_loaded:
                    r2.fetch_mode = "playwright_then_requests"
                    return r2
            return r
        return process_site_requests(session, url, timeout_sec, max_extra_pages)

    return crawl_one
