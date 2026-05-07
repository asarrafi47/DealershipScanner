"""
backend/utils/web_researcher.py
────────────────────────────────
Web research helper for the car-page chatbot.

Uses Playwright's *synchronous* API so it integrates cleanly into Flask's
sync request lifecycle — no asyncio.run() gymnastics required.

Strategy
────────
1. Open a stealth-configured Chromium context (headless=True, spoofed UA,
   realistic viewport, navigator.webdriver hidden).
2. Navigate to Brave Search (https://search.brave.com/search?q={query}).
   Brave is significantly more lenient with headless Chromium than DDG/Google.
3. Wait for 'networkidle' so JS-rendered result cards are fully in the DOM.
4. Collect ALL <a href> links on the page; keep only those that:
     a. start with "http"
     b. whose domain is not in the blocklist (brave.com, google.com, etc.)
   Take the first 3 survivors — layout-change-immune, no fragile CSS classes.
5. Navigate to the best result page; wait for domcontentloaded.
6. Extract text: try semantic containers (article → main → …) first.
   If none yield ≥ 200 chars, read body.innerText directly then drop lines
   with fewer than 10 words (kills navbars, footers, button labels).
7. Strip bare URLs / markdown noise, return the first 2 000 chars + source URL.

All failures are printed to stdout AND logged — the caller always degrades
gracefully to the inventory-only system prompt.
"""

from __future__ import annotations

import ipaddress
import logging
import os
import random
import re
import time
from typing import NamedTuple
from urllib.parse import urlparse, quote_plus

logger = logging.getLogger(__name__)

# ── Browser / stealth config ──────────────────────────────────────────────

_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

_LAUNCH_ARGS = [
    "--no-sandbox",
    "--disable-setuid-sandbox",
    "--disable-dev-shm-usage",
    "--disable-blink-features=AutomationControlled",
    "--window-size=1440,900",
]

_CONTEXT_HEADERS = {
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;"
        "q=0.9,image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "DNT": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Upgrade-Insecure-Requests": "1",
}

# ── Search engine ─────────────────────────────────────────────────────────
#
# Brave Search is significantly more lenient with headless Playwright than
# DuckDuckGo (which shows CAPTCHAs) or Google (which blocks outright).
# Results are rendered client-side; we wait for 'networkidle' to ensure
# they are fully present before we scan for links.

_SEARCH_URL = "https://search.brave.com/search?q={query}&source=web"

# ── Link blocklist ────────────────────────────────────────────────────────
#
# Substrings of hrefs to reject.  Kept as substrings so we catch subdomains
# (e.g. "news.google.com") without maintaining a separate suffix list.

_BLOCKED_HREF_SUBSTRINGS: tuple[str, ...] = (
    "brave.com",
    "google.com",
    "bing.com",
    "microsoft.com",
    "yahoo.com",
    "facebook.com",
    "instagram.com",
    "twitter.com",
    "x.com/",
    "tiktok.com",
    "youtube.com",
    "amazon.com",
    "ebay.com",
    "pinterest.com",
    "linkedin.com",
    "reddit.com",       # often paywalled / low-density content
)


def allowed_hosts_from_env() -> frozenset[str] | None:
    """
    Optional comma-separated ``WEB_RESEARCH_ALLOWED_HOSTS``.

    When non-empty, result navigation is limited to these hosts (case-insensitive),
    plus optional ``*.example.com`` patterns. When unset/empty, only the built-in
    blocklist + private-host guard apply.
    """
    raw = (os.environ.get("WEB_RESEARCH_ALLOWED_HOSTS") or "").strip()
    if not raw:
        return None
    parts = [x.strip().lower() for x in raw.split(",") if x.strip()]
    return frozenset(parts) if parts else None


def _host_matches_allowlist(hostname: str, allowlist: frozenset[str]) -> bool:
    h = hostname.lower().rstrip(".")
    if h in allowlist:
        return True
    for pat in allowlist:
        if pat.startswith("*."):
            root = pat[2:]
            if h == root or h.endswith("." + root):
                return True
    return False


def _destination_host_blocked(hostname: str) -> bool:
    """Block localhost, obvious SSRF literals, and RFC-private / special-use IPs."""
    if not hostname:
        return True
    hl = hostname.strip().lower().rstrip(".")
    if hl == "localhost" or hl.endswith(".localhost"):
        return True
    if hl.endswith(".local"):
        return True
    try:
        ip = ipaddress.ip_address(hl)
        return bool(
            ip.is_private
            or ip.is_loopback
            or ip.is_link_local
            or ip.is_multicast
            or ip.is_reserved
            or ip.is_unspecified
        )
    except ValueError:
        return False


def href_is_acceptable_result(
    href: str,
    *,
    allowed_hosts: frozenset[str] | None,
) -> bool:
    """
    Return True if *href* may be opened as a Brave search result target.

    Applies substring blocklist, private/special host guard, and optional
    ``WEB_RESEARCH_ALLOWED_HOSTS`` allowlist when configured.
    """
    if not href or not href.startswith("http"):
        return False
    try:
        parsed = urlparse(href)
    except Exception:
        return False
    host = (parsed.hostname or "").strip()
    if not host or _destination_host_blocked(host):
        return False
    low = href.lower()
    if any(blocked in low for blocked in _BLOCKED_HREF_SUBSTRINGS):
        return False
    if allowed_hosts is not None:
        if len(allowed_hosts) == 0:
            return False
        if not _host_matches_allowlist(host, allowed_hosts):
            return False
    return True


# ── Content extraction: semantic selectors (priority order) ───────────────

_CONTENT_SELECTORS = [
    "article",
    "[role='main']",
    "main",
    ".article-body",
    ".article-content",
    ".post-content",
    ".entry-content",
    ".content-body",
    "#article-body",
    "#main-content",
    "#content",
]

# Light in-place DOM cleanup before we read innerText from a container.
_CLEANUP_JS = """(el) => {
    el.querySelectorAll(
        'script, style, noscript, nav, header, footer, aside, ' +
        '.ad, .advertisement, .sidebar, [class*="sidebar"], ' +
        '[class*="nav-"], [id*="sidebar"], [id*="cookie"], ' +
        '[aria-hidden="true"]'
    ).forEach(n => n.remove());
}"""


# ── Result container ──────────────────────────────────────────────────────

class ResearchResult(NamedTuple):
    text: str       # First ≤2 000 chars of cleaned main content
    url: str        # Canonical source URL
    title: str      # Page title (for citation)


# ── WebResearcher ─────────────────────────────────────────────────────────

class WebResearcher:
    """
    Headless Playwright researcher.  Create one instance per request
    (or reuse across a request scope); do NOT share across threads.
    """

    def __init__(
        self,
        timeout_ms: int = 25_000,
        max_text_chars: int = 2_000,
        *,
        allowed_result_hosts: frozenset[str] | None = None,
    ) -> None:
        self.timeout_ms = timeout_ms
        self.max_text_chars = max_text_chars
        # None → use ``WEB_RESEARCH_ALLOWED_HOSTS`` (and built-in filters) per request
        self._allowed_result_hosts = allowed_result_hosts

    # ── Browser setup ─────────────────────────────────────────────────────

    def _make_context(self, playwright):
        """Return (browser, context) with stealth configuration."""
        try:
            browser = playwright.chromium.launch(
                headless=True,
                args=_LAUNCH_ARGS,
            )
        except Exception as exc:
            msg = str(exc)
            if (
                "Executable doesn't exist" in msg
                or "executable" in msg.lower()
                or "not found" in msg.lower()
            ):
                logger.error(
                    "Playwright Chromium binary not found. Run: python -m playwright install chromium"
                )
            raise

        context = browser.new_context(
            user_agent=_USER_AGENT,
            viewport={"width": 1440, "height": 900},
            locale="en-US",
            timezone_id="America/New_York",
            extra_http_headers=_CONTEXT_HEADERS,
        )
        # Hide the webdriver flag that headless browsers expose
        context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )
        return browser, context

    # ── Link filtering ────────────────────────────────────────────────────

    def _effective_allowed_hosts(self) -> frozenset[str] | None:
        if self._allowed_result_hosts is not None:
            return self._allowed_result_hosts
        return allowed_hosts_from_env()

    def _find_result_links(self, page, max_results: int = 3) -> list[str]:
        """
        Collect organic result URLs from the search results page.

        Scans ALL <a href> elements — no fragile CSS class selectors — and
        returns the first *max_results* that pass the blocklist filter.
        This approach is completely immune to search-engine layout changes.
        """
        allow = self._effective_allowed_hosts()
        found: list[str] = []
        try:
            anchors = page.query_selector_all("a[href]")
            logger.debug("[WebResearcher] anchor count=%d", len(anchors))
            for anchor in anchors:
                try:
                    href = (anchor.get_attribute("href") or "").strip()
                    if (
                        href_is_acceptable_result(href, allowed_hosts=allow)
                        and href not in found
                    ):
                        found.append(href)
                        if len(found) >= max_results:
                            break
                except Exception:
                    continue
        except Exception as exc:
            logger.warning("[WebResearcher] Link scan failed: %s", exc)

        return found

    # ── Content extraction ────────────────────────────────────────────────

    @staticmethod
    def _filter_short_lines(text: str, min_words: int = 10) -> str:
        """
        Remove lines with fewer than *min_words* words.

        Navigation items, breadcrumbs, button labels, cookie banners, and
        ad copy almost always sit on their own short lines ("Home", "Buy Now",
        "Accept Cookies").  Real editorial content is always in full sentences.
        Raising the threshold to 10 words is more aggressive than 8 and
        eliminates more noise with negligible loss of body text.
        """
        kept = [
            line for line in text.splitlines()
            if len(line.split()) >= min_words
        ]
        return "\n".join(kept)

    @staticmethod
    def _clean_text(raw: str) -> str:
        """Collapse whitespace; strip bare URLs and markdown link syntax."""
        text = re.sub(r"https?://\S+", "", raw)
        text = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", text)
        text = re.sub(r"[ \t]{2,}", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    def _extract_content(self, page) -> str:
        """
        Extract the main editorial text from an article page.

        Priority order:
          1. Semantic container (article, main, etc.) with in-page DOM cleanup.
          2. Full body.innerText with the short-line filter.

        The body fallback reads the DOM directly with page.inner_text("body"),
        which works even on heavily JS-rendered pages where query_selector
        sometimes returns stale/empty nodes.
        """
        # ── Priority: semantic containers ─────────────────────────────────
        for sel in _CONTENT_SELECTORS:
            try:
                el = page.query_selector(sel)
                if not el:
                    continue
                page.evaluate(_CLEANUP_JS, el)
                text = (el.inner_text() or "").strip()
                if len(text) >= 200:
                    return text
            except Exception:
                continue

        # ── Fallback: full body innerText + short-line filter ─────────────
        try:
            raw = page.inner_text("body")
            if raw:
                return self._filter_short_lines(raw.strip())
        except Exception:
            pass

        # ── Last resort: evaluate JS directly ─────────────────────────────
        try:
            raw = page.evaluate("document.body.innerText") or ""
            return self._filter_short_lines(raw.strip())
        except Exception:
            pass

        return ""

    # ── public API ────────────────────────────────────────────────────────

    def search_and_summarize(self, query: str) -> ResearchResult | None:
        """
        Search Brave for *query*, navigate to the first high-quality organic
        result, and return a ResearchResult or None on any failure.
        """
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            logger.error(
                "playwright package not installed; run: pip install playwright && python -m playwright install chromium"
            )
            return None

        search_url = _SEARCH_URL.format(query=quote_plus(query))
        logger.info(
            "[WebResearcher] starting | query_len=%d allowed_hosts=%s",
            len(query),
            "env"
            if self._allowed_result_hosts is None
            else "override",
        )
        logger.debug("[WebResearcher] search_url=%s", search_url)

        with sync_playwright() as pw:
            browser, ctx = self._make_context(pw)
            try:
                page = ctx.new_page()

                # ── 1. Load Brave Search results ──────────────────────────
                logger.debug("[WebResearcher] loading Brave search results …")
                try:
                    page.goto(
                        search_url,
                        wait_until="domcontentloaded",
                        timeout=self.timeout_ms,
                    )

                    # Wait for network to go idle (JS-rendered results settle)
                    try:
                        page.wait_for_load_state(
                            "networkidle",
                            timeout=12_000,
                        )
                        logger.debug("[WebResearcher] networkidle reached")
                    except Exception:
                        # networkidle can time out on busy pages; continue anyway
                        delay = random.uniform(1.5, 2.5)
                        logger.debug(
                            "[WebResearcher] networkidle timed out; waiting %.1fs", delay
                        )
                        time.sleep(delay)

                except Exception as exc:
                    logger.warning("[WebResearcher] Brave Search navigation failed: %s", exc)
                    return None

                # ── 2. Collect organic result URLs ────────────────────────
                candidates = self._find_result_links(page, max_results=3)
                logger.debug("[WebResearcher] candidates=%d", len(candidates))

                if not candidates:
                    logger.warning("[WebResearcher] No acceptable result links (query_len=%d)", len(query))
                    return None

                result_url   = candidates[0]
                result_title = ""
                logger.info("[WebResearcher] selected host=%s", urlparse(result_url).hostname or "")

                # ── 3. Navigate to the result page ────────────────────────
                result_page = ctx.new_page()
                try:
                    result_page.goto(
                        result_url,
                        wait_until="domcontentloaded",
                        timeout=self.timeout_ms,
                    )
                    # Brief pause for lazy-loaded content
                    time.sleep(random.uniform(0.8, 1.4))
                    result_title = result_page.title() or ""
                    logger.debug("[WebResearcher] page title len=%d", len(result_title))
                except Exception as exc:
                    logger.warning(
                        "[WebResearcher] Result page navigation failed for %s: %s",
                        result_url,
                        exc,
                    )
                    return None

                # ── 4. Extract + clean text ───────────────────────────────
                raw_text = self._extract_content(result_page)
                clean    = self._clean_text(raw_text)
                logger.debug("[WebResearcher] extracted raw_chars=%d", len(clean))

                if len(clean) < 80:
                    logger.warning(
                        "[WebResearcher] Content too short (%d chars) at %s",
                        len(clean),
                        result_url,
                    )
                    return None

                snippet = clean[: self.max_text_chars]
                logger.info("[WebResearcher] success chars=%d", len(snippet))
                return ResearchResult(
                    text=snippet,
                    url=result_url,
                    title=result_title,
                )

            finally:
                try:
                    ctx.close()
                    browser.close()
                except Exception:
                    pass
