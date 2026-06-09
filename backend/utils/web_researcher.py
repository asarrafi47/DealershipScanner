"""
backend/utils/web_researcher.py
────────────────────────────────
Web research helper for the car-page chatbot.

Uses Playwright's *synchronous* API so it integrates cleanly into Flask's
sync request lifecycle — no asyncio.run() gymnastics required.

Strategy
────────
1. Search DuckDuckGo HTML (``html.duckduckgo.com``) via plain HTTP — no CAPTCHA for
   batch/headless use. Fall back to Brave Search + Playwright when DDG returns nothing.
2. For each organic result URL, fetch page text via HTTP first (fast). If too short,
   open with headless Chromium and extract semantic main content.
3. Apply host blocklist + optional ``WEB_RESEARCH_ALLOWED_HOSTS`` on every URL.
4. Return the first result with ≥ 80 chars of cleaned editorial text.

Brave Search alone often shows a bot-verification page to headless Chromium; DDG HTML
is the primary path as of 2026.
"""

from __future__ import annotations

import logging
import os
import random
import re
import time
import urllib.error
import urllib.request
from html import unescape
from typing import NamedTuple
from urllib.parse import parse_qs, quote_plus, unquote, urlparse

from backend.utils.outbound_url import destination_host_blocked as _destination_host_blocked

logger = logging.getLogger(__name__)

# ── Browser / stealth config ──────────────────────────────────────────────

_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

_LAUNCH_ARGS = [
    "--disable-setuid-sandbox",
    "--disable-dev-shm-usage",
    "--disable-blink-features=AutomationControlled",
    "--window-size=1440,900",
]


def _playwright_launch_args() -> list[str]:
    """Chromium flags; --no-sandbox only when PLAYWRIGHT_NO_SANDBOX=1 (e.g. some containers)."""
    args = list(_LAUNCH_ARGS)
    if (os.environ.get("PLAYWRIGHT_NO_SANDBOX") or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    ):
        return ["--no-sandbox", *args]
    return args

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
_DDG_HTML_URL = "https://html.duckduckgo.com/html/?q={query}"


def _slug_for_url(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", (text or "").lower()).strip("-")


def direct_trim_guide_urls(year: int, make: str, model: str) -> list[str]:
    """
    Known URL patterns for trim/spec guides — no search engine required.

    Used first because Brave/DuckDuckGo often CAPTCHA headless or scripted clients.
    """
    mk = _slug_for_url(make)
    md = _slug_for_url(model)
    if not mk or not md or year < 1980:
        return []
    return [
        f"https://www.motortrend.com/cars/{mk}/{md}/",
        f"https://www.caranddriver.com/{mk}/{md}",
        f"https://www.carwow.co.uk/{mk}/{md}/{year}/specifications",
        f"https://www.motorpoint.co.uk/guides/{mk}-{md}-models-and-trim-levels-explained",
    ]

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


def duckduckgo_html_result_links(
    query: str,
    *,
    allowed_hosts: frozenset[str] | None,
    max_results: int = 5,
) -> list[str]:
    """Return de-duplicated organic URLs from DuckDuckGo's static HTML results page."""
    url = _DDG_HTML_URL.format(query=quote_plus(query))
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": _USER_AGENT,
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "en-US,en;q=0.9",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            html = resp.read().decode("utf-8", errors="replace")
    except (urllib.error.URLError, OSError, TimeoutError) as exc:
        logger.warning("[WebResearcher] DuckDuckGo HTML search failed: %s", exc)
        return []

    if _is_duckduckgo_bot_page(html):
        logger.warning("[WebResearcher] DuckDuckGo bot verification page — skipping")
        return []

    found: list[str] = []
    for m in re.finditer(r'href="(//duckduckgo\.com/l/\?[^"]+)"', html):
        href = "https:" + m.group(1)
        try:
            qs = parse_qs(urlparse(href).query)
            target = unquote(qs.get("uddg", [""])[0]).strip()
        except Exception:
            continue
        if (
            target
            and href_is_acceptable_result(target, allowed_hosts=allowed_hosts)
            and target not in found
        ):
            found.append(target)
            if len(found) >= max_results:
                break
    logger.debug("[WebResearcher] duckduckgo links=%d query_len=%d", len(found), len(query))
    return found


def fetch_page_text_http(url: str, *, timeout_sec: int = 20) -> tuple[str, str]:
    """
    Fetch *url* with urllib and return ``(title, plain_text)``.

    Strips scripts/styles/tags; not suitable for heavy JS SPAs but fast for guides.
    """
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": _USER_AGENT,
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "en-US,en;q=0.9",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
        html = resp.read().decode("utf-8", errors="replace")
    title_m = re.search(r"<title[^>]*>(.*?)</title>", html, flags=re.I | re.S)
    title = unescape(re.sub(r"\s+", " ", title_m.group(1))).strip() if title_m else ""
    html = re.sub(r"<script[^>]*>.*?</script>", " ", html, flags=re.I | re.S)
    html = re.sub(r"<style[^>]*>.*?</style>", " ", html, flags=re.I | re.S)
    text = re.sub(r"<[^>]+>", " ", html)
    text = unescape(re.sub(r"\s+", " ", text)).strip()
    return title, text


def _is_brave_bot_page(body_text: str) -> bool:
    low = (body_text or "").lower()[:800]
    return "verifying you're not a bot" in low or "quick check before you continue" in low


def _is_duckduckgo_bot_page(html: str) -> bool:
    low = (html or "").lower()[:4000]
    return "unfortunately, bots use duckduckgo" in low or "anomaly-modal" in low


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
                args=_playwright_launch_args(),
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

    def _collect_search_candidates(
        self,
        query: str,
        *,
        year: int = 0,
        make: str = "",
        model: str = "",
        max_results: int = 8,
    ) -> list[str]:
        allow = self._effective_allowed_hosts()
        candidates: list[str] = []

        if year and make and model:
            for url in direct_trim_guide_urls(year, make, model):
                if href_is_acceptable_result(url, allowed_hosts=allow) and url not in candidates:
                    candidates.append(url)
            if candidates:
                logger.info("[WebResearcher] direct guide urls=%d", len(candidates))

        if len(candidates) < max_results:
            for url in duckduckgo_html_result_links(
                query, allowed_hosts=allow, max_results=max_results
            ):
                if url not in candidates:
                    candidates.append(url)
            if candidates and not year:
                logger.info("[WebResearcher] duckduckgo candidates=%d", len(candidates))

        if len(candidates) < 2:
            for url in self._brave_search_links(query, max_results=max_results):
                if url not in candidates:
                    candidates.append(url)

        return candidates[:max_results]

    def _brave_search_links(self, query: str, *, max_results: int = 5) -> list[str]:
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            return []

        search_url = _SEARCH_URL.format(query=quote_plus(query))
        with sync_playwright() as pw:
            browser, ctx = self._make_context(pw)
            try:
                page = ctx.new_page()
                try:
                    page.goto(
                        search_url,
                        wait_until="domcontentloaded",
                        timeout=self.timeout_ms,
                    )
                    try:
                        page.wait_for_load_state("networkidle", timeout=12_000)
                    except Exception:
                        time.sleep(random.uniform(1.0, 2.0))
                except Exception as exc:
                    logger.warning("[WebResearcher] Brave Search navigation failed: %s", exc)
                    return []
                try:
                    body = page.inner_text("body") or ""
                except Exception:
                    body = ""
                if _is_brave_bot_page(body):
                    logger.warning("[WebResearcher] Brave bot verification page — skipping")
                    return []
                return self._find_result_links(page, max_results=max_results)
            finally:
                try:
                    ctx.close()
                    browser.close()
                except Exception:
                    pass
        return []

    def _fetch_with_playwright(self, result_url: str) -> tuple[str, str]:
        from playwright.sync_api import sync_playwright

        with sync_playwright() as pw:
            browser, ctx = self._make_context(pw)
            try:
                page = ctx.new_page()
                page.goto(
                    result_url,
                    wait_until="domcontentloaded",
                    timeout=self.timeout_ms,
                )
                time.sleep(random.uniform(0.6, 1.2))
                title = page.title() or ""
                raw_text = self._extract_content(page)
                return title, raw_text
            finally:
                try:
                    ctx.close()
                    browser.close()
                except Exception:
                    pass
        return "", ""

    def _result_from_page(self, result_url: str, title: str, raw_text: str) -> ResearchResult | None:
        filtered = self._filter_short_lines(raw_text.strip()) if raw_text else ""
        clean = self._clean_text(filtered)
        if len(clean) < 80:
            logger.debug(
                "[WebResearcher] content too short (%d chars) at %s",
                len(clean),
                result_url,
            )
            return None
        snippet = clean[: self.max_text_chars]
        logger.info(
            "[WebResearcher] success host=%s chars=%d",
            urlparse(result_url).hostname or "",
            len(snippet),
        )
        return ResearchResult(text=snippet, url=result_url, title=title)

    # ── public API ────────────────────────────────────────────────────────

    def search_and_summarize(
        self,
        query: str,
        *,
        year: int = 0,
        make: str = "",
        model: str = "",
    ) -> ResearchResult | None:
        """
        Search for *query*, fetch the first useful organic result page, and return
        cleaned text or None on failure.

        When *year*, *make*, and *model* are provided, tries direct automotive guide
        URLs before search engines (avoids CAPTCHA on Brave/DuckDuckGo).
        """
        logger.info(
            "[WebResearcher] starting | query_len=%d ymm=%s allowed_hosts=%s",
            len(query),
            f"{year} {make} {model}".strip() if year else "",
            "env" if self._allowed_result_hosts is None else "override",
        )

        candidates = self._collect_search_candidates(
            query, year=year, make=make, model=model, max_results=8
        )
        if not candidates:
            logger.warning("[WebResearcher] No search result links (query_len=%d)", len(query))
            return None

        for result_url in candidates:
            logger.info("[WebResearcher] trying host=%s", urlparse(result_url).hostname or "")

            try:
                title, raw_text = fetch_page_text_http(result_url)
                out = self._result_from_page(result_url, title, raw_text)
                if out:
                    return out
            except (urllib.error.URLError, OSError, TimeoutError) as exc:
                logger.debug("[WebResearcher] HTTP fetch failed for %s: %s", result_url, exc)

            try:
                title, raw_text = self._fetch_with_playwright(result_url)
                out = self._result_from_page(result_url, title, raw_text)
                if out:
                    return out
            except Exception as exc:
                logger.debug("[WebResearcher] Playwright fetch failed for %s: %s", result_url, exc)

        logger.warning("[WebResearcher] All candidates failed (query_len=%d)", len(query))
        return None
