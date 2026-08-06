"""Scraper fallback chain (ported from zumai's ScraperChain framework).

Two-stage pipeline, each stage a chain of strategies tried in order until one
works:

  1. Fetch    — get raw HTML for a listings/VDP URL (requests -> Playwright)
  2. Extract  — turn HTML into structured vehicles (caller-supplied extractors,
                e.g. platform API probes -> LLM -> BeautifulSoup heuristic)

Manual input bypasses fetch (caller supplies HTML directly via ``run(url,
html=...)``; the fetch strategy is then recorded as ``"manual"``).

Short-circuit semantics (preserved verbatim from zumai):

* A fetcher "wins" iff it returns non-None HTML whose ``.strip()`` is truthy.
  Empty output is recorded as ``"{name}: empty"`` and the chain continues.
  Exceptions are caught, logged at WARNING, recorded as ``"{name}: {e}"``,
  and the chain continues.
* An extractor "wins" iff it returns a non-empty list. Returning ``[]`` is NOT
  an error — it means "not my page type, fall through" and is recorded as
  ``"{name}: 0 vehicles"``. Exceptions likewise fall through.
* Each strategy has ``available() -> bool`` (default True). Unavailable
  strategies are skipped silently (INFO log) and do NOT contribute to the
  error list.
* When every strategy is skipped or failed, the stage raises ``FetchError`` /
  ``ExtractError`` whose message is ``"; ".join(errors)`` — or
  ``"no fetchers available"`` / ``"no extractors available"`` when everything
  was gated off by ``available()``.
* The result carries provenance: ``fetch_strategy`` / ``extract_strategy``
  name the winning strategies, plus the source URL.

Integration in DealershipScanner
--------------------------------
The fetch half of this chain backs
``backend.scanner.post_scan.gap_fill.fetch_listing_html`` (the post-scan
listing-HTML seam shared by gap_fill, ``vdp/html_recovery``,
``enrichment/listing_packages_service`` and
``enrichment/window_sticker_service``). The extract half is framework-only for
now: existing extraction machinery (``claude_vdp_extract``,
``vdp_spec_extract``, ``backend/scanner/scrapers/``) can opt in later by
wrapping itself in ``Extractor`` subclasses; nothing existing depends on it.

Sync-context caveat: ``PlaywrightFetcher.fetch`` uses sync Playwright, which
raises if called from inside a running asyncio event loop — call it from a
worker thread in async code paths (same constraint as the rest of the sync
Playwright helpers in this repo).
"""
from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)


@dataclass
class ScrapedVehicle:
    """One vehicle record produced by an extractor.

    All fields default so partial extractors can still construct it; the chain
    mechanics never inspect fields, only list truthiness.
    """

    vin: str | None = None
    title: str = ""
    year: int | None = None
    make: str | None = None
    model: str | None = None
    trim: str | None = None
    price: float | None = None
    mileage: int | None = None
    url: str | None = None
    image_url: str | None = None
    raw: dict[str, Any] = field(default_factory=dict)

    def normalized_title(self) -> str:
        return " ".join(self.title.split()).strip()


@dataclass
class ScrapeResult:
    vehicles: list[ScrapedVehicle]
    fetch_strategy: str
    extract_strategy: str
    source_url: str


class FetchError(Exception):
    pass


class ExtractError(Exception):
    pass


class Fetcher:
    name = "base"

    def available(self) -> bool:
        return True

    def fetch(self, url: str) -> str:  # returns HTML
        raise NotImplementedError


class Extractor:
    name = "base"

    def available(self) -> bool:
        return True

    def extract(self, html: str, source_url: str) -> list[ScrapedVehicle]:
        raise NotImplementedError


class ScraperChain:
    def __init__(
        self,
        fetchers: list[Fetcher],
        extractors: list[Extractor],
        result_filter: Callable[[list[ScrapedVehicle]], list[ScrapedVehicle]] | None = None,
    ):
        self.fetchers = fetchers
        self.extractors = extractors
        self.result_filter = result_filter

    def fetch(self, url: str) -> tuple[str, str]:
        errors: list[str] = []
        for f in self.fetchers:
            if not f.available():
                logger.info("fetcher %s unavailable, skipping", f.name)
                continue
            try:
                html = f.fetch(url)
                if html and html.strip():
                    logger.info("fetched via %s", f.name)
                    return html, f.name
                errors.append(f"{f.name}: empty")
            except Exception as e:  # noqa: BLE001
                logger.warning("fetcher %s failed: %s", f.name, e)
                errors.append(f"{f.name}: {e}")
        raise FetchError("; ".join(errors) or "no fetchers available")

    def extract(self, html: str, source_url: str) -> tuple[list[ScrapedVehicle], str]:
        errors: list[str] = []
        for ex in self.extractors:
            if not ex.available():
                logger.info("extractor %s unavailable, skipping", ex.name)
                continue
            try:
                vehicles = ex.extract(html, source_url)
                if vehicles:
                    logger.info("extracted %d vehicles via %s", len(vehicles), ex.name)
                    return vehicles, ex.name
                errors.append(f"{ex.name}: 0 vehicles")
            except Exception as e:  # noqa: BLE001
                logger.warning("extractor %s failed: %s", ex.name, e)
                errors.append(f"{ex.name}: {e}")
        raise ExtractError("; ".join(errors) or "no extractors available")

    def run(self, url: str, html: str | None = None) -> ScrapeResult:
        fetch_name = "manual"
        if html is None:
            html, fetch_name = self.fetch(url)
        vehicles, extract_name = self.extract(html, url)
        if self.result_filter:
            vehicles = self.result_filter(vehicles)
        return ScrapeResult(
            vehicles=vehicles,
            fetch_strategy=fetch_name,
            extract_strategy=extract_name,
            source_url=url,
        )


# ---------------------------------------------------------------------------
# Concrete fetchers, ordered cheapest -> heaviest (plain HTTP -> browser).
# Heavy deps are imported inside methods so this module imports with zero side
# effects even when optional deps are missing; available() guards actual use.
# ---------------------------------------------------------------------------


class ImpersonatingFetcher(Fetcher):
    """
    Plain HTTP with a real browser's TLS fingerprint.

    Most dealer platforms sit behind a Cloudflare edge that rejects on the TLS/JA3
    handshake rather than on headers: bare curl and curl carrying a complete browser
    header set receive the same 403 from the same host. ``RequestsFetcher`` therefore
    fails on those sites no matter what headers it sends, and because it fails the chain
    falls through to ``PlaywrightFetcher`` -- which is where a browser-free scan quietly
    turns into 1,656 browser fetches.

    ``curl_cffi`` reproduces a browser's handshake, so the edge answers normally. Of 182
    rooftops that yielded no inventory, 112 were returning 403 to a stock HTTP client;
    with impersonation that fell to 1. This fetcher runs ahead of both others so those
    sites resolve over HTTP and never reach the browser stage.

    Signature rotation is required, not belt-and-braces: over a 30-dealer sample
    ``chrome`` alone cleared roughly half, while chrome/chrome124/safari17_0 together
    cleared 29 of 30. Edges run different rule sets. The signature that works for a host
    is remembered so later fetches spend one request instead of three.
    """

    name = "impersonate"

    # Ordered by observed hit rate.
    PROFILES = ("chrome", "chrome124", "safari17_0")

    def __init__(self, timeout_s: float = 25.0):
        self.timeout_s = timeout_s
        self._winning: dict[str, str] = {}

    def available(self) -> bool:
        try:
            import curl_cffi  # noqa: F401

            return True
        except ImportError:
            return False

    @staticmethod
    def _host(url: str) -> str:
        from urllib.parse import urlparse

        return urlparse(url).netloc.lower()

    def fetch(self, url: str) -> str:
        from curl_cffi import requests as cffi_requests

        from backend.scanner.http_fetch import proxy_url

        proxy = proxy_url()
        proxies = {"http": proxy, "https": proxy} if proxy else None

        host = self._host(url)
        # Try the signature that already worked for this host first.
        known = self._winning.get(host)
        order = ([known] + [p for p in self.PROFILES if p != known]) if known else list(self.PROFILES)

        last_status: int | None = None
        for profile in order:
            try:
                resp = cffi_requests.get(
                    url,
                    impersonate=profile,
                    timeout=self.timeout_s,
                    proxies=proxies,
                    allow_redirects=True,
                )
            except Exception as exc:  # curl_cffi has its own error hierarchy
                last_status = None
                logger.debug("impersonate %s failed for %s: %s", profile, host, exc)
                continue
            if resp.status_code == 200 and resp.text and resp.text.strip():
                self._winning[host] = profile
                return resp.text
            last_status = resp.status_code

        raise FetchError(f"impersonation exhausted for {host} (last status {last_status})")


class RequestsFetcher(Fetcher):
    name = "requests"

    def __init__(self, timeout_s: float = 20.0, user_agent: str = _UA):
        self.timeout_s = timeout_s
        self.user_agent = user_agent

    def fetch(self, url: str) -> str:
        import requests

        resp = requests.get(
            url,
            timeout=self.timeout_s,
            headers={"User-Agent": self.user_agent},
            allow_redirects=True,
        )
        resp.raise_for_status()
        return resp.text


class PlaywrightFetcher(Fetcher):
    """Headless-Chromium fetch (renders JS), with playwright-stealth applied.

    Browser construction mirrors ``backend.scanner.orchestrator`` (which wraps
    playwright in ``Stealth().use_async(...)``), using the sync counterpart
    ``Stealth().use_sync(...)`` and falling back to plain playwright when
    playwright_stealth is not installed. For fully custom browser setups a
    ``page_factory`` context-manager callable may be injected: it must accept
    a user-agent string and yield a ready Playwright page.

    Must be called from sync context (or a worker thread) — sync Playwright
    raises inside a running asyncio event loop.
    """

    name = "playwright"

    def __init__(
        self,
        timeout_ms: int = 30000,
        wait_until: str = "networkidle",
        user_agent: str = _UA,
        page_factory: Callable[[str], Any] | None = None,
    ):
        self.timeout_ms = timeout_ms
        self.wait_until = wait_until
        self.user_agent = user_agent
        self.page_factory = page_factory

    def available(self) -> bool:
        try:
            import playwright  # noqa: F401

            return True
        except ImportError:
            return False

    def fetch(self, url: str) -> str:
        if self.page_factory is not None:
            with self.page_factory(self.user_agent) as page:
                page.goto(url, wait_until=self.wait_until, timeout=self.timeout_ms)
                return page.content()

        with self._playwright_context() as p:
            browser = p.chromium.launch(headless=True)
            try:
                page = browser.new_page(user_agent=self.user_agent)
                page.goto(url, wait_until=self.wait_until, timeout=self.timeout_ms)
                return page.content()
            finally:
                browser.close()

    @staticmethod
    def _playwright_context():
        """Stealth-wrapped sync playwright, mirroring orchestrator.py's pattern."""
        from playwright.sync_api import sync_playwright

        try:
            from playwright_stealth import Stealth

            return Stealth().use_sync(sync_playwright())
        except ImportError:
            logger.warning("playwright_stealth not found, using plain playwright")
            return sync_playwright()


# ---------------------------------------------------------------------------
# Extractors: framework-only. One thin no-LLM fallback is provided; existing
# extraction machinery can be wrapped as Extractor subclasses by callers.
# ---------------------------------------------------------------------------

# Hrefs that look like a single vehicle detail page across common dealer
# platforms, or contain a VIN-like 17-char segment.
import re as _re

_VDP_HREF = _re.compile(
    r"(/inventory/|/vehicle[s]?/|/vdp/|/used[-/]|/new[-/]|/certified[-/]|"
    r"[A-HJ-NPR-Z0-9]{17})",
    _re.I,
)
# re.I to stay consistent with _VDP_HREF: dealer platforms commonly lowercase
# VINs in VDP URLs; the captured group is normalized with .upper() at use site.
_VIN_SEG = _re.compile(r"\b([A-HJ-NPR-Z0-9]{17})\b", _re.I)


class HeuristicVehicleLinkExtractor(Extractor):
    """No-LLM fallback: scrape anchors that look like vehicle-detail links."""

    name = "beautifulsoup"

    def available(self) -> bool:
        try:
            import bs4  # noqa: F401

            return True
        except ImportError:
            return False

    def extract(self, html: str, source_url: str) -> list[ScrapedVehicle]:
        from urllib.parse import urljoin

        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html, "html.parser")
        seen: set[tuple[str, str]] = set()
        vehicles: list[ScrapedVehicle] = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if not _VDP_HREF.search(href):
                continue
            title = " ".join(a.get_text(" ", strip=True).split())
            if not title:
                continue
            abs_url = urljoin(source_url, href)
            key = (title.lower(), abs_url)
            if key in seen:
                continue
            seen.add(key)
            vin_m = _VIN_SEG.search(href)
            vehicles.append(
                ScrapedVehicle(
                    title=title,
                    url=abs_url,
                    vin=vin_m.group(1).upper() if vin_m else None,
                )
            )
        return vehicles


# Impersonation first: it clears the Cloudflare TLS-fingerprint 403 that would
# otherwise fail RequestsFetcher and escalate the chain to a browser.
DEFAULT_FETCHERS: list[Fetcher] = [
    ImpersonatingFetcher(),
    RequestsFetcher(),
    PlaywrightFetcher(),
]


def default_chain(
    extractors: list[Extractor],
    result_filter: Callable[[list[ScrapedVehicle]], list[ScrapedVehicle]] | None = None,
) -> ScraperChain:
    """Cheap->heavy fetch chain with caller-supplied extractors.

    Unlike zumai, extractors are a required argument — there is no sensible
    universal default extractor list for dealership sites yet.
    """
    return ScraperChain(
        fetchers=[ImpersonatingFetcher(), RequestsFetcher(), PlaywrightFetcher()],
        extractors=extractors,
        result_filter=result_filter,
    )


__all__ = [
    "DEFAULT_FETCHERS",
    "ExtractError",
    "Extractor",
    "FetchError",
    "Fetcher",
    "HeuristicVehicleLinkExtractor",
    "PlaywrightFetcher",
    "RequestsFetcher",
    "ScrapeResult",
    "ScrapedVehicle",
    "ScraperChain",
    "default_chain",
]
