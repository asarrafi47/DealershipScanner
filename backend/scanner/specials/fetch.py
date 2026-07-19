"""Browser-free fetch of a dealer's likely *specials* pages.

Sends the same real browser first-visit navigation header set the scanner's
classifier uses (UA + Sec-Fetch-* + text/html Accept) so dealer platforms serve
the same HTML a browser navigation would, rather than a 403 / "Just a moment"
shell. Requests are paced (sequential with a small delay) and routed through
``SCANNER_HTTP_PROXY`` when configured, via :mod:`backend.scanner.http_fetch`.
"""
from __future__ import annotations

import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from urllib.parse import urlparse

from backend.scanner.http_fetch import open_url

# Common dealer-CMS specials URL patterns, ordered by how often they hit. We try
# a handful and aggregate offers across every page that returns 200.
SPECIALS_PATHS = [
    "/new-vehicle-specials",
    "/specials",
    "/new-specials",
    "/lease-specials",
    "/manager-specials",
    "/new-car-specials",
    "/current-offers",
    "/specials-offers",
    "/new-vehicle-specials.htm",
]

_BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)
_HEADERS = {
    "User-Agent": _BROWSER_UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Upgrade-Insecure-Requests": "1",
}

_PACE_SECONDS = 1.2


@dataclass
class SpecialsPage:
    url: str
    status: int | None
    html: str | None
    error: str | None = None


@dataclass
class SpecialsFetchResult:
    dealer_id: str
    base_url: str
    pages: list[SpecialsPage] = field(default_factory=list)

    @property
    def ok_pages(self) -> list[SpecialsPage]:
        return [p for p in self.pages if p.status == 200 and p.html]


def _normalize_base(url: str) -> str:
    if "://" not in url:
        url = "https://" + url
    p = urlparse(url)
    return f"{p.scheme}://{p.netloc}"


def _fetch_one(url: str, timeout: int) -> SpecialsPage:
    req = urllib.request.Request(url, headers=_HEADERS)
    try:
        resp = open_url(req, timeout=timeout)
        raw = resp.read()
        charset = resp.headers.get_content_charset() or "utf-8"
        html = raw.decode(charset, "replace")
        status = getattr(resp, "status", None) or resp.getcode()
        return SpecialsPage(url=url, status=status, html=html)
    except urllib.error.HTTPError as e:
        return SpecialsPage(url=url, status=e.code, html=None, error=f"http_{e.code}")
    except (urllib.error.URLError, TimeoutError, OSError, ValueError) as e:
        return SpecialsPage(url=url, status=None, html=None, error=type(e).__name__)


def fetch_specials_pages(
    base_url: str,
    dealer_id: str,
    *,
    paths: list[str] | None = None,
    timeout: int = 25,
    pace_seconds: float = _PACE_SECONDS,
    stop_after_hits: int = 0,
) -> SpecialsFetchResult:
    """Fetch a dealer's candidate specials URLs (paced, proxy-aware).

    ``stop_after_hits`` (>0) stops after that many 200 pages, to keep sweeps
    cheap; 0 tries all *paths*.
    """
    base = _normalize_base(base_url)
    result = SpecialsFetchResult(dealer_id=dealer_id, base_url=base)
    hits = 0
    for i, path in enumerate(paths or SPECIALS_PATHS):
        if i:
            time.sleep(pace_seconds)
        page = _fetch_one(base + path, timeout=timeout)
        result.pages.append(page)
        if page.status == 200 and page.html:
            hits += 1
            if stop_after_hits and hits >= stop_after_hits:
                break
    return result
