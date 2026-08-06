"""Per-brand reachability probes."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

from .http import (
    PacedFetcher,
)
from .links import (
    extract_document_links,
)
from .naming import (
    official_hosts_for,
)
from .robots import (
    RobotsPolicy,
)

# --------------------------------------------------------------------------
# Per-brand reachability
# --------------------------------------------------------------------------

#: (make, host, one real index page on that host). The page is the part that
#: matters: a host can serve robots.txt happily and still 403 every document
#: index, and that difference decides whether the acquisition phase should
#: spend time on the brand.
PROBE_TARGETS: tuple[tuple[str, str, str], ...] = (
    # Toyota's index root redirects into ONE category; probing the trucks page
    # is what would have caught that (2026-08-01). See
    # TOYOTA_BROCHURE_INDEX_PAGES.
    ("toyota", "www.toyota.com", "https://www.toyota.com/brochures/cars-minivan/"),
    ("toyota", "www.toyota.com", "https://www.toyota.com/brochures/trucks/"),
    ("toyota", "www.toyota.com", "https://www.toyota.com/brochures/crossovers-suvs/"),
    ("lexus", "www.lexus.com", "https://www.lexus.com/brochures"),
    ("ford", "www.ford.com", "https://www.ford.com/support/vehicle-brochures/"),
    (
        "ford",
        "media.ford.com",
        "https://media.ford.com/content/fordmedia/fna/us/en.html",
    ),
    ("lincoln", "www.lincoln.com", "https://www.lincoln.com/support/vehicle-brochures/"),
    (
        "lincoln",
        "media.lincoln.com",
        "https://media.lincoln.com/content/lincolnmedia/lna/us/en.html",
    ),
    ("honda", "automobiles.honda.com", "https://automobiles.honda.com/tools/brochures"),
    ("nissan", "www.nissanusa.com", "https://www.nissanusa.com/brochures.html"),
    ("jeep", "www.jeep.com", "https://www.jeep.com/bmo.html"),
    ("ram", "www.ramtrucks.com", "https://www.ramtrucks.com/bmo.html"),
    ("chevrolet", "www.chevrolet.com", "https://www.chevrolet.com/download-catalog"),
    (
        "chevrolet",
        "www.chevrolet.com",
        "https://www.chevrolet.com/trucks/silverado/1500",
    ),
    ("chevrolet", "news.chevrolet.com", "https://news.chevrolet.com/newsroom.html"),
    ("gmc", "www.gmc.com", "https://www.gmc.com/browse-brochures"),
    # Registered 2026-08-01. This one DOES serve documents -- the attachment
    # anchored on an article page is a real PDF (BMW_PRESS_ATTACHMENT_EVIDENCE) --
    # so the probe's job here is to keep measuring that the article page renders
    # server-side, which is the half that works.
    (
        "bmw",
        "www.press.bmwgroup.com",
        "https://www.press.bmwgroup.com/usa/article/detail/T0458814EN_US/the-new-bmw-x5-and-ix5",
    ),
    ("bmw", "www.bmwusa.com", "https://www.bmwusa.com/vehicles/x-series/x5/bmw-x5.html"),
    ("mercedes-benz", "www.mbusa.com", "https://www.mbusa.com/en/brochures"),
    ("mercedes-benz", "media.mbusa.com", "https://media.mbusa.com/"),
    (
        "honda",
        "hondanews.com",
        "https://hondanews.com/en-US/honda-automobiles/releases/release-6f8b202ddb1c7f26ecd92774bb014b0c-2026-honda-hr-v-specifications-and-features",
    ),
    ("audi", "media.audiusa.com", "https://media.audiusa.com/en-us/models"),
    ("mazda", "news.mazdausa.com", "https://news.mazdausa.com/vehicles-2026-cx-50"),
    (
        "kia",
        "www.kiamedia.com",
        "https://www.kiamedia.com/us/en/models/sportage/2026/specifications",
    ),
    (
        "subaru",
        "media.subaru.com",
        "https://media.subaru.com/pressrelease/2026-subaru-crosstrek-press-kit",
    ),
    (
        "hyundai",
        "www.hyundainews.com",
        "https://www.hyundainews.com/en-us/releases/2026-tucson",
    ),
    ("volkswagen", "media.vw.com", "https://media.vw.com/en-us/models/atlas"),
    ("volvo", "www.media.volvocars.com", "https://www.media.volvocars.com/us/en-us/models"),
)


#: What stopped a probe, when something did. Kept as an explicit field rather
#: than left to be inferred from the verdict string, because the two causes have
#: opposite remedies and were previously reported as one thing:
#:
#:   ``our_allowlist``      the host is not in :data:`OFFICIAL_HOSTS` for this
#:                          make. WE declined. Fixed by a reviewed edit to the
#:                          allowlist -- nothing to ask the OEM for.
#:   ``their_robots_rules`` the host served robots.txt and its published rules
#:                          forbid this page. THEY declined, in writing, and we
#:                          honour it.
#:   ``their_edge``         the host would not serve us robots.txt at all
#:                          (reset, black-holed, 4xx/5xx). THEY declined, without
#:                          publishing a rule; unreadable means no.
#:   ``""``                 nothing blocked the probe.
BLOCKED_BY_US = "our_allowlist"
BLOCKED_BY_THEIR_RULES = "their_robots_rules"
BLOCKED_BY_THEIR_EDGE = "their_edge"


@dataclass
class ReachabilityResult:
    make: str
    host: str
    page_url: str
    user_agent_label: str
    robots_status: str = "unknown"
    robots_detail: str = ""
    robots_allows_page: bool = False
    page_status: str = ""
    page_bytes: int = 0
    document_links: int = 0  # links on an allowlisted host for this make
    any_pdf_links: int = 0  # links to a document anywhere (host allowlist ignored)
    host_allowlisted: bool = True
    blocked_by: str = ""
    verdict: str = "unknown"
    detail: str = ""

    def to_json(self) -> dict:
        return {
            "make": self.make,
            "host": self.host,
            "page_url": self.page_url,
            "user_agent": self.user_agent_label,
            "robots_status": self.robots_status,
            "robots_detail": self.robots_detail,
            "robots_allows_page": self.robots_allows_page,
            "page_status": self.page_status,
            "page_bytes": self.page_bytes,
            "document_links": self.document_links,
            "any_pdf_links": self.any_pdf_links,
            "host_allowlisted": self.host_allowlisted,
            "blocked_by": self.blocked_by,
            "verdict": self.verdict,
            "detail": self.detail,
        }


#: Below this many bytes an HTML response is a JavaScript application shell, not
#: a server-rendered index. Measured on media.vw.com (~2.5KB) and
#: hyundainews.com (~3.2KB), both of which contained zero anchors.
JS_SHELL_MAX_BYTES = 8000


def probe_target(
    make: str,
    host: str,
    page_url: str,
    fetcher: PacedFetcher,
    robots: RobotsPolicy,
    user_agent_label: str,
) -> ReachabilityResult:
    """
    Measure what one OEM host serves this client. Never downloads a document.

    Verdicts: ``robots_blocked`` (the host refuses to serve us its own
    robots.txt), ``robots_disallowed`` (its published rules forbid the page),
    ``http_<code>``, ``transport_error``, ``js_shell`` (200 but no
    server-rendered anchors), ``no_document_links`` (rendered HTML, no PDFs),
    ``documents_but_no_allowlist``, ``serves_documents``.

    ``host_allowlisted`` and ``blocked_by`` say which side declined -- see
    :data:`BLOCKED_BY_US` and the two ``BLOCKED_BY_THEIR_*`` values. "We have not
    allowlisted this host" and "this host will not answer us" are different
    findings with different remedies, and reading them off the verdict string
    alone was guesswork.

    An un-allowlisted host is still probed, because reading one index page is not
    fetching a document and ``documents_but_no_allowlist`` is how a host earns a
    reviewed allowlist entry. It is never followed to a download: the allowlist
    check in :func:`is_official_url` stands between this and any fetch.
    """
    result = ReachabilityResult(
        make=make, host=host, page_url=page_url, user_agent_label=user_agent_label
    )
    result.host_allowlisted = host.lower() in official_hosts_for(make)

    allowed = robots.allows(page_url)
    verdict = robots.verdict_for(host)
    if verdict is not None:
        result.robots_status = verdict.status
        result.robots_detail = verdict.detail
    result.robots_allows_page = bool(allowed)

    if not allowed:
        readable = bool(verdict and verdict.readable)
        result.verdict = "robots_disallowed" if readable else "robots_blocked"
        result.blocked_by = (
            BLOCKED_BY_THEIR_RULES if readable else BLOCKED_BY_THEIR_EDGE
        )
        result.detail = f"robots.txt {result.robots_status}: {result.robots_detail}"
        return result

    try:
        status, html = fetcher.get_text(page_url)
    except Exception as exc:  # noqa: BLE001 - transport failure is the finding
        result.verdict = "transport_error"
        result.page_status = "error"
        result.blocked_by = BLOCKED_BY_THEIR_EDGE
        result.detail = f"{type(exc).__name__}: {str(exc)[:160]}"
        return result

    result.page_status = str(status)
    result.page_bytes = len(html or "")
    if status != 200:
        result.verdict = f"http_{status}"
        result.detail = f"GET {page_url} -> HTTP {status}"
        return result

    official = extract_document_links(html or "", page_url, make)
    anywhere = extract_document_links(html or "", page_url, make, official_only=False)
    result.document_links = len(official)
    result.any_pdf_links = len(anywhere)

    if official:
        result.verdict = "serves_documents"
        result.detail = f"{len(official)} official document link(s) in raw HTML"
    elif anywhere:
        # The page does serve documents; we just have no host allowlist for this
        # make yet. That is a registration gap, not an unreachable brand -- the
        # refusal here is OURS.
        result.verdict = "documents_but_no_allowlist"
        result.blocked_by = BLOCKED_BY_US
        result.detail = (
            f"{len(anywhere)} document link(s) in raw HTML, none on a host "
            f"allowlisted for {make}: hosts seen = "
            + ", ".join(
                sorted({(urlparse(link.url).hostname or "?") for link in anywhere})[:4]
            )
        )
    elif result.page_bytes < JS_SHELL_MAX_BYTES:
        result.verdict = "js_shell"
        result.detail = (
            f"HTTP 200 but only {result.page_bytes} bytes of HTML: "
            "the index is client-rendered"
        )
    else:
        result.verdict = "no_document_links"
        result.detail = (
            f"HTTP 200, {result.page_bytes} bytes, no anchor to any document"
        )
    return result
