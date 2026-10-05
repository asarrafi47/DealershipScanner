"""
Source resolution, OEM tier first, archive tier only as a fallback.

Moved verbatim out of ``backend/scripts/fetch_oem_brochures.py`` (audit
datascripts.md F7); that script is still the CLI and re-exports these names.
"""
from __future__ import annotations

import logging
import time
from typing import Any

from backend.enrichment.brochure_sources import (
    ARCHIVE_ROOT_URL,
    PAGE_SCOPED_REASON,
    TIER_ARCHIVE,
    TIER_OEM,
    PacedFetcher,
    RobotsPolicy,
    archive_index_url,
    archive_make_index_pages,
    discovery_pages,
    extract_archive_document_links,
    extract_document_links,
    page_scoped_document,
    pick_archive_document,
    pick_best_document,
    unsupported_reason,
)

logger = logging.getLogger(__name__)


class ArchiveResolver:
    """
    The archive tier's fetcher, robots policy and page cache, in one object.

    Separate from the OEM fetcher on purpose, for three reasons that are all
    load-bearing:

    * **Pacing.** It is built through :meth:`PacedFetcher.for_tier`, so its delay
      is floored at :data:`ARCHIVE_DELAY_SECONDS` (12s) whatever ``--delay`` the
      caller passed. Sharing one fetcher would let an OEM-paced run crawl the
      archive at 4s.
    * **Request budget.** One index page per make answers every gap for that
      make and every year, so it is cached; without that, a 20-gap Mazda run
      would re-fetch a 48KB index 20 times.
    * **Honesty in the report.** ``requests`` on this object is the archive's
      own request count, so a run can state how many requests it actually made
      to a host that never asked for any.
    """

    def __init__(self, *, delay: float, user_agent: str) -> None:
        self.fetcher = PacedFetcher.for_tier(
            TIER_ARCHIVE, delay=delay, user_agent=user_agent
        )
        self.robots = RobotsPolicy(
            self.fetcher.get_text, agent=self.fetcher.robots_agent
        )
        self.page_cache: dict[str, tuple[int, str]] = {}
        self.index_pages: dict[str, str] | None = None
        self.request_timings: list[float] = []

    def get_page(self, url: str) -> tuple[int, str]:
        if url in self.page_cache:
            return self.page_cache[url]
        started = time.monotonic()
        try:
            status, html = self.fetcher.get_text(url)
        except Exception as exc:  # noqa: BLE001 - transport failure is a result
            logger.warning("archive page %s failed: %s", url, exc)
            status, html = 0, ""
        self.request_timings.append(time.monotonic() - started)
        self.page_cache[url] = (status, html)
        return status, html

    def make_index_pages(self) -> dict[str, str]:
        """The site's own make navigation, fetched once per run."""
        if self.index_pages is None:
            status, html = self.get_page(ARCHIVE_ROOT_URL)
            self.index_pages = (
                archive_make_index_pages(html, ARCHIVE_ROOT_URL) if status == 200 else {}
            )
        return self.index_pages


def _resolve_from_archive(
    result: dict[str, Any], archive: ArchiveResolver
) -> dict[str, Any]:
    """
    Second-choice resolution: the archive tier, for a gap the OEM tier refused.

    Called only after the OEM attempt has already been made and has not returned
    ``found``. It never overwrites a ``found`` result -- ``resolve_source`` is
    what enforces that, and it is the whole meaning of "OEM first".
    """
    year, make, model = result["year"], result["make"], result["model"]
    index_url = archive_index_url(make, archive.make_index_pages())
    if not index_url:
        result["archive_detail"] = f"the archive lists no index page for {make}"
        return result

    if not archive.robots.allows(index_url):
        verdict = archive.robots.verdict_for(index_url)
        result["archive_detail"] = (
            f"{index_url} disallowed"
            + (f" (robots.txt {verdict.status}: {verdict.detail})" if verdict else "")
        )
        return result

    status, html = archive.get_page(index_url)
    if status != 200:
        result["archive_detail"] = f"{index_url}: HTTP {status or 'transport error'}"
        return result

    links = extract_archive_document_links(html, index_url)
    best = pick_archive_document(links, year, make, model)
    if best is None:
        result["archive_detail"] = (
            f"{index_url}: {len(links)} archive document(s), none matching "
            f"{year} {model}"
        )
        return result

    result.update(
        status="found",
        tier=TIER_ARCHIVE,
        doc_url=best.url,
        discovery_url=index_url,
        page_scoped=False,
        detail=f"score={best.score:g} {', '.join(best.reasons)}",
        oem_refusal=result.get("detail") or result["status"],
    )
    return result


def resolve_source(
    gap: dict[str, Any],
    fetcher: PacedFetcher,
    robots: RobotsPolicy,
    page_cache: dict[str, tuple[int, str]] | None = None,
    *,
    archive: ArchiveResolver | None = None,
) -> dict[str, Any]:
    """
    Find a document URL for one gap, **OEM tier first**.

    The OEM tier is attempted in full before the archive is touched at all, and
    an OEM ``found`` returns immediately. The archive is reached only when
    ``archive`` is supplied (``--allow-archive``) and the OEM tier produced
    nothing -- including the ``unsupported_make`` case, which is the single
    biggest reason this tier exists: 16 makes have no OEM source at all.

    ``status`` is one of ``found`` / ``unsupported_make`` / ``no_discovery_page``
    / ``robots_disallowed`` / ``page_unavailable`` / ``no_document_link``, and a
    ``found`` result carries ``tier``. When the archive was tried and also found
    nothing, the OEM status is kept (it is the more informative one) and
    ``archive_detail`` says what the archive did.

    ``page_cache`` matters for the whole-line-up indexes: one
    ``toyota.com/brochures/`` page answers every Toyota gap, and re-fetching it
    per gap would be exactly the burst that gets this project soft-blocked.
    """
    year, make, model = gap["year"], gap["make"], gap["model"]
    result: dict[str, Any] = {
        "group_key": gap["group_key"],
        "year": year,
        "make": make,
        "model": model,
        "cars": gap["cars"],
        "primary_catalog_key": gap["primary_catalog_key"],
        "alias_catalog_keys": gap.get("alias_catalog_keys", []),
        "status": "no_document_link",
        "tier": "",
        "doc_url": None,
        "discovery_url": None,
        "detail": "",
    }

    reason = unsupported_reason(make)
    if reason:
        result["status"] = "unsupported_make"
        result["detail"] = reason
        return _resolve_from_archive(result, archive) if archive else result

    pages = discovery_pages(year, make, model)
    if not pages:
        result["status"] = "no_discovery_page"
        result["detail"] = "no official index page pattern for this model name"
        return _resolve_from_archive(result, archive) if archive else result

    tried: list[str] = []
    for page in pages:
        if not robots.allows(page):
            verdict = robots.verdict_for(page)
            result["status"] = "robots_disallowed"
            result["detail"] = (
                f"{page} disallowed"
                + (f" (robots.txt {verdict.status}: {verdict.detail})" if verdict else "")
            )
            tried.append(page)
            continue
        cache = page_cache if page_cache is not None else {}
        if page in cache:
            status, html = cache[page]
        else:
            try:
                status, html = fetcher.get_text(page)
            except Exception as exc:  # noqa: BLE001 - transport failure is a result
                cache[page] = (0, "")
                result["status"] = "page_unavailable"
                result["detail"] = f"{page}: {type(exc).__name__}: {str(exc)[:120]}"
                tried.append(page)
                continue
            cache[page] = (status, html)
        if status != 200:
            result["status"] = "page_unavailable"
            result["detail"] = f"{page}: HTTP {status or 'transport error'}"
            tried.append(page)
            continue

        links = extract_document_links(html, page, make)
        best = pick_best_document(links, year, model, make)
        if best is None:
            # Fall back to a page-scoped opaque document; the post-download
            # identity check is what makes that safe.
            best = page_scoped_document(links, page, year, make, model)
        if best is None:
            result["status"] = "no_document_link"
            result["detail"] = f"{page}: no candidate link matched year+model"
            tried.append(page)
            continue

        result.update(
            status="found",
            tier=TIER_OEM,
            doc_url=best.url,
            discovery_url=page,
            page_scoped=PAGE_SCOPED_REASON in best.reasons,
            detail=f"score={best.score:g} {', '.join(best.reasons)}",
        )
        return result

    result["tried"] = tried
    # Every OEM discovery page has now been tried and none produced a document.
    # Only here does the archive become reachable.
    return _resolve_from_archive(result, archive) if archive else result
