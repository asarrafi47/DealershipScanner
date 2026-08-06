"""Per-make brochure index / discovery pages."""
from __future__ import annotations

import logging
import re
from backend.enrichment.dictionary_catalog import (
    canonical_make,
)

logger = logging.getLogger(__name__)

from .naming import (
    _make_token,
    is_official_url,
    normalized_model_token,
)

# --------------------------------------------------------------------------
# Per-make discovery pages
# --------------------------------------------------------------------------


def _slugs(make: str, model: str) -> list[str]:
    """
    URL slug candidates for a model, make prefix stripped.

    ``Mazda CX-50`` -> ``cx50`` and ``cx-50``; both spellings appear in OEM
    newsroom URLs, so both are tried (paced, sequential).
    """
    token = normalized_model_token(make, model)
    if not token:
        return []
    out = [token]
    match = re.match(r"^([a-z]+)(\d+)$", token)
    if match:
        out.append(f"{match.group(1)}-{match.group(2)}")
    hyphenated = re.sub(r"[^a-z0-9]+", "-", (model or "").strip().lower()).strip("-")
    # Drop the make word from the hyphenated form too ("mazda-cx-50" -> "cx-50").
    make_word = re.sub(r"[^a-z0-9]+", "-", canonical_make(make or "").lower()).strip("-")
    if make_word and hyphenated.startswith(make_word + "-"):
        hyphenated = hyphenated[len(make_word) + 1 :]
    if hyphenated and hyphenated not in out:
        out.append(hyphenated)
    return out


def _mazda_pages(year: int, make: str, model: str) -> list[str]:
    return [
        f"https://news.mazdausa.com/vehicles-{year}-{s}" for s in _slugs(make, model)
    ]


def _kia_pages(year: int, make: str, model: str) -> list[str]:
    return [
        f"https://www.kiamedia.com/us/en/models/{s}/{year}/{page}"
        for s in _slugs(make, model)
        for page in ("specifications", "features")
    ]


def _subaru_pages(year: int, make: str, model: str) -> list[str]:
    return [
        f"https://media.subaru.com/pressrelease/{year}-subaru-{s}-press-kit"
        for s in _slugs(make, model)
    ]


def _hyundai_pages(year: int, make: str, model: str) -> list[str]:
    return [
        f"https://www.hyundainews.com/en-us/releases/{year}-{s}"
        for s in _slugs(make, model)
    ]


def _volkswagen_pages(year: int, make: str, model: str) -> list[str]:
    return [f"https://media.vw.com/en-us/models/{s}" for s in _slugs(make, model)]


#: Toyota's brochure index is split into five server-rendered category pages.
#:
#: Measured 2026-08-01: ``GET https://www.toyota.com/brochures/`` answers HTTP
#: 200 but the final URL is ``https://www.toyota.com/brochures/cars-minivan/``
#: -- the index root now redirects into ONE category and lists only its 11
#: PDFs. Its own navigation anchors the other four
#: (``/brochures/crossovers-suvs/``, ``/brochures/electrified/``,
#: ``/brochures/trucks/``, ``/brochures/other-brochures/``), which is where the
#: Tundra, Tacoma, RAV4, Highlander and 4Runner books live. Every URL here was
#: read off that page's own ``href``s, not constructed from a slug pattern.
#:
#: This is why Toyota looked "verified" while its trucks and SUVs were
#: unreachable: the single-URL version of this function resolved the whole
#: line-up against the cars/minivan list. The corpus still holds Tacoma and
#: 4Runner text from when the root page listed everything.
#:
#: Cost is bounded: ``resolve_source``'s ``page_cache`` fetches each of these at
#: most once per run however many Toyota gaps are processed.
TOYOTA_BROCHURE_INDEX_PAGES: tuple[str, ...] = (
    "https://www.toyota.com/brochures/cars-minivan/",
    "https://www.toyota.com/brochures/crossovers-suvs/",
    "https://www.toyota.com/brochures/trucks/",
    "https://www.toyota.com/brochures/electrified/",
    "https://www.toyota.com/brochures/other-brochures/",
)


def _toyota_pages(year: int, make: str, model: str) -> list[str]:
    # Server-rendered indexes, one per body-style category; the year filter is
    # done by score_document_link against the /pdf/<year>/ path segment.
    return list(TOYOTA_BROCHURE_INDEX_PAGES)


def _nissan_pages(year: int, make: str, model: str) -> list[str]:
    return ["https://www.nissanusa.com/brochures.html"]


_DISCOVERY_PAGES = {
    "mazda": _mazda_pages,
    "kia": _kia_pages,
    "toyota": _toyota_pages,
    "nissan": _nissan_pages,
    "subaru": _subaru_pages,
    "hyundai": _hyundai_pages,
    "volkswagen": _volkswagen_pages,
}


def discovery_pages(year: int, make: str, model: str) -> list[str]:
    """Official index pages that may link to this model's spec document."""
    builder = _DISCOVERY_PAGES.get(_make_token(make))
    if builder is None:
        return []
    seen: set[str] = set()
    out: list[str] = []
    for url in builder(year, make, model):
        if url in seen or not is_official_url(url, make):
            continue
        seen.add(url)
        out.append(url)
    return out
