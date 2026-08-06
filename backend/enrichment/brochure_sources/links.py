"""Document-link extraction from HTML, scoring, and page-scoped selection."""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse

logger = logging.getLogger(__name__)

from .naming import (
    _model_token,
    is_official_url,
    model_token_variants,
)

# --------------------------------------------------------------------------
# Link extraction
# --------------------------------------------------------------------------


@dataclass
class DocLink:
    url: str
    text: str = ""
    score: float = 0.0
    reasons: list[str] = field(default_factory=list)


class _AnchorParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.anchors: list[tuple[str, str]] = []
        self._href: str | None = None
        self._buf: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag != "a":
            return
        href = dict(attrs).get("href")
        if href:
            self._href = href
            self._buf = []

    def handle_data(self, data: str) -> None:
        if self._href is not None:
            self._buf.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag == "a" and self._href is not None:
            self.anchors.append((self._href, " ".join("".join(self._buf).split())))
            self._href = None
            self._buf = []


# A link is a document candidate if it ends in .pdf, or matches a known
# official document-delivery route that omits the extension.
_DOCUMENT_ROUTE = re.compile(
    r"(?:\.pdf(?:$|[?#]))"
    r"|(?:/download/specifications/pdf/\d+)"  # kia.com
    r"|(?:/view-spec\.do\?bFileId=\d+)"  # media.subaru.com
    r"|(?:/pdf\.do\?id=\d+)",  # media.subaru.com
    re.I,
)


def extract_document_links(
    html: str, base_url: str, make: str, *, official_only: bool = True
) -> list[DocLink]:
    """
    Anchor hrefs on ``html`` that point at a document on an official host.

    Links are resolved against ``base_url`` and then filtered by
    :func:`is_official_url`, so an official page linking out to a dealer or
    aggregator PDF yields nothing.

    ``official_only=False`` skips that filter and is used **only** by the
    reachability probe, to tell "this host serves no documents" apart from "this
    make has no host allowlist yet". Nothing is ever downloaded from a link
    found that way; :func:`download` paths re-check :func:`is_official_url`.
    """
    parser = _AnchorParser()
    try:
        parser.feed(html or "")
    except Exception:  # noqa: BLE001 - malformed markup is expected
        pass

    seen: set[str] = set()
    out: list[DocLink] = []
    for href, text in parser.anchors:
        href = href.strip()
        if not href or href.startswith(("javascript:", "mailto:", "#")):
            continue
        url = urljoin(base_url, href)
        if not _DOCUMENT_ROUTE.search(url):
            continue
        if official_only and not is_official_url(url, make):
            continue
        if url in seen:
            continue
        seen.add(url)
        out.append(DocLink(url=url, text=text))
    return out


# Words that mark a document as a trim/equipment source rather than a warranty
# book, owner's manual, accessory list or press photo caption.
_GOOD_WORDS = (
    ("spec deck", 6.0),
    ("specification", 4.0),
    ("specs", 4.0),
    ("brochure", 5.0),
    ("fact sheet", 3.0),
    ("features", 2.0),
    ("trim", 2.0),
)

# Powertrain / body qualifiers that make a document a DIFFERENT vehicle. A
# whole-line-up index lists "2026-nissan-rogue-brochure" next to
# "2026-nissan-rogue-plug-in-hybrid-brochure", and both contain "rogue": without
# this, a request for a plain Rogue can be answered with the PHEV's equipment.
# Matching is symmetric -- the qualifiers on the document must be exactly those
# on the requested model. That costs coverage where an OEM ships one combined
# book, and that is the intended trade: a wrong-variant spec sheet is the same
# class of error as a wrong-year one.
_VARIANT_QUALIFIERS = (
    "pluginhybrid",
    "plugin",
    "phev",
    "hybrid",
    "prime",
    "electric",
    "hatchback",
    "coupe",
    "convertible",
    "crosscountry",
    "nismo",
    "sportback",
    "allroad",
)

_BAD_WORDS = (
    ("owner", -8.0),
    ("manual", -8.0),
    ("warranty", -8.0),
    ("maintenance", -6.0),
    ("accessor", -5.0),
    ("recall", -8.0),
    ("financial", -6.0),
    ("press release", -3.0),
    ("photo", -4.0),
    ("wallpaper", -6.0),
)


def score_document_link(
    link: DocLink, year: int, model: str, make: str = ""
) -> DocLink:
    """
    Score a candidate by filename + anchor text. Requires the model token.

    A score of 0 or below means "do not use". Model-token presence is mandatory
    so that, say, the CX-50 page's site-wide safety chart is never filed as the
    CX-50 brochure.
    """
    raw = f"{link.url} {link.text}".lower()
    # OEM filenames join words with +, _, - or . ("26MY+CX5+Spec+Deck").
    # Match keywords against a separator-normalised form so "Spec+Deck",
    # "Spec_Deck" and "spec deck" all score the same.
    haystack = re.sub(r"[^a-z0-9]+", " ", raw)
    compact = re.sub(r"[^a-z0-9]+", "", raw)

    reasons: list[str] = []
    score = 0.0

    # Inventory says "Mazda CX-5"; the document says "CX-5". Accept either.
    variants = model_token_variants(make, model)
    if not any(v and v in compact for v in variants):
        link.score = 0.0
        link.reasons = ["model token absent"]
        return link
    score += 5.0
    reasons.append("model matched")

    if str(year) in compact or f"{year % 100}my" in compact:
        score += 4.0
        reasons.append("year matched")
    else:
        # Wrong-year documents are the classic silent-corruption vector.
        link.score = 0.0
        link.reasons = ["year token absent"]
        return link

    # The variant qualifiers on the document must be exactly those on the
    # requested model, or this is a different vehicle's book.
    requested = _model_token(model)
    for qualifier in _VARIANT_QUALIFIERS:
        on_doc = qualifier in compact
        on_model = qualifier in requested
        if on_doc != on_model:
            link.score = 0.0
            link.reasons = [
                f"variant mismatch: '{qualifier}' "
                f"{'on document, not on model' if on_doc else 'on model, not on document'}"
            ]
            return link

    for word, weight in _GOOD_WORDS:
        if word in haystack:
            score += weight
            reasons.append(f"+{word}")
    for word, weight in _BAD_WORDS:
        if word in haystack:
            score += weight
            reasons.append(f"{word}")

    link.score = score
    link.reasons = reasons
    return link


def pick_best_document(
    links: list[DocLink], year: int, model: str, make: str = ""
) -> DocLink | None:
    """Highest-scoring candidate, or ``None`` if none clears the bar."""
    scored = [score_document_link(link, year, model, make) for link in links]
    viable = [link for link in scored if link.score > 0]
    if not viable:
        return None
    viable.sort(key=lambda link: (-link.score, len(link.url)))
    return viable[0]


# Official routes that deliver a document behind an opaque id, carrying no year
# or model token to score against.
_OPAQUE_DOCUMENT_ROUTE = re.compile(
    r"(?:/download/specifications/pdf/\d+)"  # kiamedia.com / kia.com
    r"|(?:/view-spec\.do\?bFileId=\d+)"  # media.subaru.com
    r"|(?:/pdf\.do\?id=\d+)",  # media.subaru.com
    re.I,
)


def page_scoped_document(
    links: list[DocLink], page_url: str, year: int, make: str, model: str
) -> DocLink | None:
    """
    The one opaque document on a page that is itself year- and model-specific.

    ``kiamedia.com/us/en/models/sportage/2026/specifications`` links its spec
    sheet as ``/us/en/download/specifications/pdf/23020``: nothing in that URL
    says Sportage or 2026, so :func:`score_document_link` refuses it and Kia
    yields nothing. The year and model live in the *page* path instead.

    Accepted only when all of these hold, because a page-derived attribution is
    weaker than a filename-derived one:

    * the page URL path contains the year and a model slug;
    * exactly one opaque official document is linked (no ambiguity about which);
    * and -- enforced by the caller after download --
      :func:`verify_document_identity` confirms the PDF names that vehicle.
    """
    path = re.sub(r"[^a-z0-9]+", "", (urlparse(page_url).path or "").lower())
    if str(year) not in path:
        return None
    if not any(v and v in path for v in model_token_variants(make, model)):
        return None
    opaque = [
        link
        for link in links
        if _OPAQUE_DOCUMENT_ROUTE.search(link.url) and is_official_url(link.url, make)
    ]
    if len(opaque) != 1:
        return None
    link = opaque[0]
    link.score = 1.0
    link.reasons = [PAGE_SCOPED_REASON, f"single opaque document on {page_url}"]
    return link


#: Marks a link whose year/model attribution came from the page path, not the
#: URL. The caller must verify the downloaded document with ``require_year``.
PAGE_SCOPED_REASON = "page-scoped attribution"
