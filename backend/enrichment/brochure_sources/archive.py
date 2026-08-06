"""Archive-tier discovery (auto-brochures.com)."""
from __future__ import annotations

import logging
from urllib.parse import urljoin, urlparse

logger = logging.getLogger(__name__)

from .links import (
    DocLink,
    _AnchorParser,
    _DOCUMENT_ROUTE,
    score_document_link,
)
from .naming import (
    _make_token,
)
from .tiers import (
    ARCHIVE_HOSTS,
    is_archive_url,
)

# --------------------------------------------------------------------------
# Archive tier discovery (auto-brochures.com)
# --------------------------------------------------------------------------

#: The archive's own front page. The ONE archive URL written down here, because
#: an entry point has to come from somewhere; everything below it is read off
#: the site's own anchors rather than constructed from a slug pattern.
ARCHIVE_ROOT_URL = "https://www.auto-brochures.com/index.html"


def _https_archive_url(url: str) -> str:
    """
    The same archive URL over https.

    The site prints every internal link as ``http://www.auto-brochures.com/...``
    while serving the identical path over https. This rewrites the scheme and
    nothing else -- same host, same path -- so we are not sending requests or
    receiving PDFs in clear text. It is applied only to hosts in
    :data:`ARCHIVE_HOSTS`.
    """
    parsed = urlparse(url)
    if (parsed.hostname or "").lower() not in ARCHIVE_HOSTS or parsed.scheme != "http":
        return url
    return parsed._replace(scheme="https").geturl()


def archive_make_index_pages(html: str, base_url: str = ARCHIVE_ROOT_URL) -> dict[str, str]:
    """
    ``{make_token: index_url}`` read out of the archive's own navigation.

    The site's front page carries one anchor per make. Parsing them is how the
    per-make index URL is *observed* rather than guessed -- the same rule the OEM
    tier follows. The anchor text is the make name the site prints ("Alfa Romeo",
    "Mercedes-Benz"), so it maps onto our make tokens through
    :func:`_make_token` and therefore through ``make_aliases.json``.

    Non-make pages ("home", "about", "contact", "sitemap") carry an underscore
    prefix or a token we have no make for, and drop out.
    """
    parser = _AnchorParser()
    try:
        parser.feed(html or "")
    except Exception:  # noqa: BLE001 - malformed markup is expected
        pass

    out: dict[str, str] = {}
    for href, text in parser.anchors:
        url = _https_archive_url(urljoin(base_url, (href or "").strip()))
        if not is_archive_url(url):
            continue
        if not urlparse(url).path.lower().endswith(".html"):
            continue
        token = _make_token(text)
        if not token or token in ("home", "about", "contact", "sitemap", "email"):
            continue
        out.setdefault(token, url)
    return out


def archive_index_url(make: str, index_pages: dict[str, str]) -> str | None:
    """The archive index page for ``make``, or None if the site lists no such make."""
    return index_pages.get(_make_token(make))


def extract_archive_document_links(html: str, base_url: str) -> list[DocLink]:
    """
    Document anchors on an archive index page, https-upgraded.

    Deliberately separate from :func:`extract_document_links`, which filters by
    the per-make OEM allowlist and must keep doing exactly that. Here the filter
    is :func:`is_archive_url`: an archive page linking off-site to a forum or a
    dealer yields nothing, same as before.
    """
    parser = _AnchorParser()
    try:
        parser.feed(html or "")
    except Exception:  # noqa: BLE001
        pass

    seen: set[str] = set()
    out: list[DocLink] = []
    for href, text in parser.anchors:
        href = (href or "").strip()
        if not href or href.startswith(("javascript:", "mailto:", "#")):
            continue
        url = _https_archive_url(urljoin(base_url, href))
        if not _DOCUMENT_ROUTE.search(url) or not is_archive_url(url):
            continue
        if url in seen:
            continue
        seen.add(url)
        out.append(DocLink(url=url, text=text))
    return out


def _path_only(link: DocLink) -> DocLink:
    """
    A copy of ``link`` scored on its path and filename alone.

    The archive's hostname contains the word "brochures", which
    :data:`_GOOD_WORDS` is worth +5.0. Scoring the full URL would hand every
    archive candidate that bonus for the host it happens to live on, which is
    not evidence about the document. Dropping the host keeps the archive's
    scores on the same footing as an OEM filename's.
    """
    parsed = urlparse(link.url)
    path = f"{parsed.path}?{parsed.query}" if parsed.query else parsed.path
    return DocLink(url=path, text=link.text)


def pick_archive_document(
    links: list[DocLink], year: int, make: str, model: str
) -> DocLink | None:
    """
    Best archive candidate for one vehicle, using the OEM tier's scoring rules.

    Every gate :func:`score_document_link` enforces applies unchanged: the model
    token must be present, the year token must be present, and the variant
    qualifiers must match symmetrically. Being a fallback source does not buy
    looser matching -- it buys a second place to look.
    """
    best: DocLink | None = None
    best_score = 0.0
    for link in links:
        scored = score_document_link(_path_only(link), year, model, make)
        if scored.score <= 0:
            continue
        if best is None or scored.score > best_score or (
            scored.score == best_score and len(link.url) < len(best.url)
        ):
            best, best_score = link, scored.score
            link.score = scored.score
            link.reasons = scored.reasons
    return best
