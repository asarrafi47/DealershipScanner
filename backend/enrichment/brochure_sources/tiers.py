"""Source tiers: OEM, archive, and what counts as an allowed source URL."""
from __future__ import annotations

import logging
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

from .naming import (
    is_official_url,
)

# --------------------------------------------------------------------------
# Source tiers
# --------------------------------------------------------------------------

#: A manufacturer-operated host from :data:`OFFICIAL_HOSTS`. Always preferred.
TIER_OEM = "oem"

#: A third-party mirror of manufacturer brochures -- :data:`ARCHIVE_HOSTS`.
#: Fallback only, and only for vehicles the OEM tier will not serve.
TIER_ARCHIVE = "archive"

#: Ordered by preference. Resolution walks this list and stops at the first tier
#: that yields a document, so "OEM first" is a property of the data, not of a
#: comment somewhere.
TIER_PREFERENCE: tuple[str, ...] = (TIER_OEM, TIER_ARCHIVE)

#: The one archive host. NOT a per-make allowlist: this host mirrors every make,
#: which is precisely why it needs the authenticity checks that an OEM host does
#: not.
ARCHIVE_HOSTS: frozenset[str] = frozenset(
    {"www.auto-brochures.com", "auto-brochures.com"}
)

#: The reviewed decision, recorded where the code that acts on it lives.
#:
#: WHAT IT BUYS. The OEM tier is verified end to end for three makes
#: (:data:`VERIFIED_MAKES`). Everything else is either an unresolved discovery
#: step (:data:`DISCOVERY_UNRESOLVED`), a host that refuses automated access
#: (:data:`HOST_ACCESS_NOTES`) or an unregistered make
#: (:data:`UNSUPPORTED_MAKES`) -- and manufacturers do not keep prior model
#: years online at all, so even a working OEM path cannot answer a 2014 gap.
#: auto-brochures.com mirrors ~78 makes back to the 1950s.
#:
#: WHAT IT COSTS, stated plainly rather than argued away:
#:
#:   1. It is not the manufacturer. A copy held by a third party can have been
#:      re-saved, re-compressed, page-reordered, watermarked or edited, and we
#:      would not know from the bytes alone.
#:   2. It publishes no robots.txt (see :data:`ROBOTS_UNREADABLE_EXEMPT_HOSTS`),
#:      so it has told us nothing about what automated access it welcomes.
#:   3. Some of it is scanned paper with a thin or absent text layer, which
#:      yields nothing to quote. NOT quantified across the library -- we have not
#:      surveyed it and will not, since the scope rule is "active inventory only".
#:      What IS measured is per document, by
#:      ``fetch_oem_brochures --archive-audit``: of the 20 archive documents
#:      fetched on 2026-08-01, 0 had a completely empty text layer, though
#:      ``2018 Hyundai Tucson`` had text on only 1 of its 7 pages and was
#:      quarantined for never printing the model's name.
#:
#: WHAT CONTAINS THE COST:
#:
#:   * OEM first, always: the archive is reached only after the OEM tier has
#:     been tried for that vehicle and returned nothing.
#:   * :func:`assess_archive_document` on every archive document -- metadata
#:     toolchain recorded, structural failures refused, and compared against the
#:     OEM original wherever we hold one. Be honest about the strength of that
#:     last part: measured 2026-08-01, it is only decisive when the hashes match.
#:   * The document must print the vehicle's name as a declaration
#:     (:func:`verify_document_identity`) or it is quarantined, not merely
#:     skipped. For this tier that is the real authenticity gate.
#:   * The tier travels with the document into the citation
#:     (:func:`document_tier_for_citation`), so no rendered claim is ever
#:     tier-anonymous.
#:   * Pacing strictly more conservative than the OEM path
#:     (:data:`ARCHIVE_DELAY_SECONDS`).
#:   * Every existing gate still applies unchanged: identity, quality,
#:     year/model/variant scoring, sha256 dedupe, quarantine.
#:
#: Decided by the user 2026-08-01. Reverting it is deleting this constant and
#: the two hosts in :data:`ARCHIVE_HOSTS`; nothing else opened up.
ARCHIVE_SOURCE_NOTE = (
    "auto-brochures.com is a third-party mirror, admitted 2026-08-01 as a "
    "reviewed fallback for vehicles no manufacturer host will serve. It is "
    "never preferred over an OEM copy and every document from it is "
    "authenticity-checked before its text is used."
)

#: Seconds between consecutive archive requests. Deliberately more than
#: :data:`DEFAULT_DELAY_SECONDS` (4.0): the OEM hosts publish robots.txt and
#: tell us what they welcome, this one publishes nothing, so the polite floor is
#: higher rather than lower. Enforced as a floor in
#: :meth:`PacedFetcher.for_tier` -- a caller cannot pass ``--delay 1`` and get a
#: faster archive crawl.
ARCHIVE_DELAY_SECONDS = 12.0


def is_archive_url(url: str) -> bool:
    """True if ``url`` is on :data:`ARCHIVE_HOSTS` over http(s)."""
    try:
        parsed = urlparse(url)
    except ValueError:
        return False
    if parsed.scheme not in ("http", "https"):
        return False
    return (parsed.hostname or "").lower() in ARCHIVE_HOSTS


def source_tier_for_url(url: str, make: str) -> str:
    """
    ``"oem"`` / ``"archive"`` / ``""`` for a candidate URL.

    ``""`` means no tier admits this host for this make, which is a refusal --
    the same refusal :func:`is_official_url` gave before there were tiers.
    """
    if is_official_url(url, make):
        return TIER_OEM
    if is_archive_url(url):
        return TIER_ARCHIVE
    return ""


def is_allowed_source_url(url: str, make: str, *, allow_archive: bool = False) -> bool:
    """
    Whether a document may be downloaded from ``url`` for ``make``.

    This is the tier-aware gate. :func:`is_official_url` is deliberately left
    alone and still means exactly "on a manufacturer host for this make" -- the
    archive was added *beside* it, not by loosening it, so every existing caller
    and test of ``is_official_url`` keeps its old meaning.
    """
    tier = source_tier_for_url(url, make)
    if tier == TIER_OEM:
        return True
    return tier == TIER_ARCHIVE and bool(allow_archive)
