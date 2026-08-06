"""
Locate brochure / specification PDFs, in two tiers, OEM first.

Scope and hard rules (this module deliberately fails closed):

* **Two source tiers, and OEM always wins.** Every candidate URL resolves to a
  tier via :func:`source_tier_for_url`:

  ``tier="oem"``
      a manufacturer-operated host from :data:`OFFICIAL_HOSTS` (corporate sites
      and their own press/media newsrooms), checked per make. This is the only
      tier that is tried first and the only one that needs no extra evidence.
  ``tier="archive"``
      :data:`ARCHIVE_HOSTS` -- today exactly ``auto-brochures.com``, a
      third-party mirror of manufacturer brochures. Added 2026-08-01 as a
      **reviewed policy change**; see :data:`ARCHIVE_SOURCE_NOTE` for what it
      buys, what it costs and what is done to contain the cost.

  The archive tier is consulted only after the OEM tier has been tried for that
  vehicle and produced nothing, and only when the caller passes
  ``allow_archive=True`` (``--allow-archive`` on the fetcher). Dealer sites,
  forums, blogs and encyclopedias remain off-limits at both tiers: an
  un-tiered host is dropped exactly as before.

  The tier is recorded on the stored document (:class:`StoredDocument`, the
  content index and the fetch ledger) and is resolvable from a rendered bullet's
  citation through :func:`document_tier_for_citation`, so a bullet can always be
  asked which tier backs it.

* **robots.txt is authoritative, and unreadable means no -- with one named
  exemption.** See :class:`RobotsPolicy`. A host that will not serve us
  robots.txt is treated as disallowed rather than as unrestricted, and that
  still holds for every OEM host under both user agents below. It does **not**
  hold for hosts in :data:`ROBOTS_UNREADABLE_EXEMPT_HOSTS`, which today contains
  only ``auto-brochures.com``: that host 302-redirects ``/robots.txt`` to
  ``/404.html`` and serves the HTML 404 body with an HTTP 200, so there is no
  policy to read. The user reviewed and accepted that for this host. The
  exemption is a list, not a relaxed rule, and the compensating control is
  pacing: :data:`ARCHIVE_DELAY_SECONDS` is deliberately longer than the OEM
  default, because a host that publishes no rules has told us nothing about what
  it welcomes.

  Note what the exemption also fixes. Before it, an HTML page served as
  robots.txt with a 200 was handed to :mod:`urllib.robotparser`, which found no
  ``User-agent`` line and returned "no rules -- everything allowed". That is
  fail-open by accident, on any host, and it is now
  :attr:`RobotsVerdict.status` ``"html"`` -> blocked, unless the host is on the
  exemption list.
* **Two user agents, both declared.** :data:`BROWSER_USER_AGENT` (the default)
  is an ordinary desktop Chrome string; its robots rules are read from the
  ``*`` group, which is the conservative reading. :data:`IDENTIFIED_USER_AGENT`
  names this client and matches the ``Claude-User`` robots group. Which one a
  run used is recorded in every reachability report, so a "reachable" verdict is
  never ambiguous about how it was obtained.
* **No inference.** This module only ever returns URLs it actually saw in an
  anchor tag on a page it fetched -- at both tiers. It never synthesises a
  plausible-looking brochure URL from a template. The archive's per-make index
  pages are likewise read off the site's own navigation
  (:func:`archive_make_index_pages`) rather than guessed from a slug pattern.
  The single exception is the URL *scheme*: the archive prints its own links as
  ``http://``, and :func:`_https_archive_url` upgrades them to ``https://``,
  which is the same host and path over a protected transport.
* **One document, one copy.** :class:`ContentIndex` keys stored PDFs by sha256,
  so the same file reached by two URLs or filed under two model spellings is
  stored, counted and extracted once. A byte-identical archive copy of a
  document we already hold from the OEM therefore does not become a second
  document -- it becomes evidence, which is the point of
  :func:`compare_archive_to_oem`.
* **The document must print the model's name.** A link is chosen from a
  filename, which is not evidence. :func:`verify_document_identity` re-checks
  the extracted text before any of it reaches the corpus. Read its docstring for
  what that does and does not establish: it is a *naming* test, not proof that
  the document's subject is that vehicle.
* **And it must be ABOUT that model, not about a nameplate derived from it.**
  Added 2026-08-02, after a verifier found the M3 brochure filed as, and counted
  as coverage for, the 2018 3 Series -- it names the 3 Series once, in body
  copy, which is all a naming test can ask for.
  :func:`assess_nameplate_dominance` compares how many pages each nameplate
  presides over; see :data:`DERIVED_NAMEPLATES` for what it covers, how it was
  calibrated, and the attribution risk it deliberately leaves open.
* **A text layer that lost the document's numbers is not a document.**
  :func:`assess_text_quality` now also enforces
  :data:`MIN_DIGIT_DENSITY_PER_100_LETTERS`. The 2015 BMW X1 brochure extracted
  170,126 characters containing 362 digits, because its fonts map digit glyphs
  into the Unicode Private Use Area; the prose read perfectly and every spec
  that carries a number was silently absent.
* **An archive copy carries extra authenticity checks.** We cannot assume a
  third party's copy is the unaltered original, so every archive document also
  goes through :func:`assess_archive_document`: its PDF metadata toolchain is
  read and recorded, and where we already hold the OEM original for the same
  vehicle the two are compared (sha256 first, then extracted-text similarity).
  What actually rejects an archive document is (a) structural failure -- not a
  PDF, encrypted, unopenable -- and (b) the existing text gates,
  :func:`verify_document_identity` and :func:`assess_text_quality`, which for
  this tier are the tests with teeth. The OEM comparison is recorded, not
  decisive; :data:`DIFFERENT_DOCUMENT_SIMILARITY` records the measurement that
  forced that correction. Rejected archive documents are moved to
  :data:`ARCHIVE_QUARANTINE_DIR` with the reason -- moved, never deleted.

Nothing here parses vehicle specs. It resolves *documents*; the existing
``backend.enrichment.brochure_extract`` lane turns a downloaded PDF into
``derived/brochure_text/<year>__<make>__<model>.json``. Stored PDFs are local
verification substrate only: the product renders extracted facts, and nothing
in this repo serves or redistributes a fetched PDF.

Historically one ~3850-line module. Split by responsibility into submodules
(``quarantine``, ``agents``, ``hosts``, ``naming``, ``tiers``, ``robots``,
``links``, ``discovery``, ``archive``, ``storage``, ``http``, ``reachability``,
``identity``, ``nameplates``, ``quality``, ``archive_authenticity``). This
package re-exports every previously-public name, so
``from backend.enrichment.brochure_sources import X`` keeps working unchanged.

Module-level knobs a test rebinds (``BROCHURE_TEXT_DIR``,
``BROCHURE_TEXT_QUARANTINE_DIR``, ``DERIVED_STORES_FROM_BROCHURE_TEXT``,
``ARCHIVE_QUARANTINE_DIR``) have to be patched on the submodule whose functions
read them -- rebinding the name on this facade does not reach the reader.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
import time
import urllib.robotparser
from dataclasses import dataclass, field
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin, urlparse
from backend.enrichment.brochure_extract import parse_brochure_filename
from backend.enrichment.dictionary_catalog import (
    canonical_make,
    catalog_key,
    load_make_aliases,
)
from backend.enrichment.dictionary_paths import (
    BROCHURE_TEXT_DIR,
    BROCHURES_DIR,
    DERIVED_DIR,
)

logger = logging.getLogger(__name__)

from .quarantine import (
    BROCHURE_TEXT_QUARANTINE_DIR,
    BROCHURE_TEXT_SLIM_QUARANTINE_DIR,
    DERIVED_STORES_FROM_BROCHURE_TEXT,
    DerivedStore,
    TRIM_ADDS_BY_YEAR_QUARANTINE_DIR,
    TRIM_CANDIDATES_QUARANTINE_DIR,
    _document_key_of_citation,
    _overlay_cited_documents,
    _source_pdf_sha,
    apply_derived_quarantine,
    derived_quarantine_reason,
    is_quarantined,
    plan_derived_quarantine,
    quarantined_document_keys,
    superseded_quarantine_keys,
)
from .agents import (
    BROWSER_USER_AGENT,
    DEFAULT_DELAY_SECONDS,
    IDENTIFIED_USER_AGENT,
    REQUEST_TIMEOUT_SECONDS,
    ROBOTS_AGENT,
    ROBOTS_AGENT_BY_UA,
    USER_AGENT,
    robots_agent_for,
)
from .hosts import (
    BMW_PRESS_ATTACHMENT_EVIDENCE,
    DISCOVERY_UNRESOLVED,
    GM_LEDGER_EVIDENCE,
    HOST_ACCESS_NOTES,
    HTML_SPEC_LANE_NOTE,
    OFFICIAL_HOSTS,
    UNSUPPORTED_MAKES,
    VERIFIED_MAKES,
)
from .naming import (
    _EXTRA_MAKE_TOKENS,
    _known_make_tokens,
    _make_token,
    _model_token,
    gap_group_key,
    is_official_url,
    model_token_variants,
    normalized_model_token,
    official_hosts_for,
    unsupported_reason,
)
from .tiers import (
    ARCHIVE_DELAY_SECONDS,
    ARCHIVE_HOSTS,
    ARCHIVE_SOURCE_NOTE,
    TIER_ARCHIVE,
    TIER_OEM,
    TIER_PREFERENCE,
    is_allowed_source_url,
    is_archive_url,
    source_tier_for_url,
)
from .robots import (
    ROBOTS_UNREADABLE_EXEMPT_HOSTS,
    RobotsPolicy,
    RobotsVerdict,
    _looks_like_html,
)
from .links import (
    DocLink,
    PAGE_SCOPED_REASON,
    _AnchorParser,
    _BAD_WORDS,
    _DOCUMENT_ROUTE,
    _GOOD_WORDS,
    _OPAQUE_DOCUMENT_ROUTE,
    _VARIANT_QUALIFIERS,
    extract_document_links,
    page_scoped_document,
    pick_best_document,
    score_document_link,
)
from .discovery import (
    TOYOTA_BROCHURE_INDEX_PAGES,
    _DISCOVERY_PAGES,
    _hyundai_pages,
    _kia_pages,
    _mazda_pages,
    _nissan_pages,
    _slugs,
    _subaru_pages,
    _toyota_pages,
    _volkswagen_pages,
    discovery_pages,
)
from .archive import (
    ARCHIVE_ROOT_URL,
    _https_archive_url,
    _path_only,
    archive_index_url,
    archive_make_index_pages,
    extract_archive_document_links,
    pick_archive_document,
)
from .storage import (
    ContentIndex,
    StoredDocument,
    _TIER_BY_SHA_CACHE,
    _tier_by_sha,
    brochure_pdf_filename,
    brochure_text_filename,
    document_tier_for_citation,
    sha256_bytes,
    sha256_file,
)
from .http import (
    PacedFetcher,
    looks_like_pdf,
)
from .reachability import (
    BLOCKED_BY_THEIR_EDGE,
    BLOCKED_BY_THEIR_RULES,
    BLOCKED_BY_US,
    JS_SHELL_MAX_BYTES,
    PROBE_TARGETS,
    ReachabilityResult,
    probe_target,
)
from .identity import (
    IDENTITY_MAX_CONTEXT_ALNUM,
    MAX_NAME_SEPARATOR_RUN,
    MIN_TRIM_TEXT_CHARS,
    _declaration_context,
    _ends_a_word,
    _starts_a_word,
    find_model_name,
    model_name_pattern,
    verify_document_identity,
)
from .nameplates import (
    DERIVED_NAMEPLATES,
    SubjectVerdict,
    VARIANT_MIN_DERIVED_PAGES,
    VARIANT_PAGE_DOMINANCE_RATIO,
    assess_nameplate_dominance,
    declares_nameplate,
    derived_nameplates_for,
)
from .quality import (
    MAX_PRIVATE_USE_CODEPOINTS,
    MIN_DIGIT_DENSITY_LETTERS,
    MIN_DIGIT_DENSITY_PER_100_LETTERS,
    QualityVerdict,
    _PRIVATE_USE_AREA,
    _longest_consecutive_run,
    assess_text_quality,
)
from .archive_authenticity import (
    ARCHIVE_COMPARISON_DIR,
    ARCHIVE_QUARANTINE_DIR,
    ArchiveAuthenticity,
    DIFFERENT_DOCUMENT_SIMILARITY,
    OemArchiveComparison,
    SAME_DOCUMENT_SIMILARITY,
    SIMILARITY_SHINGLE,
    _similarity_tokens,
    assess_archive_document,
    compare_archive_to_oem,
    pdf_text_layer,
    quarantine_archive_pdf,
    read_pdf_metadata,
    text_similarity,
)

__all__ = [
    "ARCHIVE_COMPARISON_DIR",
    "ARCHIVE_DELAY_SECONDS",
    "ARCHIVE_HOSTS",
    "ARCHIVE_QUARANTINE_DIR",
    "ARCHIVE_ROOT_URL",
    "ARCHIVE_SOURCE_NOTE",
    "ArchiveAuthenticity",
    "BLOCKED_BY_THEIR_EDGE",
    "BLOCKED_BY_THEIR_RULES",
    "BLOCKED_BY_US",
    "BMW_PRESS_ATTACHMENT_EVIDENCE",
    "BROCHURES_DIR",
    "BROCHURE_TEXT_DIR",
    "BROCHURE_TEXT_QUARANTINE_DIR",
    "BROCHURE_TEXT_SLIM_QUARANTINE_DIR",
    "BROWSER_USER_AGENT",
    "ContentIndex",
    "DEFAULT_DELAY_SECONDS",
    "DERIVED_DIR",
    "DERIVED_NAMEPLATES",
    "DERIVED_STORES_FROM_BROCHURE_TEXT",
    "DIFFERENT_DOCUMENT_SIMILARITY",
    "DISCOVERY_UNRESOLVED",
    "DerivedStore",
    "DocLink",
    "GM_LEDGER_EVIDENCE",
    "HOST_ACCESS_NOTES",
    "HTMLParser",
    "HTML_SPEC_LANE_NOTE",
    "IDENTIFIED_USER_AGENT",
    "IDENTITY_MAX_CONTEXT_ALNUM",
    "JS_SHELL_MAX_BYTES",
    "MAX_NAME_SEPARATOR_RUN",
    "MAX_PRIVATE_USE_CODEPOINTS",
    "MIN_DIGIT_DENSITY_LETTERS",
    "MIN_DIGIT_DENSITY_PER_100_LETTERS",
    "MIN_TRIM_TEXT_CHARS",
    "OFFICIAL_HOSTS",
    "OemArchiveComparison",
    "PAGE_SCOPED_REASON",
    "PROBE_TARGETS",
    "PacedFetcher",
    "Path",
    "QualityVerdict",
    "REQUEST_TIMEOUT_SECONDS",
    "ROBOTS_AGENT",
    "ROBOTS_AGENT_BY_UA",
    "ROBOTS_UNREADABLE_EXEMPT_HOSTS",
    "ReachabilityResult",
    "RobotsPolicy",
    "RobotsVerdict",
    "SAME_DOCUMENT_SIMILARITY",
    "SIMILARITY_SHINGLE",
    "StoredDocument",
    "SubjectVerdict",
    "TIER_ARCHIVE",
    "TIER_OEM",
    "TIER_PREFERENCE",
    "TOYOTA_BROCHURE_INDEX_PAGES",
    "TRIM_ADDS_BY_YEAR_QUARANTINE_DIR",
    "TRIM_CANDIDATES_QUARANTINE_DIR",
    "UNSUPPORTED_MAKES",
    "USER_AGENT",
    "VARIANT_MIN_DERIVED_PAGES",
    "VARIANT_PAGE_DOMINANCE_RATIO",
    "VERIFIED_MAKES",
    "_AnchorParser",
    "_BAD_WORDS",
    "_DISCOVERY_PAGES",
    "_DOCUMENT_ROUTE",
    "_EXTRA_MAKE_TOKENS",
    "_GOOD_WORDS",
    "_OPAQUE_DOCUMENT_ROUTE",
    "_PRIVATE_USE_AREA",
    "_TIER_BY_SHA_CACHE",
    "_VARIANT_QUALIFIERS",
    "_declaration_context",
    "_document_key_of_citation",
    "_ends_a_word",
    "_https_archive_url",
    "_hyundai_pages",
    "_kia_pages",
    "_known_make_tokens",
    "_longest_consecutive_run",
    "_looks_like_html",
    "_make_token",
    "_mazda_pages",
    "_model_token",
    "_nissan_pages",
    "_overlay_cited_documents",
    "_path_only",
    "_similarity_tokens",
    "_slugs",
    "_source_pdf_sha",
    "_starts_a_word",
    "_subaru_pages",
    "_tier_by_sha",
    "_toyota_pages",
    "_volkswagen_pages",
    "annotations",
    "apply_derived_quarantine",
    "archive_index_url",
    "archive_make_index_pages",
    "assess_archive_document",
    "assess_nameplate_dominance",
    "assess_text_quality",
    "brochure_pdf_filename",
    "brochure_text_filename",
    "canonical_make",
    "catalog_key",
    "compare_archive_to_oem",
    "dataclass",
    "declares_nameplate",
    "derived_nameplates_for",
    "derived_quarantine_reason",
    "discovery_pages",
    "document_tier_for_citation",
    "extract_archive_document_links",
    "extract_document_links",
    "field",
    "find_model_name",
    "gap_group_key",
    "hashlib",
    "is_allowed_source_url",
    "is_archive_url",
    "is_official_url",
    "is_quarantined",
    "json",
    "load_make_aliases",
    "logger",
    "logging",
    "looks_like_pdf",
    "model_name_pattern",
    "model_token_variants",
    "normalized_model_token",
    "official_hosts_for",
    "page_scoped_document",
    "parse_brochure_filename",
    "pdf_text_layer",
    "pick_archive_document",
    "pick_best_document",
    "plan_derived_quarantine",
    "probe_target",
    "quarantine_archive_pdf",
    "quarantined_document_keys",
    "re",
    "read_pdf_metadata",
    "robots_agent_for",
    "score_document_link",
    "sha256_bytes",
    "sha256_file",
    "source_tier_for_url",
    "superseded_quarantine_keys",
    "text_similarity",
    "time",
    "unsupported_reason",
    "urljoin",
    "urllib",
    "urlparse",
    "verify_document_identity",
]
