#!/usr/bin/env python3
"""
Acquisition lane for brochure / specification PDFs, OEM tier first.

Three things this does, all of them safe to run repeatedly:

* **Gap list.** Rank ``(year, make, model)`` by active car count, keeping only
  vehicles with no ``derived/brochure_text/<key>.json``. Inventory spells the
  same vehicle several ways (``Mazda CX-50`` / ``Cx-50``), so combos are folded
  on :func:`gap_group_key` before counting. Written out as JSON for other
  agents; ``--brand`` slices it so parallel agents take disjoint work. The gap
  list is also the **scope limit**: only vehicles we have active inventory for
  are ever fetched, at either tier. This does not mirror a library.
* **Fetch.** Resolve a source, download it once, extract text through
  ``brochure_extract``'s lossless whole-PDF capture. Every download is
  content-hashed: a sha256 already in the index is never stored, extracted or
  counted twice, and a URL already fetched is not requested again. The PDF is
  kept even when its text is refused -- deleting acquired source PDFs after a
  bad capture is what cost this project 1,368 brochures.
* **Reachability report.** Per brand, what the OEM's own document index serves
  an automated client, under either user agent. Tells the acquisition phase
  where not to spend time.

**Source tiers.** ``resolve_source`` tries ``tier="oem"`` (a manufacturer host
from ``OFFICIAL_HOSTS``) in full and returns immediately if it finds a document.
Only when it does not, and only with ``--allow-archive``, does it fall back to
``tier="archive"`` -- ``auto-brochures.com``, admitted 2026-08-01 as a reviewed
policy change (``brochure_sources.ARCHIVE_SOURCE_NOTE``). The archive gets a
separate fetcher paced at a 12s floor, and every archive document must clear
``assess_archive_document`` (metadata recorded; compared against the OEM
original where we hold one) before its text is extracted. A failure is stored
and then moved to ``backend/data/brochures_archive_quarantine/`` with the reason
-- kept, never deleted, never extracted. The tier is written into the content
index and the fetch ledger and is resolvable back from a rendered bullet's
citation through ``brochure_sources.document_tier_for_citation``.

Stored PDFs are local verification substrate. Nothing here, and nothing in this
repo, serves or redistributes a fetched PDF; the product renders extracted facts.

``--audit-corpus`` reports duplicate stored documents and how many
``brochure_text`` files are actually usable, since a file existing is what the
gap list counts as coverage. ``--quarantine-unidentified`` re-runs
``verify_document_identity`` over that corpus and moves the failures into
``derived/brochure_text_quarantine/`` with a manifest -- moved, never deleted.
That pass then sweeps the stores BUILT from those documents
(``brochure_text_slim``, ``trim_candidates``, ``trim_adds_by_year``) into their
own sibling quarantine dirs, because quarantining the document alone does not
stop it reaching a car page; ``--quarantine-derived`` runs that sweep alone.

**HTML specification pages** (``--html-specs``, added 2026-08-02). Some makes
publish their per-trim equipment grid as an HTML table rather than as a PDF, and
being unable to find a ``.pdf`` href on their site was being recorded as
"publishes nothing we can use". Honda is the worked case: hondanews.com carries
one "Specifications & Features" release per model year whose body is a single
table with a trim per column. The lane is the same lane -- same host allowlist,
same robots policy, same pacing, same identity and text-quality gates -- and the
only thing that differs is the locator: a PDF bullet cites ``{file, page}``, an
HTML bullet cites ``{url, retrieved_at, sha256, table/row/column}`` into bytes
stored under ``backend/data/spec_pages/``. Overlays land in
``derived/html_spec_overlays/`` with ``source="html_spec_quoted"``, which no
admissibility table lists, so they render nowhere until that is changed on
purpose. See ``backend/enrichment/html_spec_sources``.

Dry run is the default: with no flags nothing is downloaded and nothing under
``backend/data/`` or ``backend/dictionary/`` is written. Downloading requires
``--download``.

Usage:
    python -m backend.scripts.fetch_oem_brochures                    # plan only
    python -m backend.scripts.fetch_oem_brochures --show-gaps 40
    python -m backend.scripts.fetch_oem_brochures --brand mazda --limit 10
    python -m backend.scripts.fetch_oem_brochures --brand mazda --download --limit 5
    python -m backend.scripts.fetch_oem_brochures --probe-reachability
    python -m backend.scripts.fetch_oem_brochures --probe-reachability \
        --user-agent both
    python -m backend.scripts.fetch_oem_brochures --audit-corpus
    python -m backend.scripts.fetch_oem_brochures --quarantine-unidentified
    python -m backend.scripts.fetch_oem_brochures --quarantine-unidentified --apply
    python -m backend.scripts.fetch_oem_brochures --quarantine-derived
    python -m backend.scripts.fetch_oem_brochures --quarantine-derived --apply
    python -m backend.scripts.fetch_oem_brochures --allow-archive --limit 20
    python -m backend.scripts.fetch_oem_brochures --allow-archive --download --limit 20
    python -m backend.scripts.fetch_oem_brochures --archive-verify-overlap 12
    python -m backend.scripts.fetch_oem_brochures --archive-audit
    python -m backend.scripts.fetch_oem_brochures --html-specs --brand honda
    python -m backend.scripts.fetch_oem_brochures --html-specs --brand honda \
        --download --limit 40
    python -m backend.scripts.fetch_oem_brochures --rebuild-html-overlays
    python -m backend.scripts.fetch_oem_brochures --reverify-html-specs
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from backend.db.connect import connection as db_connection  # noqa: E402

from backend.enrichment.brochure_extract import (  # noqa: E402
    extract_brochure_text_pdf,
    persist_brochure_text,
)
from backend.enrichment.brochure_sources import (  # noqa: E402
    ARCHIVE_COMPARISON_DIR,
    ARCHIVE_DELAY_SECONDS,
    ARCHIVE_QUARANTINE_DIR,
    ARCHIVE_ROOT_URL,
    ARCHIVE_SOURCE_NOTE,
    BROCHURE_TEXT_QUARANTINE_DIR,
    BROWSER_USER_AGENT,
    DERIVED_STORES_FROM_BROCHURE_TEXT,
    DISCOVERY_UNRESOLVED,
    HOST_ACCESS_NOTES,
    IDENTIFIED_USER_AGENT,
    PROBE_TARGETS,
    TIER_ARCHIVE,
    TIER_OEM,
    VERIFIED_MAKES,
    ContentIndex,
    PacedFetcher,
    ReachabilityResult,
    RobotsPolicy,
    apply_derived_quarantine,
    archive_index_url,
    archive_make_index_pages,
    assess_archive_document,
    assess_nameplate_dominance,
    assess_text_quality,
    brochure_pdf_filename,
    compare_archive_to_oem,
    discovery_pages,
    document_tier_for_citation,
    extract_archive_document_links,
    extract_document_links,
    gap_group_key,
    is_allowed_source_url,
    PAGE_SCOPED_REASON,
    looks_like_pdf,
    page_scoped_document,
    pdf_text_layer,
    pick_archive_document,
    pick_best_document,
    plan_derived_quarantine,
    probe_target,
    quarantine_archive_pdf,
    read_pdf_metadata,
    sha256_bytes,
    source_tier_for_url,
    unsupported_reason,
    verify_document_identity,
)
from backend.enrichment.dictionary_catalog import canonical_make, catalog_key  # noqa: E402
from backend.enrichment.dictionary_paths import (  # noqa: E402
    BROCHURES_DIR,
    BROCHURE_TEXT_DIR,
    DERIVED_DIR,
    TRIM_ADDS_BY_YEAR_DIR,
)
from backend.enrichment.html_spec_sources import (  # noqa: E402
    HONDA_NEWSROOM_HOME,
    HTML_SPEC_TEXT_DIR,
    StoredPage,
    HTML_SPEC_MAKES,
    HTML_SPEC_PAGES_DIR,
    OVERLAY_SOURCE as HTML_SPEC_OVERLAY_SOURCE,
    REFUSED_BY_DESIGN as HTML_SPEC_REFUSED_BY_DESIGN,
    build_overlay,
    build_transcript,
    honda_channel_for_model,
    honda_model_channels,
    honda_spec_releases,
    honda_specs_tab_url,
    official_spec_page,
    pick_spec_release,
    read_grids,
    store_page,
    transcript_path as html_spec_transcript_path,
    utc_now,
    verify_citation,
    verify_page_identity,
    write_transcript,
)

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

#: Append-only provenance ledger: one line per stored document.
FETCH_LOG_PATH = DERIVED_DIR / "brochure_fetch_log.jsonl"

#: sha256 -> the single stored copy. Lives next to the ledger.
CONTENT_INDEX_PATH = DERIVED_DIR / "brochure_content_index.json"

#: Artifacts for other agents. Under workspace/ because they are run output,
#: not dictionary data.
ARTIFACT_DIR = _REPO / "workspace" / "brochure_acquisition"


# --------------------------------------------------------------------------
# Gap list
# --------------------------------------------------------------------------


def _fetch_inventory_rows() -> list[tuple[int, str, str, int]]:
    with db_connection(timeout=None) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT year, make, model, COUNT(*)
                FROM cars
                WHERE listing_removed_at IS NULL
                  AND year IS NOT NULL
                  AND make IS NOT NULL AND btrim(make) <> ''
                  AND model IS NOT NULL AND btrim(model) <> ''
                GROUP BY 1, 2, 3
                """
            )
            return [(int(y), m, mo, int(c)) for y, m, mo, c in cur.fetchall()]


def build_gap_list(
    rows: list[tuple[int, str, str, int]], have: set[str]
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """
    Fold inventory rows into one entry per real vehicle, ranked by car count.

    ``have`` is the set of corpus stems (``2026__mazda__cx50``).

    Two counts come out of this and they answer different questions:

    * ``uncovered_vehicles`` -- distinct vehicles with **no** brochure text under
      any of their inventory spellings. This is the acquisition backlog.
    * ``alias_only_keys`` -- catalog keys with no corpus file whose sibling
      spelling *does* have one. Those vehicles are not missing a document; the
      exact-match lookup in ``dictionary_derived`` just cannot see it. Fetching
      for them would re-download a document we already hold, which is how the
      first run of this script stored the CX-50 spec deck twice.
    """
    groups: dict[str, dict[str, Any]] = {}
    total_cars = 0
    raw_keys: set[str] = set()

    for year, make, model, count in rows:
        total_cars += count
        ck = catalog_key(year, make, model).replace("|", "__")
        raw_keys.add(ck)
        gk = gap_group_key(year, make, model)
        entry = groups.setdefault(
            gk,
            {
                "group_key": gk,
                "year": year,
                "make": canonical_make(make),
                "cars": 0,
                "variants": [],
            },
        )
        entry["cars"] += count
        entry["variants"].append(
            {
                "make": make,
                "model": model,
                "cars": count,
                "catalog_key": ck,
                "has_text": ck in have,
            }
        )

    gaps: list[dict[str, Any]] = []
    covered_cars = 0
    covered_vehicles = 0
    alias_only: list[dict[str, Any]] = []

    for entry in groups.values():
        variants = sorted(entry["variants"], key=lambda v: (-v["cars"], v["catalog_key"]))
        entry["variants"] = variants
        entry["model"] = variants[0]["model"]
        covered_variants = [v for v in variants if v["has_text"]]

        if covered_variants:
            covered_cars += entry["cars"]
            covered_vehicles += 1
            held = covered_variants[0]["catalog_key"]
            alias_only.extend(
                {
                    "missing_key": v["catalog_key"],
                    "document_held_under": held,
                    "cars": v["cars"],
                }
                for v in variants
                if not v["has_text"]
            )
            continue

        # Where a fetched document should be filed. The corpus convention strips
        # the make prefix ("2010__mazda__3.json"), so prefer a spelling that
        # already does; otherwise the most common spelling.
        stripped = [
            v
            for v in variants
            if catalog_key(entry["year"], v["make"], v["model"]).split("|")[-1]
            == entry["group_key"].split("|")[-1]
        ]
        primary = (stripped or variants)[0]
        entry["primary"] = primary
        entry["primary_catalog_key"] = primary["catalog_key"]
        entry["alias_catalog_keys"] = [
            v["catalog_key"] for v in variants if v["catalog_key"] != primary["catalog_key"]
        ]
        entry["unsupported_reason"] = unsupported_reason(entry["make"])
        gaps.append(entry)

    gaps.sort(key=lambda g: (-g["cars"], g["group_key"]))

    stats = {
        "active_cars": total_cars,
        "corpus_files": len(have),
        "raw_catalog_key_combos": len(raw_keys),
        "distinct_vehicles": len(groups),
        "covered_vehicles": covered_vehicles,
        "uncovered_vehicles": len(gaps),
        "covered_cars": covered_cars,
        "gap_cars": total_cars - covered_cars,
        "alias_only_keys": len({a["missing_key"] for a in alias_only}),
        "alias_only_cars": sum(a["cars"] for a in alias_only),
    }
    stats["alias_only"] = sorted(alias_only, key=lambda a: -a["cars"])
    return gaps, stats


def load_gap_list() -> tuple[list[dict[str, Any]], dict[str, Any]]:
    have = {p.stem for p in BROCHURE_TEXT_DIR.glob("*.json") if not p.name.startswith("_")}
    return build_gap_list(_fetch_inventory_rows(), have)


def write_gap_artifact(
    gaps: list[dict[str, Any]], stats: dict[str, Any], path: Path, brand: str | None
) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "brand_filter": brand,
        "stats": stats,
        "gaps": gaps,
    }
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


# --------------------------------------------------------------------------
# Resolution
# --------------------------------------------------------------------------


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


# --------------------------------------------------------------------------
# Download + extract
# --------------------------------------------------------------------------


def oem_pdf_for_catalog_key(catalog_key_str: str, index: ContentIndex) -> Path | None:
    """
    A stored OEM-tier PDF for this vehicle, if we hold one.

    This is what makes the OEM-vs-archive comparison possible during an ordinary
    fetch rather than only in a dedicated pass. Documents whose tier was never
    recorded (everything fetched before 2026-08-01) count as OEM here **only**
    because every one of them came from :data:`OFFICIAL_HOSTS` -- the archive
    tier did not exist when they were fetched, so there is nothing else they
    could be. That reasoning is stated rather than assumed, and it stops being
    true the moment an untiered archive document could exist, which is why
    :meth:`ContentIndex.register` records the tier on every new document.
    """
    for doc in index.by_sha.values():
        if doc.tier == TIER_ARCHIVE:
            continue
        if catalog_key_str in doc.catalog_keys and Path(doc.path).is_file():
            return Path(doc.path)
    return None


def download_and_extract(
    resolved: dict[str, Any],
    fetcher: PacedFetcher,
    robots: RobotsPolicy,
    index: ContentIndex,
    *,
    archive: ArchiveResolver | None = None,
    apply_quarantine: bool = True,
) -> dict[str, Any]:
    """
    Download one resolved document and run the existing extraction lane.

    Content-hash dedupe happens here, in two places:

    1. Before the request, if this exact URL is already in the index (resume:
       a repeat run costs no bandwidth).
    2. After the response, on the sha256 of the bytes. A hash already stored is
       not written again; the existing file is used and the outcome is reported
       as ``duplicate_content``.

    An archive-tier document takes the archive's own fetcher (12s pacing) and,
    once downloaded, must clear :func:`assess_archive_document` before its text
    is extracted. A failure there stores the PDF and then moves it to
    :data:`ARCHIVE_QUARANTINE_DIR` with the reason -- the document is kept, its
    text is not derived, and the corpus never sees it.
    """
    year, make, model = resolved["year"], resolved["make"], resolved["model"]
    doc_url = resolved["doc_url"]
    tier = resolved.get("tier") or source_tier_for_url(doc_url, make)
    out: dict[str, Any] = dict(resolved)
    out["tier"] = tier
    ck = resolved["primary_catalog_key"].replace("__", "|")

    if tier == TIER_ARCHIVE:
        if archive is None:
            out["status"] = "rejected_archive_not_enabled"
            out["detail"] = "archive-tier URL resolved without --allow-archive"
            return out
        fetcher, robots = archive.fetcher, archive.robots
    out["user_agent"] = fetcher.user_agent

    if not is_allowed_source_url(doc_url, make, allow_archive=archive is not None):
        out["status"] = "rejected_non_official_host"
        out["detail"] = f"{doc_url} is on no tier we admit for {make}"
        return out
    if not robots.allows(doc_url):
        out["status"] = "robots_disallowed"
        out["detail"] = f"{doc_url} disallowed by robots.txt"
        return out

    known = index.seen_url(doc_url)
    if known is not None and Path(known.path).is_file():
        out["status"] = "already_downloaded"
        out["sha256"] = known.sha256
        out["pdf_path"] = known.path
        out["detail"] = f"URL already fetched -> {Path(known.path).name}"
        return out

    filename = brochure_pdf_filename(year, make, model)
    if filename is None:
        out["status"] = "unmappable_filename"
        out["detail"] = "no filename round-trips to the expected catalog_key"
        return out

    try:
        status, payload, content_type = fetcher.get_bytes(doc_url)
    except Exception as exc:  # noqa: BLE001
        out["status"] = "download_failed"
        out["detail"] = f"{type(exc).__name__}: {str(exc)[:160]}"
        return out

    if status != 200:
        out["status"] = "download_failed"
        out["detail"] = f"HTTP {status}"
        return out
    if not looks_like_pdf(payload):
        # An HTML error/consent page served with a 200 is the usual cause.
        out["status"] = "not_a_pdf"
        out["detail"] = f"content-type={content_type} first-bytes={payload[:16]!r}"
        return out

    # ------------------------------------------------------------------
    # Archive tier: authenticity BEFORE anything is derived from the bytes.
    # ------------------------------------------------------------------
    authenticity: dict[str, Any] = {}
    archive_failure = ""
    if tier == TIER_ARCHIVE:
        record = assess_archive_document(
            payload,
            oem_pdf_path=oem_pdf_for_catalog_key(ck, index),
            catalog_key_str=ck,
            archive_url=doc_url,
        )
        authenticity = record.to_json()
        out["authenticity"] = authenticity
        if not record.ok:
            archive_failure = record.reason

    fetched_at = datetime.now(timezone.utc).isoformat()
    doc, is_new = index.register(
        payload,
        preferred_path=BROCHURES_DIR / filename,
        source_url=doc_url,
        catalog_key_str=ck,
        fetched_at=fetched_at,
        tier=tier,
        authenticity=authenticity,
    )
    pdf_path = Path(doc.path)

    if archive_failure and is_new:
        # Stored first, then moved: the document is evidence about this source
        # and is never deleted. Its text is not extracted, so nothing from it can
        # reach a car page.
        #
        # `and is_new` is not belt-and-braces. Without it, an archive URL that
        # returns bytes we already hold would move the EXISTING file -- which may
        # be the OEM copy, since register() dedupes across tiers -- out of the
        # corpus on the strength of a check about a different source. A repeat
        # hash is reported as a duplicate below and nothing is moved.
        destination = quarantine_archive_pdf(
            pdf_path, archive_failure, applied=apply_quarantine
        )
        doc.path = str(destination)
        index.save()
        out.update(
            status="archive_quarantined",
            sha256=doc.sha256,
            pdf_path=str(destination),
            bytes=len(payload),
            fetched_at=fetched_at,
            detail=archive_failure,
        )
        _append_fetch_log(out)
        return out

    if not is_new:
        # Same bytes as a document we already hold, reached by a different URL
        # or a different spelling of the model. Do not store or count it twice.
        index.save()
        out.update(
            status="duplicate_content",
            sha256=doc.sha256,
            pdf_path=str(pdf_path),
            bytes=len(payload),
            detail=(
                f"sha256 {doc.sha256[:12]} already stored as {pdf_path.name} "
                f"(keys: {', '.join(sorted(set(doc.catalog_keys)))})"
            ),
        )
        return out

    def reject_text(detail: str, page_count: int | None = None) -> dict[str, Any]:
        """
        The single exit for "PDF kept, derived text refused".

        Four call sites reach it -- unparseable filename, an extraction that
        produced no pages, a document that will not name its own model, and a
        document that fails the decode/length gate -- and they all used to be
        four copies of the same block. They are one block now because the
        archive tier had to change what happens here, and changing it in three
        of four places would have been a fix that was not a fix.

        For an OEM document the behaviour is exactly what it was: the PDF stays
        in the corpus directory, the text is not persisted. For an ARCHIVE
        document the PDF is moved to ARCHIVE_QUARANTINE_DIR with the reason.

        Why the difference is not arbitrary: for an OEM document a failed
        identity check means "we mis-picked a link on a manufacturer's site",
        and the file itself is still a manufacturer document worth keeping where
        the re-ingestion lane can see it. For an archive document, "this file
        does not print the name of the vehicle it is filed under" is the
        strongest tamper-or-misfiling signal this tier can produce, and it is the
        one authenticity test with real teeth. It is the failure that
        assess_archive_document deliberately does NOT issue on its own -- see its
        docstring for why the OEM-vs-archive comparison cannot.
        """
        final_path = pdf_path
        status = "stored_text_rejected"
        if tier == TIER_ARCHIVE:
            final_path = quarantine_archive_pdf(
                pdf_path, detail, applied=apply_quarantine
            )
            doc.path = str(final_path)
            status = "archive_quarantined"
        index.save()
        out.update(
            status=status,
            sha256=doc.sha256,
            pdf_path=str(final_path),
            bytes=len(payload),
            fetched_at=fetched_at,
            detail=detail,
        )
        if page_count is not None:
            out["page_count"] = page_count
        _append_fetch_log(out)
        return out

    result = extract_brochure_text_pdf(pdf_path)
    if result is None:
        return reject_text("filename not parseable by brochure_extract")
    if result.warnings and not result.pages:
        return reject_text(";".join(result.warnings)[:200])

    # Gate BEFORE persisting text, over the WHOLE captured document rather than
    # the trim-hint pages: capture is lossless now, so "did this PDF decode" is
    # a document-level question. The PDF is kept either way -- deleting acquired
    # source PDFs after a lossy capture is the mistake that cost this project
    # 1,368 brochures, and the content index means a re-run never re-downloads
    # it. What a failed gate withholds is the derived text, not the document.
    # (An archive document is moved aside rather than left in the corpus
    # directory; see reject_text. It is moved, never deleted.)
    document_text = full_document_text(result)

    # The document must say which vehicle it is about. This is the only check
    # that survives an OEM serving documents behind an opaque id, it is what
    # makes the page-scoped fallback in resolve_source safe, and it is the
    # load-bearing authenticity test for the archive tier.
    identified, why_not = verify_document_identity(
        document_text,
        year,
        make,
        model,
        require_year=resolved.get("page_scoped", False),
    )
    if not identified:
        return reject_text(why_not, result.page_count)

    # Naming the vehicle is not being about it. A brochure for a derived
    # nameplate (the M3 book for the 3 Series) names the base model in its body
    # copy and passes the check above; this one asks which nameplate presides
    # over the document's pages. Page texts, not the flattened string: the
    # signal is a running header, which is per page by construction.
    subject = assess_nameplate_dominance(page_texts(result), make, model)
    if not subject.ok:
        return reject_text(subject.reason, result.page_count)

    verdict = assess_text_quality(document_text)
    if not verdict.ok:
        return reject_text(verdict.reason, result.page_count)

    json_path = persist_brochure_text(result)
    index.save()

    out.update(
        status="fetched",
        pdf_path=str(pdf_path),
        json_path=str(json_path),
        bytes=len(payload),
        sha256=doc.sha256,
        page_count=result.page_count,
        pages_saved=len(result.pages),
        trim_hint_pages=result.trim_hint_pages,
        text_chars=len(document_text),
        warnings=result.warnings,
        fetched_at=fetched_at,
    )
    _append_fetch_log(out)
    return out


def full_document_text(result: Any) -> str:
    """
    Every captured page's text and table lines, in page order.

    ``combined_trim_pages_text`` is only the trim-hint pages. Since capture went
    lossless that is a subset, so gating on it would judge a document by the
    pages one regex happened to flag.
    """
    return "\n".join(page_texts(result))


def page_texts(result: Any) -> list[str]:
    """
    One string per captured page: its text plus its table lines.

    Kept separate from :func:`full_document_text` because a check that asks
    "which vehicle presides over this document" has to see the page boundaries.
    Flattening first turns nine identical running headers into nine occurrences
    in one blob, which is exactly the count that cannot tell a combined A4/S4
    brochure apart from an M3 brochure filed as a 3 Series.
    """
    out: list[str] = []
    for page in result.pages:
        parts = [page.text or ""]
        if page.table_lines:
            parts.append("\n".join(page.table_lines))
        out.append("\n".join(parts))
    return out


def _append_fetch_log(record: dict[str, Any]) -> None:
    """
    One line per stored document. ``text_status`` says whether the derived text
    was persisted, so a rejected extraction is visible rather than looking like
    a document we never fetched.

    ``tier`` and, for the archive tier, ``authenticity`` are written here as well
    as into the content index. The ledger is append-only and the index is
    rewritten, so the ledger is the record that survives an index rebuild.
    """
    FETCH_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    entry = {
        "catalog_key": record["primary_catalog_key"].replace("__", "|"),
        "alias_catalog_keys": record.get("alias_catalog_keys", []),
        "year": record["year"],
        "make": record["make"],
        "model": record["model"],
        "tier": record.get("tier") or "",
        "authenticity": record.get("authenticity") or None,
        "source_url": record["doc_url"],
        "discovery_url": record.get("discovery_url"),
        "fetched_at": record["fetched_at"],
        "sha256": record["sha256"],
        "bytes": record["bytes"],
        "pdf_path": record["pdf_path"],
        "text_status": "persisted" if record.get("json_path") else "rejected",
        "text_rejected_reason": None if record.get("json_path") else record.get("detail"),
        "brochure_text_path": record.get("json_path"),
        "page_count": record.get("page_count"),
        "pages_saved": record.get("pages_saved"),
        "user_agent": record.get("user_agent"),
        "fetcher": "backend.scripts.fetch_oem_brochures",
    }
    with FETCH_LOG_PATH.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry) + "\n")


def open_content_index() -> tuple[ContentIndex, dict[str, list[str]]]:
    """Index seeded from the ledger and from whatever is already on disk."""
    index = ContentIndex(CONTENT_INDEX_PATH).load()
    index.absorb_ledger(FETCH_LOG_PATH)
    duplicates = index.scan_directory(BROCHURES_DIR)
    return index, duplicates


# --------------------------------------------------------------------------
# Corpus audit
# --------------------------------------------------------------------------


def audit_corpus_text() -> dict[str, Any]:
    """
    How much of ``derived/brochure_text`` is a usable document.

    A file existing is what the gap list counts as coverage, but a file whose
    pages hold only a disclosures page is not a brochure. Reports, per corpus
    file: no text at all / does not name its own model / fails the decode and
    length gate. Read-only.
    """
    summary: dict[str, Any] = {
        "files": 0,
        "usable": 0,
        "no_text": 0,
        "model_not_named": 0,
        "wrong_vehicle": 0,
        "failed_quality": 0,
        "failed_digit_density": 0,
        "worst_examples": [],
    }
    for path in sorted(BROCHURE_TEXT_DIR.glob("*.json")):
        if path.name.startswith("_"):
            continue
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        summary["files"] += 1
        pages = corpus_page_texts(data)
        text = "\n".join(pages)
        if not text.strip():
            summary["no_text"] += 1
            continue
        named, why = verify_document_identity(
            text, data.get("year"), data.get("make") or "", data.get("model") or ""
        )
        subject = assess_nameplate_dominance(
            pages, data.get("make") or "", data.get("model") or ""
        )
        quality = assess_text_quality(text)
        if not named:
            summary["model_not_named"] += 1
        if not subject.ok:
            summary["wrong_vehicle"] += 1
        if not quality.ok:
            summary["failed_quality"] += 1
            if "digit" in quality.reason:
                summary["failed_digit_density"] += 1
        if named and subject.ok and quality.ok:
            summary["usable"] += 1
        elif len(summary["worst_examples"]) < 10:
            summary["worst_examples"].append(
                {
                    "file": path.stem,
                    "chars": len(text),
                    "reason": why or subject.reason or quality.reason,
                }
            )
    return summary


def corpus_page_texts(data: dict[str, Any]) -> list[str]:
    """Per-page text of a stored ``derived/brochure_text`` document."""
    return [(page.get("text") or "") for page in data.get("pages") or []]


def quarantine_unidentified(apply: bool) -> int:
    """
    Re-validate every ``brochure_text`` file and move the failures aside.

    Three checks, in the order a fetch applies them:
    :func:`verify_document_identity` (does it print this model's name as a
    declaration), :func:`assess_nameplate_dominance` (is the document about this
    nameplate or about one derived from it) and :func:`assess_text_quality`
    (does the text decode, is there enough of it, does it carry the numbers).
    The last two were added 2026-08-02 after a verifier found the M3 brochure
    filed as a 3 Series and a digit-stripped X1 text layer, both of which the
    identity check alone had passed.

    Moved, never deleted, and with a manifest recording the reason per file, so
    a rule that turns out to be wrong is reversible by moving the files back.
    Anything a car page would have quoted from a quarantined file is simply not
    rendered: silence is the acceptable outcome, a wrong spec is not.

    Always ends in :func:`quarantine_derived` -- including when nothing new
    fails, because moving the document out of ``brochure_text`` does not by
    itself stop the stores that were built from it. That was not true until
    2026-07-31 and the gap rendered brochure bullets for a document already in
    quarantine.

    Dry run is the default.
    """
    failures: list[dict[str, Any]] = []
    checked = 0
    for path in sorted(BROCHURE_TEXT_DIR.glob("*.json")):
        if path.name.startswith("_"):
            continue
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            failures.append({"file": path.name, "reason": f"unreadable: {exc}"})
            continue
        checked += 1
        pages = corpus_page_texts(data)
        text = "\n".join(pages)
        make = data.get("make") or ""
        model = data.get("model") or ""
        ok, why = verify_document_identity(
            text, data.get("year") or 0, make, model
        )
        subject = assess_nameplate_dominance(pages, make, model)
        quality = assess_text_quality(text)
        if ok and not subject.ok:
            ok, why = False, subject.reason
        if ok and not quality.ok:
            ok, why = False, quality.reason
        if not ok:
            failures.append(
                {
                    "file": path.name,
                    "catalog_key": data.get("catalog_key"),
                    "make": make,
                    "model": model,
                    "chars": len(text),
                    "digits": quality.digits,
                    "digit_density": round(quality.digit_density, 3),
                    "schema_version": data.get("schema_version") or 1,
                    "reason": why,
                }
            )

    print(
        f"corpus re-validation (identity + subject + quality): {checked} corpus "
        f"file(s) checked, {len(failures)} failed"
    )
    by_kind: dict[str, int] = {}
    for row in failures:
        reason = str(row["reason"])
        kind = (
            "no extracted text"
            if reason.startswith("no extracted text")
            else "model name absent"
            if "never mentions the model" in reason
            else "named only in running prose"
            if "only inside running prose" in reason
            else "model name too short to verify"
            if "no model name long enough" in reason
            else "WRONG VEHICLE (derived nameplate)"
            if "the document's subject is" in reason
            else "digits missing from text layer"
            if "digit(s) against" in reason
            else "decode failure (U+FFFD)"
            if "U+FFFD" in reason
            else "too little text"
            if "chars of extracted text" in reason
            else "other"
        )
        by_kind[kind] = by_kind.get(kind, 0) + 1
    for kind, count in sorted(by_kind.items(), key=lambda kv: -kv[1]):
        print(f"  {kind:<32} {count}")

    for row in failures:
        if "only inside running prose" in str(row["reason"]):
            print(f"  PROSE-ONLY  {row['file']}  {row['reason']}")
        if "the document's subject is" in str(row["reason"]):
            print(f"  WRONG-VEHICLE  {row['file']}  {row['reason']}")

    if not failures:
        # No NEW failures, but earlier batches are already in quarantine and the
        # stores built from them may not have been swept yet, so still sweep.
        return quarantine_derived(apply)

    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "check": (
            "backend.enrichment.brochure_sources: verify_document_identity + "
            "assess_nameplate_dominance + assess_text_quality"
        ),
        "applied": bool(apply),
        "count": len(failures),
        "files": failures,
    }
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    manifest_path = ARTIFACT_DIR / "identity_quarantine.json"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(f"\nmanifest -> {manifest_path}")

    if not apply:
        print(
            f"DRY RUN — nothing moved. Re-run with --apply to move {len(failures)} "
            f"file(s) to {BROCHURE_TEXT_QUARANTINE_DIR}."
        )
        return quarantine_derived(apply)

    BROCHURE_TEXT_QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
    moved = 0
    for row in failures:
        src = BROCHURE_TEXT_DIR / str(row["file"])
        if not src.is_file():
            continue
        src.replace(BROCHURE_TEXT_QUARANTINE_DIR / src.name)
        moved += 1

    # The record in the quarantine directory is CUMULATIVE. This runs more than
    # once -- a later re-ingestion writes new corpus files that have never been
    # identity-checked -- and overwriting it would leave the earlier batch's
    # files sitting in quarantine with no recorded reason, which is the "moved,
    # never silently deleted" promise broken by a different route.
    record_path = BROCHURE_TEXT_QUARANTINE_DIR / "_why_these_are_here.json"
    by_file: dict[str, dict[str, Any]] = {}
    if record_path.is_file():
        try:
            previous = json.loads(record_path.read_text(encoding="utf-8"))
            for row in previous.get("files") or []:
                by_file[str(row.get("file"))] = row
        except (OSError, json.JSONDecodeError):
            logger.warning("could not read %s; starting a fresh record", record_path)
    for row in failures:
        by_file[str(row["file"])] = {**row, "quarantined_at": manifest["generated_at"]}
    record_path.write_text(
        json.dumps(
            {
                "check": manifest["check"],
                "last_run_at": manifest["generated_at"],
                "count": len(by_file),
                "files": [by_file[k] for k in sorted(by_file)],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    print(
        f"moved {moved} file(s) to {BROCHURE_TEXT_QUARANTINE_DIR}; "
        f"{len(by_file)} file(s) recorded in {record_path.name}"
    )
    return quarantine_derived(apply)


def quarantine_derived(apply: bool) -> int:
    """
    Follow every quarantined document into the stores that were built from it.

    Quarantining ``derived/brochure_text/<key>.json`` alone does not stop the
    document: ``brochure_text_slim`` is a verbatim excerpt that
    ``trim_ladder._brochure_engine_step_ups`` reads at render time, and the
    ``trim_adds_by_year`` overlay cites the document by path in every
    ``adds_provenance`` entry. This sweeps both, plus ``trim_candidates``.

    Re-derivation cannot undo it: ``build_brochure_text_slim`` and
    ``brochure_trim_candidates`` both read ``BROCHURE_TEXT_DIR``, which no longer
    holds the file, and ``reingest_brochures`` consults
    ``brochure_sources.is_quarantined`` before re-extracting the PDF.

    Moved, never deleted, with a per-file reason. Dry run is the default.
    """
    to_move, left_in_place = plan_derived_quarantine()
    by_store: dict[str, int] = {}
    for row in to_move:
        by_store[str(row["store"])] = by_store.get(str(row["store"]), 0) + 1

    print(
        f"\nderived artifacts of quarantined documents: {len(to_move)} to quarantine, "
        f"{len(left_in_place)} deliberately left in place"
    )
    for store in DERIVED_STORES_FROM_BROCHURE_TEXT:
        print(f"  {store.name:<22} {by_store.get(store.name, 0):>5}  ({store.rule})")
    for row in to_move:
        if str(row["store"]) == "trim_adds_by_year":
            print(f"  OVERLAY  {row['file']}  {row['reason']}")

    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "check": "backend.enrichment.brochure_sources.derived_quarantine_reason",
        "applied": bool(apply),
        "quarantined": to_move,
        "left_in_place": left_in_place,
    }
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    manifest_path = ARTIFACT_DIR / "derived_quarantine.json"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(f"manifest -> {manifest_path}")

    if not apply:
        print(f"DRY RUN — nothing moved. Re-run with --apply to move {len(to_move)} file(s).")
        return 0

    moved = apply_derived_quarantine(to_move)
    for store in DERIVED_STORES_FROM_BROCHURE_TEXT:
        rows = [r for r in to_move if str(r["store"]) == store.name]
        if not rows:
            continue
        # Cumulative, for the same reason the brochure_text record is: this runs
        # again after the next identity pass, and overwriting would leave the
        # earlier batch in quarantine with no recorded reason.
        record_path = store.quarantine_dir / "_why_these_are_here.json"
        by_file: dict[str, dict[str, Any]] = {}
        if record_path.is_file():
            try:
                previous = json.loads(record_path.read_text(encoding="utf-8"))
                for row in previous.get("files") or []:
                    by_file[str(row.get("file"))] = row
            except (OSError, json.JSONDecodeError):
                logger.warning("could not read %s; starting a fresh record", record_path)
        for row in rows:
            by_file[str(row["file"])] = {**row, "quarantined_at": manifest["generated_at"]}
        store.quarantine_dir.mkdir(parents=True, exist_ok=True)
        record_path.write_text(
            json.dumps(
                {
                    "store": store.name,
                    "why": store.why,
                    "check": manifest["check"],
                    "last_run_at": manifest["generated_at"],
                    "count": len(by_file),
                    "files": [by_file[k] for k in sorted(by_file)],
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
    print(f"moved {moved} derived artifact(s) into their sibling quarantine dirs")
    return 0


# --------------------------------------------------------------------------
# Archive tier: measurement
# --------------------------------------------------------------------------


def archive_verify_overlap(
    limit: int, delay: float, user_agent: str, brands: set[str] | None
) -> int:
    """
    Fetch the archive's copy of documents we ALREADY hold from the OEM, and compare.

    This is the single best evidence available about whether this source can be
    trusted at scale, and it is the reason the mode exists separately from
    acquisition: it deliberately spends requests on vehicles we do not need,
    because those are the only vehicles where the answer is checkable.

    The comparison copies land in :data:`ARCHIVE_COMPARISON_DIR`, NOT in the
    corpus. Nothing extracts text from there, so a run of this cannot put a
    single character on a car page. It is measurement, and it is kept as
    measurement.

    Byte-identical to the OEM original is the strong result. Different bytes are
    expected on their own -- re-saving a PDF changes them -- so the fallback is
    a 5-word-shingle overlap of the extracted text, reported as a number.
    """
    index, _ = open_content_index()
    candidates: list[tuple[str, Path]] = []
    seen_keys: set[str] = set()
    for doc in index.by_sha.values():
        if doc.tier == TIER_ARCHIVE or not Path(doc.path).is_file():
            continue
        for key in sorted(set(doc.catalog_keys)):
            parts = key.split("|")
            if len(parts) != 3 or key in seen_keys:
                continue
            if brands and parts[1].lower() not in brands:
                continue
            seen_keys.add(key)
            candidates.append((key, Path(doc.path)))
    candidates.sort()
    candidates = candidates[:limit]

    print(
        f"OEM-vs-archive comparison over {len(candidates)} vehicle(s) we already hold "
        f"an OEM original for\n(paced {max(delay, ARCHIVE_DELAY_SECONDS):g}s, sequential; "
        f"copies -> {ARCHIVE_COMPARISON_DIR})\n"
    )

    archive = ArchiveResolver(delay=delay, user_agent=user_agent)
    index_pages = archive.make_index_pages()
    print(f"archive make index pages read from its own navigation: {len(index_pages)}\n")

    rows: list[dict[str, Any]] = []
    for key, oem_path in candidates:
        year_str, make, model_token = key.split("|")
        year = int(year_str)
        row: dict[str, Any] = {
            "catalog_key": key,
            "oem_pdf": oem_path.name,
            "outcome": "no_archive_copy",
            "detail": "",
        }
        resolved = {
            "year": year,
            "make": make,
            "model": model_token,
            "status": "seed",
            "detail": "",
        }
        resolved = _resolve_from_archive(resolved, archive)
        if resolved.get("status") != "found":
            row["detail"] = resolved.get("archive_detail", "")
            rows.append(row)
            print(f"  {key:<34} no archive copy  {row['detail'][:70]}")
            continue

        url = resolved["doc_url"]
        try:
            status, payload, _ = archive.fetcher.get_bytes(url)
        except Exception as exc:  # noqa: BLE001
            row.update(outcome="download_failed", detail=f"{type(exc).__name__}: {exc}")
            rows.append(row)
            continue
        if status != 200 or not looks_like_pdf(payload):
            row.update(outcome="download_failed", detail=f"HTTP {status}")
            rows.append(row)
            continue

        comparison = compare_archive_to_oem(
            payload, oem_path, catalog_key_str=key, archive_url=url
        )
        metadata = read_pdf_metadata(payload)
        _, pages_with_text, page_count = pdf_text_layer(payload)
        ARCHIVE_COMPARISON_DIR.mkdir(parents=True, exist_ok=True)
        copy_path = ARCHIVE_COMPARISON_DIR / f"{key.replace('|', '__')}__archive.pdf"
        copy_path.write_bytes(payload)

        row.update(
            outcome=comparison.outcome,
            similarity=round(comparison.similarity, 4),
            archive_url=url,
            archive_sha256=comparison.archive_sha256,
            oem_sha256=comparison.oem_sha256,
            bytes=len(payload),
            pages_with_text=pages_with_text,
            page_count=page_count,
            producer=metadata.get("producer"),
            creator=metadata.get("creator"),
            title=metadata.get("title"),
            creation_date=metadata.get("creation_date"),
            comparison_copy=str(copy_path),
            detail=comparison.detail,
        )
        rows.append(row)
        print(
            f"  {key:<34} {comparison.outcome:<20} sim={comparison.similarity:.3f}  "
            f"text {pages_with_text}/{page_count}p  producer={str(metadata.get('producer'))[:34]!r}"
        )

    by_outcome: dict[str, int] = {}
    for row in rows:
        by_outcome[str(row["outcome"])] = by_outcome.get(str(row["outcome"]), 0) + 1
    print("\nOutcome counts:")
    for outcome, count in sorted(by_outcome.items(), key=lambda kv: -kv[1]):
        print(f"  {outcome:<24} {count}")
    no_text = sum(
        1 for r in rows if r.get("page_count") and not r.get("pages_with_text")
    )
    fetched = sum(1 for r in rows if r.get("page_count"))
    print(f"\narchive documents with NO text layer: {no_text} of {fetched} fetched")

    timings = archive.request_timings
    if len(timings) > 1:
        print(f"archive requests made: {archive.fetcher.request_count}")

    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    path = ARTIFACT_DIR / "archive_oem_comparison.json"
    path.write_text(
        json.dumps(
            {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "policy": ARCHIVE_SOURCE_NOTE,
                "delay_seconds": archive.fetcher.delay,
                "requests": archive.fetcher.request_count,
                "counts": by_outcome,
                "documents_without_text_layer": no_text,
                "rows": rows,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    print(f"\nreport -> {path}")
    return 0


def archive_audit() -> int:
    """
    What the archive tier has actually put on disk, read back off the disk.

    Deliberately does NOT ask the content index what it recorded and print that
    back: it re-opens every archive PDF, re-hashes it, re-reads its metadata and
    re-counts its text layer. A register confirming its own entries is not
    evidence.
    """
    index, _ = open_content_index()
    archive_docs = [d for d in index.by_sha.values() if d.tier == TIER_ARCHIVE]
    print(f"archive-tier documents in the content index: {len(archive_docs)}")

    on_disk = 0
    quarantined = 0
    no_text = 0
    producers: dict[str, int] = {}
    tier_from_citation: dict[str, int] = {}
    for doc in sorted(archive_docs, key=lambda d: d.path):
        path = Path(doc.path)
        if not path.is_file():
            print(f"  MISSING  {path}")
            continue
        on_disk += 1
        is_quarantined = ARCHIVE_QUARANTINE_DIR in path.parents
        if is_quarantined:
            quarantined += 1
        payload = path.read_bytes()
        digest = sha256_bytes(payload)
        metadata = read_pdf_metadata(payload)
        _, pages_with_text, page_count = pdf_text_layer(payload)
        if not pages_with_text:
            no_text += 1
        producer = str(metadata.get("producer") or "(none)")
        producers[producer] = producers.get(producer, 0) + 1
        for key in sorted(set(doc.catalog_keys)):
            citation = f"derived/brochure_text/{key.replace('|', '__')}.json"
            tier = document_tier_for_citation(citation)
            tier_from_citation[tier or "(unresolved)"] = (
                tier_from_citation.get(tier or "(unresolved)", 0) + 1
            )
        print(
            f"  {path.name:<52} {'QUARANTINED ' if is_quarantined else ''}"
            f"sha256 {digest[:12]}{' MISMATCH' if digest != doc.sha256 else ''}  "
            f"text {pages_with_text}/{page_count}p  producer={producer[:40]!r}"
        )

    print(
        f"\non disk {on_disk}  quarantined {quarantined}  "
        f"no text layer {no_text}"
    )
    if producers:
        print("\nPDF Producer values seen (evidence base, not a gate):")
        for producer, count in sorted(producers.items(), key=lambda kv: -kv[1]):
            print(f"  {count:>4}  {producer[:70]!r}")
    if tier_from_citation:
        print("\ntier resolved back from a brochure_text citation:")
        for tier, count in sorted(tier_from_citation.items(), key=lambda kv: -kv[1]):
            print(f"  {tier:<16} {count}")
    return 0


def audit_corpus() -> int:
    index, duplicates = open_content_index()
    print(f"stored PDFs indexed: {len(index.by_sha)} distinct sha256")
    if not duplicates:
        print("no duplicate content on disk")
    else:
        wasted = 0
        print(f"\n{len(duplicates)} document(s) stored more than once:")
        for sha, paths in sorted(duplicates.items()):
            size = Path(paths[0]).stat().st_size
            wasted += size * (len(paths) - 1)
            print(f"  sha256 {sha[:16]}  {size} bytes  x{len(paths)}")
            for path in paths:
                print(f"    {Path(path).name}")
        print(f"\n{wasted} redundant bytes on disk.")
        print(
            "Not deleted here: the derived text under each spelling is still what "
            "the exact-match lookup in dictionary_derived reads. Removing a copy is "
            "only safe once that lookup folds model spellings."
        )

    text = audit_corpus_text()
    print(
        f"\nbrochure_text corpus health ({text['files']} files):\n"
        f"  usable (right vehicle, decodes, has its numbers)  {text['usable']}\n"
        f"  no extracted text at all                         {text['no_text']}\n"
        f"  never names its own model                        {text['model_not_named']}\n"
        f"  is about a DERIVED nameplate, not this one       {text['wrong_vehicle']}\n"
        f"  fails the decode/length/digit gate               {text['failed_quality']}\n"
        f"    of which: numbers missing from the text layer  "
        f"{text['failed_digit_density']}"
    )
    print(
        "\nThe gap list counts a file's existence as coverage, so anything above "
        "that is not 'usable' is coverage this backlog does not really have."
    )
    for example in text["worst_examples"][:6]:
        print(f"  {example['file']:<36} {example['chars']:>7} chars  {example['reason']}")
    return 0


# --------------------------------------------------------------------------
# Reachability report
# --------------------------------------------------------------------------

_UA_CHOICES = {"browser": BROWSER_USER_AGENT, "identified": IDENTIFIED_USER_AGENT}


def probe_reachability(
    ua_labels: list[str], delay: float, brand: str | None
) -> list[ReachabilityResult]:
    targets = [t for t in PROBE_TARGETS if not brand or t[0] == brand.lower()]
    results: list[ReachabilityResult] = []
    for label in ua_labels:
        agent = _UA_CHOICES[label]
        fetcher = PacedFetcher(delay=delay, user_agent=agent)
        robots = RobotsPolicy(fetcher.get_text, agent=fetcher.robots_agent)
        for make, host, page_url in targets:
            result = probe_target(make, host, page_url, fetcher, robots, label)
            results.append(result)
            print(
                f"{label:<11} {make:<14} {result.host:<26} {result.verdict:<26} "
                f"blocked_by={result.blocked_by or '-':<19} "
                f"robots={result.robots_status:<8} http={result.page_status or '-':<5} "
                f"{result.detail[:90]}"
            )
    return results


def write_reachability_artifact(
    results: list[ReachabilityResult], path: Path
) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "probes": [r.to_json() for r in results],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return path


# --------------------------------------------------------------------------
# HTML specification pages
# --------------------------------------------------------------------------
#
# Same lane, different locator. A make that publishes its per-trim equipment
# grid as an HTML table is not a make that "publishes nothing we can use" -- it
# is a make whose documents have no page numbers. Everything else is unchanged:
# the host allowlist, robots.txt, the pacing, the identity gate, the text
# quality gate, "store the bytes and verify against the stored bytes".
#
# See ``backend/enrichment/html_spec_sources`` for the citation contract and for
# what the extractor refuses to read.

#: One line per stored HTML spec page, alongside the PDF ledger.
HTML_SPEC_LOG_PATH = DERIVED_DIR / "html_spec_fetch_log.jsonl"

#: Where ``--html-specs`` writes the ladder overlays it builds.
#:
#: Deliberately NOT ``derived/trim_adds_by_year``. Two independent reasons, and
#: either alone would be enough:
#:
#:   * ``source="html_spec_quoted"`` is not in
#:     ``brochure_extract.ADMISSIBLE_OVERLAY_SOURCES``, so an overlay dropped
#:     into the live directory would be inert anyway -- and would sit there
#:     looking like coverage to anything that counts files;
#:   * a file in the live directory is loaded by the render path. A new
#:     producer writing straight into it is how an unreviewed store reaches a
#:     car page. Promoting these is a reviewed step: see the note in
#:     ``html_spec_sources`` and the hand-off in this lane's report.
HTML_SPEC_OVERLAY_DIR = DERIVED_DIR / "html_spec_overlays"

#: The live overlay directory, read ONLY to refuse to shadow a file already in
#: it. Nothing here writes to it.
LIVE_OVERLAY_DIR = TRIM_ADDS_BY_YEAR_DIR


def _html_spec_targets(
    gaps: list[dict[str, Any]], makes: set[str]
) -> list[dict[str, Any]]:
    """Gap entries whose make has a measured HTML discovery chain."""
    return [g for g in gaps if canonical_make(g["make"]).lower() in makes]


def _honda_release_for(
    fetcher: PacedFetcher,
    robots: RobotsPolicy,
    year: int,
    make: str,
    model: str,
    channel_cache: dict[str, str],
) -> tuple[str | None, str, list[str]]:
    """
    Walk Honda's three anchor hops to the spec release for this vehicle.

    Returns ``(release_url, refusal_reason, urls_visited)``. Every hop reads an
    href off the page before it; no slug is constructed. Robots is consulted for
    every URL, including the index hops.
    """
    visited: list[str] = []

    def get(url: str) -> tuple[int, str] | None:
        if not robots.allows(url):
            return None
        visited.append(url)
        try:
            return fetcher.get_text(url)
        except Exception as exc:  # noqa: BLE001 - transport failures are data
            logger.info("      fetch failed %s: %s", url, str(exc)[:120])
            return None

    home = channel_cache.get("__home__")
    if home is None:
        result = get(HONDA_NEWSROOM_HOME)
        if result is None or result[0] != 200:
            return None, "hondanews.com home page not served", visited
        home = result[1]
        channel_cache["__home__"] = home

    channels = honda_model_channels(home, HONDA_NEWSROOM_HOME)
    channel_url = honda_channel_for_model(channels, make, model)
    if not channel_url:
        return (
            None,
            f"no automobile channel anchored for {make} {model} "
            f"({len(channels)} channels on the home page)",
            visited,
        )

    specs_url = channel_cache.get(channel_url)
    if specs_url is None:
        result = get(channel_url)
        if result is None or result[0] != 200:
            return None, f"channel page not served: {channel_url}", visited
        found = honda_specs_tab_url(result[1], channel_url)
        if not found:
            return None, f"channel page anchors no Specs tab: {channel_url}", visited
        specs_url = found
        channel_cache[channel_url] = specs_url

    listing = channel_cache.get(f"listing::{specs_url}")
    if listing is None:
        result = get(specs_url)
        if result is None or result[0] != 200:
            return None, f"specs tab not served: {specs_url}", visited
        listing = result[1]
        channel_cache[f"listing::{specs_url}"] = listing

    releases = honda_spec_releases(listing, specs_url)
    release, reason = pick_spec_release(releases, year, make, model)
    if release is None:
        return None, f"{reason} ({len(releases)} releases listed)", visited
    return release.url, "", visited


def _held_html_spec(year: int, make: str, model: str) -> str | None:
    """The URL an already-stored page for this vehicle was fetched from."""
    path = html_spec_transcript_path(year, make, model)
    if not path.is_file():
        return None
    try:
        blob = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return str(blob.get("source_url") or "") or None


def _append_html_spec_log(record: dict[str, Any]) -> None:
    HTML_SPEC_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with HTML_SPEC_LOG_PATH.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record) + "\n")


def fetch_html_specs(
    gaps: list[dict[str, Any]],
    *,
    limit: int,
    delay: float,
    user_agent: str,
    download: bool,
) -> dict[str, Any]:
    """
    Discover, fetch, store, parse and cite official HTML spec pages.

    Counters returned are what the run measured, not what it attempted:
    ``fetched`` is pages whose bytes are on disk, ``parsed`` is pages that
    yielded at least one readable trim grid, ``refused`` is everything the gates
    turned away with a reason, ``verified`` is citations re-derived from the
    STORED bytes by :func:`html_spec_sources.verify_citation`.
    """
    fetcher = PacedFetcher(delay=delay, user_agent=user_agent)
    robots = RobotsPolicy(fetcher.get_text, agent=fetcher.robots_agent)
    channel_cache: dict[str, str] = {}

    stats: dict[str, Any] = {
        "considered": 0,
        "discovered": 0,
        "already_held": 0,
        "fetched": 0,  # HTTP 200 with a body
        "parsed": 0,  # at least one readable trim grid
        "stored": 0,  # bytes written to disk
        "refused": 0,
        "overlays": 0,
        "citations": 0,
        "verified": 0,
        "verify_failures": {},
        "refusals": [],
    }

    targets = _html_spec_targets(gaps, set(HTML_SPEC_MAKES))
    print(
        f"HTML spec pages: {len(targets)} gap groups on makes with a measured "
        f"discovery chain ({', '.join(sorted(HTML_SPEC_MAKES))}); "
        f"taking {min(limit, len(targets))}"
    )

    def refuse(gap: dict[str, Any], stage: str, reason: str) -> None:
        stats["refused"] += 1
        stats["refusals"].append(
            {"group_key": gap["group_key"], "stage": stage, "reason": reason}
        )
        print(f"    refused ({stage}): {reason}")

    for gap in targets[:limit]:
        stats["considered"] += 1
        year, make, model = gap["year"], gap["make"], gap["primary"]["model"]
        print(f"\n  {year} {make} {model}  ({gap['cars']} active cars)")

        release_url, reason, _visited = _honda_release_for(
            fetcher, robots, year, make, model, channel_cache
        )
        if not release_url:
            refuse(gap, "discovery", reason)
            continue
        if not official_spec_page(release_url, make):
            refuse(gap, "allowlist", f"{release_url} is not on an official host")
            continue
        if not robots.allows(release_url):
            verdict = robots.verdict_for(release_url)
            refuse(gap, "robots", f"{release_url}: {verdict}")
            continue
        stats["discovered"] += 1
        print(f"    release: {release_url}")

        # Resume: a page already stored FROM THIS URL is not re-requested. The
        # stored bytes are the citable substrate, so re-fetching would only risk
        # replacing them with a newer page that our citations do not address.
        held = _held_html_spec(year, make, model)
        if held == release_url:
            stats["already_held"] += 1
            print("    already held from this URL - not re-fetched")
            continue

        if not download:
            print("    dry run - not fetched")
            continue

        try:
            status, body = fetcher.get_text(release_url)
        except Exception as exc:  # noqa: BLE001
            refuse(gap, "fetch", f"{release_url}: {str(exc)[:160]}")
            continue
        if status != 200 or not body:
            refuse(gap, "fetch", f"{release_url}: HTTP {status}, {len(body or '')} bytes")
            continue
        stats["fetched"] += 1
        payload = body.encode("utf-8")
        retrieved_at = utc_now()

        grids, grid_refusals = read_grids(body)
        if not grids:
            refuse(
                gap,
                "parse",
                f"no readable trim grid ({len(payload)} bytes); "
                + "; ".join(grid_refusals[:3]),
            )
            continue
        stats["parsed"] += 1

        identified, why = verify_page_identity(body, grids, year, make, model)
        if not identified:
            refuse(gap, "identity", why)
            continue

        page = store_page(
            payload,
            year=year,
            make=make,
            model=model,
            url=release_url,
            retrieved_at=retrieved_at,
        )
        stats["stored"] += 1
        print(
            f"    stored {page.bytes:,} bytes sha256={page.sha256[:12]}… "
            f"-> {page.path.name}; {len(grids)} grid(s), "
            f"{sum(len(g.features) for g in grids)} feature rows"
        )

        transcript = build_transcript(page, grids)
        transcript_file = write_transcript(transcript)
        transcript_rel = str(transcript_file.resolve().relative_to(_REPO))

        overlay = build_overlay(page, grids, transcript_rel)
        if overlay is None:
            refuse(gap, "extract", "no trim column stated an add against its neighbour")
            _append_html_spec_log(
                {
                    "catalog_key": transcript["catalog_key"],
                    "year": year,
                    "make": make,
                    "model": model,
                    "source_url": release_url,
                    "fetched_at": retrieved_at,
                    "sha256": page.sha256,
                    "bytes": page.bytes,
                    "html_path": str(page.path.resolve().relative_to(_REPO)),
                    "transcript_path": transcript_rel,
                    "overlay_path": None,
                    "citations": 0,
                    "verified": 0,
                    "user_agent": user_agent,
                    "fetcher": "backend.scripts.fetch_oem_brochures --html-specs",
                }
            )
            continue

        # Verify EVERY citation against the bytes we just stored, before the
        # overlay is written. This re-parses the document from scratch; it does
        # not ask the extractor whether the extractor was right.
        stored_bytes = page.path.read_bytes()
        verdicts: dict[str, int] = {}
        for entries in overlay["adds_provenance"].values():
            for entry in entries:
                verdict = verify_citation(entry, stored_bytes)
                entry["verified"] = verdict == "verified"
                entry["citation_verification"] = {
                    "verdict": verdict,
                    "checked_at": utc_now(),
                    "checker": "backend.enrichment.html_spec_sources.verify_citation",
                }
                verdicts[verdict] = verdicts.get(verdict, 0) + 1
        total = sum(verdicts.values())
        good = verdicts.get("verified", 0)
        stats["citations"] += total
        stats["verified"] += good
        for verdict, count in verdicts.items():
            if verdict != "verified":
                stats["verify_failures"][verdict] = (
                    stats["verify_failures"].get(verdict, 0) + count
                )

        HTML_SPEC_OVERLAY_DIR.mkdir(parents=True, exist_ok=True)
        stem = transcript["catalog_key"].replace("|", "__")
        overlay_path = HTML_SPEC_OVERLAY_DIR / f"{stem}.json"
        live_path = LIVE_OVERLAY_DIR / f"{stem}.json"
        overlay["shadows_live_overlay"] = (
            _overlay_source_of(live_path) if live_path.is_file() else None
        )
        overlay_path.write_text(
            json.dumps(overlay, indent=1, ensure_ascii=False), encoding="utf-8"
        )
        overlay_written = str(overlay_path.resolve().relative_to(_REPO))
        stats["overlays"] += 1
        if overlay["shadows_live_overlay"]:
            stats.setdefault("shadowing", []).append(
                {"key": transcript["catalog_key"], "live_source": overlay["shadows_live_overlay"]}
            )
        print(
            f"    {total} citations, {good} verified against the stored bytes"
            + (f"; overlay -> {overlay_path.name}" if overlay_written else "")
        )

        _append_html_spec_log(
            {
                "catalog_key": transcript["catalog_key"],
                "year": year,
                "make": make,
                "model": model,
                "source_url": release_url,
                "fetched_at": retrieved_at,
                "sha256": page.sha256,
                "bytes": page.bytes,
                "html_path": str(page.path.resolve().relative_to(_REPO)),
                "transcript_path": transcript_rel,
                "overlay_path": overlay_written,
                "overlay_source": overlay["source"],
                "trims": overlay["trims_available"],
                "citations": total,
                "verified": good,
                "verify_failures": {k: v for k, v in verdicts.items() if k != "verified"},
                "user_agent": user_agent,
                "fetcher": "backend.scripts.fetch_oem_brochures --html-specs",
            }
        )

    return stats


def rebuild_html_spec_overlays() -> int:
    """
    Re-derive transcripts and overlays from the STORED bytes. No network.

    An extraction rule change has to be re-appliable to the documents we already
    hold, offline, or the corpus quietly ends up holding output from several
    generations of rule. Re-fetching would be the wrong fix twice over: it costs
    the host requests it does not owe us, and the page may have changed, in
    which case the new overlay would no longer be about the bytes our earlier
    citations addressed.
    """
    transcripts = sorted(HTML_SPEC_TEXT_DIR.glob("*.json"))
    if not transcripts:
        print(f"No transcripts under {HTML_SPEC_TEXT_DIR}")
        return 0

    rebuilt = 0
    citations = 0
    verified = 0
    dropped = 0
    for path in transcripts:
        blob = json.loads(path.read_text(encoding="utf-8"))
        html_path = _REPO / str(blob.get("source_html") or "")
        if not html_path.is_file():
            print(f"{path.name:<40} stored HTML missing at {blob.get('source_html')}")
            continue
        payload = html_path.read_bytes()
        if sha256_bytes(payload) != str(blob.get("source_html_sha256") or ""):
            print(f"{path.name:<40} stored HTML no longer hashes to the recorded sha256")
            continue
        body = payload.decode("utf-8", errors="replace")
        year, make, model = blob["year"], blob["make"], blob["model"]
        grids, _ = read_grids(body)
        identified, why = verify_page_identity(body, grids, year, make, model)
        if not identified:
            print(f"{path.name:<40} refused on re-read: {why}")
            continue

        page = StoredPage(
            year=year,
            make=make,
            model=model,
            url=str(blob.get("source_url") or ""),
            retrieved_at=str(blob.get("retrieved_at") or ""),
            sha256=sha256_bytes(payload),
            bytes=len(payload),
            path=html_path,
            title=str(blob.get("source_title") or ""),
        )
        write_transcript(build_transcript(page, grids))
        overlay_path = HTML_SPEC_OVERLAY_DIR / f"{path.stem}.json"
        overlay = build_overlay(page, grids, str(path.resolve().relative_to(_REPO)))
        if overlay is None:
            if overlay_path.is_file():
                overlay_path.unlink()
                dropped += 1
            print(f"{path.name:<40} no citable add under the current rule")
            continue

        counts: dict[str, int] = {}
        for entries in overlay["adds_provenance"].values():
            for entry in entries:
                verdict = verify_citation(entry, payload)
                entry["verified"] = verdict == "verified"
                entry["citation_verification"] = {
                    "verdict": verdict,
                    "checked_at": utc_now(),
                    "checker": "backend.enrichment.html_spec_sources.verify_citation",
                }
                counts[verdict] = counts.get(verdict, 0) + 1
        HTML_SPEC_OVERLAY_DIR.mkdir(parents=True, exist_ok=True)
        overlay_path.write_text(
            json.dumps(overlay, indent=1, ensure_ascii=False), encoding="utf-8"
        )
        rebuilt += 1
        total = sum(counts.values())
        good = counts.get("verified", 0)
        citations += total
        verified += good
        print(f"{path.name:<40} {total:>4} citations, {good:>4} verified")

    print(
        f"\nrebuilt {rebuilt} overlays from stored bytes ({dropped} dropped as "
        f"no longer citable); {citations} citations, {verified} verified"
    )
    return 0


def reverify_html_specs() -> int:
    """
    Re-derive every stored HTML citation from the stored bytes, and report.

    Reads the overlays back off disk rather than trusting the numbers the fetch
    printed, which is the same reason ``verify_trim_citations.py`` exists for the
    PDF lane: a run that certifies its own output has proved nothing.
    """
    overlays = [
        p
        for p in sorted(HTML_SPEC_OVERLAY_DIR.glob("*.json"))
        if _overlay_source_of(p) == HTML_SPEC_OVERLAY_SOURCE
    ]
    if not overlays:
        print(f"No {HTML_SPEC_OVERLAY_SOURCE} overlays under {HTML_SPEC_OVERLAY_DIR}")
        return 0

    totals: dict[str, int] = {}
    bytes_cache: dict[str, bytes] = {}
    for path in overlays:
        overlay = json.loads(path.read_text(encoding="utf-8"))
        stem = str(overlay.get("catalog_key") or path.stem).replace("|", "__")
        html_path = HTML_SPEC_PAGES_DIR / f"{stem}.html"
        payload = bytes_cache.get(stem)
        if payload is None:
            payload = html_path.read_bytes() if html_path.is_file() else b""
            bytes_cache[stem] = payload
        counts: dict[str, int] = {}
        for entries in (overlay.get("adds_provenance") or {}).values():
            for entry in entries if isinstance(entries, list) else []:
                verdict = verify_citation(entry, payload)
                counts[verdict] = counts.get(verdict, 0) + 1
                totals[verdict] = totals.get(verdict, 0) + 1
        summary = ", ".join(f"{k}={v}" for k, v in sorted(counts.items()))
        print(f"{path.name:<44} {summary}")
    print("\nTotals:")
    for verdict, count in sorted(totals.items(), key=lambda kv: -kv[1]):
        print(f"  {verdict:<38} {count}")
    return 0


def _overlay_source_of(path: Path) -> str:
    try:
        blob = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return ""
    return str(blob.get("source") or "").strip().lower() if isinstance(blob, dict) else ""


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--download",
        action="store_true",
        help="Actually download. Without this the run is a dry run and writes nothing.",
    )
    parser.add_argument("--limit", type=int, default=25, help="Max gaps to consider.")
    parser.add_argument(
        "--brand",
        "--make",
        dest="brand",
        action="append",
        help="Restrict to brand(s) so parallel agents take disjoint slices.",
    )
    parser.add_argument("--year", type=int, action="append", help="Restrict to year(s).")
    parser.add_argument("--min-cars", type=int, default=1, help="Skip smaller gaps.")
    parser.add_argument(
        "--delay", type=float, default=4.0, help="Seconds between requests (min 3)."
    )
    parser.add_argument(
        "--user-agent",
        choices=["browser", "identified", "both"],
        default="browser",
        help="Which user agent to send. 'both' is probe-only and reports the delta.",
    )
    parser.add_argument(
        "--supported-only",
        action="store_true",
        help="Skip makes with no registered official source.",
    )
    parser.add_argument(
        "--probe-reachability",
        action="store_true",
        help="Only measure per-brand reachability and exit.",
    )
    parser.add_argument(
        "--audit-corpus",
        action="store_true",
        help="Only content-hash every stored PDF and report duplicates.",
    )
    parser.add_argument(
        "--allow-archive",
        action="store_true",
        help=(
            "Allow the archive tier (auto-brochures.com) as a FALLBACK for gaps "
            "the OEM tier cannot serve. OEM is always tried first. Paced at "
            f"{ARCHIVE_DELAY_SECONDS:g}s, more conservatively than the OEM path."
        ),
    )
    parser.add_argument(
        "--archive-verify-overlap",
        type=int,
        default=0,
        metavar="N",
        help=(
            "Measurement mode: fetch the archive's copy of N vehicles we already "
            "hold an OEM original for and compare sha256, then text. Copies go to "
            "brochures_archive_comparison/, never into the corpus."
        ),
    )
    parser.add_argument(
        "--archive-audit",
        action="store_true",
        help=(
            "Re-open every archive-tier PDF on disk and report its hash, metadata "
            "and text layer. Reads the documents, not the register."
        ),
    )
    parser.add_argument(
        "--quarantine-unidentified",
        action="store_true",
        help=(
            "Re-run the identity, subject (derived-nameplate) and quality "
            "(decode / length / digit-density) checks over derived/brochure_text "
            "and move the failures to brochure_text_quarantine/. Dry run unless "
            "--apply."
        ),
    )
    parser.add_argument(
        "--quarantine-derived",
        action="store_true",
        help=(
            "Only sweep the derived stores (brochure_text_slim, trim_candidates, "
            "trim_adds_by_year) for artifacts of an already-quarantined document. "
            "Dry run unless --apply."
        ),
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help=(
            "Perform the moves for --quarantine-unidentified / --quarantine-derived "
            "(default: dry run)."
        ),
    )
    parser.add_argument(
        "--html-specs",
        action="store_true",
        help=(
            "Acquire official HTML specification pages instead of PDFs, for the "
            "makes with a measured discovery chain. Honours --download."
        ),
    )
    parser.add_argument(
        "--rebuild-html-overlays",
        action="store_true",
        help=(
            "Re-derive HTML spec transcripts and overlays from the stored bytes. "
            "Fetches nothing."
        ),
    )
    parser.add_argument(
        "--reverify-html-specs",
        action="store_true",
        help=(
            "Re-derive every stored HTML citation from the stored bytes and "
            "report. Fetches nothing."
        ),
    )
    parser.add_argument("--show-gaps", type=int, default=0, help="Print top N gaps.")
    parser.add_argument(
        "--artifact-dir",
        type=Path,
        default=ARTIFACT_DIR,
        help="Where the JSON artifacts are written.",
    )
    parser.add_argument(
        "--no-artifact", action="store_true", help="Do not write JSON artifacts."
    )
    args = parser.parse_args()

    # Pacing is not negotiable: bursts get this project soft-blocked.
    delay = max(3.0, args.delay)

    if args.quarantine_unidentified:
        return quarantine_unidentified(args.apply)

    if args.quarantine_derived:
        return quarantine_derived(args.apply)

    if args.audit_corpus:
        return audit_corpus()

    if args.rebuild_html_overlays:
        return rebuild_html_spec_overlays()

    if args.reverify_html_specs:
        return reverify_html_specs()

    if args.archive_audit:
        return archive_audit()

    if args.archive_verify_overlap:
        return archive_verify_overlap(
            args.archive_verify_overlap,
            delay,
            _UA_CHOICES["browser" if args.user_agent == "both" else args.user_agent],
            {b.lower() for b in (args.brand or [])} or None,
        )

    if args.probe_reachability:
        labels = ["browser", "identified"] if args.user_agent == "both" else [
            args.user_agent
        ]
        brand = (args.brand or [None])[0]
        print(f"Per-brand reachability (paced {delay:g}s, sequential)\n")
        results = probe_reachability(labels, delay, brand)
        by_verdict: dict[str, int] = {}
        for result in results:
            by_verdict[result.verdict] = by_verdict.get(result.verdict, 0) + 1
        print("\nVerdict counts:")
        for verdict, count in sorted(by_verdict.items(), key=lambda kv: -kv[1]):
            print(f"  {verdict:<28} {count}")

        # "We do not allow that host" and "that host will not answer us" are
        # different findings. Reporting them together is how an unfinished
        # allowlist gets written up as an OEM refusing us.
        print("\nWho declined:")
        for label, tag in (
            ("we did (host not allowlisted)", "our_allowlist"),
            ("they did (published robots rules)", "their_robots_rules"),
            ("they did (edge refuses us)", "their_edge"),
            ("nobody (probe completed)", ""),
        ):
            hosts = sorted({r.host for r in results if r.blocked_by == tag})
            if hosts:
                print(f"  {label:<36} {len(hosts):>2}  {', '.join(hosts)}")
        if not args.no_artifact:
            path = write_reachability_artifact(
                results, args.artifact_dir / "reachability.json"
            )
            print(f"\nreport -> {path}")
        return 0

    gaps, stats = load_gap_list()
    print(
        f"corpus files={stats['corpus_files']}  active cars={stats['active_cars']}\n"
        f"distinct vehicles={stats['distinct_vehicles']} "
        f"(raw catalog_key combos={stats['raw_catalog_key_combos']})\n"
        f"covered vehicles={stats['covered_vehicles']}  "
        f"uncovered vehicles={stats['uncovered_vehicles']}\n"
        f"covered cars={stats['covered_cars']} "
        f"({stats['covered_cars'] / max(1, stats['active_cars']):.1%})  "
        f"gap cars={stats['gap_cars']}\n"
        f"alias-only catalog keys (document held, lookup key missing)="
        f"{stats['alias_only_keys']} covering {stats['alias_only_cars']} cars"
    )

    wanted_brands = {b.lower() for b in (args.brand or [])}
    wanted_years = set(args.year or [])
    filtered = [
        gap
        for gap in gaps
        if gap["cars"] >= args.min_cars
        and (not wanted_brands or gap["make"].lower() in wanted_brands)
        and (not wanted_years or gap["year"] in wanted_years)
        and (not args.supported_only or not gap["unsupported_reason"])
    ]

    if not args.no_artifact:
        path = write_gap_artifact(
            filtered,
            stats,
            args.artifact_dir / (
                f"gap_list_{'_'.join(sorted(wanted_brands))}.json"
                if wanted_brands
                else "gap_list.json"
            ),
            ",".join(sorted(wanted_brands)) or None,
        )
        print(f"\ngap artifact ({len(filtered)} entries) -> {path}")

    if args.show_gaps:
        print(f"\nTop {args.show_gaps} gaps by active cars:")
        for gap in filtered[: args.show_gaps]:
            flag = "-" if gap["unsupported_reason"] else "OK"
            spellings = (
                f"  [{len(gap['variants'])} spellings]" if len(gap["variants"]) > 1 else ""
            )
            print(
                f"{gap['cars']:6d}  {flag:<3} {gap['year']} {gap['make']} "
                f"{gap['model']}{spellings}"
            )

    if args.html_specs:
        html_stats = fetch_html_specs(
            filtered,
            limit=args.limit,
            delay=delay,
            user_agent=_UA_CHOICES[
                "browser" if args.user_agent == "both" else args.user_agent
            ],
            download=args.download,
        )
        print(
            "\nHTML spec pages: "
            f"considered={html_stats['considered']} "
            f"discovered={html_stats['discovered']} "
            f"already held={html_stats['already_held']} "
            f"fetched={html_stats['fetched']} parsed={html_stats['parsed']} "
            f"stored={html_stats['stored']} "
            f"refused={html_stats['refused']} overlays={html_stats['overlays']}\n"
            f"citations={html_stats['citations']} "
            f"verified against stored bytes={html_stats['verified']}"
        )
        if html_stats["verify_failures"]:
            print("verification failures:")
            for verdict, count in sorted(
                html_stats["verify_failures"].items(), key=lambda kv: -kv[1]
            ):
                print(f"  {verdict:<38} {count}")
        if html_stats["refusals"]:
            print("\nrefusals:")
            for row in html_stats["refusals"]:
                print(f"  {row['group_key']:<28} {row['stage']:<11} {row['reason'][:110]}")
        print("\nnot read out of these pages, by design:")
        for name, why in HTML_SPEC_REFUSED_BY_DESIGN.items():
            print(f"  {name:<16} {why}")
        if not args.no_artifact and args.download:
            path = args.artifact_dir / "html_spec_run.json"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(
                json.dumps(
                    {"generated_at": datetime.now(timezone.utc).isoformat(), **html_stats},
                    indent=2,
                    ensure_ascii=False,
                )
                + "\n",
                encoding="utf-8",
            )
            print(f"\nreport -> {path}")
        return 0

    selected = filtered[: args.limit]
    index, duplicates = open_content_index()
    if duplicates:
        print(
            f"\ncontent index: {len(index.by_sha)} distinct documents, "
            f"{len(duplicates)} stored more than once (see --audit-corpus)"
        )

    print(
        f"\n{'DOWNLOAD' if args.download else 'DRY RUN (default) - nothing will be written'}"
        f": considering {len(selected)} gaps"
        f" covering {sum(g['cars'] for g in selected)} active cars\n"
    )

    user_agent = _UA_CHOICES["browser" if args.user_agent == "both" else args.user_agent]
    fetcher = PacedFetcher.for_tier(TIER_OEM, delay=delay, user_agent=user_agent)
    robots = RobotsPolicy(fetcher.get_text, agent=fetcher.robots_agent)

    archive: ArchiveResolver | None = None
    if args.allow_archive:
        archive = ArchiveResolver(delay=delay, user_agent=user_agent)
        print(
            f"archive tier ENABLED as a fallback (paced {archive.fetcher.delay:g}s vs "
            f"{fetcher.delay:g}s for OEM)\n  {ARCHIVE_SOURCE_NOTE}\n"
        )

    counts: dict[str, int] = {}
    by_tier: dict[str, int] = {}
    fetched = 0
    would_fetch = 0
    page_cache: dict[str, tuple[int, str]] = {}

    for gap in selected:
        resolved = resolve_source(gap, fetcher, robots, page_cache, archive=archive)
        counts[resolved["status"]] = counts.get(resolved["status"], 0) + 1

        label = f"{gap['cars']:6d}  {gap['year']} {gap['make']} {gap['model']}"
        if resolved["status"] != "found":
            extra = resolved.get("archive_detail")
            print(
                f"{label}\n         SKIP [{resolved['status']}] {resolved['detail']}"
                + (f"\n           archive: {extra}" if extra else "")
            )
            continue
        by_tier[resolved["tier"]] = by_tier.get(resolved["tier"], 0) + 1

        target = BROCHURE_TEXT_DIR / f"{gap['primary_catalog_key']}.json"

        tier_tag = f"[{resolved['tier']}]"
        if not args.download:
            would_fetch += 1
            print(
                f"{label}\n"
                f"         WOULD FETCH {tier_tag} {resolved['doc_url']}\n"
                f"           via {resolved['discovery_url']}  ({resolved['detail']})\n"
                + (
                    f"           OEM tier first said: {resolved['oem_refusal']}\n"
                    if resolved.get("oem_refusal")
                    else ""
                )
                + f"           -> {target.relative_to(_REPO)}"
            )
            continue

        outcome = download_and_extract(
            resolved, fetcher, robots, index, archive=archive
        )
        counts[outcome["status"]] = counts.get(outcome["status"], 0) + 1
        if outcome["status"] == "fetched":
            fetched += 1
            authenticity = outcome.get("authenticity") or {}
            comparison = authenticity.get("oem_comparison") or {}
            print(
                f"{label}\n"
                f"         FETCHED {tier_tag} {outcome['doc_url']}\n"
                f"           {outcome['bytes']} bytes, {outcome['page_count']} pages, "
                f"{outcome['pages_saved']} saved, {outcome['text_chars']} chars, "
                f"sha256 {outcome['sha256'][:12]}\n"
                + (
                    f"           producer={authenticity.get('pdf_metadata', {}).get('producer')!r} "
                    f"text_layer={authenticity.get('pages_with_text')}/"
                    f"{authenticity.get('page_count')}p"
                    + (f" vs OEM: {comparison.get('outcome')}" if comparison else "")
                    + "\n"
                    if authenticity
                    else ""
                )
                + f"           -> {target.name}"
            )
        elif outcome["status"] in ("duplicate_content", "already_downloaded"):
            print(f"{label}\n         DEDUPED [{outcome['status']}] {outcome['detail']}")
        elif outcome["status"] == "archive_quarantined":
            print(
                f"{label}\n         ARCHIVE QUARANTINED {outcome['detail']}\n"
                f"           kept at {outcome['pdf_path']}"
            )
        elif outcome["status"] == "stored_text_rejected":
            print(
                f"{label}\n         PDF STORED, TEXT REJECTED {outcome['detail']}\n"
                f"           kept at {Path(outcome['pdf_path']).name}"
            )
        else:
            print(f"{label}\n         FAIL [{outcome['status']}] {outcome['detail']}")

    print("\nOutcome counts:")
    for status, count in sorted(counts.items(), key=lambda kv: -kv[1]):
        print(f"  {status:<28} {count}")
    if by_tier:
        print("\nResolved by tier (OEM is always tried first):")
        for tier, count in sorted(by_tier.items()):
            print(f"  {tier:<28} {count}")
    if archive is not None:
        print(
            f"\narchive requests this run: {archive.fetcher.request_count} "
            f"at a {archive.fetcher.delay:g}s floor"
        )
    if args.download:
        print(f"\nfetched {fetched} new document(s); provenance -> {FETCH_LOG_PATH}")
        print(f"content index -> {CONTENT_INDEX_PATH}")
    else:
        print(f"\nwould fetch {would_fetch} document(s); re-run with --download")

    print(f"\nmakes verified end to end: {', '.join(sorted(VERIFIED_MAKES))}")
    print("makes with an official host but an unresolved discovery step:")
    for make, reason in sorted(DISCOVERY_UNRESOLVED.items()):
        print(f"  {make:<12} {reason}")
    if HOST_ACCESS_NOTES:
        print("\nhosts measured as refusing automated access (robots.txt unreadable):")
        for host, note in sorted(HOST_ACCESS_NOTES.items()):
            print(f"  {host:<22} {note}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
