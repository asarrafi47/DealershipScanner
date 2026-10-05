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
# The library code lives in backend/enrichment/brochure_acquisition/ (audit
# datascripts.md F7): gaps, sources, download, corpus, quarantine, audits,
# reachability, html_specs, and plan (this script's default mode). This file keeps
# the argparse surface and the mode dispatch, and re-exports every name it used to
# define so ``from backend.scripts.fetch_oem_brochures import X`` keeps working.
# Monkeypatch the package module that USES a name, not this script.
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from backend.enrichment.brochure_acquisition.audits import (  # noqa: E402,F401
    archive_audit,
    archive_verify_overlap,
    audit_corpus,
)
from backend.enrichment.brochure_acquisition.corpus import (  # noqa: E402,F401
    _append_fetch_log,
    audit_corpus_text,
    corpus_page_texts,
    full_document_text,
    open_content_index,
    page_texts,
)
from backend.enrichment.brochure_acquisition.download import (  # noqa: E402,F401
    download_and_extract,
    oem_pdf_for_catalog_key,
)
from backend.enrichment.brochure_acquisition.gaps import (  # noqa: E402,F401
    _fetch_inventory_rows,
    build_gap_list,
    load_gap_list,
    write_gap_artifact,
)
from backend.enrichment.brochure_acquisition.html_specs import (  # noqa: E402,F401
    HTML_SPEC_LOG_PATH,
    HTML_SPEC_OVERLAY_DIR,
    LIVE_OVERLAY_DIR,
    _append_html_spec_log,
    _held_html_spec,
    _honda_release_for,
    _html_spec_targets,
    _overlay_source_of,
    fetch_html_specs,
    rebuild_html_spec_overlays,
    reverify_html_specs,
)
from backend.enrichment.brochure_acquisition.paths import (  # noqa: E402,F401
    ARTIFACT_DIR,
    CONTENT_INDEX_PATH,
    FETCH_LOG_PATH,
)
from backend.enrichment.brochure_acquisition.plan import (  # noqa: E402,F401
    html_specs_mode,
    paced_delay,
    plan_and_download,
)
from backend.enrichment.brochure_acquisition.quarantine import (  # noqa: E402,F401
    quarantine_derived,
    quarantine_unidentified,
)
from backend.enrichment.brochure_acquisition.reachability import (  # noqa: E402,F401
    _UA_CHOICES,
    probe_reachability,
    report_reachability,
    write_reachability_artifact,
)
from backend.enrichment.brochure_acquisition.sources import (  # noqa: E402,F401
    ArchiveResolver,
    _resolve_from_archive,
    resolve_source,
)
from backend.enrichment.brochure_sources import ARCHIVE_DELAY_SECONDS  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


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

    delay = paced_delay(args)

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
        return report_reachability(args, delay)

    # Default mode: gap list, then plan (dry run) or --download.
    return plan_and_download(args)


if __name__ == "__main__":
    raise SystemExit(main())
