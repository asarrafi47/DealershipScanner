"""Derived artifacts of a quarantined document: what has to follow the PDF out."""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from backend.enrichment.dictionary_paths import (
    BROCHURE_TEXT_DIR,
    DERIVED_DIR,
)

logger = logging.getLogger(__name__)


BROCHURE_TEXT_QUARANTINE_DIR = DERIVED_DIR / "brochure_text_quarantine"


def is_quarantined(catalog_key_str: str) -> bool:
    """
    True if this vehicle's brochure text has been quarantined.

    Anything that *writes* ``derived/brochure_text`` must consult this. The
    source PDF stays on disk after a quarantine -- deleting acquired documents
    is what made this corpus unrecoverable -- so a later re-ingestion run would
    otherwise re-extract that PDF straight back into the corpus and silently
    undo the quarantine.
    """
    stem = str(catalog_key_str or "").replace("|", "__")
    if not stem:
        return False
    return (BROCHURE_TEXT_QUARANTINE_DIR / f"{stem}.json").is_file()


# --------------------------------------------------------------------------
# Derived artifacts of a quarantined document
# --------------------------------------------------------------------------
#
# Quarantining ``derived/brochure_text/<key>.json`` does NOT by itself stop the
# document reaching a car page, because other derived stores were built from it
# and are read independently:
#
#   derived/brochure_text_slim/<key>.json   a verbatim excerpt of the same
#       document. ``trim_ladder._brochure_engine_step_ups`` reads it AT RENDER
#       TIME and turns "<TRIM> / Adds to <LOWER>" lines into ``brochure_trim_walk``
#       bullets, which ``LADDER_BULLET_STORES`` marks admissible. Quoting the
#       excerpt is quoting the quarantined document.
#   derived/trim_candidates/<key>.json      built from the document by
#       ``brochure_trim_candidates``; feeds the overlay builders.
#   derived/trim_adds_by_year/<key>.json    the overlay a car page actually
#       renders. Its ``adds_provenance`` entries cite the brochure_text file by
#       path, so once that file is quarantined the citation names a document we
#       no longer hold.
#
# The other things under derived/ were enumerated and are NOT swept, each for a
# stated reason rather than by omission:
#
#   brochure_facts/<Make>/<year>_<model>.json   written by
#       ``brochure_extract.persist_brochure_extract`` and read by nothing in this
#       repo (one file exists). It cannot render.
#   trim_spec_sheets/*.json   keyed by Complete_Options CSV name, not by
#       catalog_key, and built from the CSV stores rather than from brochure text.
#       ``LADDER_BULLET_STORES`` already refuses ``trim_spec_sheet`` outright.
#   brochure_content_index.json / brochure_fetch_log.jsonl   provenance ledgers
#       for the stored PDFs. They record what we hold; they state nothing about a
#       vehicle and put no bullet on a rung. Pruning them would destroy the
#       record that makes a quarantine reversible.
#
# NOT closed here, and reported upward instead:
# ``brochure_extract._write_marketing_trim_csv_rows`` appends brochure-derived
# rows into ``options/raw/**/*_Complete_Options.csv``. Those rows carry no
# provenance field, so rows written from a now-quarantined document cannot be
# identified, let alone withdrawn. Nothing renders from them today --
# ``complete_options_csv`` is "uncited" in ``LADDER_BULLET_STORES`` -- but the
# contamination is real and it is why that store must not be admitted later
# without a per-row source column.
#
# Measured 2026-07-31 against the 398 documents already in quarantine: 385 still
# had a slim copy, 385 a trim_candidates file, 382 an overlay -- and
# ``resolve_trim_ladder(make="Nissan", model="Rogue Plug-In Hybrid", year=2026)``
# still returned bullets WITH ``TRIM_ADDS_REQUIRE_PROVENANCE`` at its default.
#
# What the sweep cost, both measured over all 398 quarantined keys by rendering
# the ladder before and after:
#
#   gate ON  (production default): 22 bullets removed from one key
#       (2026 Nissan Rogue Plug-In Hybrid); no key gained a bullet, no other key
#       changed. 16 keys still rendered at that moment, all of them from
#       ``epa_csv``; that store was revoked in ``LADDER_BULLET_STORES`` later the
#       same day by the lane that owns it, after which a re-measurement over all
#       398 quarantined keys returned zero rendered bullets from any store.
#   gate OFF (TRIM_ADDS_REQUIRE_PROVENANCE=0, the documented escape hatch):
#       the same 22 removed, and 22 gained across 6 keys. That gain is real and
#       is the price of this sweep: ``trim_ladder._lineup_standard_features``
#       reads the same slim file to SUPPRESS equipment the brochure grid marks
#       standard on every trim, so taking the excerpt away also takes the
#       suppressor away. It only shows up with the gate off, where 5,478 uncited
#       bullets already render at those keys. Keeping a wrongly-identified
#       document readable to preserve a suppressor is the wrong trade.

#: Sibling quarantine directories, one per derived store. Siblings, not
#: subdirectories, for the same reason as :data:`BROCHURE_TEXT_QUARANTINE_DIR`:
#: every ``<store>.glob("*.json")`` in the codebase stops seeing them.
BROCHURE_TEXT_SLIM_QUARANTINE_DIR = DERIVED_DIR / "brochure_text_slim_quarantine"
TRIM_CANDIDATES_QUARANTINE_DIR = DERIVED_DIR / "trim_candidates_quarantine"
TRIM_ADDS_BY_YEAR_QUARANTINE_DIR = DERIVED_DIR / "trim_adds_by_year_quarantine"


@dataclass(frozen=True)
class DerivedStore:
    """A derived store that a ``brochure_text`` document feeds."""

    name: str
    live_dir: Path
    quarantine_dir: Path
    #: ``"excerpt"``   -- the file is (or is mechanically built from) the
    #:                    document's own text; quarantine every key.
    #: ``"citation"``  -- the file may carry a citation naming the document;
    #:                    quarantine it only when it can actually render a cited
    #:                    bullet (see :func:`derived_quarantine_reason`).
    rule: str
    why: str


#: The stores swept by ``fetch_oem_brochures.py --quarantine-derived``.
#:
#: ``trim_adds_by_year`` is deliberately ``"citation"`` and not ``"excerpt"``.
#: An overlay file is not only a bullet carrier: ``apply_overlay_provenance_gate``
#: keeps the object even after dropping every bullet, because its
#: ``trims_available`` is what stops the ladder substituting Complete_Options CSV
#: lines and generated position copy for the rungs. Moving an *uncited* overlay
#: out would therefore remove a suppressor and could put MORE text on the page --
#: fail-open, which is the wrong direction. Uncited overlays at a quarantined key
#: are already refused by ``LADDER_BULLET_STORES`` and are left in place and
#: listed in the manifest under ``left_in_place``.
DERIVED_STORES_FROM_BROCHURE_TEXT: tuple[DerivedStore, ...] = (
    DerivedStore(
        name="brochure_text_slim",
        live_dir=DERIVED_DIR / "brochure_text_slim",
        quarantine_dir=BROCHURE_TEXT_SLIM_QUARANTINE_DIR,
        rule="excerpt",
        why="verbatim excerpt of the quarantined document",
    ),
    DerivedStore(
        name="trim_candidates",
        live_dir=DERIVED_DIR / "trim_candidates",
        quarantine_dir=TRIM_CANDIDATES_QUARANTINE_DIR,
        rule="excerpt",
        why="trim names read out of the quarantined document",
    ),
    DerivedStore(
        name="trim_adds_by_year",
        live_dir=DERIVED_DIR / "trim_adds_by_year",
        quarantine_dir=TRIM_ADDS_BY_YEAR_QUARANTINE_DIR,
        rule="citation",
        why="overlay bullets cited to the quarantined document",
    ),
)


def _overlay_cited_documents(overlay: object) -> set[str]:
    """
    Every ``derived/brochure_text/...`` path an overlay names as its source.

    Reads the overlay's own ``source_brochure_text`` and each ``adds_provenance``
    entry's ``source``. A citation that names a document is the only kind this
    project accepts, so these paths are the whole set of documents the overlay
    can be said to rest on.
    """
    if not isinstance(overlay, dict):
        return set()
    refs: set[str] = set()
    top = overlay.get("source_brochure_text")
    if isinstance(top, str) and top.strip():
        refs.add(top.strip())
    provenance = overlay.get("adds_provenance")
    if isinstance(provenance, dict):
        for entries in provenance.values():
            for entry in entries or []:
                if isinstance(entry, dict):
                    source = entry.get("source")
                    if isinstance(source, str) and source.strip():
                        refs.add(source.strip())
    return {r for r in refs if "brochure_text" in r}


def _document_key_of_citation(citation: str) -> str:
    """``derived/brochure_text/2026__nissan__rogue.json`` -> ``2026__nissan__rogue``."""
    return Path(str(citation or "")).stem


def quarantined_document_keys() -> frozenset[str]:
    """
    Stems (``<year>__<make>__<model>``) of every quarantined brochure_text file.

    Deliberately unchanged by the re-acquisition work of 2026-08-01: a key with
    a quarantined copy stays quarantined here even after a *better* document for
    the same vehicle has been fetched. Releasing the key outright was tried and
    reverted, because it fails OPEN -- see :func:`superseded_quarantine_keys` for
    what was wrong with it and what replaced it.
    """
    if not BROCHURE_TEXT_QUARANTINE_DIR.is_dir():
        return frozenset()
    return frozenset(
        p.stem
        for p in BROCHURE_TEXT_QUARANTINE_DIR.glob("*.json")
        if not p.name.startswith("_")
    )


def _source_pdf_sha(path: Path) -> str:
    """``source_pdf_sha256`` recorded inside a brochure_text file, or ``""``."""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return ""
    return str((data or {}).get("source_pdf_sha256") or "") if isinstance(data, dict) else ""


def superseded_quarantine_keys() -> dict[str, Path]:
    """
    ``{key: live_brochure_text_path}`` for quarantined vehicles we have since
    re-acquired from a **different** document.

    Why this exists. On 2026-08-01 the fetcher downloaded seven current-model-year
    Toyota brochures for keys that were already in quarantine --
    ``2026__toyota__camry`` among them, whose quarantined text was 2,392
    characters of Toyota's disclosures page that never printed "Camry". The
    replacement is ``camry_ebrochure.pdf`` from ``www.toyota.com``: 28 pages,
    71,632 characters, and it passed :func:`verify_document_identity` and
    :func:`assess_text_quality` before a single character was persisted.
    Fourteen keys are in that state today (the seven Toyota fetches plus seven
    archive-tier re-acquisitions).

    The obvious fix -- drop a re-acquired key from
    :func:`quarantined_document_keys` -- was written, measured and reverted,
    because it fails open. ``derived/brochure_text_slim/<key>.json`` is still the
    excerpt of the OLD, rejected document until something rebuilds it, and
    ``trim_ladder._brochure_engine_step_ups`` reads that excerpt at render time.
    Releasing the key would have left that stale excerpt live and rendering,
    which is exactly the failure the quarantine exists to prevent.

    So the key stays quarantined and this function answers the narrower
    question: *is the document at this key now a different one?* The test is the
    ``source_pdf_sha256`` recorded in each file -- a live file whose source PDF
    is the same one that was quarantined is not a re-acquisition at all and is
    not listed. :func:`derived_quarantine_reason` uses this to decide per
    derived FILE, on mtime, and defaults to quarantining when it cannot tell.
    """
    out: dict[str, Path] = {}
    if not BROCHURE_TEXT_QUARANTINE_DIR.is_dir():
        return out
    for quarantined in BROCHURE_TEXT_QUARANTINE_DIR.glob("*.json"):
        if quarantined.name.startswith("_"):
            continue
        live = BROCHURE_TEXT_DIR / quarantined.name
        if not live.is_file():
            continue
        live_sha = _source_pdf_sha(live)
        if live_sha and live_sha != _source_pdf_sha(quarantined):
            out[quarantined.stem] = live
    return out


def derived_quarantine_reason(
    store: DerivedStore,
    path: Path,
    quarantined_keys: frozenset[str],
    superseded: dict[str, Path] | None = None,
) -> str | None:
    """
    Why this derived file must follow its source document into quarantine, or None.

    ``"excerpt"`` stores are keyed one file per document, so the key alone
    decides. For the overlay store the test is whether the file can put a *cited*
    bullet on a rung: either it names a quarantined (or missing) brochure_text
    document in its provenance, or its ``source`` is one of the admissible
    overlay sources at a quarantined key -- in which case the bullets it renders
    are quotations from a document that failed identity verification.

    ``superseded`` (:func:`superseded_quarantine_keys`) is the one narrow escape,
    and only for ``"excerpt"`` stores. At a key whose document has been
    re-acquired from a different PDF, an excerpt written **after** the live
    document is an excerpt OF the live document and there is nothing wrong with
    it. An excerpt written before it is still the old rejected document's text
    and is quarantined exactly as before.

    The tie-breaker is file mtime, which is a weak signal, so it is used in the
    fail-closed direction only: equal-or-older, unreadable, or unknown -> the
    file is quarantined. Nothing is released on a guess.
    """
    stem = path.stem
    if store.rule == "excerpt":
        if stem not in quarantined_keys:
            return None
        live = (superseded or {}).get(stem)
        if live is not None:
            try:
                if path.stat().st_mtime > live.stat().st_mtime:
                    return None
            except OSError:
                return store.why
            return (
                f"{store.why}; the document at this key was re-acquired but this "
                f"file predates it, so it is still the rejected document's text"
            )
        return store.why

    try:
        overlay = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return f"unreadable overlay: {exc}"
    if not isinstance(overlay, dict):
        return None

    from backend.enrichment.brochure_extract import ADMISSIBLE_OVERLAY_SOURCES

    source = str(overlay.get("source") or "").strip().lower()
    citable = source in ADMISSIBLE_OVERLAY_SOURCES

    for citation in sorted(_overlay_cited_documents(overlay)):
        cited_key = _document_key_of_citation(citation)
        if cited_key in quarantined_keys:
            return f"cites quarantined document {cited_key} (source={source})"
        if not (BROCHURE_TEXT_DIR / f"{cited_key}.json").is_file():
            return f"cites a document we no longer hold: {citation} (source={source})"

    if citable and stem in quarantined_keys:
        return f"admissible overlay ({source}) at a quarantined key"
    return None


def plan_derived_quarantine() -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    """
    ``(to_move, left_in_place)`` for every derived artifact of a quarantined document.

    ``left_in_place`` is not noise: it is the list of files that sit at a
    quarantined key and are NOT moved, with the reason, so "we looked and chose
    not to" is on the record rather than inferred from absence.
    """
    quarantined = quarantined_document_keys()
    superseded = superseded_quarantine_keys()
    to_move: list[dict[str, object]] = []
    left: list[dict[str, object]] = []
    for store in DERIVED_STORES_FROM_BROCHURE_TEXT:
        if not store.live_dir.is_dir():
            continue
        for path in sorted(store.live_dir.glob("*.json")):
            if path.name.startswith("_"):
                continue
            reason = derived_quarantine_reason(store, path, quarantined, superseded)
            row = {"store": store.name, "file": path.name, "reason": reason}
            if reason:
                to_move.append(row)
            elif path.stem in quarantined:
                row["reason"] = (
                    f"rebuilt after the document at this key was re-acquired "
                    f"({superseded[path.stem].name}), so it is an excerpt of the "
                    f"live document, not the quarantined one"
                    if store.rule == "excerpt" and path.stem in superseded
                    else "at a quarantined key but carries no citation to it; already "
                    "refused by LADDER_BULLET_STORES, and moving it would remove "
                    "the trims_available that blocks CSV substitution"
                )
                left.append(row)
    return to_move, left


def apply_derived_quarantine(rows: list[dict[str, object]]) -> int:
    """Move the planned files into their sibling quarantine dirs. Never deletes."""
    by_store = {s.name: s for s in DERIVED_STORES_FROM_BROCHURE_TEXT}
    moved = 0
    for row in rows:
        store = by_store.get(str(row.get("store")))
        if store is None:
            continue
        src = store.live_dir / str(row.get("file"))
        if not src.is_file():
            continue
        store.quarantine_dir.mkdir(parents=True, exist_ok=True)
        src.replace(store.quarantine_dir / src.name)
        moved += 1
    return moved
