#!/usr/bin/env python3
"""
Generate ``backend/dictionary/trim_spec_sheets/*.json`` from trim ladder definitions.

Run from repo root::

    python -m backend.scripts.build_trim_spec_sheets
    python -m backend.scripts.build_trim_spec_sheets --write-adds
    python -m backend.scripts.build_trim_spec_sheets --brochure-coverage
    python -m backend.scripts.build_trim_spec_sheets --write-quoted-overlays

``--write-quoted-overlays`` REPLACES the overlay file, which drops the
``verified`` stamp on every bullet in it. Nothing renders until the citations
have been checked against the PDFs again, so always follow it with::

    python backend/scripts/verify_trim_citations.py --apply

That order is deliberate: a freshly extracted bullet must be re-checked against
the document, not inherit a stamp issued for a previous run's text.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import tempfile
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from backend.enrichment.trim_ladder import _LADDERS_GENERATED_JSON, _LADDERS_JSON, _read_ladders_file  # noqa: E402
from backend.enrichment.trim_spec_extractor import build_spec_sheet, extract_trim_specs  # noqa: E402
from backend.enrichment.trim_diff_engine import apply_trim_diffs_to_generated_ladders  # noqa: E402

from backend.enrichment.dictionary_paths import trim_spec_sheets_dir

_SHEETS_DIR = trim_spec_sheets_dir()
_SKIP_FILES = frozenset({"jeep_grand_cherokee_wk2.json"})

#: Resumable-run checkpoint. One entry per brochure transcript, keyed by its
#: filename and carrying the sha256 of the transcript that produced the entry —
#: so a transcript that has since been re-captured is re-run rather than skipped.
PROGRESS_PATH = _REPO / "workspace" / "extraction_progress.json"
PROGRESS_SCHEMA = 1
#: Flush the checkpoint this often. Small enough that a kill loses seconds of
#: work, large enough that the run is not dominated by fsync.
CHECKPOINT_EVERY = 100


def _all_ladders() -> list[dict]:
    seen: set[str] = set()
    merged: list[dict] = []
    for path in (_LADDERS_JSON, _LADDERS_GENERATED_JSON):
        for ladder in _read_ladders_file(path):
            if not isinstance(ladder, dict):
                continue
            lid = str(ladder.get("id") or "").strip()
            if not lid or lid in seen:
                continue
            seen.add(lid)
            merged.append(ladder)
    return merged


def _iter_brochure_text_payloads():
    from backend.enrichment.dictionary_paths import BROCHURE_TEXT_DIR

    for path in sorted(BROCHURE_TEXT_DIR.glob("*.json")):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            yield path, None
            continue
        yield path, data if isinstance(data, dict) else None


# --- resumable-run checkpoint --------------------------------------------


def _digest(path: Path) -> str:
    """sha256 of a transcript file, so a re-captured transcript re-runs."""
    h = hashlib.sha256()
    try:
        with path.open("rb") as fh:
            for chunk in iter(lambda: fh.read(1 << 20), b""):
                h.update(chunk)
    except OSError:
        return ""
    return h.hexdigest()


def load_progress(path: Path = PROGRESS_PATH) -> dict[str, Any]:
    """Read the checkpoint, or a blank one. A corrupt file restarts the run.

    A half-written checkpoint means the previous process died mid-flush; the
    completed OVERLAYS are still on disk and still stamped, so the cost of
    restarting the scan is seconds, and reading a truncated file as if it were
    authoritative would silently skip work that was never done.
    """
    blank: dict[str, Any] = {"schema_version": PROGRESS_SCHEMA, "done": {}, "runs": []}
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return blank
    if not isinstance(loaded, dict) or loaded.get("schema_version") != PROGRESS_SCHEMA:
        return blank
    if not isinstance(loaded.get("done"), dict):
        return blank
    loaded.setdefault("runs", [])
    return loaded


def save_progress(progress: dict[str, Any], path: Path = PROGRESS_PATH) -> None:
    """Atomically replace the checkpoint. Never leaves a truncated file behind."""
    path.parent.mkdir(parents=True, exist_ok=True)
    progress["updated_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix=path.name, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(progress, fh, indent=1, ensure_ascii=False)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise


def _already_done(progress: dict[str, Any], path: Path, digest: str) -> bool:
    entry = progress.get("done", {}).get(path.name)
    return isinstance(entry, dict) and entry.get("digest") == digest and bool(digest)


# --- the document must still be in our hands ------------------------------

#: Reasons a transcript is refused BEFORE any bullet is quoted out of it.
SKIP_NO_YMM = "transcript_missing_year_make_model"
SKIP_UNREADABLE = "transcript_json_unreadable"
SKIP_PDF_NOT_HELD = "source_pdf_not_held"
SKIP_NO_SHA = "source_pdf_sha256_not_recorded"
SKIP_PDF_CHANGED = "source_pdf_sha256_mismatch"
SKIP_NO_WALK = "no_usable_trim_walk"
SKIP_PRIOR_ADMISSIBLE = "existing_overlay_already_admissible"
SKIP_WOULD_REGRESS = "would_replace_a_serving_overlay"
WROTE = "wrote_quoted_overlay"


def document_is_held(data: dict[str, Any]) -> str | None:
    """``None`` when the cited PDF is on disk and still hashes as recorded.

    A citation names a document so a reader can re-open it. If we no longer hold
    that document, the citation cannot be checked by anyone — not by
    ``verify_trim_citations.py``, not by a reviewer — so quoting out of the
    transcript would produce a bullet that is unverifiable by construction and
    can only ever be stamped ``verified: false``.

    Measured 2026-08-02: 2,633 of the 2,815 transcripts in
    ``derived/brochure_text`` name a PDF that is not at its recorded path and is
    not findable by filename under ``brochures/`` or ``brochure_archive/``. Those
    are refused here rather than extracted and then failed one bullet at a time.
    """
    from backend.scripts.verify_trim_citations import _find_pdf, _sha256

    pdf = _find_pdf(str(data.get("source_pdf") or "") or str(data.get("source_pdf_name") or ""))
    if pdf is None:
        return SKIP_PDF_NOT_HELD
    recorded = str(data.get("source_pdf_sha256") or "").strip()
    if not recorded:
        return SKIP_NO_SHA
    if _sha256(pdf) != recorded:
        return SKIP_PDF_CHANGED
    return None


def _scan_brochure_corpus() -> tuple[list, Counter, Counter]:
    """Run the quoted trim-walk extractor over every derived brochure_text file."""
    from backend.enrichment.brochure_trim_candidates import (
        classify_brochure_failure,
        extract_trim_walk,
    )

    usable: list = []
    failures: Counter = Counter()
    layouts: Counter = Counter()
    for path, data in _iter_brochure_text_payloads():
        if data is None:
            failures["brochure_text_json_unreadable"] += 1
            continue
        make = str(data.get("make") or "")
        model = str(data.get("model") or "")
        try:
            year = int(data.get("year") or 0)
        except (TypeError, ValueError):
            year = 0
        if not make or not model or not year:
            failures["brochure_text_missing_ymm"] += 1
            continue
        extract = extract_trim_walk(data, make=make, model=model, year=year)
        if extract.usable:
            usable.append((path, data, extract))
            layouts["+".join(extract.layouts) or "unknown"] += 1
        else:
            failures[classify_brochure_failure(data, extract, make=make, model=model)] += 1
    return usable, failures, layouts


def _report_brochure_coverage() -> int:
    usable, failures, layouts = _scan_brochure_corpus()
    total = len(usable) + sum(failures.values())
    trims = sum(len(e.adds_by_trim) for _, _, e in usable)
    bullets = sum(len(v) for _, _, e in usable for v in e.adds_by_trim.values())
    gate = sum(e.gate_rejected for _, _, e in usable)
    print(f"brochures scanned          : {total}")
    print(f"brochures yielding a walk  : {len(usable)} ({100.0 * len(usable) / max(1, total):.1f}%)")
    print(f"trims with quoted adds     : {trims}")
    print(f"quoted bullets             : {bullets}")
    print(f"bullets dropped by the display gate: {gate}")
    print("\nlayouts that parsed:")
    for name, n in layouts.most_common():
        print(f"  {n:6d}  {name}")
    print("\nfailure clusters:")
    for name, n in failures.most_common():
        print(f"  {n:6d}  {name}")
    return 0


def _overlay_payload(data: dict[str, Any], extract: Any, prior: dict) -> dict[str, Any]:
    """Build the overlay body for one extract. Pure — writes nothing."""
    quoted_trims = [t for t in extract.trims_available if extract.adds_by_trim.get(t)]
    # trims_available is the lineup THIS brochure documents, nothing more.
    # Folding the previous overlay's trim list back in was tried and
    # reverted: ``resolve_trim_ladder`` fuzzy-matches step names to overlay
    # keys, so carrying both "SEL" and "IONIQ 5 SEL", or "GT" alongside
    # "GT PLUS", produced duplicate rungs and moved the GT PLUS bullets onto
    # the GT rung.
    lineup = list(quoted_trims)
    return {
            "catalog_key": extract.catalog_key,
            "year": extract.year,
            "make": extract.make,
            "model": extract.model,
            "source": "brochure_text_quoted",
            "layouts": extract.layouts,
            "trims_available": lineup,
            "trims_quoted": quoted_trims,
            # The ORDER, and the OEM sentence behind each rung's place in it.
            # ``extract.trims_available`` is the whole printed lineup, not the
            # subset that yielded bullets: a rung with no quotable bullet still
            # sits somewhere the document states, and dropping it would leave a
            # hole in the chain of "<TRIM> adds to <LOWER>" edges. The narrower
            # ``trims_available`` above stays as it is — it feeds
            # ``resolve_trim_ladder``'s fuzzy step matching, which duplicates
            # rungs when handed near-identical names.
            #
            # These three keys are exactly what ``brochure_extract.attach_rung_order``
            # re-derives at read time when they are absent. Writing them here
            # means the read path does not have to re-run the extractor per
            # process; it does not create a new claim.
            "rung_order": list(extract.trims_available),
            "order_basis": extract.order_basis,
            "rung_edges": extract.rung_edges,
            "adds_by_trim": extract.adds_by_trim,
            "adds_provenance": extract.provenance,
            "source_brochure_text": f"derived/brochure_text/"
            f"{extract.catalog_key.replace('|', '__')}.json",
            "pages_used": extract.pages_used,
            "replaced_source": str(prior.get("source") or "") or None,
            # The document this overlay's citations point at, recorded so a
            # reviewer can re-open it without going back through the transcript.
            "source_pdf_name": str(data.get("source_pdf_name") or "") or None,
            "source_pdf_sha256": str(data.get("source_pdf_sha256") or "") or None,
        }


def _iter_transcripts() -> Iterator[tuple[Path, dict[str, Any] | None]]:
    from backend.enrichment.dictionary_paths import BROCHURE_TEXT_DIR

    for path in sorted(BROCHURE_TEXT_DIR.glob("*.json")):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            yield path, None
            continue
        yield path, data if isinstance(data, dict) else None


def _write_quoted_overlays(
    dry_run: bool = False,
    *,
    restart: bool = False,
    checkpoint_every: int = CHECKPOINT_EVERY,
    limit: int | None = None,
    progress_path: Path = PROGRESS_PATH,
) -> int:
    """Quote a page-cited overlay out of every brochure transcript we can still check.

    RESUMABLE. Every transcript that has been processed is recorded in
    ``workspace/extraction_progress.json`` along with the sha256 of the
    transcript that produced the record, and the checkpoint is flushed every
    ``checkpoint_every`` files. A run that is killed mid-flight loses at most
    that many files' worth of scanning; re-running skips everything already
    recorded, and re-runs any transcript whose bytes have changed since.

    The overlays themselves are written one at a time as they are produced, so
    the work that survives a kill is the work on disk, not a batch held in
    memory until the end.
    """
    from backend.enrichment.brochure_extract import (
        brochure_overlay_is_usable,
        overlay_source_admissible,
    )
    from backend.enrichment.brochure_trim_candidates import extract_trim_walk
    from backend.enrichment.dictionary_paths import TRIM_ADDS_BY_YEAR_DIR

    TRIM_ADDS_BY_YEAR_DIR.mkdir(parents=True, exist_ok=True)
    progress = {"schema_version": PROGRESS_SCHEMA, "done": {}, "runs": []} if restart else load_progress(progress_path)
    done: dict[str, Any] = progress.setdefault("done", {})
    progress.setdefault("started_at", datetime.now(timezone.utc).isoformat(timespec="seconds"))

    outcomes: Counter = Counter()
    resumed = 0
    processed = 0
    started = time.time()

    def flush() -> None:
        if dry_run:
            return
        progress["totals"] = dict(Counter(
            str((e or {}).get("outcome")) for e in done.values() if isinstance(e, dict)
        ))
        save_progress(progress, progress_path)

    try:
        for path, data in _iter_transcripts():
            digest = _digest(path)
            if _already_done(progress, path, digest):
                resumed += 1
                continue
            if limit is not None and processed >= limit:
                break
            processed += 1
            record: dict[str, Any] = {"digest": digest}

            if data is None:
                record["outcome"] = SKIP_UNREADABLE
                done[path.name] = record
                outcomes[SKIP_UNREADABLE] += 1
                continue

            make = str(data.get("make") or "")
            model = str(data.get("model") or "")
            try:
                year = int(data.get("year") or 0)
            except (TypeError, ValueError):
                year = 0
            if not (make and model and year):
                record["outcome"] = SKIP_NO_YMM
                done[path.name] = record
                outcomes[SKIP_NO_YMM] += 1
                continue

            # FAIL CLOSED, before a single bullet is quoted: if the PDF this
            # transcript was made from is not in our hands, no citation out of
            # it can ever be checked against the document.
            held = document_is_held(data)
            if held is not None:
                record["outcome"] = held
                done[path.name] = record
                outcomes[held] += 1
                continue

            extract = extract_trim_walk(data, make=make, model=model, year=year)
            if not extract.usable:
                record["outcome"] = SKIP_NO_WALK
                record["reject_reasons"] = sorted(set(extract.reject_reasons))[:6]
                done[path.name] = record
                outcomes[SKIP_NO_WALK] += 1
                continue

            out_path = TRIM_ADDS_BY_YEAR_DIR / f"{extract.catalog_key.replace('|', '__')}.json"
            prior: dict = {}
            if out_path.is_file():
                try:
                    loaded = json.loads(out_path.read_text(encoding="utf-8"))
                    prior = loaded if isinstance(loaded, dict) else {}
                except (OSError, json.JSONDecodeError):
                    prior = {}

            prior_source = str(prior.get("source") or "").strip().lower()
            if (
                prior_source
                and prior_source != "brochure_text_quoted"
                and overlay_source_admissible(prior_source)
            ):
                # Only an overlay the reader would actually serve is worth
                # keeping. The previous rule protected anything whose source
                # started "manual" or "curated", which meant the 123
                # ``manual_brochure_review`` files were kept in place — and the
                # provenance gate refuses every one of them, so the effect was
                # to keep a rung blank rather than let a page-cited overlay
                # fill it.
                record["outcome"] = SKIP_PRIOR_ADMISSIBLE
                done[path.name] = record
                outcomes[SKIP_PRIOR_ADMISSIBLE] += 1
                continue

            payload = _overlay_payload(data, extract, prior)
            if prior and brochure_overlay_is_usable(prior) and not brochure_overlay_is_usable(payload):
                # Never trade a serving overlay for one the reader will refuse.
                record["outcome"] = SKIP_WOULD_REGRESS
                done[path.name] = record
                outcomes[SKIP_WOULD_REGRESS] += 1
                continue

            if not dry_run:
                out_path.write_text(
                    json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8"
                )
            record["outcome"] = WROTE
            record["overlay"] = out_path.name
            record["trims"] = len(extract.adds_by_trim)
            record["bullets"] = sum(len(v) for v in extract.adds_by_trim.values())
            record["replaced_source"] = str(prior.get("source") or "") or None
            done[path.name] = record
            outcomes[WROTE] += 1

            if processed % checkpoint_every == 0:
                flush()
    finally:
        # Whatever happened — clean exit, KeyboardInterrupt, or a parser blowing
        # up on one transcript — the files already processed are recorded.
        flush()

    if not dry_run:
        progress.setdefault("runs", []).append(
            {
                "finished_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "processed": processed,
                "resumed_skipped": resumed,
                "seconds": round(time.time() - started, 1),
                "outcomes": dict(outcomes),
            }
        )
        save_progress(progress, progress_path)

    verb = "would write" if dry_run else "wrote"
    print(f"transcripts skipped (already in checkpoint) : {resumed}")
    print(f"transcripts processed this run              : {processed}")
    print(f"{verb} quoted overlays                        : {outcomes[WROTE]}")
    print("\noutcome by reason:")
    for name, n in outcomes.most_common():
        print(f"  {n:6d}  {name}")
    if not dry_run:
        print(f"\ncheckpoint: {progress_path}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Build trim spec sheets from ladder definitions.")
    parser.add_argument(
        "--write-adds",
        action="store_true",
        help="After building sheets, compute adds deltas into trim_ladders_generated.json",
    )
    parser.add_argument(
        "--brochure-coverage",
        action="store_true",
        help="Report quoted trim-walk coverage and failure clusters over derived/brochure_text",
    )
    parser.add_argument(
        "--write-quoted-overlays",
        action="store_true",
        help="Write page-cited overlays into derived/trim_adds_by_year for every brochure that parses",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="With --write-quoted-overlays, report the count without touching any file",
    )
    parser.add_argument(
        "--restart",
        action="store_true",
        help="Ignore workspace/extraction_progress.json and re-process every transcript",
    )
    parser.add_argument(
        "--checkpoint-every",
        type=int,
        default=CHECKPOINT_EVERY,
        help=f"Flush the checkpoint every N transcripts (default {CHECKPOINT_EVERY})",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Process at most N not-yet-checkpointed transcripts, then stop (for testing resume)",
    )
    args = parser.parse_args()

    if args.brochure_coverage:
        return _report_brochure_coverage()
    if args.write_quoted_overlays:
        return _write_quoted_overlays(
            dry_run=args.dry_run,
            restart=args.restart,
            checkpoint_every=max(1, args.checkpoint_every),
            limit=args.limit,
        )

    _SHEETS_DIR.mkdir(parents=True, exist_ok=True)
    extract_trim_specs.cache_clear()

    written = 0
    skipped = 0
    empty = 0

    for ladder in _all_ladders():
        lid = str(ladder.get("id") or "").strip()
        if not lid:
            continue
        out_path = _SHEETS_DIR / f"{lid}.json"
        if out_path.name in _SKIP_FILES:
            skipped += 1
            continue

        sheet = build_spec_sheet(ladder)
        if not sheet:
            empty += 1
            continue

        out_path.write_text(json.dumps(sheet, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        written += 1

    print(f"Wrote {written} trim spec sheets to {_SHEETS_DIR} ({skipped} hand-curated skipped, {empty} ladders had insufficient specs)")

    if args.write_adds:
        stats = apply_trim_diffs_to_generated_ladders(generated_path=_LADDERS_GENERATED_JSON, sheets_dir=_SHEETS_DIR)
        print(
            "Updated trim_ladders_generated.json: "
            f"ladders_updated={stats['ladders_updated']} steps_updated={stats['steps_updated']} "
            f"skipped_no_sheet={stats['skipped_no_sheet']}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
