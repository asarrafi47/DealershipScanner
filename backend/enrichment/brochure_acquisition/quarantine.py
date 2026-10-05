"""
Destructive corpus maintenance: --quarantine-unidentified / --quarantine-derived.

Moves files (never deletes); dry run unless ``apply``.

Moved verbatim out of ``backend/scripts/fetch_oem_brochures.py`` (audit
datascripts.md F7); that script is still the CLI and re-exports these names.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from backend.enrichment.brochure_acquisition.corpus import (
    corpus_page_texts,
)
from backend.enrichment.brochure_acquisition.paths import (
    ARTIFACT_DIR,
)
from backend.enrichment.brochure_sources import (
    BROCHURE_TEXT_QUARANTINE_DIR,
    DERIVED_STORES_FROM_BROCHURE_TEXT,
    apply_derived_quarantine,
    assess_nameplate_dominance,
    assess_text_quality,
    plan_derived_quarantine,
    verify_document_identity,
)
from backend.enrichment.dictionary_paths import (
    BROCHURE_TEXT_DIR,
)

logger = logging.getLogger(__name__)


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
