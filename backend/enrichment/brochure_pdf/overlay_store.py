"""Per-brochure extraction result, overlay/facts persistence, overlay load.

Split out of ``brochure_extract`` (P17C.4); ``brochure_extract`` re-exports every name.
"""

from __future__ import annotations

import json
import logging
from functools import lru_cache
from pathlib import Path
from typing import Any

from backend.enrichment.brochure_pdf.archive import (
    BrochureExtractResult,
    BrochureYMM,
    archive_source_pdf,
    parse_brochure_filename,
)
from backend.enrichment.brochure_pdf.matrix_parse import (
    _canonical_trim_name,
    _compute_adds_over_lower,
    _extract_equipment_matrix,
    _find_trim_sections,
    _ladder_trim_order,
    _map_extracted_to_ladder,
    _split_features,
)
from backend.enrichment.brochure_pdf.pdf_text import _extract_pdf_text
from backend.enrichment.dictionary_catalog import catalog_key
from backend.enrichment.dictionary_paths import (
    BROCHURE_FACTS_DIR,
    BROCHURE_TEXT_DIR,
    BROCHURES_DIR,
    CANONICAL_CSV_COLUMNS,
    OPTIONS_RAW_DIR,
    TRIM_ADDS_BY_YEAR_DIR,
)
from backend.enrichment.provenance.ladder_stores import (
    ORDER_BEARING_OVERLAY_SOURCES,
    apply_overlay_provenance_gate,
    brochure_overlay_is_usable,
    trim_adds_provenance_required,
)

__all__ = [
    "_brochure_text_file",
    "_facts_path",
    "_overlay_path",
    "_read_brochure_overlay_file",
    "_rung_order_from_brochure_text",
    "_write_marketing_trim_csv_rows",
    "attach_rung_order",
    "backfill_overlay_rung_order",
    "extract_brochure_pdf",
    "iter_brochure_pdfs",
    "load_brochure_trim_overlay",
    "persist_brochure_extract",
    "process_brochure_file",
]

logger = logging.getLogger(__name__)


def extract_brochure_pdf(path: Path) -> BrochureExtractResult | None:
    ymm = parse_brochure_filename(path)
    if not ymm:
        return None

    text, pages = _extract_pdf_text(path)
    warnings: list[str] = []
    if not text.strip():
        warnings.append("no_text_extracted")
        return BrochureExtractResult(
            ymm=ymm,
            source_pdf=str(path),
            pages_used=pages,
            warnings=warnings,
        )

    ladder_id, ladder_order = _ladder_trim_order(ymm)
    allowed = set(ladder_order) if ladder_order else None
    sections = _find_trim_sections(
        text, allowed_trims=allowed, make=ymm.make, model=ymm.model
    )
    if not sections and ladder_order:
        sections = _find_trim_sections(
            text, allowed_trims=None, make=ymm.make, model=ymm.model
        )
    extracted: dict[str, list[str]] = {}
    for i, (start, trim_name, _header) in enumerate(sections):
        end = sections[i + 1][0] if i + 1 < len(sections) else len(text)
        block = text[start:end]
        feats = _split_features(block)
        if feats:
            canon = _canonical_trim_name(trim_name, make=ymm.make, model=ymm.model)
            if canon in extracted:
                seen = {f.lower() for f in extracted[canon]}
                for f in feats:
                    if f.lower() not in seen:
                        extracted[canon].append(f)
                        seen.add(f.lower())
            else:
                extracted[canon] = feats

    matrix_column_order: list[str] = []
    if not extracted:
        matrix_extracted, matrix_pages, matrix_column_order = _extract_equipment_matrix(
            path, ymm, ladder_order=ladder_order
        )
        if matrix_extracted:
            extracted = matrix_extracted
            pages = sorted(set(pages) | set(matrix_pages))
            warnings.append("extracted_via_equipment_matrix")

    if ladder_order:
        # Base → top for delta math.
        ladder_base_top = list(reversed(ladder_order))
        mapped = _map_extracted_to_ladder(extracted, ladder_base_top, make=ymm.make, model=ymm.model)
        trims_available = [t for t in ladder_base_top if mapped.get(t)]
        if len(trims_available) < 2 and matrix_column_order:
            ladder_base_top = matrix_column_order
            mapped = extracted
            trims_available = [t for t in matrix_column_order if mapped.get(t)]
        elif len(trims_available) < 2:
            ladder_base_top = list(extracted.keys())
            mapped = extracted
            trims_available = ladder_base_top
        adds_by_trim = _compute_adds_over_lower(ladder_base_top, mapped)
        trims_available = [
            t for t in trims_available if adds_by_trim.get(t) or mapped.get(t)
        ]
    else:
        ladder_id = None
        trims_available = list(extracted.keys())
        order = trims_available
        adds_by_trim = _compute_adds_over_lower(order, extracted)
        mapped = extracted

    if not mapped:
        warnings.append("no_trim_sections_found")
    elif "extracted_via_equipment_matrix" in warnings and not adds_by_trim:
        warnings.append("matrix_found_no_adds_delta")

    return BrochureExtractResult(
        ymm=ymm,
        ladder_id=ladder_id,
        trims_available=trims_available,
        adds_by_trim={k: v for k, v in adds_by_trim.items() if v},
        standard_by_trim=mapped,
        source_pdf=str(path),
        pages_used=pages,
        warnings=warnings,
    )


def _overlay_path(ymm: BrochureYMM) -> Path:
    safe = ymm.catalog_key.replace("|", "__")
    return TRIM_ADDS_BY_YEAR_DIR / f"{safe}.json"


def _facts_path(ymm: BrochureYMM) -> Path:
    make_dir = BROCHURE_FACTS_DIR / ymm.make.replace(" ", "_")
    slug = ymm.model.replace(" ", "_").lower()
    return make_dir / f"{ymm.year}_{slug}.json"


def persist_brochure_extract(result: BrochureExtractResult) -> Path | None:
    """Write brochure_facts + trim_adds_by_year JSON. Returns overlay path if written."""
    if not result.adds_by_trim and not result.standard_by_trim:
        return None

    TRIM_ADDS_BY_YEAR_DIR.mkdir(parents=True, exist_ok=True)
    BROCHURE_FACTS_DIR.mkdir(parents=True, exist_ok=True)

    overlay = {
        "catalog_key": result.ymm.catalog_key,
        "year": result.ymm.year,
        "make": result.ymm.make,
        "model": result.ymm.model,
        "ladder_id": result.ladder_id,
        "trims_available": result.trims_available,
        "adds_by_trim": result.adds_by_trim,
        "source_pdf": result.source_pdf,
        "pages_used": result.pages_used,
        "warnings": result.warnings,
    }
    op = _overlay_path(result.ymm)
    op.write_text(json.dumps(overlay, indent=2), encoding="utf-8")

    facts = {
        "catalog_key": result.ymm.catalog_key,
        "standard_by_trim": result.standard_by_trim,
        "adds_by_trim": result.adds_by_trim,
        "warnings": result.warnings,
    }
    fp = _facts_path(result.ymm)
    fp.parent.mkdir(parents=True, exist_ok=True)
    fp.write_text(json.dumps(facts, indent=2), encoding="utf-8")

    _write_marketing_trim_csv_rows(result)
    return op


def _write_marketing_trim_csv_rows(result: BrochureExtractResult) -> None:
    """Append/update marketing trim rows in Complete_Options for dictionary_adds."""
    import csv

    ymm = result.ymm
    make_dir = OPTIONS_RAW_DIR / ymm.make.replace(" ", "_")
    make_dir.mkdir(parents=True, exist_ok=True)
    slug = ymm.model.replace(" ", "_")
    csv_path = make_dir / f"{ymm.year}_{ymm.make}_{slug}_Complete_Options.csv"

    fieldnames = list(CANONICAL_CSV_COLUMNS)
    rows: list[dict[str, str]] = []
    if csv_path.is_file():
        with csv_path.open(encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            fieldnames = list(reader.fieldnames or fieldnames)
            rows = list(reader)

    # Remove prior brochure marketing rows for this YMM.
    rows = [
        r
        for r in rows
        if not (r.get("Trim") or "").strip().startswith("[Brochure")
        and (r.get("Trim") or "").strip() not in result.adds_by_trim
    ]

    existing_trims = {(r.get("Trim") or "").strip() for r in rows}
    for trim_name, adds in result.adds_by_trim.items():
        if not adds:
            continue
        opt = "; ".join(adds)
        detail = " | ".join(f"[Brochure] {a}" for a in adds)
        if trim_name in existing_trims:
            for row in rows:
                if (row.get("Trim") or "").strip() == trim_name:
                    row["Options"] = opt
                    row["optionDetails"] = detail
                    row["Year"] = str(ymm.year)
                    row["Make"] = ymm.make
                    row["Model"] = ymm.model
            continue
        row = {col: "" for col in fieldnames}
        row["Year"] = str(ymm.year)
        row["Make"] = ymm.make
        row["Model"] = ymm.model
        row["Trim"] = trim_name
        row["Options"] = opt
        row["optionDetails"] = detail
        rows.append(row)

    with csv_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _brochure_text_file(catalog_key_value: str) -> Path:
    return BROCHURE_TEXT_DIR / f"{str(catalog_key_value).replace('|', '__')}.json"


@lru_cache(maxsize=512)
def _rung_order_from_brochure_text(
    catalog_key_value: str, make: str, model: str, year: int
) -> tuple[tuple[str, ...], str]:
    """Re-derive (order, order_basis JSON) straight from the brochure text file.

    The ordering evidence lives in the document, not in the overlay: the overlay
    is only a cache of it. This re-reads the document so a rebuilt overlay that
    lost the fields (see :func:`backfill_overlay_rung_order`) silently regains
    them instead of silently falling back to the hand-typed rank table.

    Returns ``((), "{}")`` when the document yields no order. JSON is returned
    rather than a dict so the result stays hashable/immutable under the cache.
    """
    path = _brochure_text_file(catalog_key_value)
    if not path.is_file():
        return (), "{}"
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return (), "{}"
    if not isinstance(data, dict):
        return (), "{}"
    from backend.enrichment.brochure_trim_candidates import extract_trim_walk

    try:
        extract = extract_trim_walk(data, make=make, model=model, year=year)
    except Exception:  # a malformed brochure text file must not break a VDP
        logger.exception("rung order re-derivation failed for %s", catalog_key_value)
        return (), "{}"
    if not extract.order_basis:
        return (), "{}"
    return tuple(extract.trims_available), json.dumps(extract.order_basis)


def attach_rung_order(overlay: dict[str, Any]) -> dict[str, Any]:
    """Fill ``rung_order``/``order_basis``/``rung_edges`` from the cited document.

    A no-op when the overlay already carries them, or when its source is not one
    that records ordering evidence.
    """
    if not isinstance(overlay, dict):
        return overlay
    if str(overlay.get("source") or "").strip().lower() not in ORDER_BEARING_OVERLAY_SOURCES:
        return overlay
    if isinstance(overlay.get("order_basis"), dict) and overlay.get("order_basis"):
        return overlay
    ck = str(overlay.get("catalog_key") or "")
    try:
        year = int(overlay.get("year") or 0)
    except (TypeError, ValueError):
        return overlay
    if not ck or not year:
        return overlay
    # Fallback branch: the overlay lost its order_basis. The image fences
    # derived/brochure_text/ out (P9.7), so flag it loudly; the gate test in
    # test_brochure_text_runtime_gate.py keeps this from firing for shipped data.
    logger.warning("overlay %s has no order_basis; re-deriving from brochure_text", ck)
    order, basis_json = _rung_order_from_brochure_text(
        ck, str(overlay.get("make") or ""), str(overlay.get("model") or ""), year
    )
    if not order:
        return overlay
    basis = json.loads(basis_json)
    return {
        **overlay,
        "rung_order": list(order),
        "order_basis": basis,
        "rung_edges": [
            entry["edge"]
            for trim in order
            for entry in [basis.get(trim) or {}]
            if isinstance(entry.get("edge"), dict)
            and str(entry["edge"].get("trim") or "") == trim
        ],
    }


def backfill_overlay_rung_order(*, dry_run: bool = False) -> dict[str, int]:
    """Write the document-derived rung order into every overlay that can hold one.

    Data build step, run the same way ``build_trim_spec_sheets.py`` is: the
    render path does not depend on it (``attach_rung_order`` re-derives on read
    when the fields are missing), it just saves that work per process.

    Bullets, provenance and every other field are left exactly as they were.
    """
    counts = {"scanned": 0, "written": 0, "no_order_in_document": 0, "wrong_source": 0}
    if not TRIM_ADDS_BY_YEAR_DIR.is_dir():
        return counts
    for path in sorted(TRIM_ADDS_BY_YEAR_DIR.glob("*.json")):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(data, dict):
            continue
        counts["scanned"] += 1
        if str(data.get("source") or "").strip().lower() not in ORDER_BEARING_OVERLAY_SOURCES:
            counts["wrong_source"] += 1
            continue
        stripped = {k: v for k, v in data.items() if k not in ("rung_order", "order_basis", "rung_edges")}
        filled = attach_rung_order(stripped)
        if not filled.get("order_basis"):
            counts["no_order_in_document"] += 1
            continue
        if not dry_run:
            path.write_text(
                json.dumps(filled, indent=2, ensure_ascii=False), encoding="utf-8"
            )
        counts["written"] += 1
    return counts


def _read_brochure_overlay_file(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(data, dict) or not data.get("adds_by_trim"):
        return None
    if not brochure_overlay_is_usable(data):
        return None
    return attach_rung_order(data)


def load_brochure_trim_overlay(year: Any, make: str, model: str) -> dict[str, Any] | None:
    try:
        y = int(year)
    except (TypeError, ValueError):
        return None
    for delta in (0, -1, 1, -2, 2):
        candidate = y + delta
        if candidate < 2010:
            continue
        path = TRIM_ADDS_BY_YEAR_DIR / f"{catalog_key(candidate, make, model).replace('|', '__')}.json"
        data = _read_brochure_overlay_file(path)
        if data:
            if trim_adds_provenance_required():
                # Enrichment fills empty rungs from a NEIGHBOUR YEAR's overlay or
                # from a hard-coded table, so the bullets it adds are not the ones
                # this overlay cites — they would enter uncited. Skipped while the
                # gate is on.
                #
                # ``for_year=y`` is the car's year, not ``candidate``. When this
                # loop had to walk out to a neighbouring model year, the file it
                # found is still useful for its trim LIST but is not evidence
                # about this car, so every bullet in it drops here.
                return apply_overlay_provenance_gate(data, for_year=y)
            from backend.enrichment.brochure_overlay_backfill import enrich_brochure_overlay

            return enrich_brochure_overlay(data)
    return None


def process_brochure_file(
    path: Path,
    *,
    delete_after: bool = False,
    dry_run: bool = False,
) -> BrochureExtractResult | None:
    result = extract_brochure_pdf(path)
    if not result:
        logger.warning("skip unparseable filename: %s", path.name)
        return None
    if dry_run:
        return result
    overlay = persist_brochure_extract(result)
    if not overlay:
        logger.warning("no adds persisted for %s", path.name)
        return result
    if delete_after:
        # Never drop the staging copy until the document is in the archive: the
        # derived text is not a substitute for the PDF, and losing the PDF is
        # what made this corpus unre-parseable.
        archived = archive_source_pdf(path, ymm=result.ymm)
        if not archived:
            logger.error("keeping %s: could not archive it first", path)
            result.warnings.append("delete_skipped_not_archived")
            return result
        try:
            path.unlink()
        except OSError as exc:
            logger.error("failed to delete %s: %s", path, exc)
            result.warnings.append(f"delete_failed:{exc}")
    return result


def iter_brochure_pdfs(directory: Path | None = None) -> list[Path]:
    root = directory or BROCHURES_DIR
    if not root.is_dir():
        return []
    return sorted(root.glob("*.pdf"), key=lambda p: p.name)

