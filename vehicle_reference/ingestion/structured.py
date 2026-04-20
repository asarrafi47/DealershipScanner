"""Import vehicles from structured CSV or JSON (requires SourceManifest)."""
from __future__ import annotations

import csv
import json
import sqlite3
from pathlib import Path
from typing import Any

from vehicle_reference.ingestion.bundle import ingest_vehicle_bundle
from vehicle_reference.ingestion.manifest import ManifestError, SourceManifest, require_vehicle_source


def load_manifest(path: Path) -> SourceManifest:
    raw = json.loads(path.read_text(encoding="utf-8"))
    return SourceManifest.from_dict(raw)


def ingest_json_document(conn: sqlite3.Connection, path: Path, *, brand_code: str) -> int:
    """
    JSON shape:
      { "source_manifest": {...}, "vehicles": [ { ... vehicle bundle ... }, ... ] }
    File-level manifest constrains model years; each vehicle still requires its own
    `source` object (label + url or local_document_path) per project policy.
    """
    doc = json.loads(path.read_text(encoding="utf-8"))
    m = SourceManifest.from_dict(doc["source_manifest"])
    n = 0
    for v in doc.get("vehicles", []):
        my = int(v["model_year"])
        m.assert_row_year_in_range(my)
        if not v.get("source"):
            v = dict(v)
            v["source"] = _manifest_source_dict(m)
        else:
            require_vehicle_source(v)
        ingest_vehicle_bundle(conn, v, brand_code=brand_code)
        n += 1
    return n


def _manifest_source_dict(m: SourceManifest) -> dict[str, Any]:
    return {
        "label": m.source_label,
        "url": m.source_url_or_path,
        "source_group_key": "structured_file_ingest",
        "notes": m.notes,
    }


def ingest_csv_document(
    conn: sqlite3.Connection,
    csv_path: Path,
    manifest: SourceManifest,
    *,
    brand_code: str,
    column_map: dict[str, str],
) -> int:
    """
    column_map: logical_field -> csv column header name.
    Required logical fields: model_year, series_name, variant_name (or trim), body_style,
    engine, transmission, drivetrain, fuel_type, mpg_text (optional empty),
    passenger_seating (optional).
    Provenance: file-level manifest is applied as each row's `source` (same document).
    """
    required_manifest_fields = (
        "model_year",
        "series_name",
    )
    for k in required_manifest_fields:
        if k not in column_map:
            raise ManifestError(f"column_map missing required key {k!r}")

    src = _manifest_source_dict(manifest)
    text = csv_path.read_text(encoding="utf-8-sig")
    lines = text.splitlines()
    if not lines:
        return 0
    reader = csv.DictReader(lines)
    if reader.fieldnames is None:
        raise ManifestError("CSV has no header row")

    def col(logical: str) -> str | None:
        h = column_map.get(logical)
        if not h:
            return None
        return h

    n = 0
    for row in reader:
        def pick(logical: str) -> str | None:
            h = col(logical)
            if not h:
                return None
            v = (row.get(h) or "").strip()
            return v or None

        try:
            my = int(pick("model_year") or "")
        except ValueError as e:
            raise ManifestError(f"Invalid model_year in row {row!r}") from e
        manifest.assert_row_year_in_range(my)

        trim_col = column_map.get("trim_line")
        trim_val = (row.get(trim_col) or "").strip() if trim_col else None
        trim_val = trim_val or None

        variant = pick("variant_name") or pick("trim")

        v: dict[str, Any] = {
            "model_year": my,
            "market": pick("market") or manifest.market,
            "series_name": pick("series_name") or "",
            "variant_name": variant,
            "trim_line": trim_val,
            "body_style": pick("body_style"),
            "engine": pick("engine"),
            "transmission": pick("transmission"),
            "drivetrain": pick("drivetrain"),
            "fuel_type": pick("fuel_type"),
            "mpg_text": pick("mpg_text") or pick("mpg"),
            "passenger_seating": pick("passenger_seating"),
            "uncertainty_notes": pick("uncertainty_notes"),
            "source": dict(src),
            "internal_notes": pick("internal_notes"),
            "packages": [],
            "exterior_colors": [],
            "interior_colors": [],
        }
        ext_src = pick("external_source")
        ext_id = pick("external_record_id")
        if ext_src and ext_id:
            v["external_source"] = ext_src
            v["external_record_id"] = ext_id

        if not v["series_name"]:
            raise ManifestError(f"Missing series_name in row {row!r}")

        # Row-level provenance columns (optional)
        sl = pick("source_label")
        su = pick("source_url")
        if sl and su:
            v["source"] = {"label": sl, "url": su, "notes": pick("source_notes") or manifest.notes}

        ingest_vehicle_bundle(conn, v, brand_code=brand_code)
        n += 1
    return n


def ingest_csv_with_manifest_path(
    conn: sqlite3.Connection, csv_path: Path, manifest_path: Path, *, brand_code: str
) -> int:
    m = load_manifest(manifest_path)
    extra = json.loads(manifest_path.read_text(encoding="utf-8"))
    cm = extra.get("column_map")
    if not isinstance(cm, dict) or not cm:
        raise ManifestError("Manifest JSON must include non-empty column_map for CSV ingest")
    return ingest_csv_document(conn, csv_path, m, brand_code=brand_code, column_map=cm)
