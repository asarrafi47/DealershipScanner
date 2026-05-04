"""
BMW U.S. ordering-guide / brochure CSV (post-conversion) — column mapping helpers.

After you convert an official PDF ordering guide to CSV, place it under
``data/vehicle_reference/source_files/`` with a sidecar manifest (see
``vehicle_reference.ingestion.structured.ingest_csv_with_manifest_path``).

This module documents the expected logical columns; the manifest ``column_map``
maps them to your CSV headers (which vary by document year).
"""

# Logical DB fields available for CSV column_map keys (subset used by ingest_structured).
ORDERING_GUIDE_LOGICAL_FIELDS: tuple[str, ...] = (
    "model_year",
    "series_name",
    "variant_name",
    "trim_line",
    "body_style",
    "engine",
    "transmission",
    "drivetrain",
    "fuel_type",
    "mpg_text",
    "passenger_seating",
    "uncertainty_notes",
    "external_source",
    "external_record_id",
    "internal_notes",
    "market",
    "source_label",
    "source_url",
    "source_notes",
)
