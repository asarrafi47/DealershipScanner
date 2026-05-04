"""
Merge discovery candidates into the scanner manifest (project-root ``dealers.json``).

Bulk merges use ``backend.dev.dealers.merge_manifest_export_rows_bulk`` (hash-map dedupe, single
disk write, compact JSON). Single-row edits still go through ``upsert_dealer_manifest_row`` in
the dev console.

URLs are normalized with ``normalize_manifest_url`` (scanner manifest rules), not
``normalize_url`` (which drops some third-party hosts used for discovery ranking).
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def merge_candidates_into_scanner_manifest(
    candidates: list[Any],
    *,
    manifest_path: Path,
) -> dict[str, int]:
    """
    Merge ``DealerCandidate`` rows into ``dealers.json``.

    ``skipped`` counts rows that cannot be written because the scanner manifest requires an
    HTTPS ``url`` per row. That is **not** a failure of the DMV→OSM→DDG ordering—many OSM POIs
    have no ``website`` tag, and DuckDuckGo Instant Answers often return no URL (it is not
    general web search).
    """
    from backend.dev.dealers import merge_manifest_export_rows_bulk, normalize_manifest_url
    from backend.discovery.candidate import DealerCandidate

    skipped = 0
    export: list[dict[str, Any]] = []
    for c in candidates:
        if not isinstance(c, DealerCandidate):
            skipped += 1
            continue
        raw = (c.dealer_website_url or c.website_url or "").strip()
        url = normalize_manifest_url(raw)
        if not url:
            skipped += 1
            continue
        name = (c.name or "").strip()
        if not name:
            skipped += 1
            continue
        export.append({"name": name, "url": url})

    stats = merge_manifest_export_rows_bulk(export, manifest_path=manifest_path)
    stats["skipped"] = int(stats.get("skipped", 0)) + skipped
    return stats


def merge_json_export_rows(rows: list[dict[str, Any]], *, manifest_path: Path) -> dict[str, int]:
    """
    Merge the JSON lines emitted by ``discover_dealerships.py --json`` (objects with ``name``, ``url``).

    Ignores entries with empty ``url``. Large merges use a single-pass hash map + compact JSON write
    (see ``merge_manifest_export_rows_bulk``).
    """
    from backend.dev.dealers import merge_manifest_export_rows_bulk

    if not isinstance(rows, list):
        return {"inserted": 0, "updated": 0, "skipped": 0}
    return merge_manifest_export_rows_bulk(rows, manifest_path=manifest_path)
