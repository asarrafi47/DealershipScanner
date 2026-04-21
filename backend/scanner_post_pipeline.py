"""
Post-scan SQLite repair, listing-description → packages parse, optional interior cabin vision
(Ollama LLaVA), and optional enrichment.

Repair runs only for VINs touched in the scan (not the whole ``cars`` table).
Listing description parsing fills ``packages_normalized`` / ``dealer_description_parsed`` from
each car's ``description`` (deterministic + optional LLM; see ``listing_description_extract``).
Interior vision runs by default after each scan; set ``SCANNER_POST_INTERIOR_VISION=0`` or
``--no-post-interior-vision`` to skip. Requires ``OLLAMA_HOST`` and a vision model
(default ``OLLAMA_VISION_MODEL=llava:13b``).
Enrichment is optional and requires an indexed EPA master catalog unless ``vision_only``.
Optional KBB IDWS valuation for touched VINs (``SCANNER_POST_KBB=1`` / ``--post-kbb``;
requires ``KBB_API_KEY``).
"""

from __future__ import annotations

import logging
import os
from typing import Any

logger = logging.getLogger(__name__)


def post_repair_env_enabled() -> bool:
    raw = (os.environ.get("SCANNER_POST_REPAIR") or "1").strip().lower()
    return raw not in ("0", "false", "no", "off")


def post_enrich_env_enabled() -> bool:
    return os.environ.get("SCANNER_POST_ENRICH", "").strip().lower() in ("1", "true", "yes", "on")


def post_enrich_vision_env_enabled() -> bool:
    return os.environ.get("SCANNER_POST_ENRICH_VISION", "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def post_listing_description_env_enabled() -> bool:
    raw = (os.environ.get("SCANNER_POST_LISTING_DESCRIPTION") or "1").strip().lower()
    return raw not in ("0", "false", "no", "off")


def post_interior_vision_env_enabled() -> bool:
    raw = (os.environ.get("SCANNER_POST_INTERIOR_VISION") or "1").strip().lower()
    return raw not in ("0", "false", "no", "off")


def post_kbb_env_enabled() -> bool:
    """Licensed KBB IDWS refresh for VINs touched in this scan (requires ``KBB_API_KEY``)."""
    return (os.environ.get("SCANNER_POST_KBB") or "").strip().lower() in ("1", "true", "yes", "on")


def gallery_vision_filter_env_enabled() -> bool:
    """Default on: LLaVA drops non-vehicle gallery images. Set ``SCANNER_GALLERY_VISION_FILTER=0`` to skip."""
    raw = (os.environ.get("SCANNER_GALLERY_VISION_FILTER") or "1").strip().lower()
    return raw not in ("0", "false", "no", "off")


def monroney_vision_env_enabled() -> bool:
    """Default on: LLaVA reads sticker URLs + VDP Monroney text. Set ``SCANNER_MONRONEY_VISION=0`` to skip."""
    raw = (os.environ.get("SCANNER_MONRONEY_VISION") or "1").strip().lower()
    return raw not in ("0", "false", "no", "off")


def aggregate_vins_from_dealer_results(outcomes: list[Any]) -> list[str]:
    """Stable-unique VINs from per-dealer ``run_dealer`` result dicts."""
    seen: set[str] = set()
    ordered: list[str] = []
    for o in outcomes:
        if not isinstance(o, dict):
            continue
        for vin in o.get("vins") or []:
            s = (vin or "").strip()
            if not s or s in seen:
                continue
            seen.add(s)
            ordered.append(s)
    return ordered


def run_storage_repair_for_vins(vins: list[str]) -> dict[str, Any]:
    """Normalize placeholders and merge_verified_specs backfills for given VINs only."""
    from backend.db.inventory_db import get_car_by_vin, refresh_car_data_quality_score, update_car_row_partial
    from backend.utils.inventory_repair import collect_row_storage_repairs

    stats: dict[str, Any] = {
        "vins": len(vins),
        "rows_found": 0,
        "rows_patched": 0,
        "fields": {},
    }
    fields: dict[str, int] = stats["fields"]
    for vin in vins:
        raw = get_car_by_vin(vin)
        if not raw:
            continue
        stats["rows_found"] += 1
        cid = int(raw["id"])
        patch = collect_row_storage_repairs(raw)
        if not patch:
            continue
        stats["rows_patched"] += 1
        for k in patch:
            fields[k] = fields.get(k, 0) + 1
        update_car_row_partial(cid, patch)
        refresh_car_data_quality_score(cid)
    return stats


def run_enrichment_for_car_ids(
    car_ids: list[int],
    *,
    vision_only: bool,
    max_workers: int | None = None,
) -> dict[str, Any]:
    from backend.enrichment_service import DEFAULT_MAX_WORKERS, InventoryEnricher

    if not car_ids:
        return {"skipped": True, "reason": "no_car_ids", "processed": 0}

    enricher = InventoryEnricher()
    if not vision_only and not enricher.catalog.collection_exists():
        logger.warning(
            "Post-scan enrichment skipped: EPA master catalog is not indexed in Postgres "
            "(set DATABASE_URL or PGVECTOR_URL, then run: python -m backend.vector.ingest_master_specs --reindex). "
            "Alternatively use --post-enrich-vision-only after mechanical fields are filled."
        )
        return {"skipped": True, "reason": "catalog_missing", "processed": 0}

    workers = max_workers if max_workers is not None else DEFAULT_MAX_WORKERS
    return enricher.run_all(
        limit=None,
        vision_only=vision_only,
        max_workers=max(1, int(workers)),
        only_ids=car_ids,
    )


def _car_ids_for_vins(vins: list[str]) -> list[int]:
    from backend.db.inventory_db import get_car_by_vin

    ids: list[int] = []
    seen: set[int] = set()
    for vin in vins:
        row = get_car_by_vin(vin)
        if not row:
            continue
        cid = int(row["id"])
        if cid not in seen:
            seen.add(cid)
            ids.append(cid)
    return ids


def _http_image_urls_for_row(row: dict[str, Any]) -> list[str]:
    import json

    out: list[str] = []
    seen: set[str] = set()
    g = row.get("gallery")
    if isinstance(g, str):
        try:
            g = json.loads(g)
        except (json.JSONDecodeError, TypeError):
            g = []
    if isinstance(g, list):
        for u in g:
            if not isinstance(u, str):
                continue
            s = u.strip()
            if s.lower().startswith("http") and s not in seen:
                seen.add(s)
                out.append(s)
    img = row.get("image_url")
    if isinstance(img, str):
        s = img.strip()
        if s.lower().startswith("http") and s not in seen:
            seen.add(s)
            out.append(s)
    return out


def run_interior_vision_for_vins(vins: list[str]) -> dict[str, Any]:
    """Run Ollama LLaVA interior/cabin inference for each VIN (first HTTP gallery or hero image)."""
    from backend.db.inventory_db import get_car_by_vin, refresh_car_data_quality_score, update_car_row_partial
    from backend.vision import ollama_llava
    from backend.vision.interior_vision_merge import build_updates_from_llava_result

    stats: dict[str, Any] = {
        "vins": len(vins),
        "rows_found": 0,
        "rows_applied": 0,
        "rows_skipped": 0,
        "skip_reasons": {},
    }
    reasons: dict[str, int] = stats["skip_reasons"]
    for vin in vins:
        row = get_car_by_vin(vin)
        if not row:
            stats["rows_skipped"] += 1
            reasons["not_in_db"] = reasons.get("not_in_db", 0) + 1
            continue
        stats["rows_found"] += 1
        urls = _http_image_urls_for_row(row)
        primary = urls[0] if urls else None
        if not primary:
            stats["rows_skipped"] += 1
            reasons["no_http_image"] = reasons.get("no_http_image", 0) + 1
            continue
        llava = ollama_llava.analyze_interior_from_image_url(primary)
        if not llava:
            stats["rows_skipped"] += 1
            reasons["llava_failed"] = reasons.get("llava_failed", 0) + 1
            continue
        patch = build_updates_from_llava_result(row=row, llava=llava)
        if not patch:
            stats["rows_skipped"] += 1
            reasons["no_merge_updates"] = reasons.get("no_merge_updates", 0) + 1
            continue
        update_car_row_partial(int(row["id"]), patch)
        refresh_car_data_quality_score(int(row["id"]))
        stats["rows_applied"] += 1
    return stats


def run_listing_description_parse_for_vins(vins: list[str]) -> dict[str, Any]:
    """Run ``process_listing_description_for_row`` for each VIN (after rows exist in SQLite)."""
    from backend.db.inventory_db import get_car_by_vin, refresh_car_data_quality_score, update_car_row_partial
    from backend.utils.listing_description_persist import process_listing_description_for_row

    stats: dict[str, Any] = {
        "vins": len(vins),
        "rows_found": 0,
        "rows_applied": 0,
        "rows_skipped": 0,
        "skip_reasons": {},
    }
    reasons: dict[str, int] = stats["skip_reasons"]
    for vin in vins:
        row = get_car_by_vin(vin)
        if not row:
            stats["rows_skipped"] += 1
            reasons["not_in_db"] = reasons.get("not_in_db", 0) + 1
            continue
        stats["rows_found"] += 1
        pr = process_listing_description_for_row(row, skip_if_unchanged=True, force=False)
        if not pr.get("applied"):
            stats["rows_skipped"] += 1
            r = str(pr.get("reason") or "unknown")
            reasons[r] = reasons.get(r, 0) + 1
            continue
        upd = pr.get("updates") or {}
        update_car_row_partial(int(row["id"]), upd)
        refresh_car_data_quality_score(int(row["id"]))
        stats["rows_applied"] += 1
    return stats


def run_kbb_for_vins(vins: list[str]) -> dict[str, Any]:
    """Call KBB IDWS for each VIN (rate-limited); skips rows without a valid VIN or API key."""
    from backend.db.inventory_db import get_car_by_vin, refresh_car_data_quality_score, update_car_row_partial
    from backend.kbb_idws import kbb_api_configured, patch_from_refresh_result, refresh_kbb_for_vehicle_row

    stats: dict[str, Any] = {
        "vins": len(vins),
        "rows_found": 0,
        "rows_applied": 0,
        "rows_skipped": 0,
        "skip_reasons": {},
    }
    if not kbb_api_configured():
        stats["skipped"] = True
        stats["reason"] = "kbb_api_key_missing"
        return stats

    reasons: dict[str, int] = stats["skip_reasons"]
    for vin in vins:
        row = get_car_by_vin(vin)
        if not row:
            stats["rows_skipped"] += 1
            reasons["not_in_db"] = reasons.get("not_in_db", 0) + 1
            continue
        stats["rows_found"] += 1
        res = refresh_kbb_for_vehicle_row(row)
        if not res.ok:
            stats["rows_skipped"] += 1
            r = res.message
            reasons[r] = reasons.get(r, 0) + 1
            continue
        patch = patch_from_refresh_result(res)
        cid = int(row["id"])
        if patch:
            update_car_row_partial(cid, patch)
            refresh_car_data_quality_score(cid)
        stats["rows_applied"] += 1
    return stats


def run_post_scan(
    scanned_vins: list[str],
    *,
    post_repair: bool,
    post_listing_description: bool,
    post_interior_vision: bool,
    post_enrich: bool,
    post_enrich_vision_only: bool,
    post_kbb: bool = False,
    enrichment_max_workers: int | None = None,
) -> dict[str, Any]:
    """
    Run repair, optional listing-description parse, optional interior LLaVA pass, and/or
    enrichment for VINs from this scan.

    ``post_enrich`` and ``post_enrich_vision_only`` are mutually exclusive in practice;
    if both are true, vision-only wins.
    """
    summary: dict[str, Any] = {
        "vins": len(scanned_vins),
        "repair": None,
        "listing_description": None,
        "interior_vision": None,
        "enrich": None,
        "kbb": None,
    }
    if post_repair and scanned_vins:
        logger.info("Post-scan repair: %d VIN(s) from this run", len(scanned_vins))
        summary["repair"] = run_storage_repair_for_vins(scanned_vins)
        logger.info(
            "Post-scan repair done: rows_found=%s rows_patched=%s",
            summary["repair"].get("rows_found"),
            summary["repair"].get("rows_patched"),
        )
    elif post_repair:
        logger.info("Post-scan repair skipped (no VINs in this run)")

    if post_listing_description and scanned_vins:
        logger.info("Post-scan listing description → packages: %d VIN(s)", len(scanned_vins))
        summary["listing_description"] = run_listing_description_parse_for_vins(scanned_vins)
        ld = summary["listing_description"]
        logger.info(
            "Post-scan listing description done: rows_found=%s rows_applied=%s rows_skipped=%s",
            ld.get("rows_found"),
            ld.get("rows_applied"),
            ld.get("rows_skipped"),
        )
    elif post_listing_description:
        logger.info("Post-scan listing description parse skipped (no VINs in this run)")

    if post_interior_vision and scanned_vins:
        logger.info("Post-scan interior cabin vision (Ollama LLaVA): %d VIN(s)", len(scanned_vins))
        summary["interior_vision"] = run_interior_vision_for_vins(scanned_vins)
        iv = summary["interior_vision"]
        logger.info(
            "Post-scan interior vision done: rows_found=%s rows_applied=%s rows_skipped=%s",
            iv.get("rows_found"),
            iv.get("rows_applied"),
            iv.get("rows_skipped"),
        )
    elif post_interior_vision:
        logger.info("Post-scan interior vision skipped (no VINs in this run)")

    want_enrich = post_enrich or post_enrich_vision_only
    if want_enrich and scanned_vins:
        ids = _car_ids_for_vins(scanned_vins)
        vision_only = bool(post_enrich_vision_only)
        mode = "vision_only" if vision_only else "catalog+vision"
        logger.info("Post-scan enrichment (%s): %d car id(s)", mode, len(ids))
        summary["enrich"] = run_enrichment_for_car_ids(
            ids,
            vision_only=vision_only,
            max_workers=enrichment_max_workers,
        )
        if not summary["enrich"].get("skipped"):
            logger.info(
                "Post-scan enrichment done: processed=%s ok=%s errors=%s",
                summary["enrich"].get("processed"),
                summary["enrich"].get("ok"),
                summary["enrich"].get("errors"),
            )
    elif want_enrich:
        logger.info("Post-scan enrichment skipped (no VINs in this run)")

    if post_kbb and scanned_vins:
        logger.info("Post-scan KBB IDWS: %d VIN(s)", len(scanned_vins))
        summary["kbb"] = run_kbb_for_vins(scanned_vins)
        kb = summary["kbb"]
        if kb.get("skipped"):
            logger.info("Post-scan KBB skipped: %s", kb.get("reason"))
        else:
            logger.info(
                "Post-scan KBB done: rows_found=%s rows_applied=%s rows_skipped=%s",
                kb.get("rows_found"),
                kb.get("rows_applied"),
                kb.get("rows_skipped"),
            )
    elif post_kbb:
        logger.info("Post-scan KBB skipped (no VINs in this run)")

    return summary
