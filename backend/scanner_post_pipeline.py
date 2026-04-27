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
    """LLaVA cabin-color inference. Default on; set SCANNER_POST_INTERIOR_VISION=0 to disable."""
    raw = (os.environ.get("SCANNER_POST_INTERIOR_VISION") or "1").strip().lower()
    return raw not in ("0", "false", "no", "off")


def post_kbb_env_enabled() -> bool:
    """Licensed KBB IDWS refresh for VINs touched in this scan (requires ``KBB_API_KEY``)."""
    return (os.environ.get("SCANNER_POST_KBB") or "").strip().lower() in ("1", "true", "yes", "on")


def gallery_vision_filter_env_enabled() -> bool:
    """LLaVA gallery-image classification. Default on; set SCANNER_GALLERY_VISION_FILTER=0 to opt out."""
    raw = (os.environ.get("SCANNER_GALLERY_VISION_FILTER") or "1").strip().lower()
    return raw not in ("0", "false", "no", "off")


def monroney_vision_env_enabled() -> bool:
    """LLaVA Monroney/sticker parsing. Default on; set SCANNER_MONRONEY_VISION=0 to opt out."""
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
    """
    For each VIN: placeholder cleanup + EPA/trim backfill, then NHTSA vPIC for any remaining
    spec gaps (transmission, cylinders, and other slots handled by structured backfill).
    """
    from backend.db.inventory_db import get_car_by_id, get_car_by_vin, refresh_car_data_quality_score, update_car_row_partial
    from backend.spec_structured_backfill import (
        apply_structured_spec_backfill_for_car,
        car_needs_transmission_or_cylinders_backfill,
    )
    from backend.utils.inventory_repair import collect_row_storage_repairs

    stats: dict[str, Any] = {
        "vins": len(vins),
        "rows_found": 0,
        "rows_patched": 0,
        "fields": {},
        "structured_mech": {"candidates": 0, "applied": 0},
    }
    fields: dict[str, int] = stats["fields"]
    s_mech = stats["structured_mech"]
    for vin in vins:
        raw = get_car_by_vin(vin)
        if not raw:
            continue
        stats["rows_found"] += 1
        cid = int(raw["id"])
        patch = collect_row_storage_repairs(raw)
        if patch:
            stats["rows_patched"] += 1
            for k in patch:
                fields[k] = fields.get(k, 0) + 1
            update_car_row_partial(cid, patch)
            refresh_car_data_quality_score(cid)

        car = get_car_by_id(cid, include_inactive=True)
        if not car or not car_needs_transmission_or_cylinders_backfill(car):
            continue
        s_mech["candidates"] += 1
        res = apply_structured_spec_backfill_for_car(cid, use_vpic_cache=True)
        if res.applied:
            s_mech["applied"] += 1
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


def http_listing_urls_deduped(urls: list[str]) -> list[str]:
    """Stable-unique HTTPS/HTTP listing image URLs in input order."""
    seen: set[str] = set()
    ordered: list[str] = []
    for u in urls:
        s = (u or "").strip()
        if not s.lower().startswith("http") or s in seen:
            continue
        seen.add(s)
        ordered.append(s)
    return ordered


def http_listing_image_urls_for_row(row: dict[str, Any]) -> list[str]:
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


# URL / path tokens that often name cabin (not exterior) media on dealer and OEM CDNs
_INTERIOR_PATH_NEEDLES: tuple[str, ...] = (
    "interior",
    "cabin",
    "cabinview",
    "incabin",
    "in-cabin",
    "inside",
    "dashboard",
    "upholstery",
    "cockpit",
    "/int/",
    "/int_",
    "_int_",
    "interiorview",
    "interior_",
    "_interior",
    "passenger",
    "penger",  # truncated paths seen on some hosts
    "driveseat",
    "driverseat",
    "cabin-",
    "cabin_",
    "cabin/",
)


def _url_suggests_interior_cabin_image(url: str) -> bool:
    """Heuristic: CDN paths often name cabin shots (no extra network calls)."""
    u = (url or "").strip()
    if not u.lower().startswith("http"):
        return False
    low = u.lower()
    for needle in _INTERIOR_PATH_NEEDLES:
        if needle in low:
            return True
    return False


def _classify_category_is_cabin(classify_out: dict[str, Any] | None) -> bool:
    if not isinstance(classify_out, dict):
        return False
    c = str(classify_out.get("category") or "").strip().lower().replace(" ", "_").replace("-", "_")
    return c in {
        "interior",
        "cabin",
        "cabinview",
        "cabin_view",
    }


def _interior_vision_max_gallery_classify() -> int:
    try:
        v = int((os.environ.get("INTERIOR_VISION_MAX_GALLERY_CLASSIFY") or "12").strip())
    except (TypeError, ValueError):
        return 12
    return max(0, min(48, v))


def select_url_for_cabin_vision(urls: list[str]) -> str | None:
    """
    Choose one HTTPS URL that is likely a cabin photo before running interior color LLaVA.

    Order: (1) URL path hints indicating interior, (2) first N gallery URLs classified
    as ``interior``/``cabin`` by :func:`classify_listing_image_from_url`.
    If neither finds a shot, return ``None``. See :func:`pick_listing_image_for_interior_vision`
    for the default **through-windows** fallback on exterior/hero frames.
    """
    from backend.vision import ollama_llava

    ordered = http_listing_urls_deduped(urls)
    if not ordered:
        return None
    for u in ordered:
        if _url_suggests_interior_cabin_image(u):
            return u
    cap = _interior_vision_max_gallery_classify()
    for i, u in enumerate(ordered):
        if i >= cap:
            break
        parsed = ollama_llava.classify_listing_image_from_url(u)
        if _classify_category_is_cabin(parsed):
            return u
    return None


def _exterior_through_windows_fallback_enabled() -> bool:
    """
    When no cabin URL is found, use the first listing image and ask LLaVA to read the cabin
    **through the glass** (default **on**).

    Disable with ``INTERIOR_VISION_NO_EXTERIOR_FALLBACK=1``. The legacy
    ``INTERIOR_VISION_FALLBACK_HERO=1`` still forces this path on. Set
    ``INTERIOR_VISION_FALLBACK_THROUGH_WINDOWS=0`` to restore the old "skip if no cabin" behavior.
    """
    if (os.environ.get("INTERIOR_VISION_NO_EXTERIOR_FALLBACK") or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    ):
        return False
    if (os.environ.get("INTERIOR_VISION_FALLBACK_HERO") or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    ):
        return True
    raw = (os.environ.get("INTERIOR_VISION_FALLBACK_THROUGH_WINDOWS") or "1").strip().lower()
    return raw not in ("0", "false", "no", "off")


def pick_listing_image_for_interior_vision(urls: list[str]) -> tuple[str | None, str]:
    """
    Pick one image URL and an Ollama ``inference_context`` (``cabin`` or ``through_windows``).

    Prefer a true cabin shot; otherwise, if fallback is enabled, use the first HTTP URL
    (typically the hero exterior) for through-the-window analysis.
    """
    ordered = http_listing_urls_deduped(urls)
    if not ordered:
        return None, "cabin"
    cabin = select_url_for_cabin_vision(urls)
    if cabin:
        return cabin, "cabin"
    if not _exterior_through_windows_fallback_enabled():
        return None, "through_windows"
    return ordered[0], "through_windows"


def _interior_vision_max_url_tries() -> int:
    try:
        v = int((os.environ.get("INTERIOR_VISION_MAX_URL_TRIES") or "3").strip())
    except (TypeError, ValueError):
        return 3
    return max(1, min(10, v))


def candidate_urls_for_interior_vision(urls: list[str]) -> list[tuple[str, str]]:
    """
    Candidate (url, inference_context) pairs to try in order.

    - If a cabin URL is detectable, try that as ``cabin`` first.
    - Otherwise try the first few HTTP URLs as ``through_windows`` (best-effort).
    """
    ordered = http_listing_urls_deduped(urls)
    if not ordered:
        return []
    out: list[tuple[str, str]] = []
    cabin = select_url_for_cabin_vision(ordered)
    if cabin:
        out.append((cabin, "cabin"))
    # Through-windows fallback tries: include hero and then a few more frames.
    if cabin is None and not _exterior_through_windows_fallback_enabled():
        return out
    cap = _interior_vision_max_url_tries()
    for u in ordered:
        if len(out) >= cap:
            break
        if out and out[0][0] == u:
            continue
        out.append((u, "through_windows"))
    return out


def run_interior_vision_for_vins(
    vins: list[str],
    *,
    skip_if_interior_present: bool = False,
) -> dict[str, Any]:
    """Run Ollama LLaVA interior/cabin inference for each VIN.

    Prefers a classified cabin image; when none exists, uses the first listing image and asks the
    model to read the cabin **through the windows** (see ``INTERIOR_VISION_FALLBACK_THROUGH_WINDOWS``).

    When ``skip_if_interior_present`` is true, rows with a non-empty dealer ``interior_color`` are
    skipped (saves GPU time for bulk backfills). Post-scan callers keep the default ``False`` so
    gallery passes can still refresh buckets / provenance when a listing already had interior text.
    """
    from backend.db.inventory_db import get_car_by_vin, refresh_car_data_quality_score, update_car_row_partial
    from backend.utils.field_clean import is_effectively_empty
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
        if skip_if_interior_present:
            ic = row.get("interior_color")
            if ic is not None and not is_effectively_empty(str(ic)):
                stats["rows_skipped"] += 1
                reasons["interior_already_set"] = reasons.get("interior_already_set", 0) + 1
                continue
        urls = http_listing_image_urls_for_row(row)
        if not urls:
            stats["rows_skipped"] += 1
            reasons["no_http_image"] = reasons.get("no_http_image", 0) + 1
            continue
        candidates = candidate_urls_for_interior_vision(urls)
        if not candidates:
            stats["rows_skipped"] += 1
            reasons["no_cabin_image_in_gallery"] = (
                reasons.get("no_cabin_image_in_gallery", 0) + 1
            )
            continue
        llava = None
        for primary, inference_ctx in candidates:
            llava = ollama_llava.analyze_interior_from_image_url(
                primary,
                inference_context=inference_ctx,
            )
            if llava:
                break
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
