"""
Post-scan SQLite repair, listing-description → packages parse, optional interior cabin vision
(Claude Haiku), and optional enrichment.

Repair runs only for VINs touched in the scan (not the whole ``cars`` table).
Listing description parsing fills ``packages_normalized`` / ``dealer_description_parsed`` from
each car's ``description`` (deterministic + optional LLM; see ``listing_description_extract``).
Interior vision runs by default after each scan; set ``SCANNER_POST_INTERIOR_VISION=0`` or
``--no-post-interior-vision`` to skip. Requires ``ANTHROPIC_API_KEY``.
Enrichment is optional and requires an indexed EPA master catalog unless ``vision_only``.
OEM window sticker PDF fetch + parse for touched VINs (``SCANNER_POST_WINDOW_STICKER=1``,
default on; ``--no-post-window-sticker`` to skip). Reliable: Stellantis + Ford/Lincoln.
GM: ``WINDOW_STICKER_GM_EXPERIMENTAL=1``. Stores PDF under ``car_window_stickers/``.
"""

from __future__ import annotations

import logging
import os
import time
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
    """Claude cabin-color inference. Default on; set SCANNER_POST_INTERIOR_VISION=0 to disable."""
    raw = (os.environ.get("SCANNER_POST_INTERIOR_VISION") or "1").strip().lower()
    return raw not in ("0", "false", "no", "off")


def post_window_sticker_env_enabled() -> bool:
    """
    Fetch OEM Monroney PDFs for scanned VINs (Jeep, Ford, GM, etc.) and persist locally.

    On by default. Disable with ``SCANNER_POST_WINDOW_STICKER=0`` or ``--no-post-window-sticker``.
    Cap per run via ``SCANNER_POST_WINDOW_STICKER_MAX`` (0 = no cap).
    """
    raw = (os.environ.get("SCANNER_POST_WINDOW_STICKER") or "1").strip().lower()
    return raw not in ("0", "false", "no", "off")


def _window_sticker_max_per_run() -> int:
    try:
        return max(0, int((os.environ.get("SCANNER_POST_WINDOW_STICKER_MAX") or "0").strip()))
    except ValueError:
        return 0


def run_window_sticker_for_vins(vins: list[str]) -> dict[str, Any]:
    """
    For each scanned VIN with a known OEM endpoint: download PDF, store on disk, merge packages.
    """
    from backend.db.inventory_db import get_car_by_vin
    from backend.enrichment.window_sticker_service import (
        car_sticker_packages_need_analysis,
        ensure_window_sticker_for_car,
        window_sticker_available,
    )
    from backend.scanner.window_sticker import (
        car_listing_may_have_sticker,
        get_window_sticker_url,
        should_auto_fetch_oem_window_sticker,
    )

    stats: dict[str, Any] = {
        "vins": len(vins),
        "attempted": 0,
        "stored": 0,
        "already_cached": 0,
        "reanalyzed": 0,
        "no_oem_endpoint": 0,
        "listing_sticker": 0,
        "failed": 0,
        "skipped_cap": 0,
    }
    cap = _window_sticker_max_per_run()
    for idx, vin in enumerate(vins):
        if cap > 0 and idx >= cap:
            stats["skipped_cap"] = max(0, len(vins) - cap)
            break
        vnorm = (vin or "").strip().upper()
        if len(vnorm) != 17:
            continue
        row = get_car_by_vin(vnorm)
        if not row:
            continue
        oem_url = get_window_sticker_url(vnorm)
        can_oem = bool(oem_url) and should_auto_fetch_oem_window_sticker(row)
        can_listing = car_listing_may_have_sticker(row)
        if not can_oem and not can_listing:
            stats["no_oem_endpoint"] += 1
            continue
        if window_sticker_available(row) and not car_sticker_packages_need_analysis(row):
            stats["already_cached"] += 1
            continue
        had_local = window_sticker_available(row)
        stats["attempted"] += 1
        try:
            out = ensure_window_sticker_for_car(
                int(row["id"]),
                allow_vision_fallback=False,
            )
            if out.get("fetch_error"):
                stats["failed"] += 1
            elif out.get("listing_sticker"):
                stats["listing_sticker"] += 1
            elif out.get("analyzed") and had_local:
                stats["reanalyzed"] += 1
            elif out.get("window_sticker_available") and out.get("stored"):
                stats["stored"] += 1
        except Exception as e:
            stats["failed"] += 1
            logger.debug("Post-scan window sticker failed for %s: %s", vnorm, e)
        time.sleep(1.5)
    return stats


def post_listing_gap_fill_env_enabled() -> bool:
    """
    After repair + listing parse: tier vPIC → listing-page HTML (Playwright)
    → DDG HTML search for residual mechanical fields. Condition only from listing HTML.

    On by default. Disable with ``SCANNER_POST_LISTING_GAP_FILL=0``.
    """
    raw = (os.environ.get("SCANNER_POST_LISTING_GAP_FILL") or "1").strip().lower()
    return raw not in ("0", "false", "no", "off")


def run_listing_gap_fill_stage(vins: list[str]) -> dict[str, Any]:
    """See ``backend.scanner.listing_gap_fill``."""
    from backend.scanner.listing_gap_fill import run_listing_gap_fill_for_vins

    return run_listing_gap_fill_for_vins(vins)


def post_dict_enrich_env_enabled() -> bool:
    """EPA DICTIONARY/ CSV enrichment fills spec gaps (transmission, cylinders, MPG, etc.). Default on."""
    raw = (os.environ.get("SCANNER_POST_DICT_ENRICH") or "1").strip().lower()
    return raw not in ("0", "false", "no", "off")


def run_dictionary_enrich_for_vins(vins: list[str]) -> dict[str, Any]:
    """
    Fill mechanical spec gaps from DICTIONARY/ EPA CSV files for each scanned VIN.

    Only writes fields that are currently empty/placeholder — never overwrites existing data.
    Set ``SCANNER_POST_DICT_ENRICH=0`` to skip this stage.
    """
    from backend.db.inventory_db import get_car_by_vin, update_car_row_partial

    try:
        from backend.dictionary.enrich_from_dictionary import enrich_car
    except ImportError:
        logger.warning(
            "backend.dictionary.enrich_from_dictionary not importable; dictionary enrichment skipped."
        )
        return {"vins": len(vins), "updated": 0, "skipped": True}

    stats: dict[str, Any] = {"vins": len(vins), "updated": 0, "no_match": 0}
    for vin in vins:
        row = get_car_by_vin(vin)
        if not row:
            continue
        updates = enrich_car(dict(row), fill_all=False, use_vpic=False)
        if not updates:
            stats["no_match"] += 1
            continue
        update_car_row_partial(int(row["id"]), updates)
        stats["updated"] += 1
    return stats


def gallery_vision_filter_env_enabled() -> bool:
    """Any gallery vision stage enabled (inline during scan or post-scan batch)."""
    from backend.scanner.scan_efficiency import (
        gallery_vision_inline_enabled,
        gallery_vision_post_enabled,
    )

    raw = (os.environ.get("SCANNER_GALLERY_VISION_FILTER") or "").strip().lower()
    if raw in ("0", "false", "no", "off"):
        return False
    if raw in ("1", "true", "yes", "on"):
        return True
    return gallery_vision_inline_enabled() or gallery_vision_post_enabled()


def apply_gallery_vision_filter_to_vehicles(vehicles: list[dict[str, Any]]) -> dict[str, int]:
    """
    Mutate each vehicle's ``gallery`` and ``image_url`` to drop non-vehicle images.
    Uses Claude Haiku when ANTHROPIC_API_KEY is set.
    """
    anthropic_key = (os.environ.get("ANTHROPIC_API_KEY") or "").strip()
    if anthropic_key:
        from backend.vision import claude_vision as _vis

        _filter_fn = _vis.filter_gallery_urls_for_vehicle_listing
        logger.info("Gallery vision filter: using Claude Haiku")
    else:
        logger.info(
            "Gallery vision filter: ANTHROPIC_API_KEY not set — returning URLs as-is (no filtering)"
        )

        def _filter_fn(urls, *, page_referer=None, **kw):  # type: ignore[misc]
            return [u for u in urls if isinstance(u, str) and u.strip().lower().startswith("http")]

    total_before = 0
    total_after = 0
    for v in vehicles:
        hero = v.get("image_url")
        raw_g = v.get("gallery")
        g_list = raw_g if isinstance(raw_g, list) else []
        urls: list[str] = []
        if isinstance(hero, str) and hero.strip().lower().startswith("http"):
            urls.append(hero.strip())
        for u in g_list:
            if isinstance(u, str) and u.strip().lower().startswith("http"):
                urls.append(u.strip())
        seen_u: set[str] = set()
        n_before = 0
        for u in urls:
            if u not in seen_u:
                seen_u.add(u)
                n_before += 1
        total_before += n_before
        ref = str(v.get("_detail_url") or v.get("detail_url") or v.get("source_url") or "").strip()
        page_referer = ref if ref.lower().startswith("http") else None
        filtered = _filter_fn(urls, page_referer=page_referer)
        seen_f: set[str] = set()
        n_after = 0
        for u in filtered:
            if u not in seen_f:
                seen_f.add(u)
                n_after += 1
        total_after += n_after
        v["gallery"] = filtered
        v["image_url"] = filtered[0] if filtered else ""
    dropped = max(0, total_before - total_after)
    return {
        "gallery_vision_unique_before": total_before,
        "gallery_vision_unique_after": total_after,
        "gallery_vision_unique_dropped": dropped,
    }


def post_gallery_recovery_env_enabled() -> bool:
    """Re-fetch VDP HTML for thin galleries before vision filter. Default on."""
    raw = (os.environ.get("SCANNER_POST_GALLERY_RECOVERY") or "1").strip().lower()
    return raw not in ("0", "false", "no", "off")


def run_gallery_recovery_for_vins(vins: list[str]) -> dict[str, Any]:
    """
    Post-scan: HTTP/Playwright HTML harvest for listings with thin galleries.
    Runs before gallery vision filter so Claude sees more real photos.
    """
    import json

    from backend.db.inventory_db import get_car_by_vin, update_car_row_partial
    from backend.scanner.vdp_html_recovery import (
        count_https_gallery_urls,
        recover_from_detail_page,
        thin_gallery_threshold,
    )
    from backend.utils.gallery_merge import merge_vdp_gallery_into_vehicle

    stats: dict[str, Any] = {
        "vins": len(vins),
        "rows_found": 0,
        "rows_updated": 0,
        "rows_recovered": 0,
        "urls_added_total": 0,
    }
    thresh = thin_gallery_threshold()

    for vin in vins:
        vnorm = (vin or "").strip().upper()
        if len(vnorm) != 17:
            continue
        row = get_car_by_vin(vnorm)
        if not row:
            continue
        stats["rows_found"] += 1
        g_raw = row.get("gallery")
        if isinstance(g_raw, str):
            try:
                g_list = json.loads(g_raw) if g_raw.strip() else []
            except json.JSONDecodeError:
                g_list = []
        elif isinstance(g_raw, list):
            g_list = list(g_raw)
        else:
            g_list = []
        if count_https_gallery_urls(g_list) > thresh:
            continue
        detail = (
            str(row.get("source_url") or row.get("listing_vdp_url") or row.get("_detail_url") or "")
            .strip()
        )
        if not detail.lower().startswith("http"):
            continue
        rec = recover_from_detail_page(detail, existing_gallery=g_list)
        rec_urls = rec.get("gallery_urls") or []
        if not rec_urls and not rec.get("description"):
            continue
        veh = {
            "gallery": g_list,
            "image_url": row.get("image_url"),
        }
        patch: dict[str, Any] = {}
        if rec_urls:
            gmerge = merge_vdp_gallery_into_vehicle(veh, rec_urls)
            if int(gmerge.get("added") or 0) > 0:
                patch["gallery"] = json.dumps(veh.get("gallery") or [])
                patch["image_url"] = veh.get("image_url")
                stats["urls_added_total"] += int(gmerge.get("added") or 0)
                stats["rows_recovered"] += 1
        desc = rec.get("description")
        if isinstance(desc, str) and desc.strip() and not (row.get("description") or "").strip():
            patch["description"] = desc.strip()[:2000]
        if patch:
            update_car_row_partial(int(row["id"]), patch)
            stats["rows_updated"] += 1
    return stats


def run_gallery_vision_for_vins(vins: list[str]) -> dict[str, Any]:
    """Post-scan gallery cleanup: load rows by VIN, filter galleries, write back."""
    import json

    from backend.db.inventory_db import get_car_by_vin, update_car_row_partial

    stats: dict[str, Any] = {
        "vins": len(vins),
        "rows_found": 0,
        "rows_updated": 0,
        "gallery_vision_unique_before": 0,
        "gallery_vision_unique_after": 0,
        "gallery_vision_unique_dropped": 0,
    }
    batch: list[dict[str, Any]] = []
    batch_meta: list[tuple[int, dict[str, Any]]] = []

    for vin in vins:
        vnorm = (vin or "").strip().upper()
        if len(vnorm) != 17:
            continue
        row = get_car_by_vin(vnorm)
        if not row:
            continue
        stats["rows_found"] += 1
        g_raw = row.get("gallery")
        if isinstance(g_raw, str):
            try:
                g_list = json.loads(g_raw) if g_raw.strip() else []
            except json.JSONDecodeError:
                g_list = []
        elif isinstance(g_raw, list):
            g_list = g_raw
        else:
            g_list = []
        veh = {
            "vin": vnorm,
            "image_url": row.get("image_url"),
            "gallery": g_list,
            "source_url": row.get("source_url"),
        }
        batch.append(veh)
        batch_meta.append((int(row["id"]), dict(row)))

    if not batch:
        return stats

    gv = apply_gallery_vision_filter_to_vehicles(batch)
    stats["gallery_vision_unique_before"] = gv.get("gallery_vision_unique_before", 0)
    stats["gallery_vision_unique_after"] = gv.get("gallery_vision_unique_after", 0)
    stats["gallery_vision_unique_dropped"] = gv.get("gallery_vision_unique_dropped", 0)

    for (car_id, _row), veh in zip(batch_meta, batch):
        g_out = veh.get("gallery")
        if not isinstance(g_out, list):
            g_out = []
        hero = veh.get("image_url") if isinstance(veh.get("image_url"), str) else ""
        update_car_row_partial(
            car_id,
            {
                "gallery": json.dumps(g_out),
                "image_url": hero or None,
            },
        )
        stats["rows_updated"] += 1
    return stats


def monroney_vision_env_enabled() -> bool:
    """Monroney/sticker parsing. Default on; set SCANNER_MONRONEY_VISION=0 to opt out."""
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
    from backend.enrichment.spec_structured_backfill import (
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
    from backend.enrichment.service import DEFAULT_MAX_WORKERS, InventoryEnricher

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
    Choose one HTTPS URL that is likely a cabin photo before running interior color vision.

    Order: (1) URL path hints indicating interior, (2) first URL containing "interior" in path.
    If neither finds a shot, return ``None``. See :func:`pick_listing_image_for_interior_vision`
    for the default **through-windows** fallback on exterior/hero frames.
    """
    ordered = http_listing_urls_deduped(urls)
    if not ordered:
        return None
    for u in ordered:
        if _url_suggests_interior_cabin_image(u):
            return u
    return None


def _exterior_through_windows_fallback_enabled() -> bool:
    """
    When no cabin URL is found, use the first listing image and ask Claude to read the cabin
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


_INTERIOR_BUCKET_ALLOWLIST: tuple[str, ...] = (
    "black",
    "gray",
    "beige",
    "tan",
    "brown",
    "white",
    "red",
    "blue",
    "other",
)

_CLAUDE_INTERIOR_PROMPT = (
    'What is the interior color of this car? '
    'Respond with JSON only: {"interior_color_bucket": "<one of: black, gray, beige, tan, brown, white, red, blue, other>", "confidence": <0.0-1.0>}'
)


def _analyze_interior_with_claude(image_url: str) -> dict[str, Any] | None:
    """
    Call Claude Haiku vision to classify the interior color from an image URL.

    Downloads the image, encodes as base64 JPEG, sends to Claude API.
    Returns dict with interior_color_bucket / confidence / raw, or None on failure.
    """
    import base64
    import json as _json

    api_key = (os.environ.get("ANTHROPIC_API_KEY") or "").strip()
    if not api_key:
        logger.warning("ANTHROPIC_API_KEY not set — interior vision skipped")
        return None

    try:
        import requests as _req
        from io import BytesIO

        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "image/*",
        }
        r = _req.get(image_url, headers=headers, timeout=12.0)
        r.raise_for_status()
        raw = r.content
        if len(raw) < 800:
            logger.debug("Interior vision: image too small (%d bytes) for %s", len(raw), image_url[:80])
            return None

        try:
            from PIL import Image as _PILImage
            img = _PILImage.open(BytesIO(raw)).convert("RGB")
            max_dim = 800
            w, h = img.size
            if w > max_dim or h > max_dim:
                img.thumbnail((max_dim, max_dim), _PILImage.LANCZOS)
            buf = BytesIO()
            img.save(buf, format="JPEG", quality=75)
            b64 = base64.b64encode(buf.getvalue()).decode()
        except Exception:
            b64 = base64.b64encode(raw[:300_000]).decode()

    except Exception as e:
        logger.debug("Interior vision: image fetch failed for %s: %s", image_url[:80], e)
        return None

    try:
        import requests as _req
        resp = _req.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 128,
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "image",
                                "source": {
                                    "type": "base64",
                                    "media_type": "image/jpeg",
                                    "data": b64,
                                },
                            },
                            {"type": "text", "text": _CLAUDE_INTERIOR_PROMPT},
                        ],
                    }
                ],
            },
            timeout=45.0,
        )
        resp.raise_for_status()
        data = resp.json()
        raw_text = data["content"][0]["text"].strip()
    except Exception as e:
        logger.warning("Interior vision: Claude API call failed: %s", e)
        return None

    try:
        import re as _re
        m = _re.search(r"\{[\s\S]*\}", raw_text)
        parsed = _json.loads(m.group(0)) if m else _json.loads(raw_text)
        bucket = str(parsed.get("interior_color_bucket") or "other").strip().lower()
        if bucket not in _INTERIOR_BUCKET_ALLOWLIST:
            bucket = "other"
        try:
            confidence = float(parsed.get("confidence", 0.5))
        except (TypeError, ValueError):
            confidence = 0.5
        confidence = max(0.0, min(1.0, confidence))
        return {
            "interior_color_bucket": bucket,
            "confidence": confidence,
            "raw": raw_text,
        }
    except Exception as e:
        logger.debug("Interior vision: JSON parse failed (%s) — raw: %s", e, raw_text[:200])
        return None


def run_interior_vision_for_vins(
    vins: list[str],
    *,
    skip_if_interior_present: bool = False,
) -> dict[str, Any]:
    """Run Claude Haiku interior/cabin color inference for each VIN.

    Prefers a URL whose path suggests an interior/cabin shot; otherwise falls back to the first
    listing image (through-windows inference).

    When ``skip_if_interior_present`` is true, rows with a non-empty dealer ``interior_color`` are
    skipped. Post-scan callers keep the default ``False`` so gallery passes can still refresh
    buckets / provenance when a listing already had interior text.
    """
    from backend.db.inventory_db import get_car_by_vin, refresh_car_data_quality_score, update_car_row_partial
    from backend.utils.field_clean import is_effectively_empty
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
        vision_result = None
        for primary, inference_ctx in candidates:
            vision_result = _analyze_interior_with_claude(primary)
            if vision_result:
                # Normalize to the schema that build_updates_from_llava_result expects
                bucket = vision_result.get("interior_color_bucket", "other")
                vision_result = {
                    "interior_buckets": [bucket],
                    "interior_guess_text": bucket.title() if bucket != "other" else "",
                    "confidence": vision_result.get("confidence", 0.5),
                    "evidence": vision_result.get("raw", "")[:160],
                    "model": "claude-haiku-4-5-20251001",
                    "inference_context": inference_ctx,
                }
                break
        if not vision_result:
            stats["rows_skipped"] += 1
            reasons["vision_failed"] = reasons.get("vision_failed", 0) + 1
            continue
        patch = build_updates_from_llava_result(row=row, llava=vision_result)
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


def run_post_scan(
    scanned_vins: list[str],
    *,
    post_repair: bool,
    post_listing_description: bool,
    post_interior_vision: bool,
    post_enrich: bool,
    post_enrich_vision_only: bool,
    post_window_sticker: bool = True,
    post_gallery_vision: bool = False,
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
        "window_sticker": None,
        "gallery_vision": None,
        "gallery_recovery": None,
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

    if post_window_sticker and scanned_vins:
        logger.info("Post-scan OEM window stickers: %d VIN(s)", len(scanned_vins))
        summary["window_sticker"] = run_window_sticker_for_vins(scanned_vins)
        ws = summary["window_sticker"]
        logger.info(
            "Post-scan window stickers done: attempted=%s stored=%s reanalyzed=%s already_cached=%s failed=%s no_oem=%s",
            ws.get("attempted"),
            ws.get("stored"),
            ws.get("reanalyzed"),
            ws.get("already_cached"),
            ws.get("failed"),
            ws.get("no_oem_endpoint"),
        )
    elif post_window_sticker:
        logger.info("Post-scan window stickers skipped (no VINs in this run)")

    if post_interior_vision and scanned_vins:
        logger.info("Post-scan interior cabin vision (Claude Haiku): %d VIN(s)", len(scanned_vins))
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

    if post_gallery_vision and scanned_vins and post_gallery_recovery_env_enabled():
        logger.info("Post-scan gallery HTML recovery (thin galleries): %d VIN(s)", len(scanned_vins))
        summary["gallery_recovery"] = run_gallery_recovery_for_vins(scanned_vins)
        gr = summary["gallery_recovery"]
        logger.info(
            "Post-scan gallery recovery done: rows_found=%s rows_updated=%s rows_recovered=%s urls_added=%s",
            gr.get("rows_found"),
            gr.get("rows_updated"),
            gr.get("rows_recovered"),
            gr.get("urls_added_total"),
        )

    if post_gallery_vision and scanned_vins:
        logger.info("Post-scan gallery vision (Claude Haiku): %d VIN(s)", len(scanned_vins))
        summary["gallery_vision"] = run_gallery_vision_for_vins(scanned_vins)
        gv = summary["gallery_vision"]
        logger.info(
            "Post-scan gallery vision done: rows_found=%s rows_updated=%s dropped=%s unique_urls %s→%s",
            gv.get("rows_found"),
            gv.get("rows_updated"),
            gv.get("gallery_vision_unique_dropped"),
            gv.get("gallery_vision_unique_before"),
            gv.get("gallery_vision_unique_after"),
        )
    elif post_gallery_vision:
        logger.info("Post-scan gallery vision skipped (no VINs in this run)")

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

    return summary
