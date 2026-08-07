"""
VDP (vehicle detail page) enrichment for the Playwright scanner (scanner.py).

This module does not start Playwright; the ``page`` passed in is the same as the main scanner
browser, which (when ``playwright_stealth`` is installed) is created via
``Stealth().use_async(async_playwright())`` in ``scanner.py``.

Runs during the main scan when SCANNER_VDP_EP_MAX > 0 or SCANNER_VDP_PRICE_MAX > 0: visits up to N
unique listing (VDP) URLs per phase — EP/gallery budget plus optional extra URLs for rows missing price.
per dealer, extracts analytics ep.*, network JSON, JSON-LD, inline JSON, and DOM heuristics,
then merges into vehicle rows via merge_analytics_ep_into_vehicle (conservative fallback).
Gallery URLs from network JSON and in-page extraction are merged separately (see
backend.utils.gallery_merge.merge_vdp_gallery_into_vehicle) after EP merge.

Gallery interaction (after load + ``PAGE_EXTRACT_JS`` settle): ``GALLERY_COLLECT_URLS_JS`` runs
on the main document and in each frame (SpinCar/Impel iframes, etc.; cross-origin frames are
skipped) in a loop while advancing the carousel (ArrowRight → next/chevron locators → thumbnails)
until no new unique HTTPS URLs appear for ``SCANNER_VDP_GALLERY_IDLE_ROUNDS`` rounds or
``SCANNER_VDP_GALLERY_MAX_ROUNDS``. A short randomized pointer move runs before the gallery loop
to nudge React/lazy clients. Network image URLs use ``Content-Type`` (e.g. ``image/webp``) not only
URL extensions; JSON bodies are parsed when ``Content-Type`` is JSON-like or ``text/plain`` (e.g. GraphQL).
Image ``response`` URLs merge with DOM harvest.

``SCANNER_VDP_GALLERY_OPEN_LIGHTBOX`` (default ``1``): before the gallery URL harvest loop, try
Playwright clicks to open the same full-screen photo modal a user would (e.g. “N of M Photos”,
“View all photos”); when the modal is open, ``GALLERY_COLLECT_URLS_JS`` prefers ``[role=dialog]`` /
``[aria-modal]`` and large image regions over harvesting every ``img`` on the page. Set to ``0`` to
use legacy whole-document harvest only. Validate manually on a DMS with a lightbox, or with
``python -c "from scanner_vdp import GALLERY_COLLECT_URLS_JS; assert 'pickGalleryRoot' in GALLERY_COLLECT_URLS_JS"``.

Local VDP image download (default **off**): gallery bytes are saved under
``SCANNER_VDP_IMAGE_DOWNLOAD_DIR`` (default ``vdp_images`` in the process cwd), keyed by VIN with
optional ``SCANNER_VDP_IMAGE_DOWNLOAD_KEY=vin|stock`` (default ``vin``). A ``manifest.json`` is
written per vehicle folder; a compact summary is merged into ``spec_source_json`` (``vdp_gallery_local``).
Serving and enrichment read the dealer's remote URLs from ``cars.gallery``, never these bytes, so
copies are kept only for one-off local analysis: set ``SCANNER_VDP_DOWNLOAD_IMAGES=1`` to opt in.
Reuses ``SCANNER_VDP_NAV_TIMEOUT_MS``, ``SCANNER_VDP_SETTLE_MS``, ``SCANNER_MAX_VDP_CONCURRENCY``.

VDP price hints (JSON-LD ``offers``, ``dataLayer`` keys like ``internetPrice`` / ``salePrice``, light
DOM) merge into ``price`` only when the listing has no positive price; provenance is stored under
``spec_source_json`` key ``vdp_price`` when applied (see ``backend.scanner.database.upsert_vehicles``).

Split (2026-08): env knobs live in ``vdp.config``, queue scoring in ``vdp.queue``, capture
analysis / EP fragments in ``vdp.extract``, gallery interaction in ``vdp.gallery``, page JS in
``vdp.browser_js``. Everything is re-imported here so the historical import surface
(``from backend.scanner.vdp.core import X`` and package-level ``from backend.scanner.vdp import X``)
is unchanged.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import random
import re
from collections import Counter
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from backend.parsers.base import harvest_image_urls_from_json, inventory_gallery_max
from backend.utils.analytics_ep import (
    log_exterior_downgrade_skip,
    merge_analytics_ep_into_vehicle,
    normalize_ep_field_aliases,
)
from backend.utils.gallery_merge import (
    gallery_https_bin_histogram,
    merge_vdp_gallery_into_vehicle,
)
from backend.utils.spec_provenance import merge_spec_source_json
from backend.scanner.utils.vdp_gallery_urls import merge_https_url_batches
from backend.scanner.utils.gallery_url_filter import filter_vdp_gallery_urls
from backend.scanner.utils.vdp_price_merge import (
    listing_price_is_empty,
    merge_vdp_price_into_vehicle,
    pick_vdp_price_from_hints,
)

from backend.scanner.vdp.browser_js import (  # noqa: F401
    GALLERY_COLLECT_URLS_JS,
    GALLERY_MODAL_NUDGE_JS,
    PAGE_EXTRACT_JS,
)

# Re-exports: these moved into sibling leaf modules in the 2026-08 split but stay
# importable from core (and, via __init__, from the package) for compatibility.
# Env-var reads inside them stay lazy (functions read os.environ at call time).
from backend.scanner.vdp.config import (  # noqa: F401
    _gallery_idle_rounds,
    _gallery_max_rounds,
    _max_vdp_concurrency,
    _nav_timeout_ms,
    _settle_ms,
    _vdp_description_max_per_dealer,
    _vdp_download_images_enabled,
    _vdp_drain_pending_timeout_sec,
    _vdp_gallery_loop_max_sec,
    _vdp_gallery_min_https,
    _vdp_gallery_open_lightbox_enabled,
    _vdp_gallery_priority_enabled,
    _vdp_gallery_skip_if_feed_ge,
    _vdp_image_download_dir,
    _vdp_js_timeout_ms,
    _vdp_max_per_dealer,
    _vdp_price_max_per_dealer,
    _vdp_response_text_timeout_sec,
    _vdp_spec_gap_max_per_dealer,
    _vdp_spin_capture_enabled,
    _vdp_spin_max_sec,
)
from backend.scanner.vdp.extract import (  # noqa: F401
    PRIORITY,
    VEHICLE_SIGNAL_KEYS,
    _GENERIC_VHR_VIN_ONLY,
    _analyze_json_signals,
    _build_fragments_from_vdp_capture,
    _combine_ep_fragments,
    _dom_specs_to_ep,
    _is_generic_vhr_vin_only_url,
    _ld_to_ep,
    _looks_like_vin17,
    _merge_vdp_sticker_url,
    _merge_vdp_vehicle_history_url,
    _pick_best_sticker_url,
    _pick_best_vehicle_history_url,
    _pick_vehicle_like_object,
    _response_maybe_gallery_image_url,
    _response_origin,
    _string_quality,
    _vdp_count_gallery_signals,
    _vdp_wants_json_network_capture,
)
from backend.scanner.vdp.gallery import (  # noqa: F401
    _VDP_LIGHTBOX_OPEN_TIMEOUT_MS,
    _download_vdp_gallery_images,
    _drain_pending_tasks,
    _vdp_evaluate_gallery_all_frames,
    _vdp_gallery_interaction_loop,
    _vdp_gallery_step_advance,
    _vdp_image_download_key,
    _vdp_mouse_jitter,
    _vdp_page_evaluate,
    _vdp_try_open_photo_lightbox,
)
from backend.scanner.vdp.queue import (  # noqa: F401
    _count_https_gallery_urls,
    _vdp_field_gap_score,
    _vdp_gallery_thin_boost,
    _vdp_public_incomplete_gap_score,
    _vdp_queue_sort_key,
    _vdp_rotation_enabled,
    _vdp_rotation_seed,
    _vdp_rotation_tie_hash,
    _vdp_visit_priority_tuple,
    _vehicle_needs_description_vdp,
    _vehicle_needs_spec_gap_vdp,
)

log = logging.getLogger("scanner.vdp")

MAX_JSON_BYTES = 2 * 1024 * 1024

MAX_NETWORK_ROWS = 45


def _detach_response_handler(page: Any, handler: Any) -> None:
    """Playwright Python builds differ; detach without assuming a specific API name."""
    for meth_name in ("off", "remove_listener", "removeListener"):
        meth = getattr(page, meth_name, None)
        if callable(meth):
            try:
                meth("response", handler)
                return
            except Exception:
                continue


def _apply_vdp_price_hints(
    vehicle: dict[str, Any],
    bundle: dict[str, Any] | None,
    detail_url: str,
) -> dict[str, Any]:
    if not isinstance(bundle, dict):
        return {"updated": False}
    hints = [h for h in (bundle.get("vdpPriceHints") or []) if isinstance(h, dict)]
    picked, meta = pick_vdp_price_from_hints(hints)
    src = (meta or {}).get("source") or "vdp"
    diag = merge_vdp_price_into_vehicle(vehicle, picked, provenance_source=str(src), detail_url=detail_url or "")
    if diag.get("updated"):
        vehicle["spec_source_json"] = merge_spec_source_json(
            vehicle.get("spec_source_json"),
            {
                "vdp_price": {
                    "source": "vdp_scan",
                    "origin": str(src)[:120],
                    "value": diag.get("value"),
                    "url": (detail_url or "")[:500],
                }
            },
        )
    return diag


async def _vdp_spin_capture(
    wp: Any,
    *,
    spin_asset_urls: list[str],
    spin_config_urls: list[str],
    pending: list[asyncio.Task[Any]],
) -> dict[str, Any]:
    """
    Resolve the 360-spin assets for the current VDP into
    ``{"spin_frames": [...>=8 ordered URLs], "interior_pano": str|None, "spin_source": str}``
    (empty dict when no spin viewer / no confident capture). Strategy, cheapest first:

    1. observed network frames already form a numbered sequence — use them;
    2. Impel manifest (``api.impel.io/spin/{customer}/{vin}``) — URL observed in the viewer's
       own traffic or derived from any ``swipetospin-viewers/...`` asset / iframe hash; frames
       are built from ``cdn_image_prefix`` + ``numImgEC`` and the first/last frame is validated
       with a cookie-carrying request before trusting;
    3. light nudge — scroll the viewer iframe into view and drag across it with the page mouse
       (frames load lazily on rotation for some embeds), then re-read observed URLs.
    """
    from backend.scanner.vdp.spin_capture import (
        SPIN_PROVIDER_TOKENS,
        build_impel_manifest_url_candidates,
        extract_spin_assets,
        parse_impel_spin_manifest,
    )

    if not _vdp_spin_capture_enabled():
        return {}
    deadline = asyncio.get_running_loop().time() + _vdp_spin_max_sec()

    iframe_urls: list[str] = []
    for fr in list(getattr(wp, "frames", None) or []):
        fu = getattr(fr, "url", "") or ""
        if fu and any(t in fu.lower() for t in SPIN_PROVIDER_TOKENS):
            iframe_urls.append(fu)

    if not spin_asset_urls and not spin_config_urls and not iframe_urls:
        return {}

    assets = extract_spin_assets(spin_asset_urls)
    if len(assets.get("spin_frames") or []) >= 8:
        assets["spin_source"] = "network_observed"
        return assets

    # Manifest route (Impel).
    req = wp.context.request
    for murl in build_impel_manifest_url_candidates(spin_config_urls, spin_asset_urls, iframe_urls):
        if asyncio.get_running_loop().time() >= deadline:
            break
        try:
            resp = await req.get(murl, timeout=8000)
            if resp.status != 200:
                continue
            data = json.loads(await resp.text())
        except Exception:
            continue
        parsed = parse_impel_spin_manifest(data)
        frames = parsed.get("spin_frames") or []
        if len(frames) < 8:
            continue
        ok = True
        for probe_u in (frames[0], frames[-1]):
            try:
                r2 = await req.get(probe_u, timeout=8000)
                ct2 = (r2.headers.get("content-type") or "").lower()
                if r2.status != 200 or "image/" not in ct2:
                    ok = False
                    break
            except Exception:
                ok = False
                break
        if ok:
            out: dict[str, Any] = {
                "spin_frames": frames,
                "spin_source": "impel_manifest",
                "spin_manifest_url": murl[:300],
            }
            if assets.get("interior_pano"):
                out["interior_pano"] = assets["interior_pano"]
            return out

    # Nudge route: scroll the viewer iframe into view and drag across it.
    if iframe_urls and asyncio.get_running_loop().time() < deadline:
        box = None
        try:
            loc = wp.locator(
                'iframe[src*="impel"], iframe[src*="spincar"], '
                'iframe[src*="swipetospin"], iframe[src*="webrotate"]'
            ).first
            if await loc.count() > 0:
                try:
                    await loc.scroll_into_view_if_needed(timeout=3000)
                except Exception:
                    pass
                await asyncio.sleep(0.6)
                box = await loc.bounding_box()
        except Exception:
            box = None
        if box and box.get("width", 0) >= 120 and box.get("height", 0) >= 90:
            cy = box["y"] + box["height"] / 2
            for _ in range(2):
                if asyncio.get_running_loop().time() >= deadline:
                    break
                try:
                    await wp.mouse.move(box["x"] + box["width"] * 0.72, cy, steps=3)
                    await wp.mouse.down()
                    await wp.mouse.move(box["x"] + box["width"] * 0.28, cy, steps=14)
                    await wp.mouse.up()
                except Exception:
                    break
                await asyncio.sleep(1.0)
                await _drain_pending_tasks(pending, timeout_sec=4.0)
                assets = extract_spin_assets(spin_asset_urls)
                if len(assets.get("spin_frames") or []) >= 8:
                    break
        else:
            # iframe present but not interactable — wait for any passive loads to settle
            await asyncio.sleep(0.5)
            await _drain_pending_tasks(pending, timeout_sec=4.0)
            assets = extract_spin_assets(spin_asset_urls)

    if len(assets.get("spin_frames") or []) >= 8 or assets.get("interior_pano"):
        assets["spin_source"] = "interaction"
        return assets
    return {}


async def _vdp_visit_one(
    wp: Any,
    dealer_name: str,
    v: dict[str, Any],
    u: str,
    vin: str,
    preview_lock: asyncio.Lock,
    preview_budget: list[int],
    *,
    site_profile: Any = None,
    provider: str = "",
) -> dict[str, Any]:
    """
    Visit one VDP URL on *wp*, merge analytics into *v*. Isolated per page (safe for parallel workers).

    The body is organized into per-phase inner helpers that close over the shared capture
    state: network capture-drain (``capture_response``/``on_response``), nav+extract
    (``_nav_and_extract_bundle``), gallery loop (``_gallery_loop_phase``), and the merge
    phases (``_spin_phase`` / ``_merge_ep_phase`` / ``_assemble_gallery_phase`` /
    ``_merge_dom_extras_phase`` / ``_claude_extract_phase``).
    """
    out: dict[str, Any] = {
        "visited": 0,
        "enriched": False,
        "filled": [],
        "skipped": [],
        "gallery_added": 0,
        "price_updated": False,
    }
    visit_epoch = [0]
    network_rows: list[dict[str, Any]] = []
    response_image_urls: list[str] = []
    # 360-spin viewer traffic (Impel/SpinCar/WebRotate): images and config/manifest JSON URLs.
    spin_asset_urls: list[str] = []
    spin_config_urls: list[str] = []
    pending: list[asyncio.Task[Any]] = []
    from backend.scanner.vdp.spin_capture import (
        is_spin_provider_url as _is_spin_provider_url,
        is_spin_reserved_url as _is_spin_reserved_url,
        spin_url_key as _spin_url_key,
    )

    # ----------------------- capture-drain phase ---------------------------

    async def capture_response(response) -> None:
        my_epoch = visit_epoch[0]
        try:
            if response.status != 200:
                return
            ct = (response.headers.get("content-type") or "").lower()
            url = (response.url or "").strip()
            if _response_maybe_gallery_image_url(url, ct):
                if visit_epoch[0] != my_epoch:
                    return
                if url.lower().startswith("https://"):
                    if _is_spin_provider_url(url):
                        # All spin-provider images feed spin-frame detection; frame/pano
                        # assets (reserved paths) are kept OUT of the gallery stream, while
                        # closeups etc. keep flowing to the gallery as today.
                        spin_asset_urls.append(url[:900])
                        if not _is_spin_reserved_url(url):
                            response_image_urls.append(url[:900])
                    else:
                        response_image_urls.append(url[:900])
                return
            if not _vdp_wants_json_network_capture(ct):
                return
            if _is_spin_provider_url(url) and len(spin_config_urls) < 12:
                if visit_epoch[0] == my_epoch and url.lower().startswith("https://"):
                    spin_config_urls.append(url[:900])
            try:
                text = await asyncio.wait_for(
                    response.text(),
                    timeout=_vdp_response_text_timeout_sec(),
                )
            except (asyncio.TimeoutError, Exception):
                return
            if visit_epoch[0] != my_epoch:
                return
            if not text or len(text) > MAX_JSON_BYTES:
                return
            img_hint = bool(
                re.search(
                    r"vehiclePhotos|vehiclephotos|vehicleImages|\"images\"\s*:\s*\[|imageUrls|imageurls|"
                    r"photoList|photoUrl|mediaUrl|primaryImage|dealerinspire|pictures\.dealer|getvehicleimage|"
                    r"spin|gallery|carousel|cdn\..*\.(jpe?g|png|webp)",
                    text,
                    re.I,
                )
            )
            if '"ep"' not in text and '"vin"' not in text.lower():
                if not re.search(
                    r"driveLine|drive_train|drivetrain|driveline|transmission|fuelType|cityFuelEfficiency",
                    text,
                    re.I,
                ):
                    if not img_hint:
                        return
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError:
                return
            score, key_hits, ep_objs = _analyze_json_signals(parsed)
            origin = _response_origin(response.url or "")
            urls_from_images = harvest_image_urls_from_json(parsed, origin, max_urls=96)
            if not urls_from_images and score < 6 and not ep_objs:
                return
            if (
                not ep_objs
                and score < 6
                and len(urls_from_images) < 3
                and not img_hint
            ):
                return
            network_rows.append(
                {
                    "url": (response.url or "")[:500],
                    "score": score,
                    "key_hits": key_hits[:20],
                    "ep_objects": ep_objs,
                    "parsed": parsed,
                    "image_urls": urls_from_images,
                }
            )
            if len(network_rows) > MAX_NETWORK_ROWS:
                network_rows.pop(0)
        except Exception:
            return

    def on_response(response: Any) -> None:
        task = asyncio.create_task(capture_response(response))

        def _absorb_playwright_race(t: asyncio.Task) -> None:
            if t.cancelled():
                return
            try:
                exc = t.exception()
            except asyncio.CancelledError:
                return
            if exc is None:
                return
            name = type(exc).__name__
            msg = str(exc)
            if name in ("Error", "TargetClosedError") and (
                "getResponseBody" in msg or "Target page, context or browser has been closed" in msg
            ):
                return
            logger.debug("VDP network capture task failed: %s", exc)

        task.add_done_callback(_absorb_playwright_race)
        pending.append(task)

    # ------------------------------ nav phase ------------------------------

    def _build_urls_to_try() -> list[str]:
        urls_to_try: list[str] = [u]
        # autoWALL serves color/specs via div.row>div.col on the first (correct) VDP URL.
        # Alternate URL patterns are Dealer.com-style and return no useful data — skip them.
        if provider != "autowall":
            alts = v.get("_detail_url_alternates")
            if isinstance(alts, list):
                for a in alts:
                    if isinstance(a, str):
                        au = a.strip()
                        if au.startswith("http") and au not in urls_to_try:
                            urls_to_try.append(au)
                    if len(urls_to_try) >= 6:
                        break
        return urls_to_try

    async def _nav_and_extract_bundle(try_url: str) -> Any:
        """Navigate to *try_url*, settle, drain capture tasks, then run ``PAGE_EXTRACT_JS``."""
        nav_err = None
        try:
            await wp.goto(try_url, wait_until="domcontentloaded", timeout=_nav_timeout_ms())
        except Exception as e:
            nav_err = str(e)
        await asyncio.sleep(_settle_ms() / 1000.0)
        await _drain_pending_tasks(pending)

        if nav_err:
            log.warning("VDP: %s — navigation issue: %s", dealer_name, nav_err[:120])

        try:
            bundle = await _vdp_page_evaluate(wp, PAGE_EXTRACT_JS)
        except Exception as e:
            bundle = {"error": str(e)}
        return bundle

    # -------------------------- gallery-loop phase -------------------------

    async def _gallery_loop_phase() -> None:
        """Mouse jitter, then the carousel harvest loop (or the feed-gallery skip), then flush."""
        nonlocal extra_loop_gallery
        try:
            await _vdp_mouse_jitter(wp)
        except Exception:
            pass
        if _skip_gallery_loop:
            extra_loop_gallery = []
            log.info(
                "VDP: %s — skipping carousel harvest for VIN %s (feed gallery=%d >= %d)",
                dealer_name, vin[:17], _feed_gallery_count, _gallery_skip_ge,
            )
        else:
            try:
                extra_loop_gallery = await _vdp_gallery_interaction_loop(
                    wp,
                    settle_ms=_settle_ms(),
                    response_image_urls=response_image_urls,
                    pending=pending,
                    site_profile=site_profile,
                    provider=provider,
                )
            except Exception as e:
                log.warning("VDP: %s — gallery interaction loop: %s", dealer_name, str(e)[:160])
        await _drain_pending_tasks(pending)
        # Flush images loaded between the last carousel snapshot and loop-end into
        # extra_loop_gallery (still carousel context), then clear so post-carousel
        # lazy-loads (related vehicles, marketing tiles) are not mixed in.
        _loop_seen: set[str] = set(extra_loop_gallery)
        for _img_u in response_image_urls:
            if isinstance(_img_u, str) and _img_u not in _loop_seen:
                extra_loop_gallery.append(_img_u)
                _loop_seen.add(_img_u)
        response_image_urls.clear()

    async def _log_extract_preview(bundle: Any) -> None:
        async with preview_lock:
            if preview_budget[0] > 0 and isinstance(bundle, dict):
                preview_budget[0] -= 1
                idx = 2 - preview_budget[0]
                ed = bundle.get("extractDebug") or {}
                log.info(
                    "VDP: %s — extract raw preview (%d/2) dataLayer_len=%s nested_ep=%s flat_vehicle=%s inline_ep_JSON=%s",
                    dealer_name,
                    idx,
                    ed.get("dataLayerLength"),
                    ed.get("dataLayerEpCount"),
                    ed.get("dataLayerFlatCount"),
                    ed.get("inlineEpParseCount"),
                )
                log.info(
                    "VDP: %s — dataLayer event names (sample): %s",
                    dealer_name,
                    (ed.get("dataLayerEvents") or [])[:14],
                )
                log.info(
                    "VDP: %s — analytics rows (event | keys): %s",
                    dealer_name,
                    (ed.get("analyticsEventKeys") or [])[:8],
                )
                log.info(
                    "VDP: %s — dataLayer top-level key samples (first rows): %s",
                    dealer_name,
                    (ed.get("dataLayerRowTopKeys") or [])[:4],
                )
                log.info(
                    "VDP: %s — inline script ep key candidates: %s",
                    dealer_name,
                    ed.get("inlineKeySamples"),
                )

    def _combine_phase(bundle: Any) -> tuple[dict[str, Any], int]:
        frags, hits, _berr = _build_fragments_from_vdp_capture(network_rows, bundle, vin)
        if hits:
            log.info("VDP: %s — extractors with data: %s", dealer_name, ", ".join(hits))

        combined_try = normalize_ep_field_aliases(_combine_ep_fragments(frags, vin))
        gsig = _vdp_count_gallery_signals(network_rows, last_bundle) + len(extra_loop_gallery)
        log.info(
            "VDP: %s — combined EP keys for VIN %s: %s (gallery_signal=%d)",
            dealer_name,
            vin[:17],
            sorted(combined_try.keys()),
            gsig,
        )
        return combined_try, gsig

    # ----------------------------- merge phase -----------------------------

    async def _spin_phase() -> tuple[dict[str, Any], list[str], Any, set[str]]:
        # 360-spin capture (Impel/SpinCar/WebRotate): resolve exterior frame sequence + interior
        # pano from the viewer's own traffic/manifest. Runs before gallery assembly so the frame
        # URLs can be excluded from the gallery candidates.
        spin_info: dict[str, Any] = {}
        try:
            spin_info = await _vdp_spin_capture(
                wp,
                spin_asset_urls=spin_asset_urls,
                spin_config_urls=spin_config_urls,
                pending=pending,
            )
        except Exception as _spin_err:
            log.debug("VDP spin capture failed for %s: %s", vin[:17], _spin_err)
        spin_frames_found = [
            u for u in (spin_info.get("spin_frames") or []) if isinstance(u, str) and u
        ]
        interior_pano_found = spin_info.get("interior_pano")
        spin_gallery_exclude: set[str] = {
            _spin_url_key(u) for u in spin_frames_found
        }
        if isinstance(interior_pano_found, str) and interior_pano_found:
            spin_gallery_exclude.add(_spin_url_key(interior_pano_found))
        return spin_info, spin_frames_found, interior_pano_found, spin_gallery_exclude

    def _merge_ep_phase() -> list[str]:
        filled: list[str] = []
        if combined_ep:
            log_exterior_downgrade_skip(v, combined_ep, log, "vdp_combined")
            diag: dict[str, Any] = {}
            filled = merge_analytics_ep_into_vehicle(v, combined_ep, diagnostics=diag)
            out["filled"] = list(filled)
            out["skipped"] = list(diag.get("skipped") or [])
            log.info(
                "VDP: %s — merge diagnostics VIN %s | ep_keys=%s | filled=%s | eligible=%s | skipped=%s",
                dealer_name,
                vin[:17],
                diag.get("ep_keys"),
                diag.get("filled"),
                diag.get("eligible"),
                diag.get("skipped"),
            )
        else:
            out["filled"] = []
            out["skipped"] = []

        if not v.get("source_url"):
            v["source_url"] = success_u
        return filled

    async def _assemble_gallery_phase(filled: list[str], spin_gallery_exclude: set[str]) -> dict[str, Any]:
        cand_gallery: list[str] = []
        gseen: set[str] = set()
        from backend.scanner.scan_efficiency import vdp_gallery_carousel_only, vdp_gallery_url_max

        mx_cap = vdp_gallery_url_max()
        carousel_only = vdp_gallery_carousel_only()

        def _push_g(batch: list[str]) -> None:
            # Keep 360-spin viewer assets (numbered frame sequences, panos) out of the gallery;
            # spin-provider closeups still pass (they are regular photos).
            batch = [
                b
                for b in batch
                if isinstance(b, str)
                and _spin_url_key(b) not in spin_gallery_exclude
                and not _is_spin_reserved_url(b)
            ]
            merge_https_url_batches(cand_gallery, gseen, batch, max_total=mx_cap)

        if isinstance(last_bundle, dict):
            if not carousel_only:
                _push_g([u2 for u2 in (last_bundle.get("domGalleryUrls") or []) if isinstance(u2, str)])
                _push_g([u2 for u2 in (last_bundle.get("jsonGalleryUrls") or []) if isinstance(u2, str)])
        if not carousel_only:
            for row in network_rows:
                _push_g([u2 for u2 in (row.get("image_urls") or []) if isinstance(u2, str)])
        _push_g(extra_loop_gallery)
        # response_image_urls was flushed into extra_loop_gallery right after the carousel
        # loop ended and then cleared — so this is a no-op (carousel-scope lock).
        _push_g(list(response_image_urls))
        cand_gallery = filter_vdp_gallery_urls(cand_gallery)

        # HTTP/HTML gallery recovery when Playwright harvest is still thin
        try:
            from backend.scanner.vdp.html_recovery import (
                count_https_gallery_urls,
                recover_from_detail_page,
                thin_gallery_threshold,
                vdp_html_recovery_enabled,
            )

            if vdp_html_recovery_enabled() and count_https_gallery_urls(cand_gallery) <= thin_gallery_threshold():
                rec = await asyncio.to_thread(
                    recover_from_detail_page,
                    success_u,
                    existing_gallery=cand_gallery,
                )
                rec_urls = rec.get("gallery_urls") or []
                if rec_urls:
                    _push_g([u for u in rec_urls if isinstance(u, str)])
                    out["html_gallery_recovery"] = {
                        "added": rec.get("added"),
                        "before": rec.get("before_count"),
                        "after": rec.get("after_count"),
                    }
                rec_desc = rec.get("description")
                if isinstance(rec_desc, str) and rec_desc.strip() and not v.get("description"):
                    v["description"] = rec_desc.strip()[:2000]
                    if "description" not in filled:
                        filled.append("description")
                rec_specs = rec.get("specs") if isinstance(rec.get("specs"), dict) else {}
                for sk in (
                    "engine_description",
                    "transmission",
                    "drivetrain",
                    "fuel_type",
                    "body_style",
                    "mpg_city",
                    "mpg_highway",
                    "cylinders",
                ):
                    if rec_specs.get(sk) and not v.get(sk):
                        v[sk] = rec_specs[sk]
                        if sk not in filled:
                            filled.append(sk)
                rec_colors = rec.get("colors") if isinstance(rec.get("colors"), dict) else {}
                for ck in ("exterior_color", "interior_color"):
                    if rec_colors.get(ck) and not v.get(ck):
                        v[ck] = str(rec_colors[ck])[:120]
                        if ck not in filled:
                            filled.append(ck)
        except Exception as _hre:
            log.debug("VDP HTML gallery recovery failed for %s: %s", vin[:17], _hre)

        # Inline gallery vision: filter images with Claude before merging into vehicle
        if cand_gallery:
            try:
                from backend.vision.claude_vision import filter_gallery_urls_for_vehicle_listing
                from backend.scanner.scan_efficiency import gallery_vision_inline_enabled
                if gallery_vision_inline_enabled():
                    cand_gallery = await asyncio.to_thread(
                        filter_gallery_urls_for_vehicle_listing,
                        cand_gallery,
                        page_referer=success_u,
                    )
            except Exception as _gv_err:
                log.debug("Inline gallery vision error for %s: %s", vin[:17], _gv_err)

        gmerge = merge_vdp_gallery_into_vehicle(
            v,
            cand_gallery,
            max_gallery=vdp_gallery_url_max(),
        )
        out["gallery_added"] = int(gmerge.get("added") or 0)
        out["gallery_merge_action"] = gmerge.get("action")
        return gmerge

    def _attach_spin_phase(
        spin_info: dict[str, Any],
        spin_frames_found: list[str],
        interior_pano_found: Any,
    ) -> None:
        # Attach 360-spin assets per contract: ordered exterior frames (>=8) and single
        # equirect interior pano. Each scan overwrites with freshly observed URLs — the Impel
        # {stamp} path segment rotates on re-shoots, so stale frames must not be merged.
        if len(spin_frames_found) >= 8:
            v["spin_frames"] = [u[:900] for u in spin_frames_found]
            out["spin_frames"] = len(spin_frames_found)
        if isinstance(interior_pano_found, str) and interior_pano_found:
            v["interior_pano"] = interior_pano_found[:900]
            out["interior_pano"] = True
        if len(spin_frames_found) >= 8 or (isinstance(interior_pano_found, str) and interior_pano_found):
            v["spec_source_json"] = merge_spec_source_json(
                v.get("spec_source_json"),
                {
                    "vdp_spin": {
                        "source": str(spin_info.get("spin_source") or "vdp_scan")[:60],
                        "frames": len(spin_frames_found),
                        "pano": bool(interior_pano_found),
                        "manifest_url": str(spin_info.get("spin_manifest_url") or "")[:300],
                    }
                },
            )
            log.info(
                "VDP: %s — 360 spin captured for VIN %s: %d frame(s), pano=%s (source=%s)",
                dealer_name,
                vin[:17],
                len(spin_frames_found),
                bool(interior_pano_found),
                spin_info.get("spin_source"),
            )

    def _merge_dom_extras_phase(filled: list[str]) -> bool:
        dom_carfax_updated = False
        if isinstance(last_bundle, dict):
            vhr_dom = last_bundle.get("domVehicleHistoryUrls") or []
            dom_carfax_updated = _merge_vdp_vehicle_history_url(v, vhr_dom)
            if dom_carfax_updated:
                filled.append("carfax_url")
                out["filled"] = list(filled)
            sticker_dom = last_bundle.get("domStickerUrls") or []
            if _merge_vdp_sticker_url(v, sticker_dom):
                filled.append("window_sticker_url")
                out["filled"] = list(filled)
            try:
                from backend.scanner.dealer.sticker_provider import note_sticker_signals_from_vdp

                page_html = ""
                if isinstance(last_bundle, dict):
                    snippets = last_bundle.get("domMonroneyTextSnippets") or []
                    if isinstance(snippets, list):
                        page_html = "\n".join(
                            s for s in snippets if isinstance(s, str) and s.strip()
                        )[:12000]
                note_sticker_signals_from_vdp(
                    v,
                    sticker_urls=sticker_dom if isinstance(sticker_dom, list) else [],
                    html=page_html,
                )
            except Exception:
                pass
            snips = last_bundle.get("domMonroneyTextSnippets") or []
            if isinstance(snips, list):
                clean_snips = [s for s in snips if isinstance(s, str) and s.strip()]
                if clean_snips:
                    prev_txt = v.get("_monroney_page_texts")
                    if not isinstance(prev_txt, list):
                        prev_txt = []
                    v["_monroney_page_texts"] = (prev_txt + clean_snips)[:8]

        if isinstance(last_bundle, dict):
            dom_notes = ""
            try:
                from backend.scanner.vdp.packages import (
                    dealer_notes_from_bundle,
                    merge_vdp_packages_into_vehicle,
                )

                if merge_vdp_packages_into_vehicle(v, last_bundle):
                    if "packages" not in filled:
                        filled.append("packages")
                try:
                    from backend.scanner.vdp.specs import merge_spec_sheet_into_vehicle

                    if merge_spec_sheet_into_vehicle(v, last_bundle):
                        if "spec_sheet" not in filled:
                            filled.append("spec_sheet")
                except Exception as _spec_err:
                    log.debug("VDP spec sheet merge failed for %s: %s", vin[:17], _spec_err)
                dom_notes = dealer_notes_from_bundle(last_bundle)
            except Exception as _pkg_err:
                log.debug("VDP packages merge failed for %s: %s", vin[:17], _pkg_err)
                dom_notes = str(last_bundle.get("domDealerNotes") or "").strip()
            dom_desc = dom_notes or str(last_bundle.get("domDescription") or "").strip()
            cur_desc = str(v.get("description") or "").strip()
            if dom_desc and (not cur_desc or (dom_notes and len(dom_notes) > len(cur_desc))):
                try:
                    from backend.utils.listing_description_extract import strip_dealer_description_intro

                    dom_desc = strip_dealer_description_intro(dom_desc)
                except Exception:
                    pass
                v["description"] = dom_desc[:4000]
                if "description" not in filled:
                    filled.append("description")
            if last_bundle.get("domInTransit") is True:
                v["_in_transit"] = True
                v["_availability_status"] = "in_transit"
                v["_availability_source"] = "vdp_dom"
        return dom_carfax_updated

    async def _claude_extract_phase(filled: list[str]) -> None:
        # Claude inline VDP extraction — fills missing fields from visible page text
        try:
            from backend.scanner.vdp.claude_extract import (
                extract_from_page_text,
                should_extract_from_page,
            )
            if should_extract_from_page(v):
                page_text = ""
                try:
                    page_text = await wp.inner_text("body")
                except Exception:
                    # Fallback: use domSpecText from bundle
                    if isinstance(last_bundle, dict):
                        page_text = str(last_bundle.get("domSpecText") or "")
                if page_text:
                    claude_fields = await asyncio.to_thread(
                        extract_from_page_text, page_text, dict(v)
                    )
                    for field, val in claude_fields.items():
                        if field == "packages":
                            # Merge packages list into existing JSON packages blob
                            try:
                                import json as _json
                                existing = v.get("packages")
                                pkg: dict = _json.loads(existing) if existing else {}
                                existing_opts = pkg.get("sticker_options") or []
                                seen = {s.lower() for s in existing_opts}
                                new_opts = [p for p in val if p.lower() not in seen]
                                if new_opts:
                                    pkg.setdefault("vdp_options", [])
                                    pkg["vdp_options"] = list(pkg["vdp_options"]) + new_opts
                                    v["packages"] = _json.dumps(pkg, separators=(",", ":"))
                                    if "packages" not in filled:
                                        filled.append("packages")
                            except Exception:
                                pass
                        elif field == "description" and not v.get("description"):
                            v["description"] = val
                            if "description" not in filled:
                                filled.append("description")
                        elif field == "price" and not v.get("price"):
                            v["price"] = val
                            out["price_updated"] = True
                            if "price" not in filled:
                                filled.append("price")
                        else:
                            if not v.get(field):
                                v[field] = val
                                if field not in filled:
                                    filled.append(field)
                    out["filled"] = list(filled)
        except Exception as _ce:
            log.debug("Claude VDP inline extract error for %s: %s", vin[:17], _ce)

    wp.on("response", on_response)
    urls_to_try = _build_urls_to_try()

    try:
        combined_ep: dict[str, Any] = {}
        success_u = u
        last_bundle: dict[str, Any] = {}
        extra_loop_gallery: list[str] = []

        # Decouple gallery harvest from spec/EP extraction: when the listing feed already
        # provided a sufficient gallery, skip the expensive per-VDP carousel interaction loop
        # (opt-in via SCANNER_VDP_GALLERY_SKIP_IF_FEED_GE; default 0 = always harvest). The
        # feed gallery is preserved — merge only *extends* an existing gallery of >=3 images.
        from backend.scanner.vdp.html_recovery import count_https_gallery_urls as _count_https
        _feed_gallery_urls = list(v.get("gallery") or [])
        _feed_hero = v.get("image_url")
        if isinstance(_feed_hero, str):
            _feed_gallery_urls.append(_feed_hero)
        _feed_gallery_count = _count_https(_feed_gallery_urls)
        _gallery_skip_ge = _vdp_gallery_skip_if_feed_ge()
        _skip_gallery_loop = _gallery_skip_ge > 0 and _feed_gallery_count >= _gallery_skip_ge

        for try_url in urls_to_try:
            visit_epoch[0] += 1
            network_rows.clear()
            response_image_urls.clear()
            spin_asset_urls.clear()
            spin_config_urls.clear()
            out["visited"] = int(out.get("visited") or 0) + 1

            log.info("VDP: %s — visiting %s", dealer_name, try_url[:200])

            bundle = await _nav_and_extract_bundle(try_url)
            last_bundle = bundle if isinstance(bundle, dict) else {}
            if site_profile is not None and isinstance(last_bundle, dict):
                try:
                    from backend.scanner.dealer.location import apply_vdp_location_verdict

                    verdict = apply_vdp_location_verdict(v, last_bundle, site_profile)
                    if verdict == "mismatch":
                        out["skipped"].append("sister_store_location")
                        log.info(
                            "VDP: %s — skipping VIN %s (off-lot location: %s)",
                            dealer_name,
                            vin[:17],
                            (v.get("_lot_location") or "")[:100],
                        )
                        return out
                except Exception as loc_err:
                    log.debug("VDP location check failed for %s: %s", vin[:17], loc_err)
            await _drain_pending_tasks(pending)
            await _gallery_loop_phase()

            await _log_extract_preview(bundle)

            combined_try, gsig = _combine_phase(bundle)
            if combined_try:
                combined_ep = combined_try
                success_u = try_url
                break
            if gsig >= 4:
                combined_ep = {}
                success_u = try_url
                log.info(
                    "VDP: %s — gallery-rich capture without EP fields on %s (signal=%d)",
                    dealer_name,
                    try_url[:120],
                    gsig,
                )
                break
            log.info(
                "VDP: %s — no extractable ep/vehicle fields from %s (trying alternate URL if any)",
                dealer_name,
                try_url[:120],
            )

        img_net = len(
            {u for u in response_image_urls if isinstance(u, str) and u.lower().startswith("https://")}
        )
        g_total = (
            _vdp_count_gallery_signals(network_rows, last_bundle) + len(extra_loop_gallery) + img_net
        )
        dom_vhr: list[Any] = []
        dom_mono: list[Any] = []
        if isinstance(last_bundle, dict):
            dom_vhr = last_bundle.get("domVehicleHistoryUrls") or []
            dom_mono = last_bundle.get("domMonroneyTextSnippets") or []
        has_dom_history = isinstance(dom_vhr, list) and any(
            isinstance(x, str) and x.strip().lower().startswith("http") for x in dom_vhr
        )
        has_mono_text = isinstance(dom_mono, list) and any(
            isinstance(x, str) and len(x.strip()) >= 50 for x in dom_mono
        )
        price_hints_n = 0
        if isinstance(last_bundle, dict):
            price_hints_n = len([h for h in (last_bundle.get("vdpPriceHints") or []) if isinstance(h, dict)])
        if (
            not combined_ep
            and g_total < 2
            and not has_dom_history
            and not has_mono_text
            and price_hints_n == 0
        ):
            log.info(
                "VDP: %s — no extractable ep/vehicle fields and minimal gallery signals after %d URL attempt(s)",
                dealer_name,
                len(urls_to_try),
            )
            return out

        spin_info, spin_frames_found, interior_pano_found, spin_gallery_exclude = await _spin_phase()

        filled = _merge_ep_phase()

        gmerge = await _assemble_gallery_phase(filled, spin_gallery_exclude)

        _attach_spin_phase(spin_info, spin_frames_found, interior_pano_found)

        dom_carfax_updated = _merge_dom_extras_phase(filled)

        pdiag = _apply_vdp_price_hints(v, last_bundle, success_u)
        out["price_updated"] = bool(pdiag.get("updated"))

        await _claude_extract_phase(filled)

        if _vdp_download_images_enabled():
            try:
                await _download_vdp_gallery_images(wp, v, v.get("gallery") if isinstance(v.get("gallery"), list) else [])
            except Exception as e:
                log.warning("VDP: %s — image download: %s", dealer_name, str(e)[:160])

        if filled:
            log.info(
                "VDP: %s — filled %s for VIN %s",
                dealer_name,
                filled,
                vin[:17],
            )
        elif combined_ep:
            log.info(
                "VDP: %s — merge did not add fields (already populated or no gap) for VIN %s",
                dealer_name,
                vin[:17],
            )

        if int(out.get("gallery_added") or 0) > 0 or gmerge.get("action") in ("replace", "extend"):
            log.info(
                "VDP: %s — gallery merge VIN %s action=%s added=%s final_len=%s",
                dealer_name,
                vin[:17],
                gmerge.get("action"),
                gmerge.get("added"),
                gmerge.get("final_len"),
            )
        if (
            filled
            or int(out.get("gallery_added") or 0) > 0
            or out.get("price_updated")
            or dom_carfax_updated
            or int(out.get("spin_frames") or 0) > 0
            or out.get("interior_pano")
        ):
            out["enriched"] = True
        return out
    except Exception as e:
        log.warning("VDP: %s — visit failed for %s: %s", dealer_name, u[:120], e)
        return out
    finally:
        _detach_response_handler(wp, on_response)


def _ripple_vdp_price_same_detail_url(vehicles: list[dict[str, Any]]) -> None:
    """
    After VDP visits, copy ``price`` / ``spec_source_json.vdp_price`` onto sibling rows that share
    ``_detail_url`` but did not receive the browser visit (same URL, different queue positions).
    """
    donors: dict[str, dict[str, Any]] = {}
    for v in vehicles:
        u = (v.get("_detail_url") or "").strip()
        if not u.startswith("http"):
            continue
        cur = donors.get(u)
        if cur is None:
            donors[u] = v
            continue
        if listing_price_is_empty(cur) and not listing_price_is_empty(v):
            donors[u] = v
            continue
        if listing_price_is_empty(v) or listing_price_is_empty(cur):
            continue
        try:
            if float(v.get("price") or 0) > float(cur.get("price") or 0):
                donors[u] = v
        except (TypeError, ValueError):
            pass

    for v in vehicles:
        u = (v.get("_detail_url") or "").strip()
        if not u.startswith("http"):
            continue
        donor = donors.get(u)
        if donor is None or donor is v:
            continue
        if listing_price_is_empty(v) and not listing_price_is_empty(donor):
            v["price"] = donor["price"]
            ds = donor.get("spec_source_json")
            if isinstance(ds, str) and ds.strip():
                try:
                    dj = json.loads(ds)
                    vp = dj.get("vdp_price")
                    if isinstance(vp, dict):
                        v["spec_source_json"] = merge_spec_source_json(
                            v.get("spec_source_json"),
                            {"vdp_price": dict(vp)},
                        )
                except json.JSONDecodeError:
                    pass


async def enrich_vehicles_vdp(
    page,
    vehicles: list[dict[str, Any]],
    dealer_name: str,
    *,
    dealer_id: str = "",
    site_profile: Any = None,
    provider: str = "",
    ep_max: int | None = None,
    price_max: int | None = None,
    description_max: int | None = None,
) -> dict[str, Any]:
    """
    Mutates vehicles in place: runs VDP extraction and merge_analytics_ep_into_vehicle.
    Expects vehicles deduped by VIN; uses _detail_url when present.

    Returns stats for scanner timing: vdps_visited, vehicles_enriched, rows_inventory (input len).
    """
    stats: dict[str, Any] = {
        "vdps_visited": 0,
        "vehicles_enriched": 0,
        "gallery_vdp_urls_added": 0,
        "inventory_rows": len(vehicles),
        "skipped_no_detail_url": False,
        "gallery_phase_bins": {},
    }
    # Caps come from the caller as arguments (per-dealer, race-free); when omitted
    # they fall back to the process env for standalone callers (scripts/tests).
    ep_cap = _vdp_max_per_dealer(ep_max)
    price_cap = _vdp_price_max_per_dealer(price_max)
    spec_gap_cap = _vdp_spec_gap_max_per_dealer()
    description_cap = _vdp_description_max_per_dealer(description_max)
    if ep_cap == 0 and price_cap == 0 and spec_gap_cap == 0 and description_cap == 0:
        log.info(
            "VDP: %s — enrichment skipped (SCANNER_VDP_EP_MAX=0, SCANNER_VDP_PRICE_MAX=0, SCANNER_VDP_SPEC_GAP_MAX=0)",
            dealer_name,
        )
        return stats

    bins_before = gallery_https_bin_histogram(vehicles)
    seed = _vdp_rotation_seed(dealer_id)
    rot = _vdp_rotation_enabled()
    vehicles.sort(key=lambda v: _vdp_queue_sort_key(v, seed, rotation=rot))

    log.info(
        "VDP: %s — enrichment enabled (EP cap=%d, price-extra cap=%d, spec-gap cap=%d, description cap=%d; rotation=%s)",
        dealer_name,
        ep_cap,
        price_cap,
        spec_gap_cap,
        description_cap,
        rot,
    )

    if not any(str(v.get("_detail_url") or "").strip().startswith("http") for v in vehicles):
        log.info(
            "VDP: %s — no _detail_url on inventory rows; skipping VDP visits (listing JSON may omit VDP links)",
            dealer_name,
        )
        stats["skipped_no_detail_url"] = True
        return stats

    work: list[tuple[dict[str, Any], str, str]] = []
    seen_urls: set[str] = set()
    for v in vehicles:
        if len(work) >= ep_cap:
            break
        u = (v.get("_detail_url") or "").strip()
        if not u.startswith("http"):
            continue
        if u in seen_urls:
            continue
        vin = (v.get("vin") or "").strip().upper()
        if not _looks_like_vin17(vin):
            continue
        seen_urls.add(u)
        work.append((v, u, vin))

    if price_cap > 0:
        added = 0
        for v in vehicles:
            if added >= price_cap:
                break
            if not listing_price_is_empty(v):
                continue
            u = (v.get("_detail_url") or "").strip()
            if not u.startswith("http"):
                continue
            if u in seen_urls:
                continue
            vin = (v.get("vin") or "").strip().upper()
            if not _looks_like_vin17(vin):
                continue
            seen_urls.add(u)
            work.append((v, u, vin))
            added += 1

    if spec_gap_cap > 0:
        vehicles.sort(
            key=lambda v: (
                -_vdp_field_gap_score(v),
                -_vdp_public_incomplete_gap_score(v),
            )
        )
        added = 0
        for v in vehicles:
            if added >= spec_gap_cap:
                break
            if not _vehicle_needs_spec_gap_vdp(v):
                continue
            u = (v.get("_detail_url") or "").strip()
            if not u.startswith("http"):
                continue
            if u in seen_urls:
                continue
            vin = (v.get("vin") or "").strip().upper()
            if not _looks_like_vin17(vin):
                continue
            seen_urls.add(u)
            work.append((v, u, vin))
            added += 1

    if description_cap > 0:
        added = 0
        for v in vehicles:
            if added >= description_cap:
                break
            if not _vehicle_needs_description_vdp(v):
                continue
            u = (v.get("_detail_url") or "").strip()
            if not u.startswith("http"):
                continue
            if u in seen_urls:
                continue
            vin = (v.get("vin") or "").strip().upper()
            if not _looks_like_vin17(vin):
                continue
            seen_urls.add(u)
            work.append((v, u, vin))
            added += 1

    if not work:
        return stats

    conc = min(_max_vdp_concurrency(), len(work))
    preview_lock = asyncio.Lock()
    preview_budget = [2]
    field_fill_counter: Counter[str] = Counter()
    skip_reason_counter: Counter[str] = Counter()
    vehicles_enriched = 0
    visited = 0
    gallery_urls_added_total = 0

    log.info("VDP pool: %s — %d concurrent worker page(s) (%d visit(s) queued)", dealer_name, conc, len(work))

    async def aggregate_one(r: dict[str, Any]) -> None:
        nonlocal visited, vehicles_enriched, gallery_urls_added_total
        visited += int(r.get("visited", 0))
        if r.get("enriched"):
            vehicles_enriched += 1
        gallery_urls_added_total += int(r.get("gallery_added") or 0)
        for fn in r.get("filled") or []:
            field_fill_counter[fn] += 1
        if r.get("price_updated"):
            field_fill_counter["price"] += 1
        for sk in r.get("skipped") or []:
            head = sk.split(":", 1)[0].strip() if ":" in str(sk) else str(sk).strip()
            if head:
                skip_reason_counter[head] += 1

    worker_pages: list[Any] = []

    try:
        if conc <= 1:
            for v, u, vin in work:
                try:
                    r = await _vdp_visit_one(
                        page, dealer_name, v, u, vin, preview_lock, preview_budget,
                        site_profile=site_profile, provider=provider,
                    )
                    await aggregate_one(r)
                except Exception as e:
                    log.warning("VDP: %s — visit error (continuing): %s", dealer_name, e)
        else:
            ctx = page.context
            worker_pages = [await ctx.new_page() for _ in range(conc)]
            pool: asyncio.Queue[Any] = asyncio.Queue()
            for wp in worker_pages:
                await pool.put(wp)

            async def run_item(item: tuple[dict[str, Any], str, str]) -> None:
                v, u, vin = item
                wp = await pool.get()
                try:
                    r = await _vdp_visit_one(
                        wp, dealer_name, v, u, vin, preview_lock, preview_budget,
                        site_profile=site_profile, provider=provider,
                    )
                    await aggregate_one(r)
                except Exception as e:
                    log.warning("VDP: %s — visit error (continuing): %s", dealer_name, e)
                finally:
                    await pool.put(wp)

            results = await asyncio.gather(*[run_item(w) for w in work], return_exceptions=True)
            for res in results:
                if isinstance(res, Exception):
                    log.warning("VDP: %s — worker task failed: %s", dealer_name, res)

        _ripple_vdp_price_same_detail_url(vehicles)

        stats["vdps_visited"] = visited
        stats["vehicles_enriched"] = vehicles_enriched
        stats["gallery_vdp_urls_added"] = gallery_urls_added_total
        stats["gallery_phase_bins"] = {
            "before_vdp": bins_before,
            "after_vdp": gallery_https_bin_histogram(vehicles),
        }
        log.info(
            "VDP: %s — phase summary: visits=%d vehicles_enriched=%d gallery_urls_added=%d "
            "top_fields_filled=%s common_skip_reasons=%s gallery_bins_after=%s",
            dealer_name,
            visited,
            vehicles_enriched,
            gallery_urls_added_total,
            field_fill_counter.most_common(14),
            skip_reason_counter.most_common(10),
            stats["gallery_phase_bins"].get("after_vdp"),
        )
    finally:
        for wp in worker_pages:
            try:
                await wp.close()
            except Exception:
                pass

    return stats
