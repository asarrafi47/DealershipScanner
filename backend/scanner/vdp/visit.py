"""
Single-VDP visit: navigate, capture network/DOM signals, merge into one vehicle row.

Split out of ``backend/scanner/vdp/core.py`` (2026-09). ``_vdp_visit_one`` is the 777-line
per-VDP worker (moved as one atomic unit, with its ~11 lazy in-function imports left in place to
avoid reintroducing the circular imports the original author was avoiding); ``_detach_response_handler``
is its small Playwright-response-listener teardown helper. The module logger and network-capture
size caps (``MAX_JSON_BYTES``, ``MAX_NETWORK_ROWS``) live here since this is what actually uses
them. Re-imported from ``backend.scanner.vdp.core`` for compatibility with the historical import
surface.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
from typing import Any

from backend.parsers.base import harvest_image_urls_from_json
from backend.utils.analytics_ep import (
    log_exterior_downgrade_skip,
    merge_analytics_ep_into_vehicle,
    normalize_ep_field_aliases,
)
from backend.utils.gallery_merge import merge_vdp_gallery_into_vehicle
from backend.utils.spec_provenance import merge_spec_source_json
from backend.scanner.utils.vdp_gallery_urls import merge_https_url_batches
from backend.scanner.utils.gallery_url_filter import filter_vdp_gallery_urls

from backend.scanner.vdp.browser_js import PAGE_EXTRACT_JS
from backend.scanner.vdp.config import (
    _nav_timeout_ms,
    _settle_ms,
    _vdp_download_images_enabled,
    _vdp_gallery_skip_if_feed_ge,
    _vdp_response_text_timeout_sec,
)
from backend.scanner.vdp.extract import (
    _analyze_json_signals,
    _build_fragments_from_vdp_capture,
    _combine_ep_fragments,
    _merge_vdp_sticker_url,
    _merge_vdp_vehicle_history_url,
    _response_maybe_gallery_image_url,
    _response_origin,
    _vdp_count_gallery_signals,
    _vdp_wants_json_network_capture,
)
from backend.scanner.vdp.gallery import (
    _download_vdp_gallery_images,
    _drain_pending_tasks,
    _vdp_gallery_interaction_loop,
    _vdp_mouse_jitter,
    _vdp_page_evaluate,
)
from backend.scanner.vdp.price_hints import _apply_vdp_price_hints
from backend.scanner.vdp.spin_capture import _vdp_spin_capture
from backend.scanner.vdp.vdp_recipes import record_candidate as record_vdp_recipe_candidate

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
            # Remember the endpoint itself: templated by VIN/stock it is replayable
            # over HTTP for every other car on the lot (vdp_recipes).
            try:
                req = response.request
                try:
                    req_headers = await req.all_headers()
                except Exception:
                    req_headers = dict(getattr(req, "headers", None) or {})
                record_vdp_recipe_candidate(
                    dealer_name,
                    url=response.url or "",
                    method=str(getattr(req, "method", "GET") or "GET"),
                    post_data=getattr(req, "post_data", None),
                    headers=req_headers,
                    vin=vin,
                    stock=str(v.get("stock_number") or ""),
                    score=float(score or 0),
                    ep_count=len(ep_objs or []),
                    image_count=len(urls_from_images or []),
                )
            except Exception:
                pass
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
            log.debug("VDP network capture task failed: %s", exc)

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
