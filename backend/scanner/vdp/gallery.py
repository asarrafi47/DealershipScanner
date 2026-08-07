"""
VDP gallery interaction and image harvest.

Playwright-driving helpers for the per-VDP gallery phase: lightbox opening, carousel
advancing, per-frame ``GALLERY_COLLECT_URLS_JS`` harvest, mouse jitter, pending-task
drain, and the optional local image download. May import ``config``, ``extract`` and
``browser_js``; must not import ``core``.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import random
import re
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from backend.utils.spec_provenance import merge_spec_source_json
from backend.scanner.utils.vdp_gallery_urls import merge_https_url_batches
from backend.scanner.vdp.browser_js import (
    GALLERY_COLLECT_URLS_JS,
    GALLERY_MODAL_NUDGE_JS,
)
from backend.scanner.vdp.config import (
    _gallery_idle_rounds,
    _gallery_max_rounds,
    _nav_timeout_ms,
    _vdp_download_images_enabled,
    _vdp_drain_pending_timeout_sec,
    _vdp_gallery_loop_max_sec,
    _vdp_gallery_open_lightbox_enabled,
    _vdp_image_download_dir,
    _vdp_js_timeout_ms,
)
from backend.scanner.vdp.extract import (
    _looks_like_vin17,
    _response_maybe_gallery_image_url,
)

log = logging.getLogger("scanner.vdp")


async def _vdp_page_evaluate(page_or_frame: Any, js: str, *, timeout_ms: int | None = None) -> Any:
    tmo = (timeout_ms if timeout_ms is not None else _vdp_js_timeout_ms()) / 1000.0
    return await asyncio.wait_for(page_or_frame.evaluate(js), timeout=tmo)


_VDP_LIGHTBOX_OPEN_TIMEOUT_MS = 2500


async def _vdp_try_open_photo_lightbox(wp: Any) -> None:
    """
    Best-effort: click like a user to open the main vehicle photo viewer. Never raises; VDP
    must succeed even if every step fails. On first successful click, briefly waits for paint.
    """
    if not _vdp_gallery_open_lightbox_enabled():
        return
    tmo = int(_VDP_LIGHTBOX_OPEN_TIMEOUT_MS)

    # 1) "1 of 42 Photos" (common inventory widget)
    try:
        n_of = wp.get_by_text(re.compile(r"\d+\s+of\s+\d+\s+photos?", re.I))
        if await n_of.count() > 0:
            await n_of.first.click(timeout=tmo)
            await asyncio.sleep(0.6)
            return
    except Exception:
        pass

    # 1b) "42 Photos" (Sonic / Dealer.com — not always "1 of 42" format)
    for rx in (
        re.compile(r"^\d+\s+photos?\s*$", re.I),
        re.compile(r"^\d+\s*\+\s*photos?\s*$", re.I),
    ):
        try:
            tloc = wp.get_by_text(rx, exact=True)
            if await tloc.count() > 0:
                await tloc.first.click(timeout=tmo)
                await asyncio.sleep(0.6)
                return
        except Exception:
            try:
                tloc2 = wp.get_by_text(rx)
                if await tloc2.count() > 0:
                    await tloc2.first.click(timeout=tmo)
                    await asyncio.sleep(0.6)
                    return
            except Exception:
                pass

    # 2) Common CTAs
    for rx in (
        re.compile(r"(view|see|show)\s+all\s+photos?", re.I),
        re.compile(r"^all\s+photos?\s*$", re.I),
    ):
        try:
            tloc = wp.get_by_text(rx)
            if await tloc.count() > 0:
                await tloc.first.click(timeout=tmo)
                await asyncio.sleep(0.6)
                return
        except Exception:
            pass

    # 3) Buttons / links (photo / gallery)
    for role in ("button", "link"):
        for rx in (
            re.compile(r"(photo|image|picture|slide|gallery)\b", re.I),
            re.compile(r"^more\s+photos", re.I),
        ):
            try:
                rloc = wp.get_by_role(role, name=rx)  # type: ignore[arg-type]
                c = await rloc.count()
                if 0 < c < 20:
                    await rloc.first.click(timeout=tmo)
                    await asyncio.sleep(0.6)
                    return
            except Exception:
                pass

    # 4) Hero / primary gallery image (including DealerOn vhcliaa widget patterns)
    for sel in (
        ".vehicle-image-gallery img",
        ".vehicle-photos img",
        "[class*='vdp-photos'] img",
        "[class*='vehicle-image'] img",
        "[class*='photo-gallery'] img",
        ".photo-gallery img",
        ".gallery img",
        # DealerOn / vhcliaa
        "[class*='vdp-media'] img",
        "[class*='vehicle-gallery'] img",
        "[class*='media-gallery'] img",
        ".vdp-gallery img",
        "[data-gallery] img",
    ):
        try:
            im = wp.locator(sel).first
            if await im.is_visible():
                await im.click(timeout=tmo, force=True)
                await asyncio.sleep(0.6)
                return
        except Exception:
            pass


async def _drain_pending_tasks(pending: list[asyncio.Task[Any]], *, timeout_sec: float | None = None) -> None:
    if not pending:
        return
    tmo = timeout_sec if timeout_sec is not None else _vdp_drain_pending_timeout_sec()
    try:
        await asyncio.wait_for(asyncio.gather(*pending, return_exceptions=True), timeout=tmo)
    except asyncio.TimeoutError:
        n_cancel = 0
        for task in pending:
            if not task.done():
                task.cancel()
                n_cancel += 1
        if n_cancel:
            log.warning(
                "VDP: network capture drain hit %.1fs timeout — cancelled %s straggler task(s)",
                tmo,
                n_cancel,
            )
    pending.clear()


async def _vdp_gallery_step_advance(wp: Any, thumb_rot: list[int]) -> None:
    for sel in (
        ".vehicle-image-gallery",
        ".vehicle-photos",
        ".photo-gallery",
        ".gallery",
        "[class*='photo-gallery']",
        "[class*='image-gallery']",
    ):
        try:
            loc = wp.locator(sel).first
            await loc.click(timeout=500)
            await wp.keyboard.press("ArrowRight")
            await asyncio.sleep(0.05)
            return
        except Exception:
            continue
    try:
        await wp.keyboard.press("ArrowRight")
        await asyncio.sleep(0.05)
    except Exception:
        pass
    next_selectors = [
        'button[aria-label*="next" i]',
        'a[aria-label*="next" i]',
        '[class*="gallery"] button:has-text("Next")',
        ".gallery-next",
        ".swiper-button-next",
        "[class*='chevron-right'][role='button']",
    ]
    for s in next_selectors:
        try:
            loc = wp.locator(s).first
            if await loc.count() > 0:
                await loc.click(timeout=900)
                return
        except Exception:
            continue
    try:
        thumbs = wp.locator(
            ".thumbnail, .thumbnails button, [data-gallery-thumb], "
            ".swiper-slide:not(.swiper-slide-duplicate), li.swiper-slide"
        )
        n = await thumbs.count()
        if n > 1:
            idx = thumb_rot[0] % n
            thumb_rot[0] += 1
            await thumbs.nth(idx).click(timeout=1200)
    except Exception:
        pass


async def _vdp_evaluate_gallery_all_frames(wp: Any) -> list[str]:
    """
    Run ``GALLERY_COLLECT_URLS_JS`` in the main document and in each child frame. Same-origin
    gallery iframes (e.g. some 360 / embed hosts) are included; cross-origin frames raise and are
    skipped.
    """
    merged: list[str] = []
    frames = list(getattr(wp, "frames", None) or [])
    for fr in frames:
        try:
            raw = await _vdp_page_evaluate(fr, GALLERY_COLLECT_URLS_JS)
        except Exception:
            continue
        if not isinstance(raw, list):
            continue
        for x in raw:
            if isinstance(x, str) and x.strip():
                merged.append(x)
    return merged


async def _vdp_mouse_jitter(wp: Any) -> None:
    """Small random pointer moves to nudge lazy galleries and client-side anti-bot heuristics."""
    try:
        view = await wp.evaluate(
            "() => ({ w: Math.max(0, window.innerWidth), h: Math.max(0, window.innerHeight) })"
        )
    except Exception:
        view = {"w": 0, "h": 0}
    wv = int(view.get("w") or 0)
    hv = int(view.get("h") or 0)
    if wv < 2 or hv < 2:
        wv, hv = 800, 600
    for _ in range(2):
        x = random.randint(1, max(1, wv - 1))
        y = random.randint(1, max(1, hv - 1))
        try:
            await wp.mouse.move(x, y, steps=max(1, min(8, 2 + int(random.random() * 5))))
        except Exception:
            break
        await asyncio.sleep(0.03 + random.random() * 0.05)


async def _vdp_gallery_interaction_loop(
    wp: Any,
    *,
    settle_ms: int,
    response_image_urls: list[str],
    pending: list[asyncio.Task[Any]],
    site_profile: Any = None,
    provider: str = "",
) -> list[str]:
    # autoWALL serves all gallery images in the initial network response burst — no carousel
    # lazy-loading to trigger. Skip the interaction loop to avoid burning 60-80 seconds/vehicle.
    if provider == "autowall":
        return []
    ordered: list[str] = []
    seen: set[str] = set()
    stall = 0
    from backend.scanner.scan_efficiency import vdp_gallery_url_max

    cap = vdp_gallery_url_max()
    thumb_rot = [0]
    settle_sleep = min(1200, max(240, int(settle_ms // 4)))
    loop_started = asyncio.get_running_loop().time()
    loop_deadline = loop_started + _vdp_gallery_loop_max_sec(site_profile)
    try:
        await _vdp_try_open_photo_lightbox(wp)
        try:
            await _vdp_page_evaluate(wp, GALLERY_MODAL_NUDGE_JS, timeout_ms=4000)
        except Exception:
            pass
    except Exception:
        pass
    for round_i in range(_gallery_max_rounds()):
        if asyncio.get_running_loop().time() >= loop_deadline:
            log.info(
                "VDP: gallery harvest wall-clock cap %.0fs reached (%s URL(s) collected)",
                _vdp_gallery_loop_max_sec(site_profile),
                len(ordered),
            )
            break
        if round_i > 0 and round_i % 6 == 0:
            try:
                await _vdp_page_evaluate(wp, GALLERY_MODAL_NUDGE_JS, timeout_ms=4000)
            except Exception:
                pass
        snap = list(response_image_urls)
        try:
            dom_batch = await _vdp_evaluate_gallery_all_frames(wp)
        except Exception:
            dom_batch = []
        if not isinstance(dom_batch, list):
            dom_batch = []
        n1 = merge_https_url_batches(ordered, seen, dom_batch, max_total=cap)
        n2 = merge_https_url_batches(ordered, seen, snap, max_total=cap)
        if n1 + n2 == 0:
            stall += 1
            if stall >= _gallery_idle_rounds():
                break
        else:
            stall = 0
        await _vdp_gallery_step_advance(wp, thumb_rot)
        await asyncio.sleep(settle_sleep / 1000.0)
        await _drain_pending_tasks(pending)
    return ordered


def _vdp_image_download_key(vehicle: dict[str, Any]) -> str:
    mode = (os.environ.get("SCANNER_VDP_IMAGE_DOWNLOAD_KEY") or "vin").strip().lower()
    if mode == "stock":
        s = (vehicle.get("stock_number") or "").strip()
        if s:
            return re.sub(r"[^\w.\-]+", "_", s)[:80]
    vin = (vehicle.get("vin") or "").strip().upper()
    if _looks_like_vin17(vin):
        return vin
    s = (vehicle.get("stock_number") or "").strip()
    return re.sub(r"[^\w.\-]+", "_", s)[:80] if s else "unknown"


async def _download_vdp_gallery_images(wp: Any, vehicle: dict[str, Any], urls: list[str]) -> dict[str, Any] | None:
    if not urls or not _vdp_download_images_enabled():
        return None
    dest = _vdp_image_download_dir() / _vdp_image_download_key(vehicle)
    dest.mkdir(parents=True, exist_ok=True)
    manifest: dict[str, Any] = {"files": [], "errors": []}
    req = wp.context.request
    tmo = _nav_timeout_ms()
    from backend.scanner.scan_efficiency import vdp_gallery_url_max

    cap = vdp_gallery_url_max() or 256
    for i, url in enumerate(urls[:cap]):
        if not isinstance(url, str) or not url.lower().startswith("https://"):
            continue
        try:
            resp = await req.get(url, timeout=tmo)
            if resp.status != 200:
                manifest["errors"].append({"url": url[:220], "status": int(resp.status)})
                continue
            ct = (resp.headers.get("content-type") or "").lower()
            if "image/" not in ct and not _response_maybe_gallery_image_url(url, ct):
                manifest["errors"].append({"url": url[:220], "note": "skipped_non_image"})
                continue
            body = await resp.body()
            if not body or len(body) < 80:
                manifest["errors"].append({"url": url[:220], "note": "empty_body"})
                continue
            path = urlparse(url).path or ""
            suf = Path(path).suffix.lower()
            if suf not in (".jpg", ".jpeg", ".png", ".webp", ".gif", ".avif"):
                suf = ".jpg"
            name = f"{i:03d}_{hashlib.sha256(url.encode()).hexdigest()[:14]}{suf}"
            fp = dest / name[:160]
            fp.write_bytes(body)
            manifest["files"].append({"url": url[:900], "path": str(fp)})
        except Exception as e:
            manifest["errors"].append({"url": url[:220], "err": str(e)[:160]})
    try:
        man_path = dest / "manifest.json"
        man_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    except OSError as e:
        log.warning("VDP: manifest write failed: %s", e)
    vehicle["spec_source_json"] = merge_spec_source_json(
        vehicle.get("spec_source_json"),
        {
            "vdp_gallery_local": {
                "source": "vdp_download",
                "dir": str(dest.resolve()),
                "saved": len(manifest.get("files") or []),
                "errors": len(manifest.get("errors") or []),
            }
        },
    )
    return manifest
