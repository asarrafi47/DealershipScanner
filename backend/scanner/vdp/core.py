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

Local VDP image download (default **on**): gallery bytes are saved under
``SCANNER_VDP_IMAGE_DOWNLOAD_DIR`` (default ``vdp_images`` in the process cwd), keyed by VIN with
optional ``SCANNER_VDP_IMAGE_DOWNLOAD_KEY=vin|stock`` (default ``vin``). A ``manifest.json`` is
written per vehicle folder; a compact summary is merged into ``spec_source_json`` (``vdp_gallery_local``).
Set ``SCANNER_VDP_DOWNLOAD_IMAGES=0`` to disable. Reuses ``SCANNER_VDP_NAV_TIMEOUT_MS``,
``SCANNER_VDP_SETTLE_MS``, ``SCANNER_MAX_VDP_CONCURRENCY``.

VDP price hints (JSON-LD ``offers``, ``dataLayer`` keys like ``internetPrice`` / ``salePrice``, light
DOM) merge into ``price`` only when the listing has no positive price; provenance is stored under
``spec_source_json`` key ``vdp_price`` when applied (see ``backend.scanner.database.upsert_vehicles``).
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

log = logging.getLogger("scanner.vdp")

MAX_JSON_BYTES = 2 * 1024 * 1024

_GENERIC_VHR_VIN_ONLY = re.compile(
    r"^https?://vhr\.carfax\.com/main\?vin=[0-9a-z]+(&format=\w+)?$",
    re.I,
)


def _is_generic_vhr_vin_only_url(url: str) -> bool:
    u = (url or "").strip()
    return bool(u) and bool(_GENERIC_VHR_VIN_ONLY.match(u))


def _pick_best_vehicle_history_url(candidates: list[Any]) -> str | None:
    """
    Prefer dealer-provided Carfax / AutoCheck / partner URLs (absolute https) from DOM or JSON.
    """
    good: list[str] = []
    seen: set[str] = set()
    for raw in candidates:
        if not isinstance(raw, str):
            continue
        s = raw.strip()
        if not s.lower().startswith("http"):
            continue
        if "javascript:" in s.lower():
            continue
        low = s.lower()
        if "carfax" not in low and "autocheck" not in low:
            continue
        if s in seen:
            continue
        seen.add(s)
        good.append(s[:900])
    if not good:
        return None

    def score(u: str) -> tuple[int, int]:
        low = u.lower()
        sc = 0
        if "partner" in low or "dealer" in low or "token" in low or "pid=" in low or "otp=" in low:
            sc += 6
        if "vhr.carfax.com" in low and not _is_generic_vhr_vin_only_url(u):
            sc += 4
        if "report" in low or "vehiclehistory" in low or "displayhistory" in low:
            sc += 2
        if _is_generic_vhr_vin_only_url(u):
            sc -= 3
        return (sc, len(u))

    good.sort(key=lambda u: score(u), reverse=True)
    return good[0]


def _merge_vdp_vehicle_history_url(vehicle: dict[str, Any], dom_urls: list[Any]) -> bool:
    """Set ``carfax_url`` from *dom_urls* when it improves on the listing JSON link. Returns True if updated."""
    picked = _pick_best_vehicle_history_url(dom_urls)
    if not picked:
        return False
    cur = str(vehicle.get("carfax_url") or "").strip()
    if not cur.lower().startswith("http"):
        vehicle["carfax_url"] = picked
        return True
    if _is_generic_vhr_vin_only_url(cur) and not _is_generic_vhr_vin_only_url(picked):
        vehicle["carfax_url"] = picked
        return True
    if len(picked) > len(cur) + 12 and ("partner" in picked.lower() or "token" in picked.lower()):
        vehicle["carfax_url"] = picked
        return True
    return False


def _pick_best_sticker_url(dom_urls: list[Any], vin: str | None = None) -> str | None:
    from backend.scanner.post_scan.window_sticker import pick_best_listing_sticker_url

    urls = [str(u).strip() for u in dom_urls if isinstance(u, str) and str(u).strip().startswith("http")]
    return pick_best_listing_sticker_url(urls, vin=vin)


def _merge_vdp_sticker_url(vehicle: dict[str, Any], dom_urls: list[Any]) -> bool:
    """Set ``window_sticker_url`` from VDP iPacket / Monroney links when missing or improved."""
    picked = _pick_best_sticker_url(dom_urls, str(vehicle.get("vin") or ""))
    if not picked:
        return False
    cur = str(vehicle.get("window_sticker_url") or "").strip()
    if not cur.lower().startswith("http"):
        vehicle["window_sticker_url"] = picked
        return True
    cur_low = cur.lower()
    picked_low = picked.lower()
    if "sticker-puller" in picked_low and "sticker-puller" not in cur_low:
        vehicle["window_sticker_url"] = picked
        return True
    if "token=" in picked_low and "token=" not in cur_low:
        vehicle["window_sticker_url"] = picked
        return True
    return False


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


MAX_NETWORK_ROWS = 45

VEHICLE_SIGNAL_KEYS = frozenset(
    {
        "vin",
        "vinnumber",
        "transmission",
        "transmissiontype",
        "drivetrain",
        "drive_train",
        "drivetype",
        "engine",
        "engine_description",
        "interior_color",
        "exterior_color",
        "fuel_type",
        "fueltype",
        "mpg",
        "city_fuel_economy",
        "highway_fuel_economy",
        "options",
        "features",
        "vehicleid",
        "vehicle_id",
        "chromestyleid",
        "stock_id",
        "stocknumber",
        "mf_year",
        "vehicle_make",
        "vehicle_model",
        "body_style",
        "inventory_type",
        "certified",
        "trim",
        "make",
        "model",
        "year",
        "driveline",
        "enginedescription",
        "cityfuelefficiency",
        "highwayfuelefficiency",
        "exteriorcolor",
        "vehicletransmission",
    }
)

PRIORITY = {
    "dataLayer": 100,
    "dataLayer_flat": 99,
    "inline_ep": 97,
    "network_ep": 85,
    "network_vehicle_json": 55,
    "ld_json": 42,
    "inline_json": 28,
    "dom": 18,
}


def _vdp_field_gap_score(vehicle: dict[str, Any]) -> int:
    """Prefer VDP visits for rows missing many dealer fields (CPO/EV listing gaps)."""
    keys = (
        "transmission",
        "drivetrain",
        "body_style",
        "condition",
        "exterior_color",
        "interior_color",
        "engine_description",
        "description",
    )
    n = 0
    for k in keys:
        val = vehicle.get(k)
        if val is None or (isinstance(val, str) and not str(val).strip()):
            n += 1
    return n


def _vdp_public_incomplete_gap_score(vehicle: dict[str, Any]) -> int:
    """Boost rows that fail the public listings spec sheet (Phase 3 completeness passes)."""
    try:
        from backend.utils.listing_completeness import listing_missing_field_codes

        return len(listing_missing_field_codes(vehicle, for_public_filter=True))
    except Exception:
        return 0


def _vdp_max_per_dealer() -> int:
    raw = (os.environ.get("SCANNER_VDP_EP_MAX") or "10").strip()
    try:
        return max(0, int(raw))
    except ValueError:
        return 10


def _vdp_price_max_per_dealer() -> int:
    """Extra unique listing URLs for rows still missing price after inventory JSON (aligned with Node ``scanner.js``)."""
    raw = (os.environ.get("SCANNER_VDP_PRICE_MAX") or "400").strip()
    try:
        return max(0, min(5000, int(raw)))
    except ValueError:
        return 400


def _vdp_spec_gap_max_per_dealer() -> int:
    """Extra VDP visits for inventory rows missing key specs (engine, transmission, …)."""
    from backend.scanner.scan_efficiency import effective_vdp_spec_gap_max

    return effective_vdp_spec_gap_max(10_000)


def _vehicle_needs_spec_gap_vdp(vehicle: dict[str, Any]) -> bool:
    """True when listing JSON left obvious spec gaps worth a targeted VDP visit."""
    for key in (
        "engine_description",
        "transmission",
        "drivetrain",
        "fuel_type",
        "body_style",
    ):
        val = vehicle.get(key)
        if val is None or (isinstance(val, str) and not str(val).strip()):
            return True
    return False


def _vehicle_needs_description_vdp(vehicle: dict[str, Any]) -> bool:
    """True when dealer notes / description are missing or too short."""
    desc = str(vehicle.get("description") or "").strip()
    return len(desc) < 40


def _vdp_description_max_per_dealer() -> int:
    raw = (os.environ.get("SCANNER_VDP_DESCRIPTION_MAX") or "120").strip()
    try:
        return max(0, min(2000, int(raw)))
    except ValueError:
        return 120


def _nav_timeout_ms() -> int:
    raw = (os.environ.get("SCANNER_VDP_NAV_TIMEOUT_MS") or "32000").strip()
    try:
        return max(5000, int(raw))
    except ValueError:
        return 32000


def _settle_ms() -> int:
    raw = (os.environ.get("SCANNER_VDP_SETTLE_MS") or "2200").strip()
    try:
        return max(200, int(raw))
    except ValueError:
        return 2200


def _vdp_js_timeout_ms() -> int:
    """Cap Playwright ``evaluate`` calls (gallery harvest can hang on huge DOM)."""
    raw = (os.environ.get("SCANNER_VDP_JS_TIMEOUT_MS") or "12000").strip()
    try:
        return max(2000, min(120000, int(raw)))
    except ValueError:
        return 12000


def _vdp_response_text_timeout_sec() -> float:
    raw = (os.environ.get("SCANNER_VDP_RESPONSE_TEXT_TIMEOUT_SEC") or "8").strip()
    try:
        return max(1.0, min(60.0, float(raw)))
    except ValueError:
        return 8.0


def _vdp_drain_pending_timeout_sec() -> float:
    raw = (os.environ.get("SCANNER_VDP_DRAIN_PENDING_TIMEOUT_SEC") or "12").strip()
    try:
        return max(2.0, min(120.0, float(raw)))
    except ValueError:
        return 12.0


def _vdp_gallery_loop_max_sec(site_profile: Any = None) -> float:
    """Wall-clock cap per VDP gallery carousel harvest (URL count stays uncapped)."""
    opt = ""
    if isinstance(site_profile, dict):
        opt = str(site_profile.get("optimize_for") or "").strip().lower()
    if opt == "bmw":
        raw = (os.environ.get("SCANNER_VDP_GALLERY_MAX_SEC_BMW") or "300").strip()
    else:
        raw = (os.environ.get("SCANNER_VDP_GALLERY_MAX_SEC") or "150").strip()
    try:
        return max(30.0, min(600.0, float(raw)))
    except ValueError:
        return 300.0 if opt == "bmw" else 150.0


async def _vdp_page_evaluate(page_or_frame: Any, js: str, *, timeout_ms: int | None = None) -> Any:
    tmo = (timeout_ms if timeout_ms is not None else _vdp_js_timeout_ms()) / 1000.0
    return await asyncio.wait_for(page_or_frame.evaluate(js), timeout=tmo)


def _max_vdp_concurrency() -> int:
    from backend.scanner.scan_efficiency import effective_vdp_concurrency

    return effective_vdp_concurrency()


def _vdp_gallery_min_https() -> int:
    try:
        return max(1, int((os.environ.get("SCANNER_VDP_GALLERY_MIN_HTTPS") or "3").strip()))
    except ValueError:
        return 3


def _vdp_gallery_skip_if_feed_ge() -> int:
    """
    Skip the per-VDP carousel interaction loop when the listing feed already supplied
    at least this many HTTPS gallery images for the vehicle.

    The carousel harvest loop is the dominant VDP cost (up to the wall-clock cap per
    vehicle). On platforms whose listing feed already returns full galleries
    (DealerOn cosmos, Dealer.com, eProcess results API, Algolia), that harvest only
    *extends* an already-good gallery, so it can be skipped while still doing the cheap
    nav + EP/spec/price extraction.

    Default ``0`` = disabled (always harvest — current behaviour preserved exactly).
    Set e.g. ``SCANNER_VDP_GALLERY_SKIP_IF_FEED_GE=8`` to enable.
    """
    raw = (os.environ.get("SCANNER_VDP_GALLERY_SKIP_IF_FEED_GE") or "0").strip()
    try:
        return max(0, int(raw))
    except ValueError:
        return 0


def _vdp_gallery_priority_enabled() -> bool:
    return (os.environ.get("SCANNER_VDP_GALLERY_PRIORITY") or "1").strip().lower() not in (
        "0",
        "false",
        "no",
        "off",
    )


def _gallery_max_rounds() -> int:
    try:
        return max(4, min(120, int((os.environ.get("SCANNER_VDP_GALLERY_MAX_ROUNDS") or "80").strip())))
    except ValueError:
        return 80


def _gallery_idle_rounds() -> int:
    try:
        return max(1, min(20, int((os.environ.get("SCANNER_VDP_GALLERY_IDLE_ROUNDS") or "3").strip())))
    except ValueError:
        return 3


def _vdp_gallery_open_lightbox_enabled() -> bool:
    """When truthy, try to open the dealer photo lightbox before scoped DOM gallery harvest (default: on)."""
    return (os.environ.get("SCANNER_VDP_GALLERY_OPEN_LIGHTBOX") or "1").strip().lower() not in (
        "0",
        "false",
        "no",
        "off",
    )


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


GALLERY_MODAL_NUDGE_JS = r"""
() => {
  let n = 0;
  try {
    const d = document.querySelector('[role="dialog"], [aria-modal="true"]');
    if (d) {
      d.scrollTop = d.scrollHeight;
      d.scrollTo(0, d.scrollHeight);
      n += 1;
    }
  } catch (e1) {}
  try {
    const wr = document.querySelector(
      ".swiper-wrapper, [class*='swiper-wrapper'], [class*='thumbnails'], [class*='vdp-media'], [class*='vehicle-gallery'], [data-gallery]"
    );
    if (wr) {
      wr.scrollLeft = wr.scrollWidth;
      wr.scrollTo(wr.scrollWidth, 0);
      n += 1;
    }
  } catch (e2) {}
  try {
    const tb = document.querySelector("[class*='thumbnail'], [class*='thumbs']");
    if (tb) {
      tb.scrollLeft = tb.scrollWidth;
      n += 1;
    }
  } catch (e3) {}
  return n;
}
"""


def _vdp_download_images_enabled() -> bool:
    """Default: download VDP gallery images to disk; set ``SCANNER_VDP_DOWNLOAD_IMAGES=0`` to skip."""
    raw = (os.environ.get("SCANNER_VDP_DOWNLOAD_IMAGES") or "1").strip().lower()
    return raw not in ("0", "false", "no", "off", "")


def _vdp_image_download_dir() -> Path:
    raw = (os.environ.get("SCANNER_VDP_IMAGE_DOWNLOAD_DIR") or "").strip()
    if raw:
        return Path(raw).expanduser()
    return Path(__file__).resolve().parents[2] / "vdp_images"


def _response_maybe_gallery_image_url(url: str, content_type: str) -> bool:
    """
    True when a response is likely a vehicle-gallery image. Prefer ``Content-Type`` (many CDNs
    serve ``?fmt=webp`` and similar with no file extension in the path).
    """
    u = (url or "").strip()
    if not u.lower().startswith("https://"):
        return False
    ct = (content_type or "").lower().split(";")[0].strip()
    if ct in (
        "image/jpeg",
        "image/jpg",
        "image/pjpeg",
        "image/png",
        "image/webp",
        "image/avif",
        "image/gif",
    ):
        return True
    if ct.startswith("image/") and "svg" not in ct and "x-icon" not in ct and "vnd" not in ct:
        return True
    low = u.lower()
    if re.search(r"\.(jpe?g|png|webp|gif|avif)(\?|#|$)", low):
        return True
    for frag in (
        "/image/",
        "/images/",
        "/photos/",
        "/media/",
        "/inventory/",
        "cloudinary",
        "dealerinspire",
        "dealer.com",
        "carsforsale",
        "inventoryphoto",
        "vehiclephoto",
    ):
        if frag in low:
            return True
    return False


def _vdp_wants_json_network_capture(content_type: str) -> bool:
    """True when a response body may be JSON (including GraphQL with ``text/plain``)."""
    c = (content_type or "").strip().lower()
    if not c:
        return False
    if c.startswith("image/") or c.startswith("video/") or c.startswith("audio/"):
        return False
    if c.startswith("text/css"):
        return False
    if c.startswith("text/html") and "json" not in c:
        return False
    if c.startswith("text/javascript") or "text/javascript" in c:
        return False
    if c == "application/javascript" or c.startswith("application/x-javascript"):
        return False
    if c.startswith("text/plain"):
        return True
    if "json" in c or "+json" in c:
        return True
    return False


def _response_origin(url: str) -> str:
    try:
        p = urlparse(url or "")
        if p.scheme and p.netloc:
            return f"{p.scheme}://{p.netloc}/"
    except (ValueError, TypeError):
        pass
    return "https:///"


def _count_https_gallery_urls(vehicle: dict[str, Any]) -> int:
    seen: set[str] = set()
    n = 0
    g = vehicle.get("gallery")
    if isinstance(g, list):
        for u in g:
            if isinstance(u, str) and u.strip().lower().startswith("https://") and u not in seen:
                seen.add(u)
                n += 1
    iu = vehicle.get("image_url")
    if isinstance(iu, str) and iu.strip().lower().startswith("https://") and iu not in seen:
        n += 1
    return n


def _vdp_gallery_thin_boost(vehicle: dict[str, Any]) -> int:
    """Higher score → higher priority for limited VDP budget when gallery is thin."""
    if not _vdp_gallery_priority_enabled():
        return 0
    have = _count_https_gallery_urls(vehicle)
    need = _vdp_gallery_min_https()
    if have >= need:
        return 0
    return (need - have) * 5


def _vdp_visit_priority_tuple(vehicle: dict[str, Any]) -> tuple[int, int, int]:
    """Sort key: public-incomplete boost, gallery-thin boost, then field-gap score."""
    pub_gap = _vdp_public_incomplete_gap_score(vehicle)
    field_gap = _vdp_field_gap_score(vehicle)
    thin = _vdp_gallery_thin_boost(vehicle)
    # Weight public spec gaps heavily so Phase 3 visits colors/transmission first.
    return (pub_gap * 10 + thin + field_gap, pub_gap, field_gap)


def _vdp_rotation_enabled() -> bool:
    return (os.environ.get("SCANNER_VDP_ROTATION") or "1").strip().lower() not in (
        "0",
        "false",
        "no",
        "off",
    )


def _vdp_rotation_seed(dealer_id: str) -> str:
    explicit = (os.environ.get("SCANNER_VDP_ROTATION_SEED") or "").strip()
    if explicit:
        return explicit
    from datetime import datetime, timezone

    day = datetime.now(timezone.utc).date().isoformat()
    return f"{day}|{(dealer_id or '').strip()}"


def _vdp_rotation_tie_hash(vehicle: dict[str, Any], seed: str) -> int:
    vin = (vehicle.get("vin") or "").strip().upper()
    digest = hashlib.blake2b(f"{seed}\0{vin}".encode(), digest_size=6, usedforsecurity=False).digest()
    return int.from_bytes(digest, "big")


def _vdp_queue_sort_key(vehicle: dict[str, Any], seed: str, *, rotation: bool) -> tuple[Any, ...]:
    """Descending priority: public-incomplete + field gaps first; tie-break by rotation hash or VIN."""
    t = _vdp_visit_priority_tuple(vehicle)
    if rotation:
        return (-t[0], -t[1], -t[2], _vdp_rotation_tie_hash(vehicle, seed))
    return (-t[0], -t[1], -t[2], (vehicle.get("vin") or "").strip().upper())


def _looks_like_vin17(v: str) -> bool:
    s = (v or "").strip().upper()
    return bool(re.match(r"^[A-HJ-NPR-Z0-9]{17}$", s))


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


def _analyze_json_signals(obj: Any, depth: int = 0) -> tuple[float, list[str], list[dict[str, Any]]]:
    score = 0.0
    hits: list[str] = []
    eps: list[dict[str, Any]] = []
    if obj is None or depth > 18:
        return score, hits, eps
    if isinstance(obj, dict):
        if isinstance(obj.get("ep"), dict):
            eps.append(obj["ep"])
            score += 25
        for k, val in obj.items():
            lk = str(k).replace(" ", "_").lower()
            if lk in VEHICLE_SIGNAL_KEYS:
                if val not in (None, "", [], {}):
                    score += 8
                    hits.append(str(k))
            if isinstance(val, (dict, list)):
                s2, h2, e2 = _analyze_json_signals(val, depth + 1)
                score += s2 * 0.35
                hits.extend(h2)
                eps.extend(e2)
    elif isinstance(obj, list):
        for x in obj:
            s2, h2, e2 = _analyze_json_signals(x, depth + 1)
            score += s2
            hits.extend(h2)
            eps.extend(e2)
    return round(score, 2), hits[:30], eps


def _pick_vehicle_like_object(root: Any, depth: int = 0) -> dict[str, Any] | None:
    if root is None or depth > 14:
        return None
    if isinstance(root, list):
        for x in root:
            p = _pick_vehicle_like_object(x, depth + 1)
            if p:
                return p
        return None
    if not isinstance(root, dict):
        return None
    v = root.get("vin") or root.get("VIN")
    if _looks_like_vin17(str(v or "")):
        return root
    for key in (
        "vehicle",
        "vehicles",
        "inventory",
        "inventoryItem",
        "inventoryItems",
        "vehicleDetail",
        "vehicleDetails",
        "listing",
        "listings",
        "data",
        "result",
        "results",
        "pageData",
        "payload",
    ):
        child = root.get(key)
        if isinstance(child, list) and child:
            p = _pick_vehicle_like_object(child[0], depth + 1)
            if p:
                return p
        elif isinstance(child, dict):
            p = _pick_vehicle_like_object(child, depth + 1)
            if p:
                return p
    keys = list(root.keys())
    lowered = {str(k).replace(" ", "_").lower() for k in keys}
    if len(lowered & VEHICLE_SIGNAL_KEYS) >= 2 and (root.get("vin") or root.get("VIN")) and len(keys) < 120:
        return root
    if len(lowered & VEHICLE_SIGNAL_KEYS) >= 3 and len(keys) < 120:
        return root
    for val in root.values():
        if isinstance(val, (dict, list)):
            p = _pick_vehicle_like_object(val, depth + 1)
            if p:
                return p
    return None


def _string_quality(val: Any) -> float:
    if val is None:
        return 0.0
    if isinstance(val, bool):
        return 5.0
    if isinstance(val, (int, float)):
        return 10.0
    s = str(val).strip()
    if not s or s.lower() in ("na", "n/a", "null"):
        return 0.0
    q = float(min(40, len(s)))
    if len(s.split()) > 1:
        q += 15
    if re.search(r"metallic|pearl|tri-?coat", s, re.I):
        q += 20
    return q


def _combine_ep_fragments(
    fragments: list[tuple[str, dict[str, Any], float]],
    expected_vin: str,
) -> dict[str, Any]:
    """Merge fragment dicts; higher priority wins per field when quality improves."""
    pv = expected_vin.strip().upper()
    merged: dict[str, Any] = {}
    prov: dict[str, str] = {}

    def pri_source(src: str) -> float:
        return float(PRIORITY.get(src.split(":")[0], 10))

    ordered = sorted(
        fragments,
        key=lambda x: (-pri_source(x[0]), -x[2], -_string_quality(next(iter(x[1].values()), ""))),
    )

    for source, ep, score in ordered:
        if not ep:
            continue
        ev = str(ep.get("vin") or ep.get("VIN") or "").strip().upper()
        if pv and ev and ev != pv:
            continue
        for k, val in ep.items():
            if val is None or val == "":
                continue
            prev = merged.get(k)
            pq = _string_quality(prev) if prev is not None else 0.0
            nq = _string_quality(val)
            if prev is None or nq > pq or (nq == pq and pri_source(source) > pri_source(prov.get(k, source))):
                merged[k] = val
                prov[k] = f"{source}({score:.0f})"
    return merged


PAGE_EXTRACT_JS = r"""
() => {
  const result = {
    dataLayerEps: [],
    dataLayerFlatVehicle: [],
    dataLayerRows: 0,
    ldJsonVehicle: [],
    inlineJsonHits: [],
    inlineEpObjects: [],
    domSpecs: {},
    domFeatures: [],
    domBadges: [],
    domGalleryUrls: [],
    jsonGalleryUrls: [],
    domVehicleHistoryUrls: [],
    domStickerUrls: [],
    domMonroneyTextSnippets: [],
    domLocationSnippets: [],
    domDescription: "",
    domDealerNotes: "",
    domPackagesStructured: [],
    domPackagesSections: [],
    domInTransit: false,
    pageTextSample: "",
    vdpPriceHints: [],
    scriptSrcSample: [],
    metaGenerator: "",
    galleryExtractDebug: { photosTabClicked: false, domImgSample: 0 },
    extractDebug: {
      dataLayerLength: 0,
      dataLayerRowTopKeys: [],
      dataLayerEvents: [],
      dataLayerEpCount: 0,
      dataLayerFlatCount: 0,
      inlineEpParseCount: 0,
      inlineKeySamples: [],
      analyticsEventKeys: []
    }
  };
  const epSeen = new Set();
  function pushEp(ep) {
    if (!ep || typeof ep !== "object" || Array.isArray(ep)) return;
    if (epSeen.has(ep)) return;
    epSeen.add(ep);
    result.dataLayerEps.push(ep);
  }
  function walkForEp(obj, depth, seen) {
    if (depth > 18 || !obj || typeof obj !== "object") return;
    if (seen.has(obj)) return;
    seen.add(obj);
    if (obj.ep && typeof obj.ep === "object" && !Array.isArray(obj.ep)) {
      pushEp(obj.ep);
    }
    for (const k of Object.keys(obj)) {
      const v = obj[k];
      if (!v || typeof v !== "object") continue;
      if (Array.isArray(v)) {
        for (const it of v) walkForEp(it, depth + 1, seen);
      } else {
        walkForEp(v, depth + 1, seen);
      }
    }
  }
  function hasKeyHint(keys, hint) {
    const h = hint.replace(/_/g, "").toLowerCase();
    for (const raw of keys) {
      const k = String(raw).replace(/_/g, "").toLowerCase();
      if (k === h) return true;
      if (h.length >= 6 && (k.indexOf(h) >= 0 || h.indexOf(k) >= 0)) return true;
    }
    return false;
  }
  function vehicleLikePrimitiveScore(o) {
    const keys = Object.keys(o);
    let sc = 0;
    const hints = [
      "transmission", "drivetrain", "drivetype", "drive_train", "driveline", "fueltype", "fuel",
      "engine", "bodystyle", "body_style", "vehiclemodel", "vehicle_model",
      "exteriorcolor", "exterior_color", "interiorcolor", "interior_color",
      "cityfueleconomy", "city_fuel_economy", "highwayfueleconomy", "highway_fuel_economy",
      "cityfuelefficiency", "highwayfuelefficiency",
      "inventorytype", "inventory_type", "certified", "trim", "stockid", "stock_id"
    ];
    for (const h of hints) {
      if (hasKeyHint(keys, h)) sc++;
    }
    for (const k of keys) {
      const lk = String(k).replace(/_/g, "").toLowerCase();
      if (lk === "vin" || lk === "vinnumber") sc += 2;
    }
    return sc;
  }
  function parseJsonFromBrace(txt, openBraceIdx) {
    let depth = 0;
    let start = -1;
    const lim = Math.min(openBraceIdx + 90000, txt.length);
    for (let i = openBraceIdx; i < lim; i++) {
      const c = txt[i];
      if (c === "{") {
        if (depth === 0) start = i;
        depth++;
      } else if (c === "}") {
        depth--;
        if (depth === 0 && start >= 0) {
          const chunk = txt.slice(start, i + 1);
          try {
            return JSON.parse(chunk);
          } catch (e) {
            return null;
          }
        }
      }
    }
    return null;
  }
  const flatSig = new Set();
  function extractVehicleFlat(obj, depth, seen, out) {
    if (depth > 16 || !obj || typeof obj !== "object") return;
    if (seen.has(obj)) return;
    seen.add(obj);
    const prims = {};
    let primCount = 0;
    for (const [k, v] of Object.entries(obj)) {
      if (v === null || typeof v === "string" || typeof v === "number" || typeof v === "boolean") {
        if (String(k).length < 120 && (typeof v !== "string" || v.length < 2000)) {
          prims[k] = v;
          primCount++;
        }
      }
    }
    const sc = vehicleLikePrimitiveScore(prims);
    const vin = prims.vin || prims.VIN || prims.vinNumber;
    const hasVin = vin && String(vin).replace(/\s/g, "").length === 17;
    const good =
      (sc >= 3 && primCount >= 3) ||
      (hasVin && sc >= 2 && primCount >= 3) ||
      (sc >= 4 && primCount >= 2);
    if (good) {
      const sig = JSON.stringify(prims);
      if (!flatSig.has(sig) && out.length < 22) {
        flatSig.add(sig);
        out.push(prims);
      }
    }
    for (const k of Object.keys(obj)) {
      const v = obj[k];
      if (!v || typeof v !== "object") continue;
      if (k === "ep") continue;
      if (Array.isArray(v)) {
        for (const it of v) extractVehicleFlat(it, depth + 1, seen, out);
      } else {
        extractVehicleFlat(v, depth + 1, seen, out);
      }
    }
  }
  try {
    if (window.dataLayer && Array.isArray(window.dataLayer)) {
      result.dataLayerRows = window.dataLayer.length;
      result.extractDebug.dataLayerLength = result.dataLayerRows;
      const rowSeen = new WeakSet();
      const flatSeen = new WeakSet();
      for (let i = 0; i < window.dataLayer.length; i++) {
        const row = window.dataLayer[i];
        walkForEp(row, 0, rowSeen);
        extractVehicleFlat(row, 0, flatSeen, result.dataLayerFlatVehicle);
        if (i < 24) {
          try {
            const ev = row && typeof row === "object" && row.event != null ? String(row.event) : "";
            if (ev) result.extractDebug.dataLayerEvents.push(ev.slice(0, 120));
            if (row && typeof row === "object") {
              const ks = Object.keys(row).slice(0, 50);
              result.extractDebug.dataLayerRowTopKeys.push(ks);
              result.extractDebug.analyticsEventKeys.push(
                (ev || "?").slice(0, 90) + " | " + ks.slice(0, 28).join(", ")
              );
            } else {
              result.extractDebug.dataLayerRowTopKeys.push([]);
            }
          } catch (e2) {}
        }
      }
    }
  } catch (e) {}
  result.extractDebug.dataLayerEpCount = result.dataLayerEps.length;
  result.extractDebug.dataLayerFlatCount = result.dataLayerFlatVehicle.length;
  const lds = document.querySelectorAll('script[type="application/ld+json"]');
  for (const s of lds) {
    try {
      const j = JSON.parse(s.textContent || "{}");
      const stack = Array.isArray(j) ? j : [j];
      for (const node of stack) {
        if (!node || typeof node !== "object") continue;
        const t = [].concat(node["@type"] || []);
        const ts = t.map((x) => String(x).toLowerCase());
        const hasVin = !!(node.vehicleIdentificationNumber || node.vin || node.VIN);
        if (
          ts.some((x) => /vehicle|car|automobile/.test(x)) ||
          (hasVin && ts.some((x) => x === "product"))
        ) {
          result.ldJsonVehicle.push(node);
        }
      }
    } catch (e) {}
  }
  const mg = document.querySelector('meta[name="generator"]');
  if (mg && mg.getAttribute("content")) result.metaGenerator = mg.getAttribute("content").slice(0, 200);
  const sscripts = document.querySelectorAll("script[src]");
  for (let i = 0; i < Math.min(sscripts.length, 35); i++) {
    const src = sscripts[i].getAttribute("src") || "";
    if (src) result.scriptSrcSample.push(src.slice(0, 220));
  }
  const inlineScripts = document.querySelectorAll("script:not([src])");
  const inlineKeySamples = new Set();
  const epLiteral = /["']ep["']\s*:\s*\{/g;
  for (const sc of inlineScripts) {
    const txt = (sc.textContent || "").slice(0, 220000);
    if (txt.length < 40) continue;
    if (!/["']ep["']\s*:|ep\.|vehicle_model|drive_train|driveLine|driveline|fuel_type|transmission|drivetrain|fueltype|bodystyle|inventory_type|cityFuelEfficiency|highwayFuelEfficiency|engineDescription/i.test(txt)) continue;
    let em;
    epLiteral.lastIndex = 0;
    while ((em = epLiteral.exec(txt)) !== null && result.inlineEpObjects.length < 14) {
      const braceIdx = txt.indexOf("{", em.index);
      if (braceIdx < 0) continue;
      const parsedEp = parseJsonFromBrace(txt, braceIdx);
      if (parsedEp && typeof parsedEp === "object" && !Array.isArray(parsedEp)) {
        result.inlineEpObjects.push(parsedEp);
        result.extractDebug.inlineEpParseCount++;
        Object.keys(parsedEp).slice(0, 55).forEach((k) => inlineKeySamples.add(k));
      }
    }
    if (/cityFuelEconomy|highwayFuelEconomy|cityFuelEfficiency/i.test(txt) && result.inlineEpObjects.length < 14) {
      const vinFuel = txt.match(/"vin"\\s*:\\s*"([A-HJ-NPR-Z0-9]{17})"/i);
      if (vinFuel && vinFuel.index != null) {
        const start = txt.lastIndexOf("{", vinFuel.index);
        if (start >= 0) {
          const parsedFuel = parseJsonFromBrace(txt, start);
          if (parsedFuel && typeof parsedFuel === "object" && !Array.isArray(parsedFuel)) {
            const hasFuel =
              parsedFuel.cityFuelEconomy != null ||
              parsedFuel.highwayFuelEconomy != null ||
              parsedFuel.cityFuelEfficiency != null ||
              parsedFuel.highwayFuelEfficiency != null;
            if (hasFuel) {
              result.inlineEpObjects.push(parsedFuel);
              result.extractDebug.inlineEpParseCount++;
            }
          }
        }
      }
    }
  }
  result.extractDebug.inlineKeySamples = Array.from(inlineKeySamples).slice(0, 70);
  for (const sc of inlineScripts) {
    const txt = (sc.textContent || "").slice(0, 120000);
    if (txt.length < 80) continue;
    if (!/vin|vehicle|inventory|drivetrain|driveline|driveLine|transmission|["']ep["']/i.test(txt)) continue;
    let parsed = null;
    try {
      const m = txt.match(/\\{\\s*"vin"\\s*:\\s*"[^"]+"/i);
      if (m && m.index != null) {
        let depth = 0;
        let start = -1;
        for (let i = m.index; i < Math.min(m.index + 25000, txt.length); i++) {
          const c = txt[i];
          if (c === "{") {
            if (depth === 0) start = i;
            depth++;
          } else if (c === "}") {
            depth--;
            if (depth === 0 && start >= 0) {
              const chunk = txt.slice(start, i + 1);
              try {
                parsed = JSON.parse(chunk);
                break;
              } catch (e) {
                parsed = null;
              }
            }
          }
        }
      }
    } catch (e) {}
    if (parsed && typeof parsed === "object") {
      result.inlineJsonHits.push(parsed);
      if (result.inlineJsonHits.length >= 5) break;
    }
  }
  // Expand accordion / collapsible spec sections before reading (handles + buttons like cars.com)
  try {
    const accordionTriggers = Array.from(document.querySelectorAll(
      "button[aria-expanded='false'], [role='button'][aria-expanded='false'], " +
      ".accordion-trigger:not(.active), .collapsible:not(.open), " +
      "[class*='accordion'][class*='header'], [class*='toggle'][class*='spec'], " +
      "summary, details:not([open]) > summary"
    ));
    for (const el of accordionTriggers.slice(0, 40)) {
      try {
        const t = (el.textContent || "").trim().toLowerCase();
        if (/spec|feature|convenience|suspension|powertrain|body|safety|seat|entertain|lighting|dimension|equipment|package|option|accessori|standard|dealer notes|included/i.test(t)) {
          el.click();
        }
      } catch (e) {}
    }
  } catch (e) {}

  const specSelectors = [
    "dl", "dl.vehicle-specs", ".vehicle-specs", ".specifications",
    "table.specs", ".vdp-specs", ".spec-list", "[class*='spec-table']",
    "[class*='features-list']", ".vehicle-features",
  ];
  for (const sel of specSelectors) {
    try {
      const els = document.querySelectorAll(sel);
      els.forEach((el) => {
        const rows = el.querySelectorAll("tr, dt, li, [class*='spec-item'], [class*='feature-item']");
        rows.forEach((row) => {
          const label = (row.querySelector("th, dt, .label, .name, [class*='label'], [class*='name']") || row.cells?.[0])?.textContent?.trim();
          const val = (row.querySelector("td, dd, .value, [class*='value']") || row.cells?.[1])?.textContent?.trim();
          if (label && val && label.length < 80 && val.length < 400) {
            result.domSpecs[label.slice(0, 60)] = val.slice(0, 300);
          } else if (!val && label && label.length < 120) {
            // single-column feature list item
            result.domFeatures.push(label.slice(0, 120));
          }
        });
      });
    } catch (e) {}
  }
  // autoWALL / ShopperExpress style: div.row containing exactly two div.col children (label + value)
  try {
    document.querySelectorAll(".row").forEach((row) => {
      const cols = row.querySelectorAll(":scope > .col");
      if (cols.length === 2) {
        const label = (cols[0].textContent || "").trim().replace(/:$/, "");
        const val = (cols[1].textContent || "").trim();
        if (label && val && label.length < 60 && val.length < 200) {
          result.domSpecs[label.slice(0, 60)] = val.slice(0, 300);
        }
      }
    });
  } catch (e) {}
  document.querySelectorAll("[class*='feature'], .features li, ul.features li, [class*='highlight'] li").forEach((el, idx) => {
    if (idx > 100) return;
    const t = (el.textContent || "").trim();
    if (t && t.length < 200) result.domFeatures.push(t);
  });
  document.querySelectorAll(".badge, [class*='badge'], .label-pill, [class*='tag']").forEach((el, idx) => {
    if (idx > 40) return;
    const t = (el.textContent || "").trim();
    if (t && t.length < 120) result.domBadges.push(t);
  });
  try {
    const packageSectionRe = /included packages|packages\\s*&\\s*accessories|packages\\s*&\\s*options|standard features|included options|factory installed|equipment groups|(?:^|\\s)(?:[\\w/&+-]+\\s+)*package\\s*$/i;
    const dealerNotesRe = /^dealer notes\b|^seller notes\b|^dealer comments\b|^about this vehicle\b/i;
    const priceRe = /\\$[\\d,]+(?:\\.\\d{2})?/;
    try {
      const pkgAccordions = Array.from(document.querySelectorAll(
        "h4[aria-expanded='false'], h3[aria-expanded='false'], button[aria-expanded='false'], [role='button'][aria-expanded='false']"
      )).filter((el) => {
        const t = (el.textContent || "").trim().toLowerCase();
        return /package/.test(t) && !/spec|dimension|powertrain|suspension|safety|convenience|entertainment/i.test(t);
      });
      for (const el of pkgAccordions.slice(0, 35)) {
        try { el.click(); } catch (ePkgAcc) {}
      }
      const showAllPkg = Array.from(
        document.querySelectorAll("button, a, [role='button']")
      ).find((el) => /show all package items/i.test((el.textContent || "").trim()));
      if (showAllPkg) showAllPkg.click();
    } catch (eShowAll) {}
    const pkgSeen = new Set();
    function pushPkg(section, name, priceLabel, features) {
      const n = (name || "").trim().replace(/\\s+/g, " ");
      if (!n || n.length < 2 || n.length > 180) return;
      const key = (section + "|" + n + "|" + (priceLabel || "")).toLowerCase();
      if (pkgSeen.has(key)) return;
      pkgSeen.add(key);
      const row = { section: section, name: n, features: (features || []).slice(0, 24) };
      if (priceLabel) row.price_label = priceLabel;
      const pm = (priceLabel || n).match(priceRe);
      if (pm) {
        const num = parseFloat(pm[0].replace(/[$,]/g, ""));
        if (isFinite(num) && num > 0) row.price = Math.round(num);
      }
      result.domPackagesStructured.push(row);
    }
    function sectionRootForHeading(h) {
      return (
        h.closest("section, article, [class*='package'], [class*='option'], [class*='feature'], [class*='equipment'], [class*='accessory']")
        || h.parentElement
      );
    }
    function parsePackageBlock(root, sectionName) {
      if (!root) return;
      const rows = root.querySelectorAll(
        "li, tr, [class*='package-row'], [class*='option-row'], [class*='feature-row'], " +
        "[class*='package-item'], [class*='option-item'], dt, .row"
      );
      rows.forEach((row) => {
        const txt = (row.innerText || row.textContent || "").trim().replace(/\\s+/g, " ");
        if (!txt || txt.length < 3 || txt.length > 500) return;
        const lines = txt.split(/\\n+/).map((x) => x.trim()).filter(Boolean);
        if (!lines.length) return;
        const head = lines[0];
        const pm = head.match(priceRe);
        let name = head;
        let priceLabel = "";
        if (pm) {
          priceLabel = pm[0];
          name = head.replace(priceRe, "").trim();
        }
        if (!name || name.length < 2) return;
        const feats = lines.slice(1).filter((ln) => ln.length >= 2 && ln.length <= 160);
        pushPkg(sectionName, name, priceLabel, feats);
      });
    }
    const headingTags2 = ["h1", "h2", "h3", "h4", "h5", "legend", "button", "[role='button']"];
    for (const tag of headingTags2) {
      document.querySelectorAll(tag).forEach((h) => {
        const label = (h.textContent || "").trim();
        if (!label || label.length > 120) return;
        if (dealerNotesRe.test(label)) {
          const root = sectionRootForHeading(h);
          const body = root ? (root.innerText || root.textContent || "").trim() : "";
          const cleaned = body.replace(new RegExp("^" + label.replace(/[.*+?^${}()|[\\]\\\\]/g, "\\\\$&"), "i"), "").trim();
          if (cleaned.length > (result.domDealerNotes || "").length) {
            result.domDealerNotes = cleaned.slice(0, 4000);
          }
          return;
        }
        if (!packageSectionRe.test(label)) return;
        const sectionName = label.toLowerCase().replace(/\\s+/g, "_").slice(0, 60);
        if (!result.domPackagesSections.includes(sectionName)) {
          result.domPackagesSections.push(sectionName);
        }
        const root = sectionRootForHeading(h);
        parsePackageBlock(root, sectionName);
        let sib = h.nextElementSibling;
        for (let i = 0; i < 3 && sib; i++) {
          parsePackageBlock(sib, sectionName);
          sib = sib.nextElementSibling;
        }
      });
    }
  } catch (ePkg) {}
  try {
    const tabCands = Array.from(
      document.querySelectorAll("a, button, [role='tab'], [data-tab], [data-toggle]")
    ).filter((el) => {
      const t = (el.textContent || "").trim().toLowerCase();
      if (!t || t.length > 28) return false;
      return (
        t === "photos" ||
        t === "pictures" ||
        t === "images" ||
        t === "gallery" ||
        /^photo(s)?$/i.test(t)
      );
    });
    const visible = tabCands.filter((el) => {
      try {
        const st = window.getComputedStyle(el);
        return st.display !== "none" && st.visibility !== "hidden" && el.offsetParent !== null;
      } catch (e) {
        return false;
      }
    });
    if (visible.length === 1) {
      visible[0].click();
      result.galleryExtractDebug.photosTabClicked = true;
    }
  } catch (e) {}
  try {
    window.scrollTo(0, Math.min(3200, (document.body && document.body.scrollHeight) || 0));
  } catch (e) {}
  const imgSeen = new Set();
  function isLikelyVdpJunkImage(el) {
    let cur = el;
    for (let d = 0; d < 10 && cur; d++) {
      const cls = (cur.className && String(cur.className)) || "";
      const cid = (cur.id && String(cur.id)) || "";
      const role = (cur.getAttribute && cur.getAttribute("role")) || "";
      const t = (cls + " " + cid + " " + role).toLowerCase();
      if (
        /(cfx|cfximg|ipacket|i-packet|i_packet|vpp|vehicle-?protec|warrantytile|vpp-|-vpp-|-vpp|carfax-?widget|kbb-?widget|dealer-?feature-?ti|dealer-?ti|value-?your-?trade|as-?is-?disclaim|asistile|recall-?polic|financ|about-?us-?|warranty-?ext|warranty-?flyer|vdp-?tile|quick-?link|vehicle-?broch|apply-?fin|ipocket|i-pocket|mlp-?image|fandi|fi_badge|plan-?overview|certified|bmw-?certified|m-?performance|brand-?logo|dealer-?logo|marketing|promo|social|banner)/i.test(
          t
        )
      ) {
        return true;
      }
      cur = cur.parentElement;
    }
    return false;
  }
  const imgSelectorsSpecific = [
    ".vehicle-image-gallery img",
    ".vehicle-photos img",
    ".gallery img",
    "[class*='photo-gallery'] img",
    "[class*='vehicle-photo'] img",
    "[class*='image-gallery'] img",
    "[class*='media-gallery'] img",
  ];
  for (const sel of imgSelectorsSpecific) {
    try {
      document.querySelectorAll(sel).forEach((el, idx) => {
        if (idx > 160 || result.domGalleryUrls.length >= 96) return;
        if (isLikelyVdpJunkImage(el)) return;
        const s =
          el.getAttribute("src") ||
          el.getAttribute("data-src") ||
          el.getAttribute("data-lazy-src") ||
          el.getAttribute("data-original") ||
          "";
        const t = (s || "").trim();
        if (!/^https?:\/\//i.test(t)) return;
        if (!/\.(jpe?g|png|webp|gif)(\?|$)/i.test(t)) return;
        if (imgSeen.has(t)) return;
        imgSeen.add(t);
        result.domGalleryUrls.push(t.slice(0, 900));
      });
    } catch (e) {}
    if (result.domGalleryUrls.length >= 80) break;
  }
  result.galleryExtractDebug.domImgSample = result.domGalleryUrls.length;
  const jSeen = new Set();
  function pushJsonImg(s) {
    if (!s || typeof s !== "string") return;
    const t = s.trim();
    if (!/^https?:\/\//i.test(t)) return;
    if (!/\.(jpe?g|png|webp|gif)(\?|$)/i.test(t)) return;
    if (jSeen.size >= 96) return;
    if (jSeen.has(t)) return;
    jSeen.add(t);
    result.jsonGalleryUrls.push(t.slice(0, 900));
  }
  function walkJsonImg(o, d) {
    if (d > 14 || !o || typeof o !== "object") return;
    if (Array.isArray(o)) {
      for (const x of o) walkJsonImg(x, d + 1);
      return;
    }
    for (const [k, v] of Object.entries(o)) {
      const lk = String(k).toLowerCase().replace(/_/g, "");
      if (
        /photo|image|media|gallery|spin|thumb|picture|carousel|viewer|asset/i.test(lk) &&
        (typeof v === "string" || Array.isArray(v) || (v && typeof v === "object"))
      ) {
        if (typeof v === "string") pushJsonImg(v);
        else if (Array.isArray(v)) {
          for (const it of v) {
            if (typeof it === "string") pushJsonImg(it);
            else if (it && typeof it === "object") {
              const u =
                it.url || it.URL || it.uri || it.src || it.href || it.large || it.full || it.xlarge;
              if (typeof u === "string") pushJsonImg(u);
            }
          }
        } else if (v && typeof v === "object") {
          const u = v.url || v.URL || v.uri || v.src;
          if (typeof u === "string") pushJsonImg(u);
        }
      }
      if (v && typeof v === "object") walkJsonImg(v, d + 1);
    }
  }
  try {
    if (window.dataLayer && Array.isArray(window.dataLayer)) {
      for (let i = 0; i < Math.min(14, window.dataLayer.length); i++) {
        walkJsonImg(window.dataLayer[i], 0);
      }
    }
  } catch (e) {}
  for (const node of result.inlineJsonHits || []) {
    walkJsonImg(node, 0);
  }
  for (const node of result.ldJsonVehicle || []) {
    walkJsonImg(node, 0);
  }
  function pushPriceHint(raw, source) {
    if (raw === null || raw === undefined) return;
    let num = null;
    if (typeof raw === "number" && isFinite(raw)) {
      num = raw;
    } else if (typeof raw === "string") {
      const t = raw.replace(/[$,]/g, "").trim();
      if (!t || /call|contact|request|quote|inquire/i.test(t)) return;
      const m = t.match(/(\\d{3,7})(?:\\.\\d{2})?/);
      if (m) num = parseFloat(m[1]);
    }
    if (num === null || !isFinite(num) || num < 500 || num > 2500000) return;
    result.vdpPriceHints.push({ value: num, raw: String(raw).slice(0, 60), source: String(source || "?") });
  }
  function walkDataLayerPrice(obj, depth, seen) {
    if (depth > 14 || !obj || typeof obj !== "object" || seen.has(obj)) return;
    seen.add(obj);
    const keys = [
      "internetPrice",
      "InternetPrice",
      "salePrice",
      "SalePrice",
      "sellingPrice",
      "price",
      "Price",
      "vehiclePrice",
      "askingPrice",
      "listPrice",
      "retailPrice",
      "finalPrice",
      "cashPrice",
      "msrp",
      "MSRP",
      "primaryPrice",
      "advertisedPrice",
    ];
    for (const k of keys) {
      if (Object.prototype.hasOwnProperty.call(obj, k)) pushPriceHint(obj[k], "dataLayer:" + k);
    }
    for (const v of Object.values(obj)) {
      if (v && typeof v === "object") walkDataLayerPrice(v, depth + 1, seen);
    }
  }
  try {
    if (window.dataLayer && Array.isArray(window.dataLayer)) {
      const seen = new WeakSet();
      for (let i = 0; i < Math.min(40, window.dataLayer.length); i++) {
        walkDataLayerPrice(window.dataLayer[i], 0, seen);
      }
    }
  } catch (e3) {}
  function offersFromLd(node) {
    const out = [];
    if (!node || typeof node !== "object") return out;
    const o = node.offers || node.offer;
    if (!o) return out;
    return [].concat(o);
  }
  for (const node of result.ldJsonVehicle || []) {
    for (const off of offersFromLd(node)) {
      if (!off || typeof off !== "object") continue;
      const p = off.price || off.Price || (off.priceSpecification && off.priceSpecification.price);
      pushPriceHint(p, "json_ld_offer");
    }
    const p2 = node.price || node.Price;
    if (p2) pushPriceHint(p2, "json_ld_product");
  }
  try {
    document.querySelectorAll('[itemprop="price"],[itemprop=price]').forEach((el, idx) => {
      if (idx > 12) return;
      const c = el.getAttribute("content");
      if (c) pushPriceHint(c, "dom_itemprop");
      else pushPriceHint((el.textContent || "").trim(), "dom_itemprop");
    });
  } catch (e4) {}
  try {
    document.querySelectorAll('meta[property="price"],meta[itemprop="price"]').forEach((el, idx) => {
      if (idx > 10) return;
      const c = el.getAttribute("content");
      if (c) pushPriceHint(c, "dom_meta_price");
    });
  } catch (e4b) {}
  const priceSelectors = [
    ".vehicle-price",
    ".internetPrice",
    ".internet-price",
    ".sale-price",
    ".final-price",
    ".primary-price",
    ".price-value",
    ".pricing-price",
    ".price-block",
    ".highlight-price",
    ".srp-price",
    ".priceDisplay",
    "#vehicle-price",
    "#price",
    "[class*='vehicle-price']",
    "[class*='asking-price']",
    "[class*='list-price']",
    "[data-vehicle-price]",
    "[data-price]",
    "[data-selling-price]",
    "[data-internet-price]",
    "[data-msrp]",
    "[data-final-price]",
    "[data-testid*='price']",
    "[class*='PriceDisplay']",
    "[class*='vehiclePrice']",
  ];
  for (const sel of priceSelectors) {
    try {
      const el = document.querySelector(sel);
      if (!el) continue;
      const t = (el.textContent || "").trim();
      if (t && t.length < 80) pushPriceHint(t, "dom_dealer:" + sel.slice(0, 40));
    } catch (e5) {}
  }
  result.domVehicleHistoryUrls = [];
  result.domStickerUrls = [];
  result.domMonroneyTextSnippets = [];
  function pushStickerUrl(raw) {
    const h = absUrl(raw);
    if (!/^https?:\/\//i.test(h)) return;
    const low = h.toLowerCase();
    if (
      low.indexOf("sticker-puller") >= 0 ||
      low.indexOf("autoipacket.com") >= 0 ||
      low.indexOf("ipacket.com") >= 0 ||
      /monroney|window-sticker|window_sticker/i.test(low) ||
      low.indexOf("/sticker/") >= 0
    ) {
      if (result.domStickerUrls.includes(h)) return;
      result.domStickerUrls.push(h.slice(0, 900));
    }
  }
  try {
    const html = (document.documentElement && document.documentElement.innerHTML) || "";
    const stickerRe = /https?:\/\/[^"'\\s<>]+sticker-puller\/download\/[^"'\\s<>]+/gi;
    let sm;
    while ((sm = stickerRe.exec(html)) !== null && result.domStickerUrls.length < 8) {
      pushStickerUrl(sm[0]);
    }
  } catch (eStickerHtml) {}
  try {
    document
      .querySelectorAll(
        'a[href*="sticker-puller"], a[href*="autoipacket"], a[href*="monroney"], iframe[src*="autoipacket"], iframe[src*="ipacket"], [data-sticker-url], [data-msrp-url]'
      )
      .forEach((el, idx) => {
        if (idx > 40 || result.domStickerUrls.length >= 8) return;
        pushStickerUrl(
          el.getAttribute("href") ||
            el.getAttribute("src") ||
            el.getAttribute("data-sticker-url") ||
            el.getAttribute("data-msrp-url") ||
            ""
        );
      });
  } catch (eStickerDom) {}
  function absUrl(href) {
    try {
      if (!href || typeof href !== "string") return "";
      const t = href.trim();
      if (!t || t.toLowerCase().indexOf("javascript:") === 0) return "";
      const u = new URL(t, document.baseURI);
      return u.href;
    } catch (eAbs) {
      return "";
    }
  }
  try {
    document
      .querySelectorAll(
        'a[href*="carfax"], a[href*="CARFAX"], a[href*="vhr.carfax"], a[href*="autocheck"], a[href*="AutoCheck"], area[href]'
      )
      .forEach((a, idx) => {
        if (idx > 70 || result.domVehicleHistoryUrls.length >= 16) return;
        const h = absUrl(a.getAttribute("href") || "");
        if (!/^https?:\/\//i.test(h)) return;
        const low = h.toLowerCase();
        if (low.indexOf("carfax") < 0 && low.indexOf("autocheck") < 0) return;
        if (result.domVehicleHistoryUrls.includes(h)) return;
        result.domVehicleHistoryUrls.push(h.slice(0, 900));
      });
  } catch (eVhr) {}
  try {
    document.querySelectorAll("[data-carfax-url], [data-carfax-href], [data-vhr-url]").forEach((el, idx) => {
      if (idx > 30 || result.domVehicleHistoryUrls.length >= 18) return;
      const raw =
        el.getAttribute("data-carfax-url") ||
        el.getAttribute("data-carfax-href") ||
        el.getAttribute("data-vhr-url") ||
        "";
      const h = absUrl(raw);
      if (!/^https?:\/\//i.test(h)) return;
      if (result.domVehicleHistoryUrls.includes(h)) return;
      result.domVehicleHistoryUrls.push(h.slice(0, 900));
    });
  } catch (eDa) {}
  let monoBudget = 0;
  const monoSelectors = [
    "[class*='monroney']",
    "[class*='Monroney']",
    "[class*='window-sticker']",
    "[class*='windowSticker']",
    "[class*='WindowSticker']",
    "[id*='monroney']",
    "[id*='Monroney']",
    "[data-widget*='sticker']",
  ];
  for (const sel of monoSelectors) {
    try {
      document.querySelectorAll(sel).forEach((el) => {
        if (monoBudget >= 5 || result.domMonroneyTextSnippets.length >= 5) return;
        const t = (el.textContent || "").trim().replace(/\\s+/g, " ");
        if (t.length < 50 || t.length > 3200) return;
        if (!/engine|trans|equip|option|msrp|vin|standard|included|warranty|drivetrain|fuel/i.test(t)) return;
        result.domMonroneyTextSnippets.push(t.slice(0, 2400));
        monoBudget++;
      });
    } catch (eM) {}
  }
  result.domLocationSnippets = [];
  const locSeen = new Set();
  function pushLocSnippet(t) {
    const s = (t || "").trim().replace(/\\s+/g, " ");
    if (!s || s.length < 4 || s.length > 220) return;
    const k = s.toLowerCase();
    if (locSeen.has(k)) return;
    locSeen.add(k);
    result.domLocationSnippets.push(s);
  }
  try {
    const locSelectors = [
      "[class*='located']",
      "[class*='dealer-loc']",
      "[class*='vehicle-loc']",
      "[class*='lot-loc']",
      "[class*='store-loc']",
      "[data-dealer-name]",
      "[data-location-name]",
      ".dealer-name",
      ".dealerName",
      ".vehicle-location",
      ".inventory-location",
    ];
    for (const sel of locSelectors) {
      document.querySelectorAll(sel).forEach((el, idx) => {
        if (idx > 40 || result.domLocationSnippets.length >= 12) return;
        const t = (el.textContent || el.getAttribute("data-dealer-name") || el.getAttribute("data-location-name") || "").trim();
        if (/locat|dealer|lot|store|at\\s/i.test(t) || t.length < 80) pushLocSnippet(t);
      });
    }
  } catch (eLoc) {}
  try {
    const bodyTxt = ((document.body && document.body.innerText) || "").slice(0, 12000);
    result.pageTextSample = bodyTxt.slice(0, 4000);
    const locRe = /located\\s+at\\s+([^\\n.]{4,120})/gi;
    let lm;
    while ((lm = locRe.exec(bodyTxt)) !== null && result.domLocationSnippets.length < 10) {
      pushLocSnippet(lm[1]);
    }
    const locRe2 = /(?:^|\\n)location\\s*:\\s*([^\\n,|.]{4,120})/gi;
    let lm2;
    while ((lm2 = locRe2.exec(bodyTxt)) !== null && result.domLocationSnippets.length < 10) {
      pushLocSnippet(lm2[1]);
    }
  } catch (eBody) {}
  try {
    function pushDescription(t) {
      const s = (t || "").trim().replace(/\\s+/g, " ");
      if (!s || s.length < 60) return;
      if (!result.domDescription || s.length > result.domDescription.length) {
        result.domDescription = s.slice(0, 4000);
      }
    }
    const descHeadingRe = /^description\b|^dealer notes\b|^seller notes\b|^dealer comments\b/i;
    const headingTags = ["h1", "h2", "h3", "h4", "h5", "h6", "legend", "label"];
    for (const tag of headingTags) {
      document.querySelectorAll(tag).forEach((h) => {
        const label = (h.textContent || "").trim();
        if (!descHeadingRe.test(label)) return;
        let block = h.nextElementSibling;
        for (let i = 0; i < 4 && block; i++) {
          const t = (block.innerText || block.textContent || "").trim();
          if (t.length > 60 && !/^description\\b/i.test(t)) {
            pushDescription(t);
            return;
          }
          block = block.nextElementSibling;
        }
        const section = h.closest("section, article, [class*='description'], [id*='description']");
        if (section) {
          const t = (section.innerText || section.textContent || "").trim();
          if (t.length > 80) pushDescription(t.replace(/^description\\s*/i, ""));
        }
      });
    }
    for (const sel of (
      ".vehicle-description, .vdp-description, [class*='vehicle-description'], "
      + "[class*='vdp-description'], #vehicle-description, .description-content, "
      + "[data-testid*='description'], [class*='Description']"
    ).split(", ")) {
      document.querySelectorAll(sel).forEach((el) => {
        const t = (el.innerText || el.textContent || "").trim();
        if (t.length > 60) pushDescription(t);
      });
    }
  } catch (eDesc) {}
  try {
    const bodyForTransit = ((document.body && document.body.innerText) || "").slice(0, 16000);
    if (
      /vehicle\\s+is\\s+currently\\s+in\\s+transit/i.test(bodyForTransit)
      || /vehicle\\s+in\\s+transit/i.test(bodyForTransit)
      || /\\bin\\s+transit\\b/i.test(bodyForTransit)
    ) {
      result.domInTransit = true;
    }
  } catch (eTransit) {}
  return result;
}
"""


GALLERY_COLLECT_URLS_JS = r"""
() => {
  const out = [];
  const seen = new Set();
  const bgRe = /url\\(\\s*['"]?([^'")\\s>]+)['"]?\\s*\\)/gi;
  const cdnQParam = /[?&](fmt|format|f_auto|w_auto|fit|q|w|h)=/i;
  function mightBeRasterUrl(low) {
    if (/\\.(jpe?g|png|webp|gif|avif)(\\?|#|$)/i.test(low)) return true;
    if (cdnQParam.test(low) && /(image|photo|media|cdn|dealer|inventory|vehicle|res\\.cloudinary|imgix|akamai|spin|impel|cfassets|photobucket)/i.test(low))
      return true;
    if (/(\\/image\\/|\\/images\\/|\\/photos\\/|\\/media\\/|cloudinary|dealerinspire|dealer\\.com|inventoryphoto|resizable)/i.test(low)) return true;
    return false;
  }
  const MIN_TO_TRUST = 2;
  function push(u) {
    if (!u || typeof u !== "string") return;
    let t = u.trim();
    if (t.startsWith("//")) t = "https:" + t;
    if (t.startsWith("http://")) t = "https://" + t.slice(7);
    if (!/^https:\/\//i.test(t)) return;
    const low = t.toLowerCase();
    if (!mightBeRasterUrl(low)) return;
    if (isLikelyJunkUrl(low)) return;
    if (seen.has(t)) return;
    seen.add(t);
    if (out.length < 220) out.push(t.slice(0, 900));
  }
  function isLikelyJunkUrl(low) {
    if (
      /(logo|icon|badge|banner|certified|cfximg|cfx\\/|\\/cfx\\/|placeholder|favicon|spinner|loading|m[-_]?logo|bmw[-_]?certified|m[-_]?performance|oem[-_]?vin[-_]?stock|generic-bmw|stackadapt|carnow|agent-0|marketing|promo|sprite|powered-by|value-your-trade|quick-link|warranty-tile|dealer-logo|brand-logo|social-share|facebook|instagram|youtube|twitter)/i.test(
        low
      )
    ) {
      return true;
    }
    if (/[?&](?:w|width|h|height)=\d{1,2}(?:&|$|\/)/i.test(low)) return true;
    return false;
  }
  function fromSrcset(ss) {
    if (!ss || typeof ss !== "string") return;
    for (const part of ss.split(",")) {
      const p = part.trim().split(/\\s+/)[0];
      if (p) push(p);
    }
  }
  function fromBackgroundString(bg) {
    if (!bg || typeof bg !== "string") return;
    const s = bg.trim();
    if (!s || /^none$|^initial$|^inherit$/i.test(s)) return;
    let m;
    const r = new RegExp(bgRe.source, "gi");
    while ((m = r.exec(s)) !== null) {
      if (m[1]) push(m[1].replace(/^["']|["']$/g, ""));
    }
  }
  function countImgs(node) {
    if (!node || !node.querySelectorAll) return 0;
    try {
      return node.querySelectorAll("img").length;
    } catch (eC) {
      return 0;
    }
  }
  function pickGalleryRoot() {
    let best = null;
    let bestN = 0;
    const dialogRows = [];
    try {
      document.querySelectorAll('[role="dialog"], [aria-modal="true"]').forEach((el) => {
        const n = countImgs(el);
        if (n >= MIN_TO_TRUST) dialogRows.push({ el, n });
      });
    } catch (eD) {}
    for (const row of dialogRows) {
      if (row.n > bestN) {
        bestN = row.n;
        best = row.el;
      }
    }
    if (best) return best;
    const fallbacks = [
      ".lightbox",
      ".media-modal",
      ".photo-viewer",
      ".gallery-modal",
      ".image-modal",
      "[class*='MuiDialog']",
      "[class*='MuiModal']",
      "[class*='dealer-image-gallery']",
      "[class*='image-gallery--']",
      "[class*='lightbox']",
      "[class*='media-modal']",
      "[class*='photo-viewer']",
      "[class*='gallery-modal']",
      "[class*='image-lightbox']",
    ];
    for (const sel of fallbacks) {
      try {
        const els = document.querySelectorAll(sel);
        for (const el of els) {
          const n = countImgs(el);
          if (n >= MIN_TO_TRUST && n > bestN) {
            bestN = n;
            best = el;
          }
        }
      } catch (eF) {}
    }
    if (best) return best;
    return null;
  }
  function isLikelyVdpJunkContext(img) {
    let cur = img;
    for (let d = 0; d < 10 && cur; d++) {
      const cls = (cur.className && String(cur.className)) || "";
      const cid = (cur.id && String(cur.id)) || "";
      const role = (cur.getAttribute && cur.getAttribute("role")) || "";
      const t = (cls + " " + cid + " " + role).toLowerCase();
      if (
        /(cfx|cfximg|ipacket|i-packet|i_packet|vpp|vehicle-?protec|warrantytile|vpp-|-vpp-|-vpp|carfax-?widget|kbb-?widget|dealer-?feature-?ti|dealer-?ti|value-?your-?trade|as-?is-?disclaim|asistile|recall-?polic|financ|about-?us-?|warranty-?ext|warranty-?flyer|vdp-?tile|quick-?link|vehicle-?broch|apply-?fin|ipocket|i-pocket|mlp-?image|fandi|fi_badge|plan-?overview|certified|bmw-?certified|m-?performance|brand-?logo|dealer-?logo|marketing|promo|social|banner)/i.test(
          t
        )
      ) {
        return true;
      }
      cur = cur.parentElement;
    }
    return false;
  }
  const picked = pickGalleryRoot();
  const roots = [];
  if (picked) {
    roots.push(picked);
  } else {
    try {
      document
        .querySelectorAll(
          ".vehicle-image-gallery, .vehicle-photos, [class*='vdp-photo'], [class*='VDP-Photo'], [class*='image-gallery__'], [class*='media-gallery'], [class*='vdp-media'], [class*='vehicle-gallery'], .vdp-gallery, [data-gallery]"
        )
        .forEach((h) => roots.push(h));
    } catch (eP) {}
  }
  if (roots.length === 0) {
    try {
      roots.push(document);
    } catch (eD) {
      return out;
    }
  }
  for (const root of roots) {
    try {
      root.querySelectorAll("img").forEach((img, idx) => {
        if (idx > 220) return;
        if (isLikelyVdpJunkContext(img)) return;
        try {
          if (img.currentSrc) push(img.currentSrc);
        } catch (e0) {}
        push(img.getAttribute("src"));
        fromSrcset(img.getAttribute("srcset"));
        const lazy = [
          "data-src",
          "data-lazy-src",
          "data-original",
          "data-lazy",
          "data-image",
          "data-zoom-src",
          "data-fullsrc",
        ];
        for (const a of lazy) push(img.getAttribute(a));
      });
    } catch (e1) {}
    try {
      root.querySelectorAll("picture source[srcset], picture source[src]").forEach((src, idx) => {
        if (idx > 80) return;
        fromSrcset(src.getAttribute("srcset"));
        push(src.getAttribute("src"));
      });
    } catch (e2) {}
  }
  return out;
}
"""


def _dom_specs_to_ep(dom_specs: dict[str, str]) -> dict[str, Any]:
    flat: dict[str, Any] = {}
    for label, val in dom_specs.items():
        lk = label.lower()
        if re.search(r"vin", lk):
            flat["vin"] = val
        elif re.search(r"trans", lk):
            flat["transmission"] = val
        elif re.search(r"drive|drivetrain|driveline|wheel\s*drive", lk):
            flat["drive_train"] = val
        elif re.search(r"exterior|ext\.?\s*color", lk):
            flat["exterior_color"] = val
        elif re.search(r"interior|int\.?\s*color", lk):
            flat["interior_color"] = val
        elif re.search(r"engine", lk):
            flat["engine"] = val
        elif re.search(r"fuel", lk):
            flat["fuel_type"] = val
        elif re.search(r"mpg|fuel economy", lk):
            m = re.search(r"(\d+)\s*[/|]\s*(\d+)", val)
            if m:
                flat["city_fuel_economy"] = m.group(1)
                flat["highway_fuel_economy"] = m.group(2)
            else:
                m1 = re.search(r"(\d{1,2})", val)
                if m1 and re.search(r"city", lk):
                    flat["city_fuel_economy"] = m1.group(1)
                elif m1 and re.search(r"highway|hwy", lk):
                    flat["highway_fuel_economy"] = m1.group(1)
    return flat


def _ld_to_ep(node: dict[str, Any]) -> dict[str, Any]:
    flat: dict[str, Any] = {}
    if node.get("name") or node.get("model"):
        flat["vehicle_model"] = str(node.get("name") or node.get("model") or "")[:200]
    if node.get("vehicleIdentificationNumber"):
        flat["vin"] = str(node["vehicleIdentificationNumber"])
    elif node.get("vin"):
        flat["vin"] = str(node["vin"])
    if node.get("vehicleInteriorColor"):
        flat["interior_color"] = str(node["vehicleInteriorColor"])
    if node.get("color"):
        flat.setdefault("exterior_color", str(node["color"])[:120])
    if node.get("bodyType"):
        flat["body_style"] = str(node["bodyType"])
    vt = node.get("vehicleTransmission") or node.get("transmission")
    if vt:
        if isinstance(vt, dict):
            t = vt.get("name") or vt.get("value")
            if t:
                flat["transmission"] = str(t)[:120]
        else:
            flat["transmission"] = str(vt)[:120]
    dw = node.get("driveWheelConfiguration")
    if dw:
        if isinstance(dw, dict) and dw.get("name"):
            flat["drive_train"] = str(dw["name"])[:120]
        else:
            flat["drive_train"] = str(dw)[:120]
    fts = node.get("fuelType")
    if fts:
        if isinstance(fts, dict) and fts.get("name"):
            flat["fuel_type"] = str(fts["name"])[:80]
        else:
            flat["fuel_type"] = str(fts)[:80]
    eng = node.get("vehicleEngine")
    if isinstance(eng, dict):
        nm = eng.get("name") or eng.get("description")
        if nm:
            flat["engine"] = str(nm)[:500]
    elif isinstance(eng, str) and eng.strip():
        flat["engine"] = eng[:500]
    return flat


def _build_fragments_from_vdp_capture(
    network_rows: list[dict[str, Any]],
    bundle: dict[str, Any] | None,
    expected_vin: str,
) -> tuple[list[tuple[str, dict[str, Any], float]], list[str], str | None]:
    fragments: list[tuple[str, dict[str, Any], float]] = []
    extractor_hits: list[str] = []
    bundle_err = (bundle or {}).get("error") if isinstance(bundle, dict) else None

    for row in network_rows:
        sc = float(row.get("score") or 0)
        for ep in row.get("ep_objects") or []:
            if isinstance(ep, dict):
                fragments.append(("network_ep", ep, sc))
        parsed = row.get("parsed")
        if parsed and sc >= 15:
            sub = _pick_vehicle_like_object(parsed)
            if sub:
                eff_sc = float(sc) if not row.get("ep_objects") else min(float(sc), 72.0)
                fragments.append(("network_vehicle_json", sub, eff_sc))

    if network_rows:
        extractor_hits.append("network")

    dle = (bundle or {}).get("dataLayerEps") or []
    if dle:
        extractor_hits.append("analytics_ep")
        log.info("VDP: analytics_ep hit for VIN %s (%d ep fragment(s))", expected_vin[:17], len(dle))
    for ep in dle:
        if isinstance(ep, dict):
            fragments.append(("dataLayer", ep, 95.0))

    for flat in (bundle or {}).get("dataLayerFlatVehicle") or []:
        if isinstance(flat, dict):
            fragments.append(("dataLayer_flat", flat, 99.0))
    if (bundle or {}).get("dataLayerFlatVehicle"):
        extractor_hits.append("dataLayer_flat")

    for ep_inline in (bundle or {}).get("inlineEpObjects") or []:
        if isinstance(ep_inline, dict):
            fragments.append(("inline_ep", ep_inline, 97.0))
    if (bundle or {}).get("inlineEpObjects"):
        extractor_hits.append("inline_ep")

    for node in (bundle or {}).get("ldJsonVehicle") or []:
        if isinstance(node, dict):
            fe = _ld_to_ep(node)
            if fe:
                fragments.append(("ld_json", fe, 40.0))
    if (bundle or {}).get("ldJsonVehicle"):
        extractor_hits.append("ld_json")

    for hit in (bundle or {}).get("inlineJsonHits") or []:
        if isinstance(hit, dict) and not hit.get("_rawSnippet"):
            fragments.append(("inline_json", hit, 25.0))
    if (bundle or {}).get("inlineJsonHits"):
        extractor_hits.append("inline_json")

    ds = (bundle or {}).get("domSpecs") or {}
    if isinstance(ds, dict) and ds:
        dom_ep = _dom_specs_to_ep({str(k): str(v) for k, v in ds.items()})
        if dom_ep:
            fragments.append(("dom", dom_ep, 18.0))
            extractor_hits.append("dom")

    return fragments, list(dict.fromkeys(extractor_hits)), bundle_err


def _vdp_count_gallery_signals(
    network_rows: list[dict[str, Any]],
    bundle: dict[str, Any] | None,
) -> int:
    n = 0
    for row in network_rows:
        n += len(row.get("image_urls") or [])
    if isinstance(bundle, dict):
        n += len(bundle.get("domGalleryUrls") or [])
        n += len(bundle.get("jsonGalleryUrls") or [])
    return n


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
    pending: list[asyncio.Task[Any]] = []

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
                    response_image_urls.append(url[:900])
                return
            if not _vdp_wants_json_network_capture(ct):
                return
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

    wp.on("response", on_response)
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
            out["visited"] = int(out.get("visited") or 0) + 1

            log.info("VDP: %s — visiting %s", dealer_name, try_url[:200])

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

        cand_gallery: list[str] = []
        gseen: set[str] = set()
        from backend.scanner.scan_efficiency import vdp_gallery_carousel_only, vdp_gallery_url_max

        mx_cap = vdp_gallery_url_max()
        carousel_only = vdp_gallery_carousel_only()

        def _push_g(batch: list[str]) -> None:
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
            try:
                from backend.scanner.vdp.history import merge_vdp_history_highlights_into_vehicle

                if merge_vdp_history_highlights_into_vehicle(v, last_bundle):
                    if "history_highlights" not in filled:
                        filled.append("history_highlights")
            except Exception as _hist_err:
                log.debug("VDP history highlights merge failed for %s: %s", vin[:17], _hist_err)
            if last_bundle.get("domInTransit") is True:
                v["_in_transit"] = True
                v["_availability_status"] = "in_transit"
                v["_availability_source"] = "vdp_dom"

        pdiag = _apply_vdp_price_hints(v, last_bundle, success_u)
        out["price_updated"] = bool(pdiag.get("updated"))

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
        if filled or int(out.get("gallery_added") or 0) > 0 or out.get("price_updated") or dom_carfax_updated:
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
    ep_cap = _vdp_max_per_dealer()
    price_cap = _vdp_price_max_per_dealer()
    spec_gap_cap = _vdp_spec_gap_max_per_dealer()
    description_cap = _vdp_description_max_per_dealer()
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
