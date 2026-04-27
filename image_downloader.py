#!/usr/bin/env python3
"""
Carousel-based image downloader for all cars in the database.

For each car with a source_url, Playwright navigates to the VDP, clicks the
right-arrow through the image carousel, skips any 360/spin slide, and downloads
every unique regular image to:

    car_images/<dealer_id>/<vin>/000_<hash>.jpg
    car_images/<dealer_id>/<vin>/manifest.json

No LLaVA / LLM processing is performed.

Usage:
    python image_downloader.py
    python image_downloader.py --dealer-id kefferjeep-com
    python image_downloader.py --limit 10 --settle-ms 1000
    python image_downloader.py --output-dir /data/car_images
    python image_downloader.py --overwrite          # re-download even if already on disk
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import os
import re
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

ROOT = Path(__file__).resolve().parent
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))

try:
    from backend.utils.project_env import load_project_dotenv
    load_project_dotenv()
except ImportError:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("image_dl")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DB_PATH = os.environ.get("INVENTORY_DB_PATH", "inventory.db")
DEFAULT_OUTPUT_DIR = "car_images"
DEFAULT_SETTLE_MS = 1000       # ms to wait after each carousel click
MAX_ROUNDS = 80                # hard cap on carousel clicks per car
MAX_IDLE_ROUNDS = 4            # stop when no new URL for this many consecutive rounds
MAX_SPIN_STREAK = 8            # bail early if active slide is spin this many rounds in a row
NAV_TIMEOUT_MS = 30_000
PAGE_SETTLE_S = 2.5            # initial wait after page load

# Playwright selectors to try for the "next" button, in priority order
NEXT_SELECTORS = [
    "button.slick-next",
    "button[aria-label='Next']",
    "button[aria-label='next']",
    "button[aria-label*='next' i]",
    "button[aria-label*='Next slide' i]",
    ".swiper-button-next",
    "[class*='gallery-next']",
    "[class*='carousel-next']",
    "[class*='arrow-right'][role='button']",
    "[class*='arrow-right'][tabindex]",
    "[data-direction='next']",
    "[data-slide='next']",
    "[data-action='next']",
    "button[class*='right']:not([disabled])",
]


# ---------------------------------------------------------------------------
# JavaScript — detect current active slide image
# ---------------------------------------------------------------------------
GET_ACTIVE_IMAGE_JS = r"""
() => {
  function norm(u) {
    if (!u || typeof u !== 'string') return null;
    u = u.trim();
    if (u.startsWith('//')) u = 'https:' + u;
    if (!u.startsWith('https://')) return null;
    if (/placeholder|blank\.gif|data:|\.svg(\?|$)/i.test(u)) return null;
    return u;
  }

  function isSpin(el) {
    if (!el) return false;
    if (el.querySelector('iframe, canvas')) return true;
    const text = (el.className + ' ' + (el.getAttribute('data-type') || '')).toLowerCase();
    return /spincar|impel|spin-?car|360-?view|three-?sixty|spin_viewer/.test(text);
  }

  // Slick carousel (dealer.com, most dealer sites)
  const slickCur = document.querySelector('.slick-slide.slick-current:not(.slick-cloned)');
  if (slickCur) {
    if (isSpin(slickCur)) return { url: null, isSpin: true, method: 'slick_spin' };
    const img = slickCur.querySelector('img');
    if (img) {
      const u = norm(img.currentSrc || img.getAttribute('src'));
      if (u) return { url: u, isSpin: false, method: 'slick_current' };
    }
  }

  // Swiper
  const swiperActive = document.querySelector('.swiper-slide-active:not(.swiper-slide-duplicate)');
  if (swiperActive) {
    if (isSpin(swiperActive)) return { url: null, isSpin: true, method: 'swiper_spin' };
    const img = swiperActive.querySelector('img');
    if (img) {
      const u = norm(img.currentSrc || img.getAttribute('src'));
      if (u) return { url: u, isSpin: false, method: 'swiper_active' };
    }
  }

  // Generic active/selected slide
  for (const sel of [
    '[class*="gallery"] .active:not([class*="thumb"]) img',
    '[class*="carousel"] .active img',
    '[class*="slide"].active img',
    '.gallery-hero img', '.main-image > img',
    '[class*="main-photo"] img', '[class*="primary-image"] img',
    '[class*="vehicle-image"]:not([class*="thumb"]) img',
    '[class*="vdp-gallery"] > img',
  ]) {
    try {
      const el = document.querySelector(sel);
      if (el) { const u = norm(el.currentSrc || el.getAttribute('src')); if (u) return { url: u, isSpin: false, method: sel }; }
    } catch(e) {}
  }

  // Global spin indicator (SpinCar/Impel iframes visible in gallery area)
  for (const sel of ['iframe[src*="spincar"]', 'iframe[src*="impel"]', '.spincar', '[class*="spin-viewer"]']) {
    try {
      const el = document.querySelector(sel);
      if (el) { const r = el.getBoundingClientRect(); if (r.width > 150 && r.height > 100) return { url: null, isSpin: true, method: 'global_spin' }; }
    } catch(e) {}
  }

  // Fallback: largest visible non-thumbnail image
  let best = null, bestArea = 0;
  document.querySelectorAll('img').forEach(img => {
    try {
      const rect = img.getBoundingClientRect();
      if (rect.width < 280 || rect.height < 180) return;
      const u = norm(img.currentSrc || img.getAttribute('src'));
      if (!u) return;
      if (img.closest('[class*="thumb"],[class*="nav-dot"],[class*="pagination"],.thumbnail,[class*="filmstrip"]')) return;
      const area = rect.width * rect.height;
      if (area > bestArea) { bestArea = area; best = u; }
    } catch(e) {}
  });
  return { url: best, isSpin: false, method: 'fallback_largest' };
}
"""

# Collect ALL raster image URLs visible in the DOM (thumbnails + main).
# Copied from scanner_vdp.py — proven to work across dealer platforms.
GALLERY_COLLECT_URLS_JS = r"""
() => {
  const out = [];
  const seen = new Set();
  function push(u) {
    if (!u || typeof u !== 'string') return;
    let t = u.trim();
    if (t.startsWith('//')) t = 'https:' + t;
    if (t.startsWith('http://')) t = 'https://' + t.slice(7);
    if (!/^https:\/\//i.test(t)) return;
    const low = t.toLowerCase();
    const ok = /\.(jpe?g|png|webp|gif|avif)(\?|#|$)/i.test(low) ||
               /(\/image\/|\/images\/|\/photos\/|\/media\/|cloudinary|dealerinspire|dealer\.com|inventoryphoto|vehiclephoto|cai-media|spincar)/i.test(low);
    if (!ok) return;
    if (seen.has(t)) return;
    seen.add(t);
    if (out.length < 220) out.push(t.slice(0, 900));
  }
  document.querySelectorAll('img').forEach(img => {
    push(img.currentSrc); push(img.getAttribute('src'));
    push(img.getAttribute('data-src')); push(img.getAttribute('data-lazy-src'));
    push(img.getAttribute('data-original')); push(img.getAttribute('data-zoom-src'));
  });
  document.querySelectorAll('picture source[srcset]').forEach(src => {
    (src.getAttribute('srcset') || '').split(',').forEach(p => push(p.trim().split(/\s+/)[0]));
  });
  return out;
}
"""


# ---------------------------------------------------------------------------
# URL helpers
# ---------------------------------------------------------------------------

_RESIZE_QUERY_PARAMS = frozenset([
    'w', 'h', 'width', 'height', 'q', 'quality', 'fmt', 'format',
    'resize', 'fit', 'crop', 'auto', 'f_auto', 'w_auto', 'thumbnail',
    'impolicy', 'downsize', 'size', 'scale',
])

# Path-based resize patterns: /resize/640x480/, /resize/640/, /800x600/
_PATH_RESIZE_RE = re.compile(
    r'/resize/\d+[xX]\d+/'    # /resize/640x480/
    r'|/resize/\d+/'           # /resize/640/
    r'|/\d{3,4}[xX]\d{3,4}/', # /800x600/  (standalone dimension segment)
    re.I,
)


def strip_resize_params(url: str) -> str:
    """Remove CDN resize/quality params — both query-string and path-based."""
    try:
        # Path-based: e.g. cai-media-management.com/resize/640x640/common-vehicle-media/...
        url = _PATH_RESIZE_RE.sub('/', url)
        # Query-string: e.g. ?w=800&impolicy=downsize
        p = urlparse(url)
        qs = parse_qs(p.query, keep_blank_values=False)
        cleaned = {k: v for k, v in qs.items() if k.lower() not in _RESIZE_QUERY_PARAMS}
        return urlunparse(p._replace(query=urlencode(cleaned, doseq=True)))
    except Exception:
        return url


def url_key(url: str) -> str:
    """Stable dedup key: strip resize then lowercase."""
    return strip_resize_params(url).lower()


def is_spin_url(url: str) -> bool:
    """True if URL looks like a 360/spin asset — skip these."""
    low = (url or "").lower()
    return any(tok in low for tok in ('spincar', 'impel', '/spin/', '360view', 'three-sixty'))


def image_suffix(url: str, content_type: str = "") -> str:
    """Determine file extension from Content-Type or URL."""
    ct = content_type.lower().split(";")[0].strip()
    ct_map = {
        "image/jpeg": ".jpg", "image/pjpeg": ".jpg",
        "image/png": ".png", "image/webp": ".webp",
        "image/gif": ".gif", "image/avif": ".avif",
    }
    if ct in ct_map:
        return ct_map[ct]
    path = urlparse(url).path.lower()
    m = re.search(r'\.(jpe?g|png|webp|gif|avif)($|\?)', path)
    if m:
        ext = m.group(1)
        return f".{ext}" if ext != "jpeg" else ".jpg"
    return ".jpg"


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def read_cars(db_path: str, dealer_id: str | None = None, limit: int | None = None) -> list[dict[str, Any]]:
    """Return cars that have a source_url, ordered by dealer then recency."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        wheres = ["source_url IS NOT NULL", "source_url != ''", "listing_active != 0"]
        params: list[Any] = []
        if dealer_id:
            wheres.append("dealer_id = ?")
            params.append(dealer_id)
        where_sql = " AND ".join(wheres)
        query = (
            f"SELECT vin, dealer_id, dealer_name, source_url, gallery "
            f"FROM cars WHERE {where_sql} ORDER BY dealer_id, scraped_at DESC"
        )
        if limit:
            query += f" LIMIT {int(limit)}"
        rows = conn.execute(query, params).fetchall()
        result = []
        for row in rows:
            d = dict(row)
            try:
                d["gallery"] = json.loads(d["gallery"] or "[]")
            except (json.JSONDecodeError, TypeError):
                d["gallery"] = []
            result.append(d)
        suffix = f" (dealer_id={dealer_id})" if dealer_id else ""
        log.info("DB: found %d car(s) with source_url%s", len(result), suffix)
        return result
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Carousel interaction
# ---------------------------------------------------------------------------

async def _click_next(page: Any) -> bool:
    """
    Advance carousel to the next slide.
    Tries Playwright native locator clicks first (reliable with React apps),
    falls back to keyboard ArrowRight.
    Returns True when something was clicked/pressed.
    """
    for sel in NEXT_SELECTORS:
        try:
            loc = page.locator(sel).first
            if await loc.count() > 0 and await loc.is_visible(timeout=400):
                await loc.click(timeout=800, force=True)
                return True
        except Exception:
            continue
    # Keyboard fallback — focus the gallery container first
    for gsel in [".slick-slider", ".swiper", "[class*='gallery']", "[class*='carousel']"]:
        try:
            gloc = page.locator(gsel).first
            if await gloc.count() > 0:
                await gloc.focus(timeout=400)
                break
        except Exception:
            continue
    try:
        await page.keyboard.press("ArrowRight")
        return True
    except Exception:
        return False


async def collect_carousel_images(page: Any, vin: str, settle_s: float) -> list[str]:
    """
    Dual-mode image collection:
      1. Network capture  — listen for every HTTPS image response the browser loads.
      2. DOM active-slide — read the active carousel slide's img src after each click.

    Clicks carousel right-arrow (Playwright native) until:
      • We loop back to the first real image (DOM mode confirms it)
      • No new images appear for MAX_IDLE_ROUNDS consecutive rounds
      • Spin detected MAX_SPIN_STREAK rounds in a row (car has only 360, no photos)
      • MAX_ROUNDS hard cap

    Returns deduplicated list of full-quality HTTPS image URLs.
    """
    # --- network capture state ---
    net_seen: set[str] = set()
    net_urls: list[str] = []

    async def _on_response(response: Any) -> None:
        try:
            if response.status != 200:
                return
            url = (response.url or "").strip()
            if not url.startswith("https://") or is_spin_url(url):
                return
            ct = (response.headers.get("content-type") or "").lower().split(";")[0].strip()
            low = url.lower()
            is_image = ct.startswith("image/") and "svg" not in ct and "icon" not in ct
            if not is_image:
                # Also accept URLs that look like images by extension
                if not re.search(r'\.(jpe?g|png|webp|gif|avif)(\?|$)', low):
                    return
            key = url_key(url)
            if key not in net_seen:
                net_seen.add(key)
                net_urls.append(strip_resize_params(url))
        except Exception:
            pass

    page.on("response", _on_response)

    try:
        # Brief settle for JS bundles / lazy images on initial load
        await asyncio.sleep(settle_s)

        # DOM scrape — catches thumbnails and any images already in the DOM
        try:
            dom_batch = await page.evaluate(GALLERY_COLLECT_URLS_JS)
            if isinstance(dom_batch, list):
                for u in dom_batch:
                    if isinstance(u, str) and u.startswith("https://") and not is_spin_url(u):
                        k = url_key(u)
                        if k not in net_seen:
                            net_seen.add(k)
                            net_urls.append(strip_resize_params(u))
        except Exception:
            pass

        # --- Carousel click loop ---
        dom_collected: list[str] = []
        dom_keys: set[str] = set()
        first_key: str | None = None
        idle = 0
        spin_streak = 0

        for round_n in range(MAX_ROUNDS):
            # Read active slide
            try:
                result = await page.evaluate(GET_ACTIVE_IMAGE_JS)
            except Exception:
                result = {}
            if not isinstance(result, dict):
                result = {}

            is_spin = bool(result.get("isSpin"))
            raw_url = result.get("url") or ""

            if is_spin or not raw_url or is_spin_url(raw_url):
                spin_streak += 1
                if spin_streak >= MAX_SPIN_STREAK:
                    log.debug(
                        "VIN %s — spin detected %d rounds in a row, stopping carousel loop",
                        vin, spin_streak,
                    )
                    break
            else:
                spin_streak = 0
                full_url = strip_resize_params(raw_url)
                key = url_key(raw_url)

                if first_key is None:
                    first_key = key
                elif key == first_key and len(dom_collected) > 1:
                    # Looped back to the first image — we've seen everything
                    log.debug(
                        "VIN %s — carousel looped back after %d images (%d rounds)",
                        vin, len(dom_collected), round_n,
                    )
                    break

                if key not in dom_keys:
                    dom_collected.append(full_url)
                    dom_keys.add(key)
                    idle = 0
                    log.debug(
                        "VIN %s — slide [%d] %s  (via %s)",
                        vin, len(dom_collected), full_url[:80], result.get("method", "?"),
                    )
                else:
                    idle += 1
                    if idle >= MAX_IDLE_ROUNDS:
                        log.debug("VIN %s — %d idle rounds, stopping", vin, idle)
                        break

            # Click to next slide
            await _click_next(page)
            await asyncio.sleep(settle_s)

    finally:
        try:
            page.remove_listener("response", _on_response)
        except Exception:
            pass

    # --- Merge DOM active-slide tracking + network capture ---
    merged: list[str] = []
    merged_keys: set[str] = set()
    # DOM-tracked URLs come first (ordered as carousel)
    for u in dom_collected:
        k = url_key(u)
        if k not in merged_keys:
            merged_keys.add(k)
            merged.append(u)
    # Then any additional URLs captured from network responses
    for u in net_urls:
        k = url_key(u)
        if k not in merged_keys and not is_spin_url(u):
            merged_keys.add(k)
            merged.append(u)

    return merged


# ---------------------------------------------------------------------------
# Image download
# ---------------------------------------------------------------------------

async def _download_one(client: Any, idx: int, url: str, car_dir: Path) -> dict[str, Any]:
    """Download a single image. Returns a manifest entry dict."""
    url_hash = hashlib.sha256(url.encode()).hexdigest()[:12]
    tmp = car_dir / f"{idx:03d}_{url_hash}.tmp"
    try:
        async with client.stream("GET", url, timeout=25, follow_redirects=True) as resp:
            if resp.status_code != 200:
                return {"url": url, "error": f"HTTP {resp.status_code}"}
            ct = resp.headers.get("content-type", "")
            if "image/" not in ct and not re.search(r'\.(jpe?g|png|webp|gif|avif)', url.lower()):
                return {"url": url, "error": f"non-image content-type: {ct[:40]}"}
            data = await resp.aread()
        if len(data) < 512:
            return {"url": url, "error": "body too small"}
        ext = image_suffix(url, ct)
        final = car_dir / f"{idx:03d}_{url_hash}{ext}"
        final.write_bytes(data)
        return {"url": url, "path": str(final), "bytes": len(data)}
    except Exception as e:
        return {"url": url, "error": str(e)[:200]}
    finally:
        tmp.unlink(missing_ok=True)


async def download_all_images(urls: list[str], car_dir: Path) -> list[dict[str, Any]]:
    """Download all URLs concurrently (max 4 at a time)."""
    import httpx
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://www.google.com/",
    }
    sem = asyncio.Semaphore(4)

    async def _bounded(idx: int, url: str) -> dict[str, Any]:
        async with sem:
            async with httpx.AsyncClient(headers=headers, follow_redirects=True) as client:
                return await _download_one(client, idx, url, car_dir)

    return list(await asyncio.gather(*[_bounded(i, u) for i, u in enumerate(urls)]))


# ---------------------------------------------------------------------------
# DB write-back helpers
# ---------------------------------------------------------------------------

def _filesystem_paths_to_web_urls(ok_entries: list[dict[str, Any]], output_root: Path) -> list[str]:
    """
    Convert downloaded file dicts (with "path" key) to web-accessible URLs
    like ``/car-images/<dealer_id>/<vin>/000_hash.jpg``.
    """
    web: list[str] = []
    for e in ok_entries:
        p = e.get("path")
        if not p:
            continue
        try:
            rel = Path(p).relative_to(output_root)
            web.append("/car-images/" + rel.as_posix())
        except ValueError:
            pass
    return web


def _update_db_with_local_images(db_path: str, vin: str, web_urls: list[str]) -> None:
    """
    Write local /car-images/ web paths to cars.image_url + cars.gallery,
    then sync the incomplete-listings sidecar so the "images" gap clears.
    """
    if not web_urls or not vin:
        return
    try:
        conn = sqlite3.connect(db_path)
        try:
            conn.execute(
                "UPDATE cars SET image_url = ?, gallery = ? WHERE vin = ?",
                (web_urls[0], json.dumps(web_urls), vin),
            )
            conn.commit()
            row = conn.execute("SELECT id FROM cars WHERE vin = ?", (vin,)).fetchone()
            car_id = int(row[0]) if row else None
        finally:
            conn.close()

        if car_id is not None:
            try:
                from backend.db.incomplete_listings_db import sync_incomplete_listing_for_car_id
                sync_incomplete_listing_for_car_id(car_id)
            except Exception as exc:
                log.debug("sync_incomplete_listing_for_car_id failed (non-fatal): %s", exc)
    except Exception as exc:
        log.warning("DB write-back failed for VIN %s: %s", vin, exc)


# ---------------------------------------------------------------------------
# Per-car pipeline
# ---------------------------------------------------------------------------

async def process_car(
    page: Any,
    car: dict[str, Any],
    output_root: Path,
    *,
    settle_ms: int,
    overwrite: bool,
) -> dict[str, Any]:
    vin = (car.get("vin") or "").strip().upper()
    dealer_id = re.sub(r"[^\w.\-]+", "_", (car.get("dealer_id") or "unknown").strip())[:60]
    source_url = (car.get("source_url") or "").strip()

    result: dict[str, Any] = {
        "vin": vin,
        "dealer_id": dealer_id,
        "images_downloaded": 0,
        "images_skipped": 0,
        "error": None,
    }

    if not source_url:
        result["error"] = "no source_url"
        return result

    car_dir = output_root / dealer_id / vin
    manifest_path = car_dir / "manifest.json"

    # Skip if already downloaded (unless --overwrite)
    if not overwrite and manifest_path.exists():
        try:
            manifest = json.loads(manifest_path.read_text())
            ok_files = [e for e in manifest.get("files", []) if "error" not in e]
            n_ok = len(ok_files)
            if n_ok > 0:
                log.info("SKIP %s — already has %d image(s) on disk", vin, n_ok)
                result["images_skipped"] = n_ok
                # Still sync DB in case it was never written back (e.g. first run pre-fix).
                web_paths = _filesystem_paths_to_web_urls(ok_files, output_root)
                if web_paths:
                    _update_db_with_local_images(car.get("_db_path", DB_PATH), vin, web_paths)
                return result
        except Exception:
            pass

    car_dir.mkdir(parents=True, exist_ok=True)

    log.info("Processing %s — %s", vin, source_url[:100])
    t0 = time.perf_counter()

    # Navigate
    try:
        await page.goto(source_url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
    except Exception as e:
        log.warning("VIN %s — navigation failed: %s", vin, str(e)[:120])
        result["error"] = f"nav_failed: {str(e)[:120]}"
        return result

    await asyncio.sleep(PAGE_SETTLE_S)

    # Collect image URLs by stepping through carousel
    urls = await collect_carousel_images(page, vin, settle_ms / 1000.0)

    if not urls:
        log.warning("VIN %s — no images found (car may only have a 360 spin, no static photos)", vin)
        result["error"] = "no_images_found"
        manifest_path.write_text(json.dumps({
            "vin": vin, "source_url": source_url, "files": [],
            "note": "only_360_or_no_photos",
        }, indent=2))
        return result

    log.info("VIN %s — found %d image URL(s), downloading...", vin, len(urls))

    entries = await download_all_images(urls, car_dir)
    ok = [e for e in entries if "error" not in e]
    fail = [e for e in entries if "error" in e]
    elapsed = round(time.perf_counter() - t0, 1)
    result["images_downloaded"] = len(ok)

    manifest_path.write_text(json.dumps({
        "vin": vin,
        "dealer_id": car.get("dealer_id"),
        "dealer_name": car.get("dealer_name"),
        "source_url": source_url,
        "files": ok,
        "errors": fail,
        "elapsed_s": elapsed,
    }, indent=2))

    # Write local web paths back to the DB so the webapp can serve them.
    if ok:
        web_paths = _filesystem_paths_to_web_urls(ok, output_root)
        if web_paths:
            _update_db_with_local_images(car.get("_db_path", DB_PATH), vin, web_paths)

    log.info("VIN %s — done: %d saved, %d errors (%.1fs)", vin, len(ok), len(fail), elapsed)
    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main(
    *,
    dealer_id: str | None,
    limit: int | None,
    output_dir: str,
    settle_ms: int,
    overwrite: bool,
    db_path: str,
    concurrency: int,
) -> None:
    cars = read_cars(db_path, dealer_id=dealer_id, limit=limit)
    if not cars:
        log.warning("No cars found in DB matching criteria.")
        return
    # Tag each car with the resolved DB path so process_car can write back.
    for c in cars:
        c["_db_path"] = db_path

    output_root = Path(output_dir).expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    log.info("Output root: %s", output_root)
    log.info(
        "Processing %d car(s) | concurrency=%d | settle_ms=%d | overwrite=%s",
        len(cars), concurrency, settle_ms, overwrite,
    )

    try:
        from playwright_stealth import Stealth
        from playwright.async_api import async_playwright
        cm = Stealth().use_async(async_playwright())
    except ImportError:
        from playwright.async_api import async_playwright
        cm = async_playwright()

    async with cm as p:
        browser = await p.chromium.launch(headless=True)
        sem = asyncio.Semaphore(concurrency)
        ctx_opts: dict[str, Any] = {"viewport": {"width": 1920, "height": 1080}}
        ua = (os.environ.get("SCANNER_USER_AGENT") or "").strip()
        if ua:
            ctx_opts["user_agent"] = ua

        async def run_car(car: dict[str, Any]) -> dict[str, Any]:
            async with sem:
                context = await browser.new_context(**ctx_opts)
                page = await context.new_page()
                try:
                    return await process_car(
                        page, car, output_root,
                        settle_ms=settle_ms,
                        overwrite=overwrite,
                    )
                except Exception as e:
                    log.exception("VIN %s — unexpected error: %s", car.get("vin"), e)
                    return {"vin": car.get("vin"), "error": str(e)[:200], "images_downloaded": 0}
                finally:
                    try:
                        await context.close()
                    except Exception:
                        pass

        results = list(await asyncio.gather(*[run_car(c) for c in cars]))
        await browser.close()

    total_dl = sum(r.get("images_downloaded", 0) for r in results if isinstance(r, dict))
    total_skip = sum(r.get("images_skipped", 0) for r in results if isinstance(r, dict))
    no_photos = [r["vin"] for r in results if isinstance(r, dict) and r.get("error") == "no_images_found"]
    hard_errors = [r["vin"] for r in results if isinstance(r, dict) and r.get("error") and r.get("error") != "no_images_found" and not r.get("images_skipped")]

    log.info(
        "Done — %d cars | %d images downloaded | %d already on disk | "
        "%d no-photos (360-only) | %d hard errors",
        len(results), total_dl, total_skip, len(no_photos), len(hard_errors),
    )
    if no_photos:
        log.info("360-only / no static photos: %s", no_photos[:20])
    if hard_errors:
        log.info("Hard errors: %s", hard_errors[:20])


if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description="Download all carousel images for every car in the DB (no LLaVA)."
    )
    ap.add_argument("--dealer-id", metavar="ID", default=None,
                    help="Only process cars from this dealer_id (e.g. kefferjeep-com).")
    ap.add_argument("--limit", type=int, default=None, metavar="N",
                    help="Max number of cars to process (useful for testing).")
    ap.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR, metavar="DIR",
                    help=f"Root directory for saved images (default: {DEFAULT_OUTPUT_DIR}).")
    ap.add_argument("--settle-ms", type=int, default=DEFAULT_SETTLE_MS, metavar="MS",
                    help=f"Wait after each carousel click, ms (default: {DEFAULT_SETTLE_MS}).")
    ap.add_argument("--overwrite", action="store_true",
                    help="Re-download even if car folder already has a manifest.")
    ap.add_argument("--db", default=DB_PATH, metavar="PATH",
                    help=f"Path to inventory.db (default: {DB_PATH}).")
    ap.add_argument("--concurrency", type=int, default=2, metavar="N",
                    help="Parallel Playwright pages (default: 2).")
    args = ap.parse_args()

    try:
        asyncio.run(main(
            dealer_id=args.dealer_id,
            limit=args.limit,
            output_dir=args.output_dir,
            settle_ms=args.settle_ms,
            overwrite=args.overwrite,
            db_path=args.db,
            concurrency=args.concurrency,
        ))
    except KeyboardInterrupt:
        log.info("Stopped by user.")
        sys.exit(0)
